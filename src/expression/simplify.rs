use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::visitor_mut::{walk_mut_expr, VisitorMut};
use crate::expression::{BinaryOperator, ScalarExpression, UnaryOperator};
use crate::types::evaluator::EvaluatorFactory;
use crate::types::value::DataValue;
use crate::types::LogicalType;
use std::mem;

#[derive(Debug)]
enum Replace {
    Binary(ReplaceBinary),
    Unary(ReplaceUnary),
}

#[derive(Debug)]
struct ReplaceBinary {
    column_expr: ScalarExpression,
    val_expr: ScalarExpression,
    op: BinaryOperator,
    ty: LogicalType,
    is_column_left: bool,
}

#[derive(Debug)]
struct ReplaceUnary {
    child_expr: ScalarExpression,
    op: UnaryOperator,
    ty: LogicalType,
}

pub struct ConstantCalculator;

impl VisitorMut<'_> for ConstantCalculator {
    fn visit(&mut self, expr: &'_ mut ScalarExpression) -> Result<(), DatabaseError> {
        match expr {
            ScalarExpression::Unary {
                op,
                expr: arg_expr,
                evaluator,
                ty,
            } => {
                self.visit(arg_expr)?;

                if let ScalarExpression::Constant(unary_val) = arg_expr.as_ref() {
                    let value = if let Some(evaluator) = evaluator {
                        evaluator.0.unary_eval(unary_val)
                    } else {
                        EvaluatorFactory::unary_create(ty.clone(), *op)?
                            .0
                            .unary_eval(unary_val)
                    };
                    let _ = mem::replace(expr, ScalarExpression::Constant(value));
                }
            }
            ScalarExpression::Binary {
                op,
                left_expr,
                right_expr,
                ..
            } => {
                let ty = LogicalType::max_logical_type(
                    &left_expr.return_type(),
                    &right_expr.return_type(),
                )?;
                self.visit(left_expr)?;
                self.visit(right_expr)?;

                if let (
                    ScalarExpression::Constant(left_val),
                    ScalarExpression::Constant(right_val),
                ) = (left_expr.as_mut(), right_expr.as_mut())
                {
                    let evaluator = EvaluatorFactory::binary_create(ty.clone(), *op)?;

                    if left_val.logical_type() != ty {
                        *left_val = left_val.clone().cast(&ty)?;
                    }
                    if right_val.logical_type() != ty {
                        *right_val = right_val.clone().cast(&ty)?;
                    }
                    let value = evaluator.0.binary_eval(left_val, right_val);
                    let _ = mem::replace(expr, ScalarExpression::Constant(value));
                }
            }
            _ => walk_mut_expr(self, expr)?,
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct Simplify {
    replaces: Vec<Replace>,
}

impl VisitorMut<'_> for Simplify {
    fn visit(&mut self, expr: &'_ mut ScalarExpression) -> Result<(), DatabaseError> {
        match expr {
            ScalarExpression::Unary {
                op,
                expr: arg_expr,
                ty,
                ..
            } => {
                let op = *op;
                let ty = ty.clone();
                let arg_expr = arg_expr.as_ref().clone();
                if let Some(value) = expr.unpack_val() {
                    let _ = mem::replace(expr, ScalarExpression::Constant(value));
                } else {
                    self.replaces.push(Replace::Unary(ReplaceUnary {
                        child_expr: arg_expr,
                        op,
                        ty,
                    }));
                }
            }
            ScalarExpression::Binary {
                op,
                left_expr,
                right_expr,
                ty,
                ..
            } => {
                self.fix_expr(left_expr, right_expr, op)?;

                // `(c1 - 1) and (c1 + 2)` cannot fix!
                self.fix_expr(right_expr, left_expr, op)?;

                if Self::is_arithmetic(op) {
                    match (left_expr.unpack_col(false), right_expr.unpack_col(false)) {
                        (Some(col), None) => {
                            self.replaces.push(Replace::Binary(ReplaceBinary {
                                column_expr: ScalarExpression::ColumnRef(col),
                                val_expr: mem::replace(right_expr, ScalarExpression::Empty),
                                op: *op,
                                ty: ty.clone(),
                                is_column_left: true,
                            }));
                        }
                        (None, Some(col)) => {
                            self.replaces.push(Replace::Binary(ReplaceBinary {
                                column_expr: ScalarExpression::ColumnRef(col),
                                val_expr: mem::replace(left_expr, ScalarExpression::Empty),
                                op: *op,
                                ty: ty.clone(),
                                is_column_left: false,
                            }));
                        }
                        (None, None) => {
                            if self.replaces.is_empty() {
                                return Ok(());
                            }

                            match (left_expr.unpack_col(true), right_expr.unpack_col(true)) {
                                (Some(col), None) => {
                                    self.replaces.push(Replace::Binary(ReplaceBinary {
                                        column_expr: ScalarExpression::ColumnRef(col),
                                        val_expr: mem::replace(right_expr, ScalarExpression::Empty),
                                        op: *op,
                                        ty: ty.clone(),
                                        is_column_left: true,
                                    }));
                                }
                                (None, Some(col)) => {
                                    self.replaces.push(Replace::Binary(ReplaceBinary {
                                        column_expr: ScalarExpression::ColumnRef(col),
                                        val_expr: mem::replace(left_expr, ScalarExpression::Empty),
                                        op: *op,
                                        ty: ty.clone(),
                                        is_column_left: false,
                                    }));
                                }
                                _ => (),
                            }
                        }
                        _ => (),
                    }
                }
            }
            ScalarExpression::TypeCast { .. } => {
                if let Some(val) = expr.unpack_val() {
                    let _ = mem::replace(expr, ScalarExpression::Constant(val));
                }
            }
            ScalarExpression::IsNull { .. } => {
                if let Some(val) = expr.unpack_val() {
                    let _ = mem::replace(
                        expr,
                        ScalarExpression::Constant(DataValue::Boolean(val.is_null())),
                    );
                }
            }
            ScalarExpression::In {
                negated,
                expr: arg_expr,
                args,
            } => {
                if args.is_empty() {
                    return Ok(());
                }

                let (op_1, op_2) = if *negated {
                    (BinaryOperator::NotEq, BinaryOperator::And)
                } else {
                    (BinaryOperator::Eq, BinaryOperator::Or)
                };
                let mut new_expr = ScalarExpression::Binary {
                    op: op_1,
                    left_expr: arg_expr.clone(),
                    right_expr: Box::new(args.remove(0)),
                    evaluator: None,
                    ty: LogicalType::Boolean,
                };

                for arg in args.drain(..) {
                    new_expr = ScalarExpression::Binary {
                        op: op_2,
                        left_expr: Box::new(ScalarExpression::Binary {
                            op: op_1,
                            left_expr: arg_expr.clone(),
                            right_expr: Box::new(arg),
                            evaluator: None,
                            ty: LogicalType::Boolean,
                        }),
                        right_expr: Box::new(new_expr),
                        evaluator: None,
                        ty: LogicalType::Boolean,
                    }
                }
                let _ = mem::replace(expr, new_expr);

                walk_mut_expr(self, expr)?;
            }
            ScalarExpression::Between {
                negated,
                expr: arg_expr,
                left_expr,
                right_expr,
            } => {
                let (op, left_op, right_op) = if *negated {
                    (BinaryOperator::Or, BinaryOperator::Lt, BinaryOperator::Gt)
                } else {
                    (
                        BinaryOperator::And,
                        BinaryOperator::GtEq,
                        BinaryOperator::LtEq,
                    )
                };
                let new_expr = ScalarExpression::Binary {
                    op,
                    left_expr: Box::new(ScalarExpression::Binary {
                        op: left_op,
                        left_expr: arg_expr.clone(),
                        right_expr: mem::replace(left_expr, Box::new(ScalarExpression::Empty)),
                        evaluator: None,
                        ty: LogicalType::Boolean,
                    }),
                    right_expr: Box::new(ScalarExpression::Binary {
                        op: right_op,
                        left_expr: mem::replace(arg_expr, Box::new(ScalarExpression::Empty)),
                        right_expr: mem::replace(right_expr, Box::new(ScalarExpression::Empty)),
                        evaluator: None,
                        ty: LogicalType::Boolean,
                    }),
                    evaluator: None,
                    ty: LogicalType::Boolean,
                };

                let _ = mem::replace(expr, new_expr);

                walk_mut_expr(self, expr)?;
            }
            _ => walk_mut_expr(self, expr)?,
        }

        Ok(())
    }
}

impl Simplify {
    fn is_arithmetic(op: &mut BinaryOperator) -> bool {
        matches!(
            op,
            BinaryOperator::Plus
                | BinaryOperator::Divide
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
        )
    }

    fn fix_expr(
        &mut self,
        left_expr: &mut Box<ScalarExpression>,
        right_expr: &mut Box<ScalarExpression>,
        op: &mut BinaryOperator,
    ) -> Result<(), DatabaseError> {
        self.visit(left_expr)?;

        if Self::is_arithmetic(op) {
            return Ok(());
        }
        while let Some(replace) = self.replaces.pop() {
            match replace {
                Replace::Binary(binary) => Self::fix_binary(binary, left_expr, right_expr, op),
                Replace::Unary(unary) => {
                    Self::fix_unary(unary, left_expr, right_expr, op);
                    self.fix_expr(left_expr, right_expr, op)?;
                }
            }
        }

        Ok(())
    }

    fn fix_unary(
        replace_unary: ReplaceUnary,
        col_expr: &mut Box<ScalarExpression>,
        val_expr: &mut Box<ScalarExpression>,
        op: &mut BinaryOperator,
    ) {
        let ReplaceUnary {
            child_expr,
            op: fix_op,
            ty: fix_ty,
        } = replace_unary;
        let _ = mem::replace(col_expr, Box::new(child_expr));

        let expr = mem::replace(val_expr, Box::new(ScalarExpression::Empty));
        let _ = mem::replace(
            val_expr,
            Box::new(ScalarExpression::Unary {
                op: fix_op,
                expr,
                evaluator: None,
                ty: fix_ty,
            }),
        );
        let _ = mem::replace(
            op,
            match fix_op {
                UnaryOperator::Plus => *op,
                UnaryOperator::Minus => match *op {
                    BinaryOperator::Plus => BinaryOperator::Minus,
                    BinaryOperator::Minus => BinaryOperator::Plus,
                    BinaryOperator::Multiply => BinaryOperator::Divide,
                    BinaryOperator::Divide => BinaryOperator::Multiply,
                    BinaryOperator::Gt => BinaryOperator::Lt,
                    BinaryOperator::Lt => BinaryOperator::Gt,
                    BinaryOperator::GtEq => BinaryOperator::LtEq,
                    BinaryOperator::LtEq => BinaryOperator::GtEq,
                    source_op => source_op,
                },
                UnaryOperator::Not => match *op {
                    BinaryOperator::Gt => BinaryOperator::Lt,
                    BinaryOperator::Lt => BinaryOperator::Gt,
                    BinaryOperator::GtEq => BinaryOperator::LtEq,
                    BinaryOperator::LtEq => BinaryOperator::GtEq,
                    source_op => source_op,
                },
            },
        );
    }

    fn fix_binary(
        replace_binary: ReplaceBinary,
        left_expr: &mut Box<ScalarExpression>,
        right_expr: &mut Box<ScalarExpression>,
        op: &mut BinaryOperator,
    ) {
        let ReplaceBinary {
            column_expr,
            val_expr,
            op: fix_op,
            ty: fix_ty,
            is_column_left,
        } = replace_binary;
        let op_flip = |op: BinaryOperator| match op {
            BinaryOperator::Plus => BinaryOperator::Minus,
            BinaryOperator::Minus => BinaryOperator::Plus,
            BinaryOperator::Multiply => BinaryOperator::Divide,
            BinaryOperator::Divide => BinaryOperator::Multiply,
            _ => unreachable!(),
        };
        let comparison_flip = |op: BinaryOperator| match op {
            BinaryOperator::Gt => BinaryOperator::Lt,
            BinaryOperator::GtEq => BinaryOperator::LtEq,
            BinaryOperator::Lt => BinaryOperator::Gt,
            BinaryOperator::LtEq => BinaryOperator::GtEq,
            source_op => source_op,
        };
        let temp_expr = mem::replace(right_expr, Box::new(ScalarExpression::Empty));
        let (fixed_op, fixed_left_expr, fixed_right_expr) = if is_column_left {
            (op_flip(fix_op), temp_expr, Box::new(val_expr))
        } else {
            if matches!(fix_op, BinaryOperator::Minus | BinaryOperator::Multiply) {
                let _ = mem::replace(op, comparison_flip(*op));
            }
            (fix_op, Box::new(val_expr), temp_expr)
        };

        let _ = mem::replace(left_expr, Box::new(column_expr));
        let _ = mem::replace(
            right_expr,
            Box::new(ScalarExpression::Binary {
                op: fixed_op,
                left_expr: fixed_left_expr,
                right_expr: fixed_right_expr,
                evaluator: None,
                ty: fix_ty,
            }),
        );
    }

    fn _is_belong(table_name: &str, col: &ColumnRef) -> bool {
        matches!(
            col.table_name().map(|name| table_name == name.as_str()),
            Some(true)
        )
    }
}

impl ScalarExpression {
    pub(crate) fn unpack_val(&self) -> Option<DataValue> {
        match self {
            ScalarExpression::Constant(val) => Some(val.clone()),
            ScalarExpression::Alias { expr, .. } => expr.unpack_val(),
            ScalarExpression::TypeCast { expr, ty, .. } => {
                expr.unpack_val().and_then(|val| val.cast(ty).ok())
            }
            ScalarExpression::IsNull { expr, .. } => expr
                .unpack_val()
                .map(|val| DataValue::Boolean(val.is_null())),
            ScalarExpression::Unary {
                expr,
                op,
                evaluator,
                ty,
                ..
            } => {
                let value = expr.unpack_val()?;
                let unary_value = if let Some(evaluator) = evaluator {
                    evaluator.0.unary_eval(&value)
                } else {
                    EvaluatorFactory::unary_create(ty.clone(), *op)
                        .ok()?
                        .0
                        .unary_eval(&value)
                };
                Some(unary_value)
            }
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                op,
                ty,
                evaluator,
                ..
            } => {
                let mut left = left_expr.unpack_val()?;
                let mut right = right_expr.unpack_val()?;
                if &left.logical_type() != ty {
                    left = left.cast(ty).ok()?;
                }
                if &right.logical_type() != ty {
                    right = right.cast(ty).ok()?;
                }
                let binary_value = if let Some(evaluator) = evaluator {
                    evaluator.0.binary_eval(&left, &right)
                } else {
                    EvaluatorFactory::binary_create(ty.clone(), *op)
                        .ok()?
                        .0
                        .binary_eval(&left, &right)
                };
                Some(binary_value)
            }
            _ => None,
        }
    }

    pub(crate) fn unpack_col(&self, is_deep: bool) -> Option<ColumnRef> {
        match self {
            ScalarExpression::ColumnRef(col) => Some(col.clone()),
            ScalarExpression::Alias { expr, .. } => expr.unpack_col(is_deep),
            ScalarExpression::Unary { expr, .. } => expr.unpack_col(is_deep),
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                ..
            } => {
                if !is_deep {
                    return None;
                }

                left_expr
                    .unpack_col(true)
                    .or_else(|| right_expr.unpack_col(true))
            }
            _ => None,
        }
    }
}
