use crate::errors::DatabaseError;
use crate::expression::visitor_mut::VisitorMut;
use crate::expression::{BindEvaluator, ScalarExpression, TryReference};
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{MatchPattern, NormalizationRule};
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::Operator;
use std::sync::LazyLock;

static EXPRESSION_REMAPPER_RULE: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |_| true,
    children: PatternChildrenPredicate::None,
});

static EVALUATOR_BIND_RULE: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |_| true,
    children: PatternChildrenPredicate::None,
});

#[derive(Clone)]
pub struct ExpressionRemapper;

impl ExpressionRemapper {
    fn _apply(
        output_exprs: &mut Vec<ScalarExpression>,
        node_id: HepNodeId,
        graph: &mut HepGraph,
    ) -> Result<(), DatabaseError> {
        if let Some(child_id) = graph.eldest_child_at(node_id) {
            Self::_apply(output_exprs, child_id, graph)?;
        }
        // for join
        let mut left_len = 0;
        if let Operator::Join(_) = graph.operator(node_id) {
            let mut second_output_exprs = Vec::new();
            if let Some(child_id) = graph.youngest_child_at(node_id) {
                Self::_apply(&mut second_output_exprs, child_id, graph)?;
            }
            left_len = output_exprs.len();
            output_exprs.append(&mut second_output_exprs);
        }
        let operator = graph.operator_mut(node_id);
        match operator {
            Operator::Join(op) => {
                match &mut op.on {
                    JoinCondition::On { on, filter } => {
                        for (left_expr, right_expr) in on {
                            TryReference::new(&output_exprs[0..left_len]).visit(left_expr)?;
                            TryReference::new(&output_exprs[left_len..]).visit(right_expr)?;
                        }
                        if let Some(expr) = filter {
                            TryReference::new(output_exprs).visit(expr)?;
                        }
                    }
                    JoinCondition::None => {}
                }

                return Ok(());
            }
            Operator::Aggregate(op) => {
                for expr in op.agg_calls.iter_mut().chain(op.groupby_exprs.iter_mut()) {
                    TryReference::new(output_exprs).visit(expr)?;
                }
            }
            Operator::Filter(op) => {
                TryReference::new(output_exprs).visit(&mut op.predicate)?;
            }
            Operator::Project(op) => {
                for expr in op.exprs.iter_mut() {
                    TryReference::new(output_exprs).visit(expr)?;
                }
            }
            Operator::Sort(op) => {
                for sort_field in op.sort_fields.iter_mut() {
                    TryReference::new(output_exprs).visit(&mut sort_field.expr)?;
                }
            }
            Operator::FunctionScan(op) => {
                for expr in op.table_function.args.iter_mut() {
                    TryReference::new(output_exprs).visit(expr)?;
                }
            }
            Operator::Update(op) => {
                for (_, expr) in op.value_exprs.iter_mut() {
                    TryReference::new(output_exprs).visit(expr)?;
                }
            }
            Operator::Dummy
            | Operator::TableScan(_)
            | Operator::Limit(_)
            | Operator::Values(_)
            | Operator::ShowTable
            | Operator::ShowView
            | Operator::Explain
            | Operator::Describe(_)
            | Operator::Insert(_)
            | Operator::Delete(_)
            | Operator::Analyze(_)
            | Operator::AddColumn(_)
            | Operator::DropColumn(_)
            | Operator::CreateTable(_)
            | Operator::CreateIndex(_)
            | Operator::CreateView(_)
            | Operator::DropTable(_)
            | Operator::DropView(_)
            | Operator::DropIndex(_)
            | Operator::Truncate(_)
            | Operator::CopyFromFile(_)
            | Operator::CopyToFile(_)
            | Operator::Union(_) => (),
        }
        if let Some(exprs) = operator.output_exprs() {
            *output_exprs = exprs;
        }

        Ok(())
    }
}

impl MatchPattern for ExpressionRemapper {
    fn pattern(&self) -> &Pattern {
        &EXPRESSION_REMAPPER_RULE
    }
}

impl NormalizationRule for ExpressionRemapper {
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), DatabaseError> {
        Self::_apply(&mut Vec::new(), node_id, graph)?;
        // mark changed to skip this rule batch
        graph.version += 1;

        Ok(())
    }
}

#[derive(Clone)]
pub struct EvaluatorBind;

impl EvaluatorBind {
    fn _apply(node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), DatabaseError> {
        if let Some(child_id) = graph.eldest_child_at(node_id) {
            Self::_apply(child_id, graph)?;
        }
        // for join
        if let Operator::Join(_) = graph.operator(node_id) {
            if let Some(child_id) = graph.youngest_child_at(node_id) {
                Self::_apply(child_id, graph)?;
            }
        }
        let operator = graph.operator_mut(node_id);

        match operator {
            Operator::Join(op) => {
                match &mut op.on {
                    JoinCondition::On { on, filter } => {
                        for (left_expr, right_expr) in on {
                            BindEvaluator.visit(left_expr)?;
                            BindEvaluator.visit(right_expr)?;
                        }
                        if let Some(expr) = filter {
                            BindEvaluator.visit(expr)?;
                        }
                    }
                    JoinCondition::None => {}
                }

                return Ok(());
            }
            Operator::Aggregate(op) => {
                for expr in op.agg_calls.iter_mut().chain(op.groupby_exprs.iter_mut()) {
                    BindEvaluator.visit(expr)?;
                }
            }
            Operator::Filter(op) => {
                BindEvaluator.visit(&mut op.predicate)?;
            }
            Operator::Project(op) => {
                for expr in op.exprs.iter_mut() {
                    BindEvaluator.visit(expr)?;
                }
            }
            Operator::Sort(op) => {
                for sort_field in op.sort_fields.iter_mut() {
                    BindEvaluator.visit(&mut sort_field.expr)?;
                }
            }
            Operator::FunctionScan(op) => {
                for expr in op.table_function.args.iter_mut() {
                    BindEvaluator.visit(expr)?;
                }
            }
            Operator::Update(op) => {
                for (_, expr) in op.value_exprs.iter_mut() {
                    BindEvaluator.visit(expr)?;
                }
            }
            Operator::Dummy
            | Operator::TableScan(_)
            | Operator::Limit(_)
            | Operator::Values(_)
            | Operator::ShowTable
            | Operator::ShowView
            | Operator::Explain
            | Operator::Describe(_)
            | Operator::Insert(_)
            | Operator::Delete(_)
            | Operator::Analyze(_)
            | Operator::AddColumn(_)
            | Operator::DropColumn(_)
            | Operator::CreateTable(_)
            | Operator::CreateIndex(_)
            | Operator::CreateView(_)
            | Operator::DropTable(_)
            | Operator::DropView(_)
            | Operator::DropIndex(_)
            | Operator::Truncate(_)
            | Operator::CopyFromFile(_)
            | Operator::CopyToFile(_)
            | Operator::Union(_) => (),
        }

        Ok(())
    }
}

impl MatchPattern for EvaluatorBind {
    fn pattern(&self) -> &Pattern {
        &EVALUATOR_BIND_RULE
    }
}

impl NormalizationRule for EvaluatorBind {
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), DatabaseError> {
        Self::_apply(node_id, graph)?;
        // mark changed to skip this rule batch
        graph.version += 1;

        Ok(())
    }
}
