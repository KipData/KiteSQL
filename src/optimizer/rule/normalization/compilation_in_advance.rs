// Copyright 2024 KipData/KiteSQL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::errors::DatabaseError;
use crate::expression::visitor_mut::VisitorMut;
use crate::expression::BindEvaluator;
use crate::optimizer::core::rule::NormalizationRule;
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};

#[derive(Clone)]
pub struct EvaluatorBind;

pub(crate) fn evaluator_bind_current(plan: &mut LogicalPlan) -> Result<(), DatabaseError> {
    let operator = &mut plan.operator;

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
        Operator::TopK(op) => {
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
        | Operator::ScalarSubquery(_)
        | Operator::Values(_)
        | Operator::ShowTable
        | Operator::ShowView
        | Operator::Explain
        | Operator::Describe(_)
        | Operator::Insert(_)
        | Operator::Delete(_)
        | Operator::Analyze(_)
        | Operator::AddColumn(_)
        | Operator::ChangeColumn(_)
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
        | Operator::Union(_)
        | Operator::Except(_) => (),
    }

    Ok(())
}

impl EvaluatorBind {
    fn _apply(plan: &mut LogicalPlan) -> Result<(), DatabaseError> {
        match plan.childrens.as_mut() {
            Childrens::Only(child) => Self::_apply(child)?,
            Childrens::Twins { left, right } => {
                Self::_apply(left)?;
                if matches!(
                    plan.operator,
                    Operator::Join(_) | Operator::Union(_) | Operator::Except(_)
                ) {
                    Self::_apply(right)?;
                }
            }
            Childrens::None => {}
        }

        evaluator_bind_current(plan)
    }
}

impl NormalizationRule for EvaluatorBind {
    fn apply(&self, plan: &mut LogicalPlan) -> Result<bool, DatabaseError> {
        Self::_apply(plan)?;
        Ok(true)
    }
}
