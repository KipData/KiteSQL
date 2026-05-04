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

use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::{BinaryOperator, ScalarExpression};
use crate::optimizer::core::rule::NormalizationRule;
use crate::planner::operator::mark_apply::{MarkApplyKind, MarkApplyQuantifier};
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::{Operator, PhysicalOption, PlanImpl};
use crate::planner::{Childrens, LogicalPlan};
use crate::types::index::{IndexLookup, IndexType};
use crate::types::tuple::Schema;

pub(crate) struct ParameterizeMarkApply;

impl NormalizationRule for ParameterizeMarkApply {
    fn apply(&self, plan: &mut LogicalPlan) -> Result<bool, DatabaseError> {
        let (op, new_probe) = match (&mut plan.operator, plan.childrens.as_mut()) {
            (Operator::MarkApply(op), Childrens::Twins { left, right }) => {
                let new_probe = find_parameterized_probe(
                    op.kind.clone(),
                    op.predicates(),
                    left.output_schema().as_ref(),
                    right.output_schema().as_ref(),
                )
                .and_then(|(right_column, left_expr)| {
                    parameterize_right_subtree(right, &right_column).then_some(left_expr)
                });
                (op, new_probe)
            }
            _ => return Ok(false),
        };

        let changed = op.parameterized_probe().cloned() != new_probe;
        op.set_parameterized_probe(new_probe);
        Ok(changed)
    }
}

fn find_parameterized_probe(
    kind: MarkApplyKind,
    predicates: &[ScalarExpression],
    left_schema: &Schema,
    right_schema: &Schema,
) -> Option<(ColumnRef, ScalarExpression)> {
    match kind {
        MarkApplyKind::Exists => predicates.iter().find_map(|predicate| {
            extract_parameterized_probe(predicate, left_schema, right_schema)
        }),
        MarkApplyKind::Quantified(MarkApplyQuantifier::Any) => {
            predicates.first().and_then(|predicate| {
                extract_parameterized_probe(predicate, left_schema, right_schema)
            })
        }
        MarkApplyKind::Quantified(MarkApplyQuantifier::All) => None,
    }
}

fn extract_parameterized_probe(
    predicate: &ScalarExpression,
    left_schema: &Schema,
    right_schema: &Schema,
) -> Option<(ColumnRef, ScalarExpression)> {
    match predicate.unpack_alias_ref() {
        ScalarExpression::Binary {
            op: BinaryOperator::Eq,
            left_expr,
            right_expr,
            ..
        } => extract_parameterized_probe_side(left_expr, right_expr, left_schema, right_schema)
            .or_else(|| {
                extract_parameterized_probe_side(right_expr, left_expr, left_schema, right_schema)
            }),
        _ => None,
    }
}

fn extract_parameterized_probe_side(
    right_expr: &ScalarExpression,
    left_expr: &ScalarExpression,
    left_schema: &Schema,
    right_schema: &Schema,
) -> Option<(ColumnRef, ScalarExpression)> {
    let (right_column, _) = right_expr.unpack_alias_ref().unpack_bound_col(false)?;

    if !schema_contains_column(right_schema, &right_column) {
        return None;
    }
    if !left_expr.all_referenced_columns(true, |candidate| {
        schema_contains_column(left_schema, candidate)
    }) {
        return None;
    }
    if left_expr.any_referenced_column(true, |candidate| {
        schema_contains_column(right_schema, candidate)
    }) {
        return None;
    }

    Some((right_column, left_expr.clone()))
}

fn parameterize_right_subtree(plan: &mut LogicalPlan, right_column: &ColumnRef) -> bool {
    if matches!(plan.operator, Operator::TableScan(_)) {
        let index_info = {
            let Operator::TableScan(scan_op) = &mut plan.operator else {
                unreachable!();
            };
            let Some(target_index) = pick_parameterized_index_position(scan_op, right_column)
            else {
                return false;
            };
            scan_op.index_infos[target_index].lookup = Some(IndexLookup::Probe);
            scan_op.index_infos[target_index].clone()
        };
        let sort_option = index_info.sort_option.clone();
        plan.physical_option = Some(PhysicalOption::new(
            PlanImpl::IndexScan(Box::new(index_info)),
            sort_option,
        ));
        return true;
    }

    let passthrough = matches!(
        plan.operator,
        Operator::Filter(_)
            | Operator::Project(_)
            | Operator::Limit(_)
            | Operator::Sort(_)
            | Operator::TopK(_)
    );

    if !passthrough {
        return false;
    }

    match plan.childrens.as_mut() {
        Childrens::Only(child) => parameterize_right_subtree(child, right_column),
        _ => false,
    }
}

fn pick_parameterized_index_position(
    scan_op: &TableScanOperator,
    right_column: &ColumnRef,
) -> Option<usize> {
    let column_id = right_column.id()?;
    let table_name = right_column.table_name()?;

    if &scan_op.table_name != table_name {
        return None;
    }

    scan_op
        .index_infos
        .iter()
        .enumerate()
        .filter(|(_, index_info)| {
            index_info.meta.table_name == *table_name
                && index_info.meta.column_ids.first().copied() == Some(column_id)
        })
        .min_by_key(|(_, index_info)| index_priority(index_info.meta.ty))
        .map(|(position, _)| position)
}

fn index_priority(index_type: IndexType) -> usize {
    match index_type {
        IndexType::PrimaryKey { .. } => 0,
        IndexType::Unique => 1,
        IndexType::Composite => 2,
        IndexType::Normal => 3,
    }
}

fn schema_contains_column(schema: &Schema, column: &ColumnRef) -> bool {
    schema.iter().any(|candidate| candidate.same_column(column))
}
