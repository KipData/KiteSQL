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
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{MatchPattern, NormalizationRule};
use crate::optimizer::plan_utils::{only_child_mut, replace_with_only_child};
use crate::planner::operator::sort::SortField;
use crate::planner::operator::{Operator, PhysicalOption, PlanImpl, SortOption};
use crate::planner::{Childrens, LogicalPlan};
use std::sync::LazyLock;

static REDUNDANT_SORT_PATTERN: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |op| matches!(op, Operator::Sort(_)),
    children: PatternChildrenPredicate::None,
});

pub struct EliminateRedundantSort;

impl MatchPattern for EliminateRedundantSort {
    fn pattern(&self) -> &Pattern {
        &REDUNDANT_SORT_PATTERN
    }
}

impl NormalizationRule for EliminateRedundantSort {
    fn apply(&self, plan: &mut LogicalPlan) -> Result<bool, DatabaseError> {
        let sort_fields = match &plan.operator {
            Operator::Sort(sort_op) => sort_op.sort_fields.clone(),
            _ => return Ok(false),
        };

        let child = match only_child_mut(plan) {
            Some(child) => child,
            None => return Ok(false),
        };
        mark_sort_preserving_indexes(child, &sort_fields);
        let can_remove = ensure_index_order(child, &sort_fields);

        if !can_remove {
            return Ok(false);
        }

        Ok(replace_with_only_child(plan))
    }
}

pub fn annotate_sort_preserving_indexes(plan: &mut LogicalPlan) {
    fn visit(plan: &mut LogicalPlan) {
        if let Operator::Sort(sort_op) = &plan.operator {
            let sort_fields = sort_op.sort_fields.clone();
            mark_sort_preserving_indexes(plan, &sort_fields);
        }
        match plan.childrens.as_mut() {
            Childrens::Only(child) => visit(child),
            Childrens::Twins { left, right } => {
                visit(left);
                visit(right);
            }
            Childrens::None => {}
        }
    }
    visit(plan);
}

fn mark_sort_preserving_indexes(plan: &mut LogicalPlan, required: &[SortField]) {
    if required.is_empty() {
        return;
    }

    match &mut plan.operator {
        Operator::Filter(_)
        | Operator::Project(_)
        | Operator::Limit(_)
        | Operator::TopK(_)
        | Operator::Sort(_) => {
            if let Childrens::Only(child) = plan.childrens.as_mut() {
                mark_sort_preserving_indexes(child, required);
            }
        }
        Operator::TableScan(scan_op) => {
            let table_columns: Vec<ColumnRef> = scan_op.columns.values().cloned().collect();
            let required_from_table = required.iter().all(|field| {
                let referenced = field.expr.referenced_columns(true);
                referenced
                    .iter()
                    .all(|column| table_columns.contains(column))
            });
            if !required_from_table {
                return;
            }
            for index_info in scan_op.index_infos.iter_mut() {
                if covers(required, &index_info.sort_option) {
                    let covered = required.len();
                    index_info.sort_elimination_hint = Some(
                        index_info
                            .sort_elimination_hint
                            .map_or(covered, |old| old.max(covered)),
                    );
                }
            }
        }
        _ => {}
    }
}

fn ensure_index_order(plan: &mut LogicalPlan, required: &[SortField]) -> bool {
    if let Some(PhysicalOption {
        plan: PlanImpl::IndexScan(index_info),
        ..
    }) = plan.physical_option.as_ref()
    {
        if covers(required, &index_info.sort_option) {
            return true;
        }
    }

    if let Some(physical_option) = plan.physical_option.as_ref() {
        if matches!(physical_option.sort_option(), SortOption::Follow) {
            if let Childrens::Only(child) = plan.childrens.as_mut() {
                if ensure_index_order(child, required) {
                    return true;
                }
            }
        }
    }

    false
}

fn covers(required: &[SortField], provided: &SortOption) -> bool {
    if required.is_empty() {
        return true;
    }

    match provided {
        SortOption::OrderBy {
            fields,
            ignore_prefix_len,
        } => {
            if fields.is_empty() {
                return false;
            }
            let max_skip = (*ignore_prefix_len).min(fields.len());

            for skip in 0..=max_skip {
                if fields.len() < skip + required.len() {
                    continue;
                }
                if required
                    .iter()
                    .zip(fields.iter().skip(skip))
                    .all(|(lhs, rhs)| lhs == rhs)
                {
                    return true;
                }
            }
            false
        }
        SortOption::Follow | SortOption::None => false,
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::EliminateRedundantSort;
    use crate::catalog::{ColumnCatalog, ColumnRef, TableName};
    use crate::errors::DatabaseError;
    use crate::expression::range_detacher::Range;
    use crate::expression::ScalarExpression;
    use crate::optimizer::core::rule::NormalizationRule;
    use crate::planner::operator::filter::FilterOperator;
    use crate::planner::operator::sort::{SortField, SortOperator};
    use crate::planner::operator::table_scan::TableScanOperator;
    use crate::planner::operator::{Operator, PhysicalOption, PlanImpl, SortOption};
    use crate::planner::{Childrens, LogicalPlan};
    use crate::types::index::{IndexInfo, IndexMeta, IndexType};
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::collections::BTreeMap;
    use std::ops::Bound;
    use std::sync::Arc;
    use ulid::Ulid;

    fn make_sort_field(name: &str) -> SortField {
        let column = ColumnRef::from(ColumnCatalog::new_dummy(name.to_string()));
        SortField::new(ScalarExpression::column_expr(column), true, true)
    }

    fn build_plan(
        required_fields: Vec<SortField>,
        index_fields: Vec<SortField>,
        ignore_prefix_len: usize,
    ) -> LogicalPlan {
        let (index_info, index_sort_option) = build_index_info(index_fields, ignore_prefix_len);

        let mut leaf = LogicalPlan::new(Operator::Dummy, Childrens::None);
        leaf.physical_option = Some(PhysicalOption::new(
            PlanImpl::IndexScan(index_info),
            index_sort_option,
        ));

        let mut filter = LogicalPlan::new(
            Operator::Filter(FilterOperator {
                predicate: ScalarExpression::Constant(DataValue::Boolean(true)),
                is_optimized: false,
                having: false,
            }),
            Childrens::Only(Box::new(leaf)),
        );
        filter.physical_option = Some(PhysicalOption::new(PlanImpl::Filter, SortOption::Follow));

        LogicalPlan::new(
            Operator::Sort(SortOperator {
                sort_fields: required_fields,
                limit: None,
            }),
            Childrens::Only(Box::new(filter)),
        )
    }

    fn build_index_info(
        index_fields: Vec<SortField>,
        ignore_prefix_len: usize,
    ) -> (IndexInfo, SortOption) {
        let len = index_fields.len();
        let sort_option = SortOption::OrderBy {
            fields: index_fields,
            ignore_prefix_len,
        };
        let table_name: TableName = Arc::from("t1");
        let meta = Arc::new(IndexMeta {
            id: 1,
            column_ids: (0..len).map(|_| Ulid::new()).collect(),
            table_name,
            pk_ty: LogicalType::Integer,
            value_ty: LogicalType::Integer,
            name: "idx".to_string(),
            ty: IndexType::PrimaryKey {
                is_multiple: len > 1,
            },
        });
        let index_info = IndexInfo {
            meta,
            sort_option: sort_option.clone(),
            range: None,
            covered_deserializers: None,
            cover_mapping: None,
            sort_elimination_hint: None,
        };
        (index_info, sort_option)
    }

    #[test]
    fn remove_sort_when_index_matches_order() -> Result<(), DatabaseError> {
        let sort_field = make_sort_field("c1");
        let mut plan = build_plan(vec![sort_field.clone()], vec![sort_field], 0);
        let rule = EliminateRedundantSort;

        assert!(rule.apply(&mut plan)?);
        assert!(matches!(plan.operator, Operator::Filter(_)));
        Ok(())
    }

    #[test]
    fn remove_sort_when_prefix_can_be_ignored() -> Result<(), DatabaseError> {
        let c1 = make_sort_field("c1");
        let c2 = make_sort_field("c2");
        let mut plan = build_plan(vec![c2.clone()], vec![c1, c2], 1);
        super::annotate_sort_preserving_indexes(&mut plan);
        let rule = EliminateRedundantSort;

        assert!(rule.apply(&mut plan)?);
        Ok(())
    }

    #[test]
    fn annotate_sets_sort_hint_on_table_scan() -> Result<(), DatabaseError> {
        let column = ColumnRef::from(ColumnCatalog::new_dummy("c1".to_string()));
        let sort_field = SortField::new(ScalarExpression::column_expr(column.clone()), true, true);
        let (index_info, _) = build_index_info(vec![sort_field.clone()], 0);

        let mut columns = BTreeMap::new();
        columns.insert(0, column);
        let table_name: TableName = Arc::from("t");
        let table_scan = LogicalPlan::new(
            Operator::TableScan(TableScanOperator {
                table_name: table_name.clone(),
                primary_keys: vec![],
                columns,
                limit: (None, None),
                index_infos: vec![index_info],
                with_pk: false,
            }),
            Childrens::None,
        );

        let mut plan = LogicalPlan::new(
            Operator::Sort(SortOperator {
                sort_fields: vec![sort_field],
                limit: None,
            }),
            Childrens::Only(Box::new(table_scan)),
        );

        super::annotate_sort_preserving_indexes(&mut plan);

        let table_plan = plan.childrens.pop_only();
        match table_plan.operator {
            Operator::TableScan(scan_op) => assert!(
                scan_op
                    .index_infos
                    .iter()
                    .any(|info| info.sort_elimination_hint.is_some()),
                "expected sort elimination hint on at least one index"
            ),
            _ => unreachable!("expected table scan under sort"),
        }
        Ok(())
    }

    #[test]
    fn keep_sort_when_order_not_covered() -> Result<(), DatabaseError> {
        let c1 = make_sort_field("c1");
        let c2 = make_sort_field("c2");
        let mut plan = build_plan(vec![c2.clone()], vec![c1.clone(), c2], 0);
        super::annotate_sort_preserving_indexes(&mut plan);
        let rule = EliminateRedundantSort;

        assert!(!rule.apply(&mut plan)?);
        assert!(matches!(plan.operator, Operator::Sort(_)));
        Ok(())
    }

    #[test]
    fn promote_index_to_remove_sort() -> Result<(), DatabaseError> {
        let column = ColumnRef::from(ColumnCatalog::new_dummy("c_first".to_string()));
        let sort_field = SortField::new(ScalarExpression::column_expr(column.clone()), true, true);
        let (mut index_info, _) = build_index_info(vec![sort_field.clone()], 0);
        index_info.range = Some(Range::Scope {
            min: Bound::Unbounded,
            max: Bound::Unbounded,
        });

        let mut columns = BTreeMap::new();
        columns.insert(0, column);

        let mut scan_plan = LogicalPlan::new(
            Operator::TableScan(TableScanOperator {
                table_name: Arc::from("t"),
                primary_keys: vec![],
                columns,
                limit: (None, None),
                index_infos: vec![index_info],
                with_pk: false,
            }),
            Childrens::None,
        );
        if let Operator::TableScan(scan_op) = &scan_plan.operator {
            let index_info = scan_op.index_infos[0].clone();
            scan_plan.physical_option = Some(PhysicalOption::new(
                PlanImpl::IndexScan(index_info.clone()),
                index_info.sort_option.clone(),
            ));
        }

        let mut filter = LogicalPlan::new(
            Operator::Filter(FilterOperator {
                predicate: ScalarExpression::Constant(DataValue::Boolean(true)),
                is_optimized: false,
                having: false,
            }),
            Childrens::Only(Box::new(scan_plan)),
        );
        filter.physical_option = Some(PhysicalOption::new(PlanImpl::Filter, SortOption::Follow));

        let mut plan = LogicalPlan::new(
            Operator::Sort(SortOperator {
                sort_fields: vec![sort_field],
                limit: None,
            }),
            Childrens::Only(Box::new(filter)),
        );

        super::annotate_sort_preserving_indexes(&mut plan);
        let rule = EliminateRedundantSort;
        assert!(rule.apply(&mut plan)?);
        assert!(matches!(plan.operator, Operator::Filter(_)));

        let table_plan = plan.childrens.pop_only();
        assert!(matches!(
            table_plan.physical_option,
            Some(PhysicalOption {
                plan: PlanImpl::IndexScan(_),
                ..
            })
        ));
        Ok(())
    }
}
