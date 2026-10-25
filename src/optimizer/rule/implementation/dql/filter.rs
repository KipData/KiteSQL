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
use crate::expression::visitor::ExprVisitor;
use crate::expression::{BinaryOperator, ScalarExpression};
use crate::optimizer::core::pattern::Pattern;
use crate::optimizer::core::rule::{
    keep_best_physical_option, BestPhysicalOption, ImplementationRule, MatchPattern,
};
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::optimizer::plan_utils::scan_column;
use crate::planner::operator::{Operator, PhysicalOption, PlanImpl, RowCountOption, SortOption};
use crate::planner::{Childrens, ExprRef, LogicalPlan, MetaArena, PlanArena};
use crate::types::value::DataValue;
use std::collections::HashSet;
use std::sync::LazyLock;

static FILTER_PATTERN: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |op| matches!(op, Operator::Filter(_)),
});

#[derive(Clone)]
pub struct FilterImplementation;

impl MatchPattern for FilterImplementation {
    fn pattern(&self) -> &Pattern {
        &FILTER_PATTERN
    }
}

impl ImplementationRule for FilterImplementation {
    fn update_best_option(
        &self,
        plan: &LogicalPlan,
        arena: &PlanArena,
        loader: &StatisticMetaLoader<'_>,
        best_physical_option: &mut BestPhysicalOption,
    ) -> Result<(), DatabaseError> {
        let rows = estimate_filter_rows(plan, arena, loader)?;
        let row_count = rows.map_or(RowCountOption::Follow, |rows| RowCountOption::Estimated {
            rows,
            ndv: None,
        });
        keep_best_physical_option(
            best_physical_option,
            PhysicalOption::new(PlanImpl::Filter, SortOption::Follow, row_count),
            None,
        );
        Ok(())
    }
}

struct PointFilterVisitor<'a, A> {
    arena: &'a A,
    column: Option<ColumnRef>,
    values: HashSet<&'a DataValue>,
    supported: bool,
}

impl<'a, A: MetaArena> PointFilterVisitor<'a, A> {
    fn bind_column(&mut self, column: ColumnRef) {
        if self
            .column
            .is_some_and(|previous| !self.arena.same_column(previous, column))
        {
            self.supported = false;
        } else {
            self.column = Some(column);
        }
    }

    fn add_value(&mut self, column: ColumnRef, expr: ExprRef) {
        let arena = self.arena;
        let ScalarExpression::Constant(value) = arena.expression(expr) else {
            self.supported = false;
            return;
        };
        if value.is_null() {
            return;
        }
        if value.logical_type() != *arena.column(column).datatype() {
            self.supported = false;
            return;
        }
        self.values.insert(value);
    }
}

impl<A: MetaArena> ExprVisitor<A> for PointFilterVisitor<'_, A> {
    fn visit_expression_ref(&mut self, expr: ExprRef, _arena: &A) -> Result<bool, DatabaseError> {
        if !self.supported {
            return Ok(false);
        }
        let arena = self.arena;
        match arena.expression(expr) {
            ScalarExpression::Binary {
                op: BinaryOperator::Or,
                ..
            } => return Ok(true),
            ScalarExpression::Binary {
                op: BinaryOperator::Eq,
                left_expr,
                right_expr,
                ..
            } => {
                let (column, value) =
                    match (arena.expression(*left_expr), arena.expression(*right_expr)) {
                        (
                            ScalarExpression::ColumnRef { column, .. },
                            ScalarExpression::Constant(_),
                        ) => (*column, *right_expr),
                        (
                            ScalarExpression::Constant(_),
                            ScalarExpression::ColumnRef { column, .. },
                        ) => (*column, *left_expr),
                        _ => {
                            self.supported = false;
                            return Ok(false);
                        }
                    };
                self.bind_column(column);
                self.add_value(column, value);
            }
            ScalarExpression::In {
                negated: false,
                expr,
                args,
                ..
            } => {
                let ScalarExpression::ColumnRef { column, .. } = arena.expression(*expr) else {
                    self.supported = false;
                    return Ok(false);
                };
                self.bind_column(*column);
                for &arg in args {
                    self.add_value(*column, arg);
                }
            }
            _ => self.supported = false,
        }
        Ok(false)
    }
}

fn estimate_filter_rows(
    plan: &LogicalPlan,
    arena: &PlanArena,
    loader: &StatisticMetaLoader<'_>,
) -> Result<Option<usize>, DatabaseError> {
    let (Operator::Filter(filter), Childrens::Only(input)) =
        (&plan.operator, plan.childrens.as_ref())
    else {
        return Ok(None);
    };
    let Some(rows) = input.estimated_rows() else {
        return Ok(None);
    };
    // Do not apply the same point selectivity again to an already restricted
    // index or an earlier estimated filter. Start with unrestricted scan inputs.
    if !matches!(
        input.row_count_option().map(|option| &option.plan),
        Some(PlanImpl::SeqScan)
    ) {
        return Ok(None);
    }
    let mut visitor = PointFilterVisitor {
        arena,
        column: None,
        values: HashSet::new(),
        supported: true,
    };
    visitor.visit(filter.predicate, arena)?;
    let Some(column) = visitor.column.filter(|_| visitor.supported) else {
        return Ok(None);
    };
    let Some((table, column)) = scan_column(input, column, arena)? else {
        return Ok(None);
    };
    let Some(meta) = loader.load_column(table, column) else {
        return Ok(None);
    };
    let ndv = meta.distinct_values_len();
    if ndv == 0 || visitor.values.is_empty() || meta.values_len() == 0 {
        return Ok(Some(0));
    }
    // Uniform single-column estimate only: no products of correlated selectivities.
    let non_null_fraction =
        meta.values_len() as f64 / meta.values_len().saturating_add(meta.null_count()) as f64;
    let selectivity = visitor.values.len().min(ndv) as f64 / ndv as f64;
    Ok(Some(
        ((rows as f64 * non_null_fraction * selectivity).ceil() as usize).min(rows),
    ))
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::expression::AliasType;
    use crate::optimizer::core::histogram::{HistogramMetaBuilder, DEFAULT_HLL_PRECISION};
    use crate::optimizer::heuristic::optimizer::HepOptimizerPipeline;
    use crate::optimizer::rule::implementation::ImplementationRuleImpl;
    use crate::planner::operator::filter::FilterOperator;
    use crate::planner::operator::limit::LimitOperator;
    use crate::planner::operator::project::ProjectOperator;
    use crate::planner::operator::table_scan::TableScanOperator;
    use crate::planner::TableArenaCell;
    use crate::storage::StatisticsMetaCache;
    use crate::types::index::{IndexInfo, IndexMeta, IndexType};
    use crate::types::LogicalType;

    fn input(arena: &mut PlanArena, rows: usize) -> (LogicalPlan, [ExprRef; 2]) {
        let mut columns = Vec::new();
        let mut keys = Vec::new();
        for id in 0..2 {
            let mut column = ColumnCatalog::new(
                format!("c{id}"),
                true,
                ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
            );
            column.set_ref_table("t".into(), id, false);
            let column = arena.alloc_column(column);
            columns.push(column);
            keys.push(arena.alloc_expression(ScalarExpression::column_expr(column, id as usize)));
        }
        let mut plan = LogicalPlan::new(
            Operator::TableScan(TableScanOperator {
                table_name: "t".into(),
                columns,
                limit: (None, None),
                index_infos: vec![],
                with_pk: false,
            }),
            Childrens::None,
        );
        plan.physical_option = Some(PhysicalOption::new(
            PlanImpl::SeqScan,
            SortOption::None,
            RowCountOption::Estimated { rows, ndv: None },
        ));
        (plan, [keys[0], keys[1]])
    }

    fn statistics(non_null: usize, nulls: usize, ndv: usize) -> StatisticsMetaCache {
        let mut builder = HistogramMetaBuilder::new(DEFAULT_HLL_PRECISION).unwrap();
        for i in 0..non_null {
            builder.append(&DataValue::Int32((i % ndv) as i32));
        }
        for _ in 0..nulls {
            builder.append(&DataValue::Null);
        }
        let mut cache = StatisticsMetaCache::default();
        cache.columns.insert(("t".into(), 0), builder.build());
        cache
    }

    fn binary(arena: &mut PlanArena, op: BinaryOperator, left: ExprRef, right: ExprRef) -> ExprRef {
        arena.alloc_expression(ScalarExpression::Binary {
            op,
            left_expr: left,
            right_expr: right,
            evaluator: None,
            ty: LogicalType::Boolean,
        })
    }

    fn equal(arena: &mut PlanArena, column: ExprRef, value: DataValue) -> ExprRef {
        let value = arena.alloc_expression(ScalarExpression::Constant(value));
        binary(arena, BinaryOperator::Eq, column, value)
    }

    fn in_list(
        arena: &mut PlanArena,
        column: ExprRef,
        values: &[DataValue],
        negated: bool,
    ) -> ExprRef {
        let args = values
            .iter()
            .map(|value| arena.alloc_expression(ScalarExpression::Constant(value.clone())))
            .collect();
        arena.alloc_expression(ScalarExpression::In {
            negated,
            expr: column,
            args,
            evaluator: None,
        })
    }

    #[test]
    fn point_estimates_deduplicate_in_and_or_values() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (scan, keys) = input(&mut arena, 100);
        let cache = statistics(100, 0, 10);
        let loader = StatisticMetaLoader::new(&cache);
        let eq = equal(&mut arena, keys[0], DataValue::Int32(1));
        let list = in_list(
            &mut arena,
            keys[0],
            &[
                DataValue::Int32(1),
                DataValue::Int32(1),
                DataValue::Int32(2),
                DataValue::Null,
            ],
            false,
        );
        let union = binary(&mut arena, BinaryOperator::Or, eq, list);
        for (predicate, expected) in [(eq, 10), (list, 20), (union, 20)] {
            let plan = FilterOperator::build(predicate, scan.clone(), false);
            assert_eq!(
                estimate_filter_rows(&plan, &arena, &loader)?,
                Some(expected)
            );
        }
        let value = arena.alloc_expression(ScalarExpression::Constant(DataValue::Int32(1)));
        let reversed = binary(&mut arena, BinaryOperator::Eq, value, keys[0]);
        assert_eq!(
            estimate_filter_rows(
                &FilterOperator::build(reversed, scan.clone(), false),
                &arena,
                &loader
            )?,
            Some(10)
        );
        let many = in_list(
            &mut arena,
            keys[0],
            &(0..20).map(DataValue::Int32).collect::<Vec<_>>(),
            false,
        );
        assert_eq!(
            estimate_filter_rows(&FilterOperator::build(many, scan, false), &arena, &loader)?,
            Some(100)
        );
        Ok(())
    }

    #[test]
    fn point_estimates_account_for_nulls_empty_inputs_and_missing_stats(
    ) -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (scan, keys) = input(&mut arena, 100);
        let eq = equal(&mut arena, keys[0], DataValue::Int32(1));
        let null = equal(&mut arena, keys[0], DataValue::Null);
        let mixed = in_list(
            &mut arena,
            keys[0],
            &[DataValue::Null, DataValue::Int32(1)],
            false,
        );
        let all_null = in_list(&mut arena, keys[0], &[DataValue::Null], false);
        for (non_null, nulls, ndv, expected) in [(75, 25, 5, 15), (0, 100, 0, 0), (0, 0, 0, 0)] {
            let cache = statistics(non_null, nulls, ndv);
            let loader = StatisticMetaLoader::new(&cache);
            assert_eq!(
                estimate_filter_rows(
                    &FilterOperator::build(eq, scan.clone(), false),
                    &arena,
                    &loader
                )?,
                Some(expected)
            );
            assert_eq!(
                estimate_filter_rows(
                    &FilterOperator::build(mixed, scan.clone(), false),
                    &arena,
                    &loader
                )?,
                Some(expected)
            );
            for predicate in [null, all_null] {
                assert_eq!(
                    estimate_filter_rows(
                        &FilterOperator::build(predicate, scan.clone(), false),
                        &arena,
                        &loader
                    )?,
                    Some(0)
                );
            }
        }
        let empty_cache = StatisticsMetaCache::default();
        let loader = StatisticMetaLoader::new(&empty_cache);
        let plan = FilterOperator::build(eq, scan, false);
        let mut best = None;
        FilterImplementation.update_best_option(&plan, &arena, &loader, &mut best)?;
        assert_eq!(best.unwrap().0.row_count, RowCountOption::Follow);
        let (empty, _) = input(&mut arena, 0);
        let cache = statistics(100, 0, 10);
        assert_eq!(
            estimate_filter_rows(
                &FilterOperator::build(eq, empty, false),
                &arena,
                &StatisticMetaLoader::new(&cache)
            )?,
            Some(0)
        );
        Ok(())
    }

    #[test]
    fn unsupported_predicates_and_restricted_inputs_keep_follow() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (mut scan, keys) = input(&mut arena, 100);
        let cache = statistics(100, 0, 10);
        let loader = StatisticMetaLoader::new(&cache);
        let eq = equal(&mut arena, keys[0], DataValue::Int32(1));
        let other = equal(&mut arena, keys[1], DataValue::Int32(2));
        let multi_column = binary(&mut arena, BinaryOperator::Or, eq, other);
        let and = binary(&mut arena, BinaryOperator::And, eq, eq);
        let column_eq = binary(&mut arena, BinaryOperator::Eq, keys[0], keys[1]);
        let not_in = in_list(&mut arena, keys[0], &[DataValue::Int32(1)], true);
        let wrong_type = equal(&mut arena, keys[0], DataValue::Int64(1));
        let cast = arena.alloc_expression(ScalarExpression::TypeCast {
            expr: keys[0],
            ty: LogicalType::Integer,
            evaluator: None,
        });
        let cast_eq = equal(&mut arena, cast, DataValue::Int32(1));
        for predicate in [multi_column, and, column_eq, not_in, wrong_type, cast_eq] {
            assert_eq!(
                estimate_filter_rows(
                    &FilterOperator::build(predicate, scan.clone(), false),
                    &arena,
                    &loader
                )?,
                None
            );
        }
        scan.physical_option.as_mut().unwrap().row_count = RowCountOption::Unknown;
        assert_eq!(
            estimate_filter_rows(
                &FilterOperator::build(eq, scan.clone(), false),
                &arena,
                &loader
            )?,
            None
        );
        scan.physical_option.as_mut().unwrap().row_count = RowCountOption::Estimated {
            rows: 100,
            ndv: None,
        };
        let Operator::TableScan(op) = &mut scan.operator else {
            unreachable!()
        };
        op.limit = (Some(1), Some(5));
        assert_eq!(
            estimate_filter_rows(
                &FilterOperator::build(eq, scan.clone(), false),
                &arena,
                &loader
            )?,
            None
        );
        let limited = LogicalPlan::new(
            Operator::Limit(LimitOperator {
                offset: None,
                limit: Some(5),
            }),
            Childrens::Only(Box::new(scan)),
        );
        assert_eq!(
            estimate_filter_rows(&FilterOperator::build(eq, limited, false), &arena, &loader)?,
            None
        );
        let (scan, _) = input(&mut arena, 100);
        let mut estimated = FilterOperator::build(eq, scan, false);
        estimated.physical_option = Some(PhysicalOption::new(
            PlanImpl::Filter,
            SortOption::Follow,
            RowCountOption::Estimated {
                rows: 10,
                ndv: None,
            },
        ));
        assert_eq!(
            estimate_filter_rows(
                &FilterOperator::build(eq, estimated, false),
                &arena,
                &loader
            )?,
            None
        );
        Ok(())
    }

    #[test]
    fn point_selectivity_is_not_applied_again_to_index_output() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (mut scan, keys) = input(&mut arena, 10);
        let meta = arena.alloc_index(IndexMeta {
            id: 1,
            table_name: "t".into(),
            name: "idx".into(),
            column_ids: vec![0],
            ty: IndexType::Normal,
            pk_ty: LogicalType::Integer,
            value_ty: LogicalType::Integer,
        });
        scan.physical_option.as_mut().unwrap().plan = PlanImpl::IndexScan(Box::new(IndexInfo {
            meta,
            sort_option: SortOption::None,
            lookup: None,
            residual_predicate: None,
            covered_deserializers: None,
            cover_mapping: None,
            sort_elimination_hint: None,
            stream_aggregate_hint: None,
        }));
        let predicate = equal(&mut arena, keys[0], DataValue::Int32(1));
        let cache = statistics(100, 0, 10);
        let mut plan = FilterOperator::build(predicate, scan, false);
        let mut best = None;
        FilterImplementation.update_best_option(
            &plan,
            &arena,
            &StatisticMetaLoader::new(&cache),
            &mut best,
        )?;
        plan.physical_option = best.map(|(option, _)| option);
        assert_eq!(
            plan.physical_option.as_ref().unwrap().row_count,
            RowCountOption::Follow
        );
        assert_eq!(plan.estimated_rows(), Some(10));
        Ok(())
    }

    #[test]
    fn direct_aliases_and_dynamic_implementation_path_use_column_stats() -> Result<(), DatabaseError>
    {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (scan, keys) = input(&mut arena, 100);
        let alias = arena.alloc_expression(ScalarExpression::Alias {
            expr: keys[0],
            alias: AliasType::Name("alias".into()),
        });
        let column = alias.output_column_ref(&mut arena);
        let key = arena.alloc_expression(ScalarExpression::column_expr(column, 0));
        let mut project = LogicalPlan::new(
            Operator::Project(ProjectOperator::new(vec![alias])),
            Childrens::Only(Box::new(scan)),
        );
        project.physical_option = Some(PhysicalOption::new(
            PlanImpl::Project,
            SortOption::Follow,
            RowCountOption::Follow,
        ));
        let eq = equal(&mut arena, key, DataValue::Int32(1));
        let plan = FilterOperator::build(eq, project, false);
        let cache = statistics(100, 0, 10);
        let loader = StatisticMetaLoader::new(&cache);
        assert_eq!(estimate_filter_rows(&plan, &arena, &loader)?, Some(10));
        let pipeline = HepOptimizerPipeline::builder()
            .implementations(vec![ImplementationRuleImpl::Filter])
            .build();
        let mut plan = pipeline
            .instantiate(plan)
            .find_best(Some(&loader), &mut arena)?;
        assert_eq!(plan.estimated_rows(), Some(10));
        assert_eq!(plan.estimated_startup_work()?, Some(0));
        Ok(())
    }
}
// GRCOV_EXCL_STOP
