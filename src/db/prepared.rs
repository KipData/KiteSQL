use super::*;
use crate::expression::range_detacher::RangeDetacher;
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::visitor_mut::OperatorVisitorMut;
use crate::planner::operator::{PhysicalOption, PlanImpl, SortOption};
use crate::planner::MetaArena;
use crate::planner::ParamArena;
use crate::types::index::IndexLookup;
use crate::types::LogicalType;

struct ParameterBinder<'a> {
    params: &'a [(usize, DataValue)],
}

impl ParameterBinder<'_> {
    fn bind_index_infos(
        &self,
        index_infos: &mut [crate::types::index::IndexInfo],
    ) -> Result<(), DatabaseError> {
        for info in index_infos {
            if let Some(lookup) = &mut info.lookup {
                lookup.bind_parameters(self.params)?;
            }
        }
        Ok(())
    }
}

impl<'plan> OperatorVisitorMut<'plan> for ParameterBinder<'_> {
    fn visit_table_scan(
        &mut self,
        TableScanOperator { index_infos, .. }: &'plan mut TableScanOperator,
    ) -> Result<(), DatabaseError> {
        self.bind_index_infos(index_infos)
    }

    fn visit_physical_option(
        &mut self,
        physical_option: &'plan mut PhysicalOption,
    ) -> Result<(), DatabaseError> {
        if let PlanImpl::IndexScan(info) = &mut physical_option.plan {
            self.bind_index_infos(std::slice::from_mut(info))?;
        }
        Ok(())
    }
}

/// Extend the selected static index range using its bound residual predicate.
struct SpecializeIndexRange<'a, 'p> {
    arena: &'a mut (dyn MetaArena + 'p),
}

impl<'plan> OperatorVisitorMut<'plan> for SpecializeIndexRange<'_, '_> {
    fn visit_operator(
        &mut self,
        operator: &'plan mut Operator,
        physical_option: Option<&'plan mut PhysicalOption>,
    ) -> Result<(), DatabaseError> {
        let Operator::TableScan(scan) = operator else {
            return Ok(());
        };
        let Some(option) = physical_option else {
            return Ok(());
        };
        let PlanImpl::IndexScan(index) = &mut option.plan else {
            return Ok(());
        };
        let (Some(params_predicate), Some(IndexLookup::Static(original))) =
            (index.residual_predicate, &index.lookup)
        else {
            return Ok(());
        };
        let SortOption::OrderBy {
            ignore_prefix_len, ..
        } = &index.sort_option
        else {
            return Ok(());
        };
        let Some(range) = RangeDetacher::specialize_range(
            index.meta,
            original,
            params_predicate,
            *ignore_prefix_len,
            self.arena,
        )?
        else {
            return Ok(());
        };
        index.lookup = Some(IndexLookup::Static(range));
        for candidate in &mut scan.index_infos {
            if candidate.meta == index.meta {
                candidate.lookup = index.lookup.clone();
            }
        }
        Ok(())
    }
}

/// A bound and optimized reusable plan.
#[derive(Clone)]
pub struct PreparedPlan<'db> {
    pub(crate) plan: LogicalPlan,
    pub(crate) arena: PlanArena<'db>,
}

impl<'db> PreparedPlan<'db> {
    pub(crate) fn bind_parameters(
        &self,
        params: &[(usize, DataValue)],
    ) -> Result<(LogicalPlan, ParamArena<'_>), DatabaseError> {
        let mut arena = ParamArena::new(&self.arena, params)?;
        let mut plan = self.plan.clone();
        ParameterBinder { params }.visit_plan(&mut plan)?;
        SpecializeIndexRange { arena: &mut arena }.visit_plan(&mut plan)?;
        Ok((plan, arena))
    }
}

impl<S: Storage> Database<S> {
    /// Prepare a plan with explicit positional parameter types.
    /// Parameter values are supplied separately for each execution.
    pub fn prepare(
        &self,
        sql: &str,
        params: &[(usize, LogicalType)],
    ) -> Result<PreparedPlan<'_>, DatabaseError> {
        let statement = crate::binder::parse_statement(sql)?;
        let transaction = self
            .storage
            .transaction_with_isolation(self.transaction_isolation)?;
        self.state.prepare_plan(&statement, params, &transaction)
    }
}

impl<'db, S: Storage> DBTransaction<'db, S> {
    pub fn prepare(
        &self,
        sql: &str,
        params: &[(usize, LogicalType)],
    ) -> Result<PreparedPlan<'db>, DatabaseError> {
        let statement = crate::binder::parse_statement(sql)?;
        self.state.prepare_plan(&statement, params, &self.inner)
    }
}

impl<S: Storage> State<S> {
    pub(crate) fn prepare_plan<'a, 'txn>(
        &'a self,
        statement: &Statement,
        params: &[(usize, LogicalType)],
        transaction: &S::TransactionType<'txn>,
    ) -> Result<PreparedPlan<'a>, DatabaseError>
    where
        S: 'txn,
    {
        if matches!(
            crate::binder::command_type(statement)?,
            crate::binder::CommandType::DDL | crate::binder::CommandType::Analyze
        ) {
            return Err(DatabaseError::UnsupportedStmt(
                "DDL and ANALYZE require ddl/analyze".into(),
            ));
        }
        let (mut plan, mut arena) = self.build_plan(params, transaction, |binder, arena| {
            binder.bind(statement, arena)
        })?;
        plan.output_schema(&mut arena);
        Ok(PreparedPlan { plan, arena })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::expression::range_detacher::Range;
    use crate::expression::{BinaryOperator, ScalarExpression};
    use crate::planner::operator::filter::FilterOperator;
    use crate::planner::operator::sort::SortField;
    use crate::planner::operator::table_scan::TableScanOperator;
    use crate::planner::{Childrens, LogicalPlan, TableArenaCell};
    use crate::types::index::{IndexInfo, IndexMeta, IndexType};
    use crate::types::tuple::TupleLike;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::ops::Bound;

    #[test]
    fn prepare_caches_scalar_output_schema() -> Result<(), DatabaseError> {
        let db = DataBaseBuilder::path(".").build_in_memory()?;
        let plan = db.prepare(
            "select (($1 * 3 + 7) % 97) + ($1 / 2)",
            &[(1, LogicalType::Bigint)],
        )?;
        for (value, expected) in [(7, 31.5), (23, 87.5)] {
            let mut iter = db.execute(&plan, [(1, DataValue::Int64(value))])?;
            iter.schema(|schema| {
                assert_eq!(schema.len(), 1);
                assert_eq!(
                    schema.iter().next().unwrap().datatype(),
                    &LogicalType::Double
                );
            });
            assert_eq!(
                iter.next_tuple(|_, row| row.values.clone())?,
                Some(vec![DataValue::Float64(expected.into())])
            );
            assert!(iter.next_tuple(|_, _| ())?.is_none());
            iter.done()?;
        }
        let mut tx = db.new_transaction()?;
        let mut iter = tx.execute(&plan, [(1, DataValue::Int64(7))])?;
        assert_eq!(
            iter.next_tuple(|_, row| row.values.clone())?,
            Some(vec![DataValue::Float64(31.5.into())])
        );
        iter.done()?;
        tx.commit()?;
        Ok(())
    }

    #[test]
    fn specialize_selected_index_and_preserve_plan_metadata() -> Result<(), DatabaseError> {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let mut column = ColumnCatalog::new(
            "id".into(),
            false,
            ColumnDesc::new(LogicalType::Integer, None, false, None)?,
        );
        column.set_ref_table("t".into(), 1, false);
        let column = arena.alloc_column(column);
        let col_expr = arena.alloc_expression(ScalarExpression::column_expr(column, 0));
        let meta = arena.alloc_index(IndexMeta {
            id: 1,
            column_ids: vec![1],
            table_name: "t".into(),
            pk_ty: LogicalType::Integer,
            value_ty: LogicalType::Integer,
            name: "pk".into(),
            ty: IndexType::PrimaryKey { is_multiple: false },
        });
        let original = Range::Scope {
            min: Bound::Unbounded,
            max: Bound::Excluded(DataValue::Int32(5)),
        };
        let expected = Range::Scope {
            min: Bound::Included(DataValue::Int32(3)),
            max: Bound::Excluded(DataValue::Int32(5)),
        };
        let sort = SortOption::OrderBy {
            fields: vec![SortField::new(col_expr, true, false)],
            ignore_prefix_len: 0,
        };
        for (lookup, has_residual, parameter, should_change) in [
            (
                IndexLookup::Static(original.clone()),
                true,
                DataValue::Int32(5),
                true,
            ),
            (
                IndexLookup::Static(original.clone()),
                false,
                DataValue::Int32(5),
                false,
            ),
            (IndexLookup::Probe, true, DataValue::Int32(5), false),
            (
                IndexLookup::Static(original.clone()),
                true,
                DataValue::Int32(i32::MIN),
                false,
            ),
            (
                IndexLookup::Static(original.clone()),
                true,
                DataValue::Null,
                false,
            ),
        ] {
            let left = arena.alloc_expression(ScalarExpression::Constant(parameter));
            let right = arena.alloc_expression(ScalarExpression::Constant(DataValue::Int32(2)));
            let boundary = arena.alloc_expression(ScalarExpression::Binary {
                op: BinaryOperator::Minus,
                left_expr: left,
                right_expr: right,
                evaluator: None,
                ty: LogicalType::Integer,
            });
            let predicate = arena.alloc_expression(ScalarExpression::Binary {
                op: BinaryOperator::GtEq,
                left_expr: col_expr,
                right_expr: boundary,
                evaluator: None,
                ty: LogicalType::Boolean,
            });
            let index = IndexInfo {
                meta,
                lookup: Some(lookup),
                residual_predicate: has_residual.then_some(predicate),
                sort_option: sort.clone(),
                covered_deserializers: None,
                cover_mapping: None,
                sort_elimination_hint: None,
                stream_aggregate_hint: None,
            };
            let mut other = index.clone();
            other.meta = arena.alloc_index(IndexMeta {
                id: 2,
                column_ids: vec![1],
                table_name: "t".into(),
                pk_ty: LogicalType::Integer,
                value_ty: LogicalType::Integer,
                name: "other".into(),
                ty: IndexType::Normal,
            });
            let mut scan = LogicalPlan::new(
                Operator::TableScan(TableScanOperator {
                    table_name: "t".into(),
                    columns: vec![column],
                    limit: (None, None),
                    index_infos: vec![index.clone(), other.clone()],
                    with_pk: false,
                }),
                Childrens::None,
            );
            scan.physical_option = Some(PhysicalOption::new(
                PlanImpl::IndexScan(Box::new(index.clone())),
                sort.clone(),
            ));
            let filter = FilterOperator {
                predicate,
                having: false,
                is_optimized: true,
            };
            let mut plan = LogicalPlan::new(
                Operator::Filter(filter.clone()),
                Childrens::Only(Box::new(scan)),
            );
            let mut expected_index = index.clone();
            if should_change {
                expected_index.lookup = Some(IndexLookup::Static(expected.clone()));
            }
            SpecializeIndexRange { arena: &mut arena }.visit_plan(&mut plan)?;
            assert_eq!(plan.operator, Operator::Filter(filter));
            let child = plan.childrens.pop_only();
            let option = child.physical_option.unwrap();
            assert_eq!(
                option.plan,
                PlanImpl::IndexScan(Box::new(expected_index.clone()))
            );
            assert_eq!(option.sort_option(), &sort);
            let Operator::TableScan(scan) = child.operator else {
                panic!("expected scan")
            };
            assert_eq!(scan.index_infos[0], expected_index);
            assert_eq!(scan.index_infos[1], other);
            assert_eq!(
                arena.expression(boundary),
                &ScalarExpression::Binary {
                    op: BinaryOperator::Minus,
                    left_expr: left,
                    right_expr: right,
                    evaluator: None,
                    ty: LogicalType::Integer,
                }
            );
        }
        Ok(())
    }

    #[test]
    fn parameter_order_dependent_predicates_remain_correct() -> Result<(), DatabaseError> {
        fn has_filter(plan: &LogicalPlan) -> bool {
            matches!(plan.operator, Operator::Filter(_)) || plan.childrens.iter().any(has_filter)
        }

        let mut db = DataBaseBuilder::path(".")
            .histogram_buckets(2)
            .build_in_memory()?;
        db.ddl("create table parameter_order(id int primary key)")?;
        db.run("insert into parameter_order values(1),(2),(3),(4)")?
            .done()?;
        db.analyze("parameter_order")?;

        let lower_bounds = db.prepare(
            "select id from parameter_order where id >= $1 and id >= $2 order by id",
            &[(1, LogicalType::Integer), (2, LogicalType::Integer)],
        )?;
        assert!(has_filter(&lower_bounds.plan));
        let mut iter = db.execute(
            &lower_bounds,
            [(1, DataValue::Int32(3)), (2, DataValue::Int32(1))],
        )?;
        let mut rows = Vec::new();
        while iter
            .next_tuple(|_, row| rows.push(row.value_at(0).clone()))?
            .is_some()
        {}
        iter.done()?;
        assert_eq!(rows, vec![DataValue::Int32(3), DataValue::Int32(4)]);

        let equalities = db.prepare(
            "select id from parameter_order where id = $1 and id = $2",
            &[(1, LogicalType::Integer), (2, LogicalType::Integer)],
        )?;
        let mut iter = db.execute(
            &equalities,
            [(1, DataValue::Int32(2)), (2, DataValue::Int32(2))],
        )?;
        assert_eq!(
            iter.next_tuple(|_, row| row.value_at(0).clone())?,
            Some(DataValue::Int32(2))
        );
        assert!(iter.next_tuple(|_, _| ())?.is_none());
        iter.done()?;

        let mut iter = db.execute(
            &equalities,
            [(1, DataValue::Int32(2)), (2, DataValue::Int32(3))],
        )?;
        assert!(iter.next_tuple(|_, _| ())?.is_none());
        iter.done()?;
        Ok(())
    }

    #[test]
    fn repeated_execution_and_null_do_not_stale_parameters() -> Result<(), DatabaseError> {
        let db = DataBaseBuilder::path(".").build_in_memory()?;
        let plan = db.prepare(
            "values ($1 + $2), ($2)",
            &[(1, LogicalType::Integer), (2, LogicalType::Integer)],
        )?;
        for (value, expected) in [
            (DataValue::Null, DataValue::Null),
            (DataValue::Int32(3), DataValue::Int32(13)),
        ] {
            let mut iter = db.execute(&plan, [(1, DataValue::Int32(10)), (2, value.clone())])?;
            assert_eq!(
                iter.next_tuple(|_, row| row.value_at(0).clone())?,
                Some(expected)
            );
            assert_eq!(
                iter.next_tuple(|_, row| row.value_at(0).clone())?,
                Some(value)
            );
            assert!(iter.next_tuple(|_, _| ())?.is_none());
            iter.done()?;
        }

        assert!(matches!(
            db.execute(&plan, [(1, DataValue::Int32(10))]),
            Err(DatabaseError::ParametersNotFound { name, .. }) if name == "$2"
        ));
        let mut iter = db.execute(&plan, [(1, DataValue::Int32(10)), (2, DataValue::Int32(4))])?;
        assert_eq!(
            iter.next_tuple(|_, row| row.value_at(0).clone())?,
            Some(DataValue::Int32(14))
        );
        iter.done()?;

        Ok(())
    }

    #[test]
    fn composite_index_ranges_bind_for_each_execution() -> Result<(), DatabaseError> {
        fn has_index_scan(plan: &LogicalPlan) -> bool {
            plan.physical_option
                .as_ref()
                .is_some_and(|option| matches!(option.plan, PlanImpl::IndexScan(_)))
                || plan.childrens.iter().any(has_index_scan)
        }

        let mut db = DataBaseBuilder::path(".")
            .histogram_buckets(2)
            .build_in_memory()?;
        db.ddl("create table t(w int, k int, primary key(w,k))")?;
        db.run("insert into t values(1,1),(1,2),(1,3),(2,1),(2,2),(2,4)")?
            .done()?;
        db.analyze("t")?;
        let plan = db.prepare(
            "select k from t where w=$1 and k >= $2 and k < $3 order by k",
            &[
                (1, LogicalType::Integer),
                (2, LogicalType::Integer),
                (3, LogicalType::Integer),
            ],
        )?;
        assert!(has_index_scan(&plan.plan));
        let mut iter = db.execute(
            &plan,
            [
                (1, DataValue::Int32(2)),
                (2, DataValue::Int32(2)),
                (3, DataValue::Int32(5)),
            ],
        )?;
        let mut rows = Vec::new();
        while iter
            .next_tuple(|_, row| rows.push(row.value_at(0).clone()))?
            .is_some()
        {}
        iter.done()?;
        assert_eq!(rows, vec![DataValue::Int32(2), DataValue::Int32(4)]);
        let mut iter = db.execute(
            &plan,
            [
                (1, DataValue::Int32(1)),
                (2, DataValue::Int32(1)),
                (3, DataValue::Int32(2)),
            ],
        )?;
        assert_eq!(
            iter.next_tuple(|_, row| row.value_at(0).clone())?,
            Some(DataValue::Int32(1))
        );
        assert!(iter.next_tuple(|_, _| ())?.is_none());
        iter.done()?;
        Ok(())
    }
}
