use super::*;
#[cfg(test)]
use crate::planner::operator::{PhysicalOption, PlanImpl, SortOption};
use crate::planner::{ExprRef, ParamArena, PlanInput};
#[cfg(test)]
use crate::types::index::IndexLookup;
use crate::types::LogicalType;

/// A bound and optimized reusable plan.
#[derive(Clone)]
pub struct PreparedPlan<'db> {
    pub(crate) plan: LogicalPlan,
    pub(crate) arena: PlanArena<'db>,
    parameter_expressions: Vec<ExprRef>,
}

impl<'db> PreparedPlan<'db> {
    pub(crate) fn bind_parameters(
        &self,
        params: &[(usize, DataValue)],
    ) -> Result<ParamArena<'_>, DatabaseError> {
        ParamArena::new(&self.arena, &self.parameter_expressions, params)
    }
}

impl<S: Storage> Database<S> {
    /// Executes an already prepared SQL or ORM plan with the supplied parameters.
    pub fn execute<'a>(
        &'a self,
        prepared: &'a PreparedPlan<'_>,
        params: impl AsRef<[(usize, DataValue)]>,
    ) -> Result<DatabaseIter<'a, S>, DatabaseError> {
        if !std::ptr::eq(prepared.arena.table_arena_cell(), self.state.table_arena()) {
            return Err(DatabaseError::UnsupportedStmt(
                "plan belongs to another database".into(),
            ));
        }
        let arena = prepared.bind_parameters(params.as_ref())?;
        BindSource::execute(self, |_, _| {
            Ok((PlanInput::Borrowed(&prepared.plan), arena))
        })
    }

    /// Prepare SQL with explicit positional parameter types.
    /// Parameter values are supplied separately for each execution.
    #[cfg(feature = "parser")]
    pub fn prepare_sql(
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
    /// Executes a prepared SQL or ORM plan inside this transaction.
    pub fn execute<'a>(
        &'a mut self,
        prepared: &'a PreparedPlan<'db>,
        params: impl AsRef<[(usize, DataValue)]>,
    ) -> Result<TransactionIter<'a, S::TransactionType<'db>>, DatabaseError> {
        if !std::ptr::eq(prepared.arena.table_arena_cell(), self.state.table_arena()) {
            return Err(DatabaseError::UnsupportedStmt(
                "plan belongs to another database".into(),
            ));
        }
        let arena = prepared.bind_parameters(params.as_ref())?;
        BindSource::execute(self, |_, _| {
            Ok((PlanInput::Borrowed(&prepared.plan), arena))
        })
    }

    #[cfg(feature = "parser")]
    pub fn prepare_sql(
        &self,
        sql: &str,
        params: &[(usize, LogicalType)],
    ) -> Result<PreparedPlan<'db>, DatabaseError> {
        let statement = crate::binder::parse_statement(sql)?;
        self.state.prepare_plan(&statement, params, &self.inner)
    }
}

impl<S: Storage> State<S> {
    #[cfg(feature = "parser")]
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
        self.prepare_plan_with(params, transaction, |binder, arena| {
            binder.bind(statement, arena)
        })
    }

    pub(crate) fn prepare_plan_with<'a, T: Transaction, A: AsRef<[(usize, LogicalType)]>, F>(
        &'a self,
        params: A,
        transaction: &T,
        build: F,
    ) -> Result<PreparedPlan<'a>, DatabaseError>
    where
        F: for<'bind> FnOnce(
            &mut Binder<'bind, '_, T, A>,
            &mut PlanArena<'a>,
        ) -> Result<LogicalPlan, DatabaseError>,
    {
        let (plan, mut arena) = self.build_plan(params, transaction, build)?;
        let parameter_expressions = arena.parameter_expressions();
        Ok(PreparedPlan {
            plan,
            arena,
            parameter_expressions,
        })
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
        let plan = db.prepare_sql(
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
            let plan = LogicalPlan::new(
                Operator::Filter(filter.clone()),
                Childrens::Only(Box::new(scan)),
            );
            let mut expected_index = index.clone();
            if should_change {
                expected_index.lookup = Some(IndexLookup::Static(expected.clone()));
            }
            let Childrens::Only(scan) = plan.childrens.as_ref() else {
                panic!("expected scan")
            };
            let Operator::TableScan(scan_op) = &scan.operator else {
                panic!("expected table scan")
            };
            let Some(PhysicalOption {
                plan: PlanImpl::IndexScan(info),
                ..
            }) = &scan.physical_option
            else {
                panic!("expected index scan")
            };
            if !matches!(info.lookup, Some(IndexLookup::Static(_))) {
                continue;
            }
            let mut execution_arena = crate::execution::ExecArena::<
                <crate::storage::memory::MemoryStorage as Storage>::TransactionType<'_>,
            >::with_capacity(0);
            let executor = crate::execution::dql::index_scan::IndexScan::new(
                scan_op,
                info,
                info.lookup.as_ref().expect("lookup"),
            );
            let ranges = executor.ranges(&mut execution_arena, &mut arena)?;
            let expected_range = match &expected_index.lookup {
                Some(IndexLookup::Static(range)) => range,
                _ => unreachable!(),
            };
            if let IndexLookup::Static(original) = index.lookup.as_ref().unwrap() {
                let want = if should_change {
                    expected_range
                } else {
                    original
                };
                let mut ranges = ranges;
                assert_eq!(ranges.next(), Some(want));
            }
            assert_eq!(
                scan.physical_option.as_ref().unwrap().plan,
                PlanImpl::IndexScan(Box::new(index.clone()))
            );
            assert_eq!(scan.physical_option.as_ref().unwrap().sort_option(), &sort);
            assert_eq!(plan.operator, Operator::Filter(filter));
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

        let lower_bounds = db.prepare_sql(
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

        let equalities = db.prepare_sql(
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
        let plan = db.prepare_sql(
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
        fn index_range(plan: &LogicalPlan) -> Option<&Range> {
            plan.physical_option
                .as_ref()
                .and_then(|option| match &option.plan {
                    PlanImpl::IndexScan(info) => match &info.lookup {
                        Some(IndexLookup::Static(range)) => Some(range),
                        _ => None,
                    },
                    _ => None,
                })
                .or_else(|| plan.childrens.iter().find_map(index_range))
        }

        let mut db = DataBaseBuilder::path(".")
            .histogram_buckets(2)
            .build_in_memory()?;
        db.ddl("create table t(w int, k int, primary key(w,k))")?;
        db.run("insert into t values(1,1),(1,2),(1,3),(2,1),(2,2),(2,4)")?
            .done()?;
        db.analyze("t")?;
        let plan = db.prepare_sql(
            "select k from t where w=$1 and k >= $2 and k < $3 order by k",
            &[
                (1, LogicalType::Integer),
                (2, LogicalType::Integer),
                (3, LogicalType::Integer),
            ],
        )?;
        assert!(index_range(&plan.plan).is_some());
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

        // The lower bound is computed only after binding; it must still be
        // intersected with the prepared upper bound instead of scanning the
        // whole equality prefix and filtering rows afterwards.
        let plan = db.prepare_sql(
            "select k from t where w=$1 and k<$2 and k>=($3-20)",
            &[
                (1, LogicalType::Integer),
                (2, LogicalType::Integer),
                (3, LogicalType::Integer),
            ],
        )?;
        let mut bound_arena = plan.bind_parameters(&[
            (1, DataValue::Int32(2)),
            (2, DataValue::Int32(5)),
            (3, DataValue::Int32(22)),
        ])?;
        fn find_scan(plan: &LogicalPlan) -> Option<&LogicalPlan> {
            matches!(plan.operator, Operator::TableScan(_))
                .then_some(plan)
                .or_else(|| plan.childrens.iter().find_map(find_scan))
        }
        let scan = find_scan(&plan.plan).expect("table scan");
        let Operator::TableScan(scan_op) = &scan.operator else {
            unreachable!()
        };
        let Some(PhysicalOption {
            plan: PlanImpl::IndexScan(info),
            ..
        }) = &scan.physical_option
        else {
            panic!("expected index scan")
        };
        let mut execution_arena = crate::execution::ExecArena::<
            <crate::storage::memory::MemoryStorage as Storage>::TransactionType<'_>,
        >::with_capacity(0);
        let ranges = crate::execution::dql::index_scan::IndexScan::new(
            scan_op,
            info,
            info.lookup.as_ref().expect("lookup"),
        )
        .ranges(&mut execution_arena, &mut bound_arena)?;
        let mut ranges = ranges;
        assert_eq!(
            ranges.next(),
            Some(&Range::Scope {
                min: Bound::Included(DataValue::Tuple(vec![
                    DataValue::Int32(2),
                    DataValue::Int32(2),
                ])),
                max: Bound::Excluded(DataValue::Tuple(vec![
                    DataValue::Int32(2),
                    DataValue::Int32(5),
                ])),
            })
        );
        Ok(())
    }
}
