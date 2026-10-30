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

use super::*;
// GRCOV_EXCL_START
#[cfg(test)]
use crate::planner::operator::{PhysicalOption, PlanImpl, RowCountOption, SortOption};
// GRCOV_EXCL_STOP
use crate::planner::ParamArena;
// GRCOV_EXCL_START
#[cfg(test)]
use crate::types::index::IndexLookup;
// GRCOV_EXCL_STOP
use crate::types::LogicalType;
use std::mem::ManuallyDrop;
use std::ptr;

/// A bound and optimized plan with its executor tree, reused by every execution.
pub struct Program<'p> {
    pub(crate) executor: Executor<'p>,
    pub(crate) arena: ParamArena<'p>,
}

impl<'p> Program<'p> {
    /// Runs the program with `params` in `transaction`. The returned iterator puts the
    /// program back when it ends.
    pub fn execute<'a, 'txn, S: Storage>(
        &'a mut self,
        transaction: &'a mut Transaction<'txn, S>,
        params: impl AsRef<[(usize, DataValue)]>,
    ) -> Result<ProgramIter<'a, 'p, S::TransactionType<'txn>>, DatabaseError>
    where
        'txn: 'p,
        'p: 'a,
    {
        let guard = AbortOnUnwind;
        // SAFETY: `*self` is moved out until the iterator, which borrows it, writes the
        // program back when the execution ends, or until the failed start below does.
        // `guard` aborts on a panic in between, which would otherwise drop it twice.
        let result = unsafe { ptr::read(self) }.start(transaction, params.as_ref());
        mem::forget(guard);
        match result {
            Ok(iter) => Ok(ProgramIter {
                iter: ManuallyDrop::new(iter),
                program: self,
            }),
            Err(err) => {
                let ExecuteError { error, program } = *err;
                unsafe { ptr::write(self, program) };
                Err(error)
            }
        }
    }

    #[allow(clippy::type_complexity)]
    fn start<'a, 'txn, S: Storage>(
        mut self,
        transaction: &'a mut Transaction<'txn, S>,
        params: &[(usize, DataValue)],
    ) -> Result<
        TransactionIter<'a, 'p, S::TransactionType<'txn>, ParamArena<'p>>,
        Box<ExecuteError<Self>>,
    >
    where
        'txn: 'p,
        'p: 'a,
    {
        if let Err(error) = self.bind(transaction.state.table_arena(), params) {
            return Err(ExecuteError::new(error, self));
        }
        let read_only = self.read_only();
        BindSource::execute(transaction, read_only, self, |_, _, program| {
            Ok((program.executor, program.arena))
        })
    }

    fn bind(
        &mut self,
        table_arena: &TableArenaCell,
        params: &[(usize, DataValue)],
    ) -> Result<(), DatabaseError> {
        if !std::ptr::eq(self.arena.plan_arena().table_arena_cell(), table_arena) {
            return Err(DatabaseError::UnsupportedStmt(
                "plan belongs to another database".into(),
            ));
        }
        self.arena.bind(params)
    }

    pub(crate) fn read_only(&self) -> bool {
        self.executor.plan().operator.command_type().is_read_only()
    }
}

impl<'p> Reclaim<'p, ParamArena<'p>> for Program<'p> {
    fn reclaim(executor: Executor<'p>, arena: ParamArena<'p>) -> Self {
        Program { executor, arena }
    }
}

impl<'a, 'p, T: StorageTransaction + 'p> TransactionIter<'a, 'p, T, ParamArena<'p>> {
    /// Ends the execution, dropping the rows not read yet, and hands back its program.
    fn into_program(mut self) -> Result<Program<'p>, Box<ExecuteError<Program<'p>>>> {
        let finished = self.finish_statement_scope();
        let Self {
            guard,
            executor,
            plan_arena,
            ..
        } = self;
        drop(guard);
        let program = Program {
            executor,
            arena: plan_arena.into_parent(),
        };
        match finished {
            Ok(()) => Ok(program),
            Err(error) => Err(ExecuteError::new(error, program)),
        }
    }
}

impl<S: Storage> Database<S> {
    /// Prepares SQL with explicit positional parameter types into a program.
    /// Parameter values are supplied separately for each execution.
    #[cfg(feature = "parser")]
    pub fn program_sql(
        &self,
        sql: &str,
        params: &[(usize, LogicalType)],
    ) -> Result<Program<'_>, DatabaseError> {
        let statement = self.state.parse_statement(sql)?;
        let transaction = self
            .storage
            .transaction_with_isolation(self.transaction_isolation, true)?;
        self.state.prepare_plan(&statement, params, &transaction)
    }
}

impl<'db, S: Storage> Transaction<'db, S> {
    #[cfg(feature = "parser")]
    pub fn program_sql(
        &self,
        sql: &str,
        params: &[(usize, LogicalType)],
    ) -> Result<Program<'db>, DatabaseError> {
        let statement = self.state.parse_statement(sql)?;
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
    ) -> Result<Program<'a>, DatabaseError>
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

    pub(crate) fn prepare_plan_with<
        'a,
        T: StorageTransaction,
        A: AsRef<[(usize, LogicalType)]>,
        F,
    >(
        &'a self,
        params: A,
        transaction: &T,
        build: F,
    ) -> Result<Program<'a>, DatabaseError>
    where
        F: for<'bind> FnOnce(
            &mut Binder<'bind, '_, T, A>,
            &mut PlanArena<'a>,
        ) -> Result<LogicalPlan, DatabaseError>,
    {
        let (plan, mut arena) = self.build_plan(params, transaction, build)?;
        let parameter_expressions = arena.parameter_expressions();
        Ok(Program {
            executor: Executor::new(plan, &arena),
            arena: ParamArena::new(arena, parameter_expressions),
        })
    }
}

struct AbortOnUnwind;

impl Drop for AbortOnUnwind {
    fn drop(&mut self) {
        std::process::abort()
    }
}

/// Rows of one execution of a [`Program`]; ending it puts the program back.
pub struct ProgramIter<'a, 'p, T: StorageTransaction> {
    iter: ManuallyDrop<TransactionIter<'a, 'p, T, ParamArena<'p>>>,
    program: &'a mut Program<'p>,
}

impl<T: StorageTransaction> ProgramIter<'_, '_, T> {
    /// Ends the execution and writes the program back; called exactly once.
    fn end(&mut self) -> Result<(), DatabaseError> {
        let guard = AbortOnUnwind;
        // SAFETY: `iter` is taken only here, and `end` runs once: from `done`, which
        // forgets `self` afterwards, or from `drop`.
        let iter = unsafe { ManuallyDrop::take(&mut self.iter) };
        let (program, result) = match iter.into_program() {
            Ok(program) => (program, Ok(())),
            Err(err) => {
                let ExecuteError { error, program } = *err;
                (program, Err(error))
            }
        };
        // SAFETY: `*self.program` was moved out by `Program::execute`.
        unsafe { ptr::write(self.program, program) };
        mem::forget(guard);
        result
    }
}

impl<T: StorageTransaction> Drop for ProgramIter<'_, '_, T> {
    fn drop(&mut self) {
        let _ = self.end();
    }
}

impl<T: StorageTransaction> ProgramIter<'_, '_, T> {
    #[inline]
    pub fn schema<R>(&self, f: impl FnOnce(&SchemaView<'_, '_>) -> R) -> R {
        self.iter.schema(f)
    }

    #[inline]
    pub fn next_tuple<R>(
        &mut self,
        f: impl FnOnce(&SchemaView<'_, '_>, &mut TupleImpl) -> R,
    ) -> Result<Option<R>, DatabaseError> {
        self.iter.next_tuple(f)
    }

    #[inline]
    pub fn done(mut self) -> Result<(), DatabaseError> {
        while self.next_tuple(|_, _| ())?.is_some() {}
        let result = self.end();
        mem::forget(self);
        result
    }
}

impl<T: StorageTransaction> ResultIter for ProgramIter<'_, '_, T> {
    fn schema<R>(&self, f: impl FnOnce(&SchemaView<'_, '_>) -> R) -> R {
        ProgramIter::schema(self, f)
    }

    fn next_tuple<R>(
        &mut self,
        f: impl FnOnce(&SchemaView<'_, '_>, &mut TupleImpl) -> R,
    ) -> Result<Option<R>, DatabaseError> {
        ProgramIter::next_tuple(self, f)
    }

    fn done(self) -> Result<(), DatabaseError> {
        ProgramIter::done(self)
    }
}

// GRCOV_EXCL_START
#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::execution::dql::index_scan::IndexScan;
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
        let mut plan = db.program_sql(
            "select (($1 * 3 + 7) % 97) + ($1 / 2)",
            &[(1, LogicalType::Bigint)],
        )?;
        let mut tx = db.new_transaction(false)?;
        for (value, expected) in [(7, 31_i64), (23, 87_i64)] {
            let mut iter = plan.execute(&mut tx, [(1, DataValue::Int64(value))])?;
            iter.schema(|schema| {
                assert_eq!(schema.len(), 1);
                assert_eq!(
                    schema.iter().next().unwrap().datatype(),
                    &LogicalType::Bigint
                );
            });
            assert_eq!(
                iter.next_tuple(|_, row| row.clone())?.unwrap(),
                vec![DataValue::Int64(expected)]
            );
            assert!(iter.next_tuple(|_, _| ())?.is_none());
            iter.done()?;
        }
        let mut iter = plan.execute(&mut tx, [(1, DataValue::Int64(7))])?;
        assert_eq!(
            iter.next_tuple(|_, row| row.clone())?.unwrap(),
            vec![DataValue::Int64(31)]
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
        let tables = TableCache::default();
        let context = &tables;
        let storage = MemoryStorage::new();
        let transaction = storage.transaction()?;
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
                    deserializers: crate::storage::deserializers_for_test(vec![column]),
                    limit: (None, None),
                    index_infos: vec![index.clone(), other.clone()],
                    with_pk: false,
                }),
                Childrens::None,
            );
            scan.physical_option = Some(PhysicalOption::new(
                PlanImpl::IndexScan(Box::new(index.clone())),
                sort.clone(),
                RowCountOption::Unknown,
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
            let Operator::TableScan(_) = &scan.operator else {
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
            let mut execution_arena = ExecArena::new(context, &transaction);
            let ranges = IndexScan::ranges(
                info,
                info.lookup.as_ref().expect("lookup"),
                &mut execution_arena,
                &mut arena,
            )?;
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
        let mut tx = db.new_transaction(false)?;

        let mut lower_bounds = db.program_sql(
            "select id from parameter_order where id >= $1 and id >= $2 order by id",
            &[(1, LogicalType::Integer), (2, LogicalType::Integer)],
        )?;
        assert!(has_filter(lower_bounds.executor.plan()));
        let mut iter = lower_bounds.execute(
            &mut tx,
            [(1, DataValue::Int32(3)), (2, DataValue::Int32(1))],
        )?;
        let mut rows = Vec::new();
        while iter
            .next_tuple(|_, row| rows.push(row.value_at(0).clone()))?
            .is_some()
        {}
        iter.done()?;
        assert_eq!(rows, vec![DataValue::Int32(3), DataValue::Int32(4)]);

        let mut equalities = db.program_sql(
            "select id from parameter_order where id = $1 and id = $2",
            &[(1, LogicalType::Integer), (2, LogicalType::Integer)],
        )?;
        let mut iter = equalities.execute(
            &mut tx,
            [(1, DataValue::Int32(2)), (2, DataValue::Int32(2))],
        )?;
        assert_eq!(
            iter.next_tuple(|_, row| row.value_at(0).clone())?,
            Some(DataValue::Int32(2))
        );
        assert!(iter.next_tuple(|_, _| ())?.is_none());
        iter.done()?;

        let mut iter = equalities.execute(
            &mut tx,
            [(1, DataValue::Int32(2)), (2, DataValue::Int32(3))],
        )?;
        assert!(iter.next_tuple(|_, _| ())?.is_none());
        iter.done()?;

        let mut unions = db.program_sql(
            "select id from parameter_order where (id=$1 or id=$2) and id>$3 order by id",
            &[
                (1, LogicalType::Integer),
                (2, LogicalType::Integer),
                (3, LogicalType::Integer),
            ],
        )?;
        for (first, second, lower, expected) in [
            (
                DataValue::Int32(1),
                DataValue::Int32(4),
                DataValue::Int32(2),
                vec![DataValue::Int32(4)],
            ),
            (
                DataValue::Int32(4),
                DataValue::Int32(1),
                DataValue::Int32(0),
                vec![DataValue::Int32(1), DataValue::Int32(4)],
            ),
            (
                DataValue::Int32(2),
                DataValue::Int32(2),
                DataValue::Int32(0),
                vec![DataValue::Int32(2)],
            ),
            (
                DataValue::Null,
                DataValue::Int32(3),
                DataValue::Int32(0),
                vec![DataValue::Int32(3)],
            ),
            (
                DataValue::Int32(1),
                DataValue::Int32(4),
                DataValue::Null,
                vec![],
            ),
        ] {
            let mut iter = unions.execute(&mut tx, [(1, first), (2, second), (3, lower)])?;
            let mut rows = Vec::new();
            while iter
                .next_tuple(|_, row| rows.push(row.value_at(0).clone()))?
                .is_some()
            {}
            iter.done()?;
            assert_eq!(rows, expected);
        }
        Ok(())
    }

    #[test]
    fn repeated_execution_and_null_do_not_stale_parameters() -> Result<(), DatabaseError> {
        let db = DataBaseBuilder::path(".").build_in_memory()?;
        let mut plan = db.program_sql(
            "values ($1 + $2), ($2)",
            &[(1, LogicalType::Integer), (2, LogicalType::Integer)],
        )?;
        let mut tx = db.new_transaction(false)?;
        for (value, expected) in [
            (DataValue::Null, DataValue::Null),
            (DataValue::Int32(3), DataValue::Int32(13)),
        ] {
            let mut iter =
                plan.execute(&mut tx, [(1, DataValue::Int32(10)), (2, value.clone())])?;
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
            plan.execute(&mut tx, [(1, DataValue::Int32(10))]),
            Err(DatabaseError::ParametersNotFound { name, .. }) if name == "$2"
        ));
        let mut iter = plan.execute(
            &mut tx,
            [(1, DataValue::Int32(10)), (2, DataValue::Int32(4))],
        )?;
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
        let mut tx = db.new_transaction(false)?;
        let mut plan = db.program_sql(
            "select k from t where w=$1 and k >= $2 and k < $3 order by k",
            &[
                (1, LogicalType::Integer),
                (2, LogicalType::Integer),
                (3, LogicalType::Integer),
            ],
        )?;
        assert!(index_range(plan.executor.plan()).is_some());
        let mut iter = plan.execute(
            &mut tx,
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
        let mut iter = plan.execute(
            &mut tx,
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
        let mut plan = db.program_sql(
            "select k from t where w=$1 and k<$2 and k>=($3-20)",
            &[
                (1, LogicalType::Integer),
                (2, LogicalType::Integer),
                (3, LogicalType::Integer),
            ],
        )?;
        plan.arena.bind(&[
            (1, DataValue::Int32(2)),
            (2, DataValue::Int32(5)),
            (3, DataValue::Int32(22)),
        ])?;
        fn find_scan(plan: &LogicalPlan) -> Option<&LogicalPlan> {
            matches!(plan.operator, Operator::TableScan(_))
                .then_some(plan)
                .or_else(|| plan.childrens.iter().find_map(find_scan))
        }
        let scan = find_scan(plan.executor.plan()).expect("table scan");
        let Operator::TableScan(_) = &scan.operator else {
            unreachable!()
        };
        let Some(PhysicalOption {
            plan: PlanImpl::IndexScan(info),
            ..
        }) = &scan.physical_option
        else {
            panic!("expected index scan")
        };
        let tables = TableCache::default();
        let context = &tables;
        let storage = MemoryStorage::new();
        let transaction = storage.transaction()?;
        let mut execution_arena = ExecArena::new(context, &transaction);
        let ranges = IndexScan::ranges(
            info,
            info.lookup.as_ref().expect("lookup"),
            &mut execution_arena,
            &mut plan.arena,
        )?;
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
// GRCOV_EXCL_STOP
