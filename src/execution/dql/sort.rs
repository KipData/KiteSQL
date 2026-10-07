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
use crate::execution::{
    build_read, ExecArena, ExecId, ExecNode, ExecutionContext, ExecutorNode, ReadExecutor,
};
use crate::planner::operator::sort::{SortField, SortOperator};
use crate::planner::LogicalPlan;
use crate::planner::ScalarQueryRef;
use crate::planner::{ExecMetaArena, MetaArena};
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use bumpalo::Bump;
use std::cmp::Ordering;
use std::mem::{transmute, MaybeUninit};
use std::ops::{Deref, DerefMut};

pub(crate) type BumpVec<'bump, T> = bumpalo::collections::Vec<'bump, T>;

#[derive(Debug)]
pub(crate) struct NullableVec<'a, T>(pub(crate) BumpVec<'a, MaybeUninit<T>>);

impl<'a, T> NullableVec<'a, T> {
    #[inline]
    pub(crate) fn new(arena: &'a Bump) -> NullableVec<'a, T> {
        NullableVec(BumpVec::new_in(arena))
    }

    #[inline]
    pub(crate) fn put(&mut self, item: T) {
        self.0.push(MaybeUninit::new(item));
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub(crate) fn pop(&mut self) -> Option<T> {
        self.0.pop().map(|item| unsafe { item.assume_init() })
    }
}

impl<T> Drop for NullableVec<'_, T> {
    fn drop(&mut self) {
        while self.pop().is_some() {}
    }
}

impl<T> Deref for NullableVec<'_, T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.0.as_ptr().cast(), self.0.len()) }
    }
}

impl<T> DerefMut for NullableVec<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.0.as_mut_ptr().cast(), self.0.len()) }
    }
}

pub(crate) fn sort_tuples<A: MetaArena>(
    sort_fields: &[SortField],
    tuples: &mut NullableVec<'_, (usize, SortTuple)>,
    plan_arena: &mut ExecMetaArena<A>,
) -> Result<(), DatabaseError> {
    // Extract the results of calculating SortFields to avoid double calculation
    // of data during comparison.
    let width = sort_fields.len();
    let mut eval_values = Vec::with_capacity(tuples.len() * width);

    for (_, row) in tuples.iter_mut() {
        plan_arena.restore_outer_values(std::mem::take(&mut row.outer_values));
        for SortField { expr, .. } in sort_fields {
            let value = plan_arena
                .expression(*expr)
                .eval(plan_arena, Some(&row.tuple))?;
            eval_values.push(value.into_owned());
        }
        row.outer_values = plan_arena.take_outer_values();
    }

    tuples.0.sort_by(|tuple_1, tuple_2| {
        let (i_1, _) = unsafe { tuple_1.assume_init_ref() };
        let (i_2, _) = unsafe { tuple_2.assume_init_ref() };
        compare_sort_keys(
            sort_fields,
            eval_values[*i_1 * width..(*i_1 + 1) * width].iter(),
            eval_values[*i_2 * width..(*i_2 + 1) * width].iter(),
        )
    });
    drop(eval_values);

    Ok(())
}

pub(crate) fn compare_sort_keys<'a>(
    sort_fields: &[SortField],
    left: impl Iterator<Item = &'a DataValue>,
    right: impl Iterator<Item = &'a DataValue>,
) -> Ordering {
    for (
        (value_1, value_2),
        SortField {
            asc, nulls_first, ..
        },
    ) in left.zip(right).zip(sort_fields.iter())
    {
        let null_ordering = if *nulls_first {
            Ordering::Greater
        } else {
            Ordering::Less
        };
        let ordering = match (value_1.is_null(), value_2.is_null()) {
            (false, true) => null_ordering,
            (true, false) => null_ordering.reverse(),
            _ => {
                let mut ordering = value_1.partial_cmp(value_2).unwrap_or(Ordering::Equal);
                if !*asc {
                    ordering = ordering.reverse();
                }
                ordering
            }
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

pub(crate) struct SortTuple {
    tuple: Tuple,
    outer_values: Vec<(ScalarQueryRef, DataValue)>,
}

pub struct Sort<'a> {
    rows: NullableVec<'static, (usize, SortTuple)>,
    _arena: Box<Bump>,
    sort_fields: &'a [SortField],
    input: ExecId,
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Sort<'a> {
    type Input = (&'a SortOperator, &'a LogicalPlan);

    fn into_executor(
        (SortOperator { sort_fields }, input): Self::Input,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let input = build_read(arena, plan_arena, input, cache, transaction);
        let sort_arena = Box::<Bump>::default();
        let rows = unsafe {
            transmute::<NullableVec<'_, (usize, SortTuple)>, NullableVec<'static, (usize, SortTuple)>>(
                NullableVec::new(&sort_arena),
            )
        };
        arena.push(ExecNode::Sort(Sort {
            rows,
            _arena: sort_arena,
            sort_fields,
            input,
        }))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for Sort<'a> {
    fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        loop {
            if let Some((_, row)) = self.rows.pop() {
                plan_arena.restore_outer_values(row.outer_values);
                arena.produce_tuple(row.tuple);
                return Ok(());
            }
            while arena.next_tuple(self.input, plan_arena)? {
                let offset = self.rows.len();
                self.rows.put((
                    offset,
                    SortTuple {
                        tuple: arena.materialize_tuple(),
                        outer_values: plan_arena.take_outer_values(),
                    },
                ));
            }
            if self.rows.is_empty() {
                arena.finish();
                return Ok(());
            }
            sort_tuples(self.sort_fields, &mut self.rows, plan_arena)?;
            self.rows.reverse();
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::errors::DatabaseError;
    use crate::execution::dql::sort::{sort_tuples, NullableVec, SortTuple};
    use crate::expression::ScalarExpression;
    use crate::planner::operator::sort::SortField;
    use crate::planner::{ExecMetaArena, PlanArena};
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use bumpalo::Bump;
    use std::cell::Cell;

    #[test]
    fn memory_sort_restores_scalar_values_for_each_output_row() -> Result<(), DatabaseError> {
        use super::Sort;
        use crate::execution::{empty_context, ExecArena, ReadExecutor};
        use crate::planner::operator::scalar_query_init::ScalarQueryInitOperator;
        use crate::planner::operator::scalar_subquery::ScalarSubqueryOperator;
        use crate::planner::operator::values::ValuesOperator;
        use crate::planner::operator::{sort::SortOperator, Operator};
        use crate::planner::{
            Childrens, ExecMetaArena, LogicalPlan, MetaArena, PlanArena, TableArenaCell,
        };
        use crate::storage::memory::MemoryStorage;
        use crate::storage::{StatisticsMetaCache, Storage, TableCache, ViewCache};

        let storage = MemoryStorage::new();
        let transaction = storage.transaction()?;
        let tables = TableCache::default();
        let views = ViewCache::default();
        let stats = StatisticsMetaCache::default();
        let cache = empty_context(&tables, &views, &stats);
        let catalog = TableArenaCell::default();
        let mut metadata = PlanArena::new(&catalog);
        let column = metadata.alloc_column(ColumnCatalog::new(
            "id".into(),
            false,
            ColumnDesc::new(LogicalType::Integer, None, false, None)?,
        ));
        let input_expr = metadata.alloc_expression(ScalarExpression::column_expr(column, 0));
        let param = metadata.alloc_scalar_query_ref(false);
        let result = metadata.alloc_scalar_query_ref(true);
        let marker = metadata.alloc_expression(ScalarExpression::OuterValue {
            id: result,
            ty: LogicalType::Integer,
        });
        let param_expr = metadata.alloc_expression(ScalarExpression::OuterParam {
            id: param,
            ty: LogicalType::Integer,
        });
        let values = [3, 1, 2]
            .into_iter()
            .map(|value| {
                metadata.alloc_expression(ScalarExpression::Constant(DataValue::Int32(value)))
            })
            .collect();
        let input = LogicalPlan::new(
            Operator::Values(ValuesOperator::new(values, 3, vec![column])),
            Childrens::None,
        );
        let query = ScalarSubqueryOperator::build(LogicalPlan::new(
            Operator::Values(ValuesOperator::new(vec![param_expr], 1, vec![column])),
            Childrens::None,
        ));
        let mut plan =
            ScalarQueryInitOperator::build(input, query, marker, vec![(param, input_expr)]);
        plan.populate_output_schema_recursive(&mut metadata);
        let op = SortOperator {
            sort_fields: vec![SortField {
                expr: marker,
                asc: true,
                nulls_first: false,
            }],
        };
        let mut arena = ExecArena::with_capacity(0);
        arena.init_context(cache, &transaction);
        // Select the memory executor explicitly even when the spill feature is enabled.
        let root = <Sort as ReadExecutor<_>>::into_executor(
            (&op, &plan),
            &mut arena,
            &mut metadata,
            cache,
            &transaction,
        );
        let mut metadata = ExecMetaArena::new(metadata);
        for expected in [1, 2, 3] {
            assert!(arena.next_tuple(root, &mut metadata)?);
            assert_eq!(
                arena.result_tuple().values,
                vec![DataValue::Int32(expected)]
            );
            assert_eq!(
                metadata.init_value(result),
                Some(&DataValue::Int32(expected))
            );
            assert_eq!(
                metadata
                    .expression(marker)
                    .eval(&metadata, Some(arena.result_tuple()))?
                    .as_ref(),
                &DataValue::Int32(expected)
            );
        }
        assert!(!arena.next_tuple(root, &mut metadata)?);
        Ok(())
    }

    #[test]
    fn nullable_vec_drops_values() {
        struct DropValue<'a>(&'a Cell<usize>);

        impl Drop for DropValue<'_> {
            fn drop(&mut self) {
                self.0.set(self.0.get() + 1);
            }
        }

        let dropped = Cell::new(0);
        let arena = Bump::new();
        {
            let mut values = NullableVec::new(&arena);
            values.put(DropValue(&dropped));
            values.put(DropValue(&dropped));
        }
        assert_eq!(dropped.get(), 2);
    }

    fn sorted_rows<'a>(
        sort_fields: &[SortField],
        mut tuples: NullableVec<'a, (usize, SortTuple)>,
        plan_arena: &mut ExecMetaArena<PlanArena<'_>>,
    ) -> Result<impl Iterator<Item = Tuple> + 'a, DatabaseError> {
        sort_tuples(sort_fields, &mut tuples, plan_arena)?;
        let mut rows = Vec::with_capacity(tuples.len());
        while let Some((_, row)) = tuples.pop() {
            rows.push(row.tuple);
        }
        rows.reverse();
        Ok(rows.into_iter())
    }

    #[test]
    fn test_single_value_desc_and_null_first() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let sort_column = plan_arena.alloc_column(ColumnCatalog::new(
            String::new(),
            false,
            ColumnDesc::new(LogicalType::Integer, Some(0), false, None).unwrap(),
        ));
        let sort_expr = plan_arena.alloc_expression(ScalarExpression::ColumnRef {
            column: sort_column,
            position: 0,
        });
        let fn_sort_fields = |asc: bool, nulls_first: bool| {
            vec![SortField {
                expr: sort_expr,
                asc,
                nulls_first,
            }]
        };
        let _schema = [plan_arena.alloc_column(ColumnCatalog::new(
            "c1".to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
        ))];

        let mut plan_arena = ExecMetaArena::new(plan_arena);
        let arena = Bump::new();
        let fn_tuples = || {
            let mut vec = NullableVec::new(&arena);
            vec.put((
                0_usize,
                SortTuple {
                    tuple: Tuple::new(None, vec![DataValue::Null]),
                    outer_values: Vec::new(),
                },
            ));
            vec.put((
                1_usize,
                SortTuple {
                    tuple: Tuple::new(None, vec![DataValue::Int32(0)]),
                    outer_values: Vec::new(),
                },
            ));
            vec.put((
                2_usize,
                SortTuple {
                    tuple: Tuple::new(None, vec![DataValue::Int32(1)]),
                    outer_values: Vec::new(),
                },
            ));
            vec
        };

        let fn_asc_and_nulls_last_eq = |mut iter: Box<dyn Iterator<Item = Tuple>>| {
            if let Some(tuple) = iter.next() {
                assert_eq!(tuple.values, vec![DataValue::Int32(0)])
            } else {
                unreachable!()
            }
            if let Some(tuple) = iter.next() {
                assert_eq!(tuple.values, vec![DataValue::Int32(1)])
            } else {
                unreachable!()
            }
            if let Some(tuple) = iter.next() {
                assert_eq!(tuple.values, vec![DataValue::Null])
            } else {
                unreachable!()
            }
        };
        let fn_desc_and_nulls_last_eq = |mut iter: Box<dyn Iterator<Item = Tuple>>| {
            if let Some(tuple) = iter.next() {
                assert_eq!(tuple.values, vec![DataValue::Int32(1)])
            } else {
                unreachable!()
            }
            if let Some(tuple) = iter.next() {
                assert_eq!(tuple.values, vec![DataValue::Int32(0)])
            } else {
                unreachable!()
            }
            if let Some(tuple) = iter.next() {
                assert_eq!(tuple.values, vec![DataValue::Null])
            } else {
                unreachable!()
            }
        };
        let fn_asc_and_nulls_first_eq = |mut iter: Box<dyn Iterator<Item = Tuple>>| {
            if let Some(tuple) = iter.next() {
                assert_eq!(tuple.values, vec![DataValue::Null])
            } else {
                unreachable!()
            }
            if let Some(tuple) = iter.next() {
                assert_eq!(tuple.values, vec![DataValue::Int32(0)])
            } else {
                unreachable!()
            }
            if let Some(tuple) = iter.next() {
                assert_eq!(tuple.values, vec![DataValue::Int32(1)])
            } else {
                unreachable!()
            }
        };
        let fn_desc_and_nulls_first_eq = |mut iter: Box<dyn Iterator<Item = Tuple>>| {
            if let Some(tuple) = iter.next() {
                assert_eq!(tuple.values, vec![DataValue::Null])
            } else {
                unreachable!()
            }
            if let Some(tuple) = iter.next() {
                assert_eq!(tuple.values, vec![DataValue::Int32(1)])
            } else {
                unreachable!()
            }
            if let Some(tuple) = iter.next() {
                assert_eq!(tuple.values, vec![DataValue::Int32(0)])
            } else {
                unreachable!()
            }
        };

        fn_asc_and_nulls_first_eq(Box::new(sorted_rows(
            &fn_sort_fields(true, true),
            fn_tuples(),
            &mut plan_arena,
        )?));
        fn_asc_and_nulls_last_eq(Box::new(sorted_rows(
            &fn_sort_fields(true, false),
            fn_tuples(),
            &mut plan_arena,
        )?));
        fn_desc_and_nulls_first_eq(Box::new(sorted_rows(
            &fn_sort_fields(false, true),
            fn_tuples(),
            &mut plan_arena,
        )?));
        fn_desc_and_nulls_last_eq(Box::new(sorted_rows(
            &fn_sort_fields(false, false),
            fn_tuples(),
            &mut plan_arena,
        )?));

        Ok(())
    }

    #[test]
    fn test_mixed_value_desc_and_null_first() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let sort_column_1 = plan_arena.alloc_column(ColumnCatalog::new(
            String::new(),
            false,
            ColumnDesc::new(LogicalType::Integer, Some(0), false, None).unwrap(),
        ));
        let sort_column_2 = plan_arena.alloc_column(ColumnCatalog::new(
            String::new(),
            false,
            ColumnDesc::new(LogicalType::Integer, Some(0), false, None).unwrap(),
        ));
        let sort_expr_1 = plan_arena.alloc_expression(ScalarExpression::ColumnRef {
            column: sort_column_1,
            position: 0,
        });
        let sort_expr_2 = plan_arena.alloc_expression(ScalarExpression::ColumnRef {
            column: sort_column_2,
            position: 1,
        });
        let fn_sort_fields =
            |asc_1: bool, nulls_first_1: bool, asc_2: bool, nulls_first_2: bool| {
                vec![
                    SortField {
                        expr: sort_expr_1,
                        asc: asc_1,
                        nulls_first: nulls_first_1,
                    },
                    SortField {
                        expr: sort_expr_2,
                        asc: asc_2,
                        nulls_first: nulls_first_2,
                    },
                ]
            };
        let _schema = [
            plan_arena.alloc_column(ColumnCatalog::new(
                "c1".to_string(),
                true,
                ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
            )),
            plan_arena.alloc_column(ColumnCatalog::new(
                "c2".to_string(),
                true,
                ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
            )),
        ];
        let mut plan_arena = ExecMetaArena::new(plan_arena);
        let arena = Bump::new();

        let fn_tuples = || {
            let mut vec = NullableVec::new(&arena);
            vec.put((
                0_usize,
                SortTuple {
                    tuple: Tuple::new(None, vec![DataValue::Null, DataValue::Null]),
                    outer_values: Vec::new(),
                },
            ));
            vec.put((
                1_usize,
                SortTuple {
                    tuple: Tuple::new(None, vec![DataValue::Int32(0), DataValue::Null]),
                    outer_values: Vec::new(),
                },
            ));
            vec.put((
                2_usize,
                SortTuple {
                    tuple: Tuple::new(None, vec![DataValue::Int32(1), DataValue::Null]),
                    outer_values: Vec::new(),
                },
            ));
            vec.put((
                3_usize,
                SortTuple {
                    tuple: Tuple::new(None, vec![DataValue::Null, DataValue::Int32(0)]),
                    outer_values: Vec::new(),
                },
            ));
            vec.put((
                4_usize,
                SortTuple {
                    tuple: Tuple::new(None, vec![DataValue::Int32(0), DataValue::Int32(0)]),
                    outer_values: Vec::new(),
                },
            ));
            vec.put((
                5_usize,
                SortTuple {
                    tuple: Tuple::new(None, vec![DataValue::Int32(1), DataValue::Int32(0)]),
                    outer_values: Vec::new(),
                },
            ));
            vec
        };
        let fn_asc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq =
            |mut iter: Box<dyn Iterator<Item = Tuple>>| {
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Null, DataValue::Null])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Null, DataValue::Int32(0)])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(0), DataValue::Null])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(0), DataValue::Int32(0)])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(1), DataValue::Null])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(1), DataValue::Int32(0)])
                } else {
                    unreachable!()
                }
            };
        let fn_asc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq =
            |mut iter: Box<dyn Iterator<Item = Tuple>>| {
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(0), DataValue::Null])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(0), DataValue::Int32(0)])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(1), DataValue::Null])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(1), DataValue::Int32(0)])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Null, DataValue::Null])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Null, DataValue::Int32(0)])
                } else {
                    unreachable!()
                }
            };
        let fn_desc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq =
            |mut iter: Box<dyn Iterator<Item = Tuple>>| {
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Null, DataValue::Null])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Null, DataValue::Int32(0)])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(1), DataValue::Null])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(1), DataValue::Int32(0)])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(0), DataValue::Null])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(0), DataValue::Int32(0)])
                } else {
                    unreachable!()
                }
            };
        let fn_desc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq =
            |mut iter: Box<dyn Iterator<Item = Tuple>>| {
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(1), DataValue::Null])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(1), DataValue::Int32(0)])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(0), DataValue::Null])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Int32(0), DataValue::Int32(0)])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Null, DataValue::Null])
                } else {
                    unreachable!()
                }
                if let Some(tuple) = iter.next() {
                    assert_eq!(tuple.values, vec![DataValue::Null, DataValue::Int32(0)])
                } else {
                    unreachable!()
                }
            };

        fn_asc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq(Box::new(sorted_rows(
            &fn_sort_fields(true, true, true, true),
            fn_tuples(),
            &mut plan_arena,
        )?));
        fn_asc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq(Box::new(sorted_rows(
            &fn_sort_fields(true, false, true, true),
            fn_tuples(),
            &mut plan_arena,
        )?));
        fn_desc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq(Box::new(sorted_rows(
            &fn_sort_fields(false, true, true, true),
            fn_tuples(),
            &mut plan_arena,
        )?));
        fn_desc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq(Box::new(sorted_rows(
            &fn_sort_fields(false, false, true, true),
            fn_tuples(),
            &mut plan_arena,
        )?));

        Ok(())
    }
}
