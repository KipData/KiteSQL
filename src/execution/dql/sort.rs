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
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use bumpalo::Bump;
use std::cmp::Ordering;
use std::mem::{self, transmute, MaybeUninit};
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
    pub(crate) fn iter(&self) -> impl Iterator<Item = &T> {
        self.0.iter().map(|item| unsafe { item.assume_init_ref() })
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

pub(crate) fn sort_tuples(
    sort_fields: &[SortField],
    tuples: &mut NullableVec<'_, (usize, Tuple)>,
) -> Result<(), DatabaseError> {
    // Extract the results of calculating SortFields to avoid double calculation
    // of data during comparison.
    let mut eval_values = vec![Vec::with_capacity(tuples.len()); sort_fields.len()];

    for (x, SortField { expr, .. }) in sort_fields.iter().enumerate() {
        for (_, tuple) in tuples.iter() {
            eval_values[x].push(expr.eval(Some(tuple))?);
        }
    }

    tuples.0.sort_by(|tuple_1, tuple_2| {
        let (i_1, _) = unsafe { tuple_1.assume_init_ref() };
        let (i_2, _) = unsafe { tuple_2.assume_init_ref() };
        compare_sort_keys(
            sort_fields,
            eval_values.iter().map(|values| &values[*i_1]),
            eval_values.iter().map(|values| &values[*i_2]),
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

pub struct Sort {
    rows: NullableVec<'static, (usize, Tuple)>,
    _arena: Box<Bump>,
    sort_fields: Vec<SortField>,
    input: ExecId,
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Sort {
    type Input = (SortOperator, LogicalPlan);

    fn into_executor(
        (SortOperator { sort_fields }, input): Self::Input,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let input = build_read(arena, plan_arena, input, cache, transaction);
        let sort_arena = Box::<Bump>::default();
        let rows = unsafe {
            transmute::<NullableVec<'_, (usize, Tuple)>, NullableVec<'static, (usize, Tuple)>>(
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

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for Sort {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        loop {
            if let Some((_, tuple)) = self.rows.pop() {
                arena.produce_tuple(tuple);
                return Ok(());
            }
            while arena.next_tuple(self.input, plan_arena)? {
                let offset = self.rows.len();
                self.rows.put((offset, mem::take(arena.result_tuple_mut())));
            }
            if self.rows.is_empty() {
                arena.finish();
                return Ok(());
            }
            sort_tuples(&self.sort_fields, &mut self.rows)?;
            self.rows.reverse();
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::errors::DatabaseError;
    use crate::execution::dql::sort::{sort_tuples, NullableVec};
    use crate::expression::ScalarExpression;
    use crate::planner::operator::sort::SortField;
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use bumpalo::Bump;
    use std::cell::Cell;

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
        mut tuples: NullableVec<'a, (usize, Tuple)>,
    ) -> Result<impl Iterator<Item = Tuple> + 'a, DatabaseError> {
        sort_tuples(sort_fields, &mut tuples)?;
        let mut rows = Vec::with_capacity(tuples.len());
        while let Some((_, tuple)) = tuples.pop() {
            rows.push(tuple);
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
        let fn_sort_fields = |asc: bool, nulls_first: bool| {
            vec![SortField {
                expr: ScalarExpression::ColumnRef {
                    column: sort_column,
                    position: 0,
                },
                asc,
                nulls_first,
            }]
        };
        let _schema = [plan_arena.alloc_column(ColumnCatalog::new(
            "c1".to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
        ))];

        let arena = Bump::new();
        let fn_tuples = || {
            let mut vec = NullableVec::new(&arena);
            vec.put((0_usize, Tuple::new(None, vec![DataValue::Null])));
            vec.put((1_usize, Tuple::new(None, vec![DataValue::Int32(0)])));
            vec.put((2_usize, Tuple::new(None, vec![DataValue::Int32(1)])));
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
        )?));
        fn_asc_and_nulls_last_eq(Box::new(sorted_rows(
            &fn_sort_fields(true, false),
            fn_tuples(),
        )?));
        fn_desc_and_nulls_first_eq(Box::new(sorted_rows(
            &fn_sort_fields(false, true),
            fn_tuples(),
        )?));
        fn_desc_and_nulls_last_eq(Box::new(sorted_rows(
            &fn_sort_fields(false, false),
            fn_tuples(),
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
        let fn_sort_fields =
            |asc_1: bool, nulls_first_1: bool, asc_2: bool, nulls_first_2: bool| {
                vec![
                    SortField {
                        expr: ScalarExpression::ColumnRef {
                            column: sort_column_1,
                            position: 0,
                        },
                        asc: asc_1,
                        nulls_first: nulls_first_1,
                    },
                    SortField {
                        expr: ScalarExpression::ColumnRef {
                            column: sort_column_2,
                            position: 1,
                        },
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
        let arena = Bump::new();

        let fn_tuples = || {
            let mut vec = NullableVec::new(&arena);
            vec.put((
                0_usize,
                Tuple::new(None, vec![DataValue::Null, DataValue::Null]),
            ));
            vec.put((
                1_usize,
                Tuple::new(None, vec![DataValue::Int32(0), DataValue::Null]),
            ));
            vec.put((
                2_usize,
                Tuple::new(None, vec![DataValue::Int32(1), DataValue::Null]),
            ));
            vec.put((
                3_usize,
                Tuple::new(None, vec![DataValue::Null, DataValue::Int32(0)]),
            ));
            vec.put((
                4_usize,
                Tuple::new(None, vec![DataValue::Int32(0), DataValue::Int32(0)]),
            ));
            vec.put((
                5_usize,
                Tuple::new(None, vec![DataValue::Int32(1), DataValue::Int32(0)]),
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
        )?));
        fn_asc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq(Box::new(sorted_rows(
            &fn_sort_fields(true, false, true, true),
            fn_tuples(),
        )?));
        fn_desc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq(Box::new(sorted_rows(
            &fn_sort_fields(false, true, true, true),
            fn_tuples(),
        )?));
        fn_desc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq(Box::new(sorted_rows(
            &fn_sort_fields(false, false, true, true),
            fn_tuples(),
        )?));

        Ok(())
    }
}
