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
use bumpalo::Bump;
use std::cmp::Ordering;
use std::mem::{self, transmute, MaybeUninit};

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
    pub(crate) fn into_iter(self) -> impl Iterator<Item = T> + 'a {
        self.0
            .into_iter()
            .map(|item| unsafe { item.assume_init_read() })
    }
}

pub(crate) fn sort_tuples<'a>(
    sort_fields: &[SortField],
    mut tuples: NullableVec<'a, (usize, Tuple)>,
) -> Result<impl Iterator<Item = Tuple> + 'a, DatabaseError> {
    let fn_nulls_first = |nulls_first: bool| {
        if nulls_first {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    };
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
        let mut ordering = Ordering::Equal;

        for (
            x,
            SortField {
                asc, nulls_first, ..
            },
        ) in sort_fields.iter().enumerate()
        {
            let value_1 = &eval_values[x][*i_1];
            let value_2 = &eval_values[x][*i_2];

            ordering = match (value_1.is_null(), value_2.is_null()) {
                (false, true) => fn_nulls_first(*nulls_first),
                (true, false) => fn_nulls_first(*nulls_first).reverse(),
                _ => {
                    let mut ordering = value_1.partial_cmp(value_2).unwrap_or(Ordering::Equal);
                    if !*asc {
                        ordering = ordering.reverse();
                    }
                    ordering
                }
            };
            if ordering != Ordering::Equal {
                break;
            }
        }

        ordering
    });
    drop(eval_values);

    Ok(tuples.into_iter().map(|(_, tuple)| tuple))
}

pub struct Sort {
    output: Option<Box<dyn Iterator<Item = Tuple>>>,
    arena: Box<Bump>,
    sort_fields: Vec<SortField>,
    limit: Option<usize>,
    input: ExecId,
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Sort {
    type Input = (SortOperator, LogicalPlan);

    fn into_executor(
        (SortOperator { sort_fields, limit }, input): Self::Input,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let input = build_read(arena, plan_arena, input, cache, transaction);
        arena.push(ExecNode::Sort(Sort {
            output: None,
            arena: Box::<Bump>::default(),
            sort_fields,
            limit,
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
        if self.output.is_none() {
            let mut tuples = NullableVec::new(&self.arena);

            while arena.next_tuple(self.input, plan_arena)? {
                let offset = tuples.len();
                tuples.put((offset, mem::take(arena.result_tuple_mut())));
            }

            let limit = self.limit.unwrap_or(tuples.len());
            let rows = sort_tuples(&self.sort_fields, tuples)?;
            // The arena lives at a stable boxed address, so we can keep the iterator
            // and resume it across executor polls.
            self.output = Some(unsafe {
                transmute::<Box<dyn Iterator<Item = Tuple> + '_>, Box<dyn Iterator<Item = Tuple>>>(
                    Box::new(rows.take(limit)),
                )
            });
        }

        if let Some(tuple) = self.output.as_mut().and_then(std::iter::Iterator::next) {
            arena.produce_tuple(tuple);
        } else {
            arena.finish();
        }
        Ok(())
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

        fn_asc_and_nulls_first_eq(Box::new(sort_tuples(
            &fn_sort_fields(true, true),
            fn_tuples(),
        )?));
        fn_asc_and_nulls_last_eq(Box::new(sort_tuples(
            &fn_sort_fields(true, false),
            fn_tuples(),
        )?));
        fn_desc_and_nulls_first_eq(Box::new(sort_tuples(
            &fn_sort_fields(false, true),
            fn_tuples(),
        )?));
        fn_desc_and_nulls_last_eq(Box::new(sort_tuples(
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

        fn_asc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq(Box::new(sort_tuples(
            &fn_sort_fields(true, true, true, true),
            fn_tuples(),
        )?));
        fn_asc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq(Box::new(sort_tuples(
            &fn_sort_fields(true, false, true, true),
            fn_tuples(),
        )?));
        fn_desc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq(Box::new(sort_tuples(
            &fn_sort_fields(false, true, true, true),
            fn_tuples(),
        )?));
        fn_desc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq(Box::new(sort_tuples(
            &fn_sort_fields(false, false, true, true),
            fn_tuples(),
        )?));

        Ok(())
    }
}
