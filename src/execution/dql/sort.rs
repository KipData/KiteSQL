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
use crate::execution::{build_read, spawn_executor, Executor, ReadExecutor};
use crate::planner::operator::sort::{SortField, SortOperator};
use crate::planner::LogicalPlan;
use crate::storage::table_codec::BumpBytes;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple::{Schema, Tuple};
use bumpalo::Bump;
use std::cmp::Ordering;
use std::mem::MaybeUninit;

pub(crate) type BumpVec<'bump, T> = bumpalo::collections::Vec<'bump, T>;

#[derive(Debug)]
pub(crate) struct NullableVec<'a, T>(pub(crate) BumpVec<'a, MaybeUninit<T>>);

impl<'a, T> NullableVec<'a, T> {
    #[inline]
    pub(crate) fn new(arena: &'a Bump) -> NullableVec<'a, T> {
        NullableVec(BumpVec::new_in(arena))
    }

    #[inline]
    pub(crate) fn with_capacity(capacity: usize, arena: &'a Bump) -> NullableVec<'a, T> {
        NullableVec(BumpVec::with_capacity_in(capacity, arena))
    }

    #[inline]
    pub(crate) fn fill_capacity(capacity: usize, arena: &'a Bump) -> NullableVec<'a, T> {
        let mut data = BumpVec::with_capacity_in(capacity, arena);
        for _ in 0..capacity {
            data.push(MaybeUninit::uninit());
        }
        NullableVec(data)
    }

    #[inline]
    pub(crate) fn put(&mut self, item: T) {
        self.0.push(MaybeUninit::new(item));
    }

    #[inline]
    pub(crate) fn set(&mut self, pos: usize, item: T) {
        self.0[pos] = MaybeUninit::new(item);
    }

    #[inline]
    pub(crate) fn take(&mut self, offset: usize) -> T {
        unsafe { self.0[offset].assume_init_read() }
    }

    #[inline]
    pub(crate) fn get(&self, offset: usize) -> &T {
        unsafe { self.0[offset].assume_init_ref() }
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline]
    pub(crate) fn iter(&self) -> impl Iterator<Item = &T> {
        self.0.iter().map(|item| unsafe { item.assume_init_ref() })
    }

    #[inline]
    pub(crate) fn take_iter(&mut self) -> impl Iterator<Item = T> + '_ {
        self.0
            .iter_mut()
            .map(|item| unsafe { item.assume_init_read() })
    }

    #[inline]
    pub(crate) fn into_iter(self) -> impl Iterator<Item = T> + 'a {
        self.0
            .into_iter()
            .map(|item| unsafe { item.assume_init_read() })
    }
}

pub struct RemappingIterator<'a, T> {
    tuples: NullableVec<'a, (usize, Tuple)>,
    indices: T,
}

impl<T: Iterator<Item = usize>> RemappingIterator<'_, T> {
    pub fn new(tuples: NullableVec<(usize, Tuple)>, indices: T) -> RemappingIterator<T> {
        RemappingIterator { tuples, indices }
    }
}

impl<T: Iterator<Item = usize>> Iterator for RemappingIterator<'_, T> {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        self.indices.next().map(|pos| self.tuples.take(pos).1)
    }
}

const BUCKET_SIZE: usize = u8::MAX as usize + 1;

// LSD Radix Sort
pub(crate) fn radix_sort<'a, T, A: AsRef<[u8]>>(
    tuples: &mut NullableVec<'a, (T, A)>,
    arena: &'a Bump,
) {
    if tuples.is_empty() {
        return;
    }
    let max_len = tuples
        .iter()
        .map(|(_, bytes)| bytes.as_ref().len())
        .max()
        .unwrap();

    let mut buf = NullableVec::fill_capacity(tuples.len(), arena);

    let mut count = [0usize; BUCKET_SIZE];
    let mut pos = [0usize; BUCKET_SIZE];

    for i in (0..max_len).rev() {
        count.fill(0);

        for (_, value) in tuples.iter() {
            let bytes = value.as_ref();
            let idx = if bytes.len() > i { bytes[i] } else { 0 };
            count[idx as usize] += 1;
        }

        {
            let mut sum = 0;
            for j in 0..BUCKET_SIZE {
                let c = count[j];
                pos[j] = sum;
                sum += c;
            }
        }

        for (t, value) in tuples.take_iter() {
            let bytes = value.as_ref();
            let idx = if bytes.len() > i { bytes[i] } else { 0 };
            let p = pos[idx as usize];
            buf.set(p, (t, value));
            pos[idx as usize] += 1;
        }
        std::mem::swap(tuples, &mut buf);
    }
}

pub enum SortBy {
    Radix,
    Fast,
}

impl SortBy {
    pub(crate) fn sorted_tuples<'a>(
        &self,
        arena: &'a Bump,
        schema: &Schema,
        sort_fields: &[SortField],
        mut tuples: NullableVec<'a, (usize, Tuple)>,
    ) -> Result<Box<dyn Iterator<Item = Tuple> + 'a>, DatabaseError> {
        match self {
            SortBy::Radix => {
                let mut sort_keys = NullableVec::with_capacity(tuples.len(), arena);

                for (i, (_, tuple)) in tuples.iter().enumerate() {
                    let mut full_key = BumpVec::new_in(arena);

                    for SortField {
                        expr,
                        nulls_first,
                        asc,
                    } in sort_fields
                    {
                        let mut key = BumpBytes::new_in(arena);

                        expr.eval(Some((tuple, schema)))?
                            .memcomparable_encode_with_null_order(&mut key, *nulls_first)?;

                        if !asc && key.len() > 1 {
                            for byte in key.iter_mut().skip(1) {
                                *byte ^= 0xFF;
                            }
                        }
                        full_key.extend(key);
                    }
                    sort_keys.put((i, full_key))
                }
                radix_sort(&mut sort_keys, arena);

                Ok(Box::new(RemappingIterator::new(
                    tuples,
                    sort_keys.into_iter().map(|(i, _)| i),
                )))
            }
            SortBy::Fast => {
                let fn_nulls_first = |nulls_first: bool| {
                    if nulls_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                };
                // Extract the results of calculating SortFields to avoid double calculation
                // of data during comparison
                let mut eval_values = vec![Vec::with_capacity(sort_fields.len()); tuples.len()];

                for (x, SortField { expr, .. }) in sort_fields.iter().enumerate() {
                    for (_, tuple) in tuples.iter() {
                        eval_values[x].push(expr.eval(Some((tuple, schema)))?);
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
                                let mut ordering =
                                    value_1.partial_cmp(value_2).unwrap_or(Ordering::Equal);
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

                Ok(Box::new(tuples.into_iter().map(|(_, tuple)| tuple)))
            }
        }
    }
}

pub struct Sort {
    arena: Bump,
    sort_fields: Vec<SortField>,
    limit: Option<usize>,
    input: LogicalPlan,
}

impl From<(SortOperator, LogicalPlan)> for Sort {
    fn from((SortOperator { sort_fields, limit }, input): (SortOperator, LogicalPlan)) -> Self {
        Sort {
            arena: Default::default(),
            sort_fields,
            limit,
            input,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Sort {
    fn execute(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let Sort {
                arena,
                sort_fields,
                limit,
                mut input,
            } = self;

            let arena: *const Bump = &arena;
            let schema = input.output_schema().clone();
            let mut tuples = NullableVec::new(unsafe { &*arena });

            let mut coroutine = build_read(input, cache, transaction);

            for (offset, tuple) in coroutine.by_ref().enumerate() {
                tuples.put((offset, throw!(co, tuple)));
            }

            let sort_by = if tuples.len() > 256 {
                SortBy::Radix
            } else {
                SortBy::Fast
            };
            let mut limit = limit.unwrap_or(tuples.len());

            for tuple in throw!(
                co,
                sort_by.sorted_tuples(unsafe { &*arena }, &schema, &sort_fields, tuples)
            ) {
                if limit != 0 {
                    co.yield_(Ok(tuple)).await;
                    limit -= 1;
                }
            }
        })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::errors::DatabaseError;
    use crate::execution::dql::sort::{radix_sort, NullableVec, SortBy};
    use crate::expression::ScalarExpression;
    use crate::planner::operator::sort::SortField;
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use bumpalo::Bump;
    use std::sync::Arc;

    #[test]
    fn test_radix_sort() {
        let arena = Bump::new();
        {
            let mut indices = NullableVec::with_capacity(4, &arena);
            indices.put((0usize, "abc".as_bytes().to_vec()));
            indices.put((1, "abz".as_bytes().to_vec()));
            indices.put((2, "abe".as_bytes().to_vec()));
            indices.put((3, "abcd".as_bytes().to_vec()));

            radix_sort(&mut indices, &arena);

            let mut iter = indices.iter();

            assert_eq!(Some(&(0, "abc".as_bytes().to_vec())), iter.next());
            assert_eq!(Some(&(3, "abcd".as_bytes().to_vec())), iter.next());
            assert_eq!(Some(&(2, "abe".as_bytes().to_vec())), iter.next());
            assert_eq!(Some(&(1, "abz".as_bytes().to_vec())), iter.next());
        }
    }

    #[test]
    fn test_single_value_desc_and_null_first() -> Result<(), DatabaseError> {
        let fn_sort_fields = |asc: bool, nulls_first: bool| {
            vec![SortField {
                expr: ScalarExpression::ColumnRef {
                    column: ColumnRef(Arc::new(ColumnCatalog::new(
                        String::new(),
                        false,
                        ColumnDesc::new(LogicalType::Integer, Some(0), false, None).unwrap(),
                    ))),
                    position: Some(0),
                },
                asc,
                nulls_first,
            }]
        };
        let schema = Arc::new(vec![ColumnRef::from(ColumnCatalog::new(
            "c1".to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
        ))]);

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

        // RadixSort
        fn_asc_and_nulls_first_eq(SortBy::Radix.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(true, true),
            fn_tuples(),
        )?);
        fn_asc_and_nulls_last_eq(SortBy::Radix.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(true, false),
            fn_tuples(),
        )?);
        fn_desc_and_nulls_first_eq(SortBy::Radix.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(false, true),
            fn_tuples(),
        )?);
        fn_desc_and_nulls_last_eq(SortBy::Radix.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(false, false),
            fn_tuples(),
        )?);

        // FastSort
        fn_asc_and_nulls_first_eq(SortBy::Fast.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(true, true),
            fn_tuples(),
        )?);
        fn_asc_and_nulls_last_eq(SortBy::Fast.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(true, false),
            fn_tuples(),
        )?);
        fn_desc_and_nulls_first_eq(SortBy::Fast.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(false, true),
            fn_tuples(),
        )?);
        fn_desc_and_nulls_last_eq(SortBy::Fast.sorted_tuples(
            &arena,
            &schema,
            &&fn_sort_fields(false, false),
            fn_tuples(),
        )?);

        Ok(())
    }

    #[test]
    fn test_mixed_value_desc_and_null_first() -> Result<(), DatabaseError> {
        let fn_sort_fields = |asc_1: bool,
                              nulls_first_1: bool,
                              asc_2: bool,
                              nulls_first_2: bool| {
            vec![
                SortField {
                    expr: ScalarExpression::ColumnRef {
                        column: ColumnRef(Arc::new(ColumnCatalog::new(
                            String::new(),
                            false,
                            ColumnDesc::new(LogicalType::Integer, Some(0), false, None).unwrap(),
                        ))),
                        position: Some(0),
                    },
                    asc: asc_1,
                    nulls_first: nulls_first_1,
                },
                SortField {
                    expr: ScalarExpression::ColumnRef {
                        column: ColumnRef(Arc::new(ColumnCatalog::new(
                            String::new(),
                            false,
                            ColumnDesc::new(LogicalType::Integer, Some(0), false, None).unwrap(),
                        ))),
                        position: Some(1),
                    },
                    asc: asc_2,
                    nulls_first: nulls_first_2,
                },
            ]
        };
        let schema = Arc::new(vec![
            ColumnRef::from(ColumnCatalog::new(
                "c1".to_string(),
                true,
                ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
            )),
            ColumnRef::from(ColumnCatalog::new(
                "c2".to_string(),
                true,
                ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
            )),
        ]);
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

        // RadixSort
        fn_asc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq(SortBy::Radix.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(true, true, true, true),
            fn_tuples(),
        )?);
        fn_asc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq(SortBy::Radix.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(true, false, true, true),
            fn_tuples(),
        )?);
        fn_desc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq(SortBy::Radix.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(false, true, true, true),
            fn_tuples(),
        )?);
        fn_desc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq(SortBy::Radix.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(false, false, true, true),
            fn_tuples(),
        )?);

        // FastSort
        fn_asc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq(SortBy::Fast.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(true, true, true, true),
            fn_tuples(),
        )?);
        fn_asc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq(SortBy::Fast.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(true, false, true, true),
            fn_tuples(),
        )?);
        fn_desc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq(SortBy::Fast.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(false, true, true, true),
            fn_tuples(),
        )?);
        fn_desc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq(SortBy::Fast.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(false, false, true, true),
            fn_tuples(),
        )?);

        Ok(())
    }
}
