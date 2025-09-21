use crate::errors::DatabaseError;
use crate::execution::{build_read, Executor, ReadExecutor};
use crate::planner::operator::sort::{SortField, SortOperator};
use crate::planner::LogicalPlan;
use crate::storage::table_codec::BumpBytes;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple::{Schema, Tuple};
use bumpalo::Bump;
use std::cmp::Ordering;
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;

pub(crate) type BumpVec<'bump, T> = bumpalo::collections::Vec<'bump, T>;

#[derive(Clone)]
pub(crate) struct NullableVec<'a, T>(pub(crate) BumpVec<'a, Option<T>>);

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
    pub(crate) fn put(&mut self, item: T) {
        self.0.push(Some(item));
    }

    #[inline]
    pub(crate) fn take(&mut self, offset: usize) -> T {
        self.0[offset].take().unwrap()
    }

    #[inline]
    pub(crate) fn get(&self, offset: usize) -> &T {
        self.0[offset].as_ref().unwrap()
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }
}

pub struct RemappingIterator<'a> {
    pos: usize,
    tuples: NullableVec<'a, (usize, Tuple)>,
    indices: BumpVec<'a, usize>,
}

impl RemappingIterator<'_> {
    pub fn new<'a>(
        pos: usize,
        tuples: NullableVec<'a, (usize, Tuple)>,
        indices: BumpVec<'a, usize>,
    ) -> RemappingIterator<'a> {
        RemappingIterator {
            pos,
            tuples,
            indices,
        }
    }
}

impl Iterator for RemappingIterator<'_> {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos > self.indices.len() - 1 {
            return None;
        }
        let (_, tuple) = self.tuples.take(self.indices[self.pos]);
        self.pos += 1;

        Some(tuple)
    }
}

const BUCKET_SIZE: usize = u8::MAX as usize + 1;

// LSD Radix Sort
pub(crate) fn radix_sort<'a, T, A: AsRef<[u8]>>(
    mut tuples: BumpVec<'a, (T, A)>,
    arena: &'a Bump,
) -> BumpVec<'a, T> {
    if let Some(max_len) = tuples.iter().map(|(_, bytes)| bytes.as_ref().len()).max() {
        // init buckets
        let mut temp_buckets = BumpVec::with_capacity_in(BUCKET_SIZE, arena);
        for _ in 0..BUCKET_SIZE {
            temp_buckets.push(BumpVec::new_in(arena));
        }

        for i in (0..max_len).rev() {
            for (t, value) in tuples.drain(..) {
                let bytes = value.as_ref();
                let index = if bytes.len() > i { bytes[i] } else { 0 };

                temp_buckets[index as usize].push((t, value));
            }
            for bucket in temp_buckets.iter_mut() {
                tuples.append(bucket);
            }
        }
    }
    let mut result = BumpVec::with_capacity_in(tuples.len(), arena);
    for (item, _) in tuples {
        result.push(item);
    }
    result
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
                let mut sort_keys = BumpVec::with_capacity_in(tuples.len(), arena);

                for (i, tuple) in tuples.0.iter().enumerate() {
                    debug_assert!(tuple.is_some());

                    let mut full_key = BumpVec::new_in(arena);

                    for SortField {
                        expr,
                        nulls_first,
                        asc,
                    } in sort_fields
                    {
                        let mut key = BumpBytes::new_in(arena);
                        let tuple = tuple.as_ref().map(|(_, tuple)| tuple).unwrap();

                        expr.eval(Some((tuple, schema)))?
                            .memcomparable_encode(&mut key)?;
                        if !asc {
                            for byte in key.iter_mut() {
                                *byte ^= 0xFF;
                            }
                        }
                        key.push(if *nulls_first { u8::MIN } else { u8::MAX });
                        full_key.extend(key);
                    }
                    sort_keys.push((i, full_key))
                }
                let indices = radix_sort(sort_keys, arena);

                Ok(Box::new(RemappingIterator::new(0, tuples, indices)))
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
                    for tuple in tuples.0.iter() {
                        debug_assert!(tuple.is_some());

                        let (_, tuple) = tuple.as_ref().unwrap();
                        eval_values[x].push(expr.eval(Some((tuple, schema)))?);
                    }
                }

                tuples.0.sort_by(|tuple_1, tuple_2| {
                    debug_assert!(tuple_1.is_some());
                    debug_assert!(tuple_2.is_some());

                    let (i_1, _) = tuple_1.as_ref().unwrap();
                    let (i_2, _) = tuple_2.as_ref().unwrap();
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

                Ok(Box::new(
                    tuples
                        .0
                        .into_iter()
                        .map(|tuple| tuple.map(|(_, tuple)| tuple).unwrap()),
                ))
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
        Box::new(
            #[coroutine]
            move || {
                let Sort {
                    arena,
                    sort_fields,
                    limit,
                    mut input,
                } = self;

                let arena: *const Bump = &arena;
                let schema = input.output_schema().clone();
                let mut tuples = NullableVec::new(unsafe { &*arena });
                let mut offset = 0;

                let mut coroutine = build_read(input, cache, transaction);

                while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                    tuples.put((offset, throw!(tuple)));
                    offset += 1;
                }

                let sort_by = if tuples.len() > 256 {
                    SortBy::Radix
                } else {
                    SortBy::Fast
                };
                let mut limit = limit.unwrap_or(tuples.len());

                for tuple in
                    throw!(sort_by.sorted_tuples(unsafe { &*arena }, &schema, &sort_fields, tuples))
                {
                    if limit != 0 {
                        yield Ok(tuple);
                        limit -= 1;
                    }
                }
            },
        )
    }
}

#[cfg(test)]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::errors::DatabaseError;
    use crate::execution::dql::sort::{radix_sort, BumpVec, NullableVec, SortBy};
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
            let mut indices = BumpVec::new_in(&arena);
            indices.push((0, "abc".as_bytes().to_vec()));
            indices.push((1, "abz".as_bytes().to_vec()));
            indices.push((2, "abe".as_bytes().to_vec()));
            indices.push((3, "abcd".as_bytes().to_vec()));

            let indices = radix_sort(indices, &arena);
            assert_eq!(indices.as_slice(), &[0, 3, 2, 1]);
            drop(indices)
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
        let mut inner = BumpVec::new_in(&arena);
        inner.push(Some((0_usize, Tuple::new(None, vec![DataValue::Null]))));
        inner.push(Some((1_usize, Tuple::new(None, vec![DataValue::Int32(0)]))));
        inner.push(Some((2_usize, Tuple::new(None, vec![DataValue::Int32(1)]))));
        let tuples = NullableVec(inner);

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
            tuples.clone(),
        )?);
        fn_asc_and_nulls_last_eq(SortBy::Radix.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(true, false),
            tuples.clone(),
        )?);
        fn_desc_and_nulls_first_eq(SortBy::Radix.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(false, true),
            tuples.clone(),
        )?);
        fn_desc_and_nulls_last_eq(SortBy::Radix.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(false, false),
            tuples.clone(),
        )?);

        // FastSort
        fn_asc_and_nulls_first_eq(SortBy::Fast.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(true, true),
            tuples.clone(),
        )?);
        fn_asc_and_nulls_last_eq(SortBy::Fast.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(true, false),
            tuples.clone(),
        )?);
        fn_desc_and_nulls_first_eq(SortBy::Fast.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(false, true),
            tuples.clone(),
        )?);
        fn_desc_and_nulls_last_eq(SortBy::Fast.sorted_tuples(
            &arena,
            &schema,
            &&fn_sort_fields(false, false),
            tuples.clone(),
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
        let mut inner = BumpVec::new_in(&arena);
        inner.push(Some((
            0_usize,
            Tuple::new(None, vec![DataValue::Null, DataValue::Null]),
        )));
        inner.push(Some((
            1_usize,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Null]),
        )));
        inner.push(Some((
            2_usize,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Null]),
        )));
        inner.push(Some((
            3_usize,
            Tuple::new(None, vec![DataValue::Null, DataValue::Int32(0)]),
        )));
        inner.push(Some((
            4_usize,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Int32(0)]),
        )));
        inner.push(Some((
            5_usize,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Int32(0)]),
        )));
        let tuples = NullableVec(inner);
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
            tuples.clone(),
        )?);
        fn_asc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq(SortBy::Radix.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(true, false, true, true),
            tuples.clone(),
        )?);
        fn_desc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq(SortBy::Radix.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(false, true, true, true),
            tuples.clone(),
        )?);
        fn_desc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq(SortBy::Radix.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(false, false, true, true),
            tuples.clone(),
        )?);

        // FastSort
        fn_asc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq(SortBy::Fast.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(true, true, true, true),
            tuples.clone(),
        )?);
        fn_asc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq(SortBy::Fast.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(true, false, true, true),
            tuples.clone(),
        )?);
        fn_desc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq(SortBy::Fast.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(false, true, true, true),
            tuples.clone(),
        )?);
        fn_desc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq(SortBy::Fast.sorted_tuples(
            &arena,
            &schema,
            &fn_sort_fields(false, false, true, true),
            tuples.clone(),
        )?);

        Ok(())
    }
}
