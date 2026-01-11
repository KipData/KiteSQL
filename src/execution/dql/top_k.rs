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
use crate::execution::dql::sort::BumpVec;
use crate::execution::{build_read, spawn_executor, Executor, ReadExecutor};
use crate::planner::operator::sort::SortField;
use crate::planner::operator::top_k::TopKOperator;
use crate::planner::LogicalPlan;
use crate::storage::table_codec::BumpBytes;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple::{Schema, Tuple};
use bumpalo::Bump;
use std::cmp::Ordering;
use std::collections::BTreeSet;

#[derive(Eq, PartialEq, Debug)]
struct CmpItem<'a> {
    key: BumpVec<'a, u8>,
    tuple: Tuple,
}

impl Ord for CmpItem<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key).then_with(|| Ordering::Greater)
    }
}

impl PartialOrd for CmpItem<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[allow(clippy::mutable_key_type)]
fn top_sort<'a>(
    arena: &'a Bump,
    schema: &Schema,
    sort_fields: &[SortField],
    heap: &mut BTreeSet<CmpItem<'a>>,
    tuple: Tuple,
    keep_count: usize,
) -> Result<(), DatabaseError> {
    let mut full_key = BumpBytes::new_in(arena);
    for SortField {
        expr,
        nulls_first,
        asc,
    } in sort_fields
    {
        let mut key = BumpBytes::new_in(arena);
        expr.eval(Some((&tuple, &**schema)))?
            .memcomparable_encode_with_null_order(&mut key, *nulls_first)?;
        if !asc && key.len() > 1 {
            for byte in key.iter_mut().skip(1) {
                *byte ^= 0xFF;
            }
        }
        full_key.extend(key);
    }

    if heap.len() < keep_count {
        heap.insert(CmpItem {
            key: full_key,
            tuple,
        });
    } else if let Some(cmp_item) = heap.last() {
        if full_key.as_slice() < cmp_item.key.as_slice() {
            heap.pop_last();
            heap.insert(CmpItem {
                key: full_key,
                tuple,
            });
        }
    }
    Ok(())
}

pub struct TopK {
    arena: Bump,
    sort_fields: Vec<SortField>,
    limit: usize,
    offset: Option<usize>,
    input: LogicalPlan,
}

impl From<(TopKOperator, LogicalPlan)> for TopK {
    fn from(
        (
            TopKOperator {
                sort_fields,
                limit,
                offset,
            },
            input,
        ): (TopKOperator, LogicalPlan),
    ) -> Self {
        TopK {
            arena: Default::default(),
            sort_fields,
            limit,
            offset,
            input,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for TopK {
    #[allow(clippy::mutable_key_type)]
    fn execute(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let TopK {
                arena,
                sort_fields,
                limit,
                offset,
                mut input,
            } = self;

            let arena: *const Bump = &arena;

            let schema = input.output_schema().clone();
            let keep_count = offset.unwrap_or(0) + limit;
            let mut set = BTreeSet::new();
            let coroutine = build_read(input, cache, transaction);

            for tuple in coroutine {
                throw!(
                    co,
                    top_sort(
                        unsafe { &*arena },
                        &schema,
                        &sort_fields,
                        &mut set,
                        throw!(co, tuple),
                        keep_count,
                    )
                );
            }

            let mut i: usize = 0;

            while let Some(item) = set.pop_first() {
                i += 1;
                if i - 1 < offset.unwrap_or(0) {
                    continue;
                }
                co.yield_(Ok(item.tuple)).await;
            }
        })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[allow(clippy::mutable_key_type)]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::errors::DatabaseError;
    use crate::execution::dql::top_k::{top_sort, CmpItem};
    use crate::expression::ScalarExpression;
    use crate::planner::operator::sort::SortField;
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use bumpalo::Bump;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    #[test]
    fn test_top_k_sort() -> Result<(), DatabaseError> {
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

        let fn_asc_and_nulls_last_eq = |mut heap: BTreeSet<CmpItem<'_>>| {
            if let Some(reverse) = heap.pop_first() {
                assert_eq!(reverse.tuple.values, vec![DataValue::Int32(0)])
            } else {
                unreachable!()
            }
            if let Some(reverse) = heap.pop_first() {
                assert_eq!(reverse.tuple.values, vec![DataValue::Int32(1)])
            } else {
                unreachable!()
            }
        };
        let fn_desc_and_nulls_last_eq = |mut heap: BTreeSet<CmpItem<'_>>| {
            if let Some(reverse) = heap.pop_first() {
                assert_eq!(reverse.tuple.values, vec![DataValue::Int32(1)])
            } else {
                unreachable!()
            }
            if let Some(reverse) = heap.pop_first() {
                assert_eq!(reverse.tuple.values, vec![DataValue::Int32(0)])
            } else {
                unreachable!()
            }
        };
        let fn_asc_and_nulls_first_eq = |mut heap: BTreeSet<CmpItem<'_>>| {
            if let Some(reverse) = heap.pop_first() {
                assert_eq!(reverse.tuple.values, vec![DataValue::Null])
            } else {
                unreachable!()
            }
            if let Some(reverse) = heap.pop_first() {
                assert_eq!(reverse.tuple.values, vec![DataValue::Int32(0)])
            } else {
                unreachable!()
            }
        };
        let fn_desc_and_nulls_first_eq = |mut heap: BTreeSet<CmpItem<'_>>| {
            if let Some(reverse) = heap.pop_first() {
                assert_eq!(reverse.tuple.values, vec![DataValue::Null])
            } else {
                unreachable!()
            }
            if let Some(reverse) = heap.pop_first() {
                assert_eq!(reverse.tuple.values, vec![DataValue::Int32(1)])
            } else {
                unreachable!()
            }
        };

        let mut indices = BTreeSet::new();

        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Null]),
            2,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(0)]),
            2,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(1)]),
            2,
        )?;
        fn_asc_and_nulls_first_eq(indices);

        let mut indices = BTreeSet::new();

        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, false),
            &mut indices,
            Tuple::new(None, vec![DataValue::Null]),
            2,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, false),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(0)]),
            2,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, false),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(1)]),
            2,
        )?;
        fn_asc_and_nulls_last_eq(indices);

        let mut indices = BTreeSet::new();

        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Null]),
            2,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(0)]),
            2,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(1)]),
            2,
        )?;
        fn_desc_and_nulls_first_eq(indices);

        let mut indices = BTreeSet::new();

        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, false),
            &mut indices,
            Tuple::new(None, vec![DataValue::Null]),
            2,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, false),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(0)]),
            2,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, false),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(1)]),
            2,
        )?;
        fn_desc_and_nulls_last_eq(indices);

        Ok(())
    }

    #[test]
    fn test_top_k_sort_mix_values() -> Result<(), DatabaseError> {
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

        let fn_asc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq =
            |mut heap: BTreeSet<CmpItem<'_>>| {
                if let Some(reverse) = heap.pop_first() {
                    assert_eq!(reverse.tuple.values, vec![DataValue::Null, DataValue::Null])
                } else {
                    unreachable!()
                }
                if let Some(reverse) = heap.pop_first() {
                    assert_eq!(
                        reverse.tuple.values,
                        vec![DataValue::Null, DataValue::Int32(0)]
                    )
                } else {
                    unreachable!()
                }
                if let Some(reverse) = heap.pop_first() {
                    assert_eq!(
                        reverse.tuple.values,
                        vec![DataValue::Int32(0), DataValue::Null]
                    )
                } else {
                    unreachable!()
                }
                if let Some(reverse) = heap.pop_first() {
                    assert_eq!(
                        reverse.tuple.values,
                        vec![DataValue::Int32(0), DataValue::Int32(0)]
                    )
                } else {
                    unreachable!()
                }
            };
        let fn_asc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq =
            |mut heap: BTreeSet<CmpItem<'_>>| {
                if let Some(reverse) = heap.pop_first() {
                    assert_eq!(
                        reverse.tuple.values,
                        vec![DataValue::Int32(0), DataValue::Null]
                    )
                } else {
                    unreachable!()
                }
                if let Some(reverse) = heap.pop_first() {
                    assert_eq!(
                        reverse.tuple.values,
                        vec![DataValue::Int32(0), DataValue::Int32(0)]
                    )
                } else {
                    unreachable!()
                }
                if let Some(reverse) = heap.pop_first() {
                    assert_eq!(
                        reverse.tuple.values,
                        vec![DataValue::Int32(1), DataValue::Null]
                    )
                } else {
                    unreachable!()
                }
                if let Some(reverse) = heap.pop_first() {
                    assert_eq!(
                        reverse.tuple.values,
                        vec![DataValue::Int32(1), DataValue::Int32(0)]
                    )
                } else {
                    unreachable!()
                }
            };
        let fn_desc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq =
            |mut heap: BTreeSet<CmpItem<'_>>| {
                if let Some(reverse) = heap.pop_first() {
                    assert_eq!(reverse.tuple.values, vec![DataValue::Null, DataValue::Null])
                } else {
                    unreachable!()
                }
                if let Some(reverse) = heap.pop_first() {
                    assert_eq!(
                        reverse.tuple.values,
                        vec![DataValue::Null, DataValue::Int32(0)]
                    )
                } else {
                    unreachable!()
                }
                if let Some(reverse) = heap.pop_first() {
                    assert_eq!(
                        reverse.tuple.values,
                        vec![DataValue::Int32(1), DataValue::Null]
                    )
                } else {
                    unreachable!()
                }
                if let Some(reverse) = heap.pop_first() {
                    assert_eq!(
                        reverse.tuple.values,
                        vec![DataValue::Int32(1), DataValue::Int32(0)]
                    )
                } else {
                    unreachable!()
                }
            };
        let fn_desc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq =
            |mut heap: BTreeSet<CmpItem<'_>>| {
                if let Some(reverse) = heap.pop_first() {
                    assert_eq!(
                        reverse.tuple.values,
                        vec![DataValue::Int32(1), DataValue::Null]
                    )
                } else {
                    unreachable!()
                }
                if let Some(reverse) = heap.pop_first() {
                    assert_eq!(
                        reverse.tuple.values,
                        vec![DataValue::Int32(1), DataValue::Int32(0)]
                    )
                } else {
                    unreachable!()
                }
                if let Some(reverse) = heap.pop_first() {
                    assert_eq!(
                        reverse.tuple.values,
                        vec![DataValue::Int32(0), DataValue::Null]
                    )
                } else {
                    unreachable!()
                }
                if let Some(reverse) = heap.pop_first() {
                    assert_eq!(
                        reverse.tuple.values,
                        vec![DataValue::Int32(0), DataValue::Int32(0)]
                    )
                } else {
                    unreachable!()
                }
            };

        let mut indices = BTreeSet::new();

        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, true, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Null, DataValue::Null]),
            4,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, true, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Null]),
            4,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, true, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Null]),
            4,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, true, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Null, DataValue::Int32(0)]),
            4,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, true, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Int32(0)]),
            4,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, true, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Int32(0)]),
            4,
        )?;
        fn_asc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq(indices);

        let mut indices = BTreeSet::new();

        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, false, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Null, DataValue::Null]),
            4,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, false, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Null]),
            4,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, false, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Null]),
            4,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, false, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Null, DataValue::Int32(0)]),
            4,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, false, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Int32(0)]),
            4,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(true, false, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Int32(0)]),
            4,
        )?;
        fn_asc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq(indices);

        let mut indices = BTreeSet::new();

        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, true, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Null, DataValue::Null]),
            4,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, true, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Null]),
            4,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, true, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Null]),
            4,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, true, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Null, DataValue::Int32(0)]),
            4,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, true, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Int32(0)]),
            4,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, true, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Int32(0)]),
            4,
        )?;
        fn_desc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq(indices);

        let mut indices = BTreeSet::new();

        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, false, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Null, DataValue::Null]),
            4,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, false, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Null]),
            4,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, false, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Null]),
            4,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, false, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Null, DataValue::Int32(0)]),
            4,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, false, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Int32(0)]),
            4,
        )?;
        top_sort(
            &arena,
            &schema,
            &fn_sort_fields(false, false, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Int32(0)]),
            4,
        )?;
        fn_desc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq(indices);

        Ok(())
    }
}
