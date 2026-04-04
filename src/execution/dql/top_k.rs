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
use crate::execution::{build_read, ExecArena, ExecId, ExecNode, ExecutionCaches, ExecutorNode};
use crate::planner::operator::sort::SortField;
use crate::planner::operator::top_k::TopKOperator;
use crate::planner::LogicalPlan;
use crate::storage::table_codec::BumpBytes;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use bumpalo::Bump;
use std::cmp::Ordering;
use std::collections::{btree_set::IntoIter as BTreeSetIntoIter, BTreeSet};
use std::mem::transmute;

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
        expr.eval(Some(&tuple))?
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
    output: Option<std::iter::Skip<BTreeSetIntoIter<CmpItem<'static>>>>,
    arena: Box<Bump>,
    sort_fields: Vec<SortField>,
    limit: usize,
    offset: Option<usize>,
    input: ExecId,
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for TopK {
    type Input = (TopKOperator, LogicalPlan);

    fn into_executor(
        (
            TopKOperator {
                sort_fields,
                limit,
                offset,
            },
            input,
        ): Self::Input,
        arena: &mut ExecArena<'a, T>,
        cache: ExecutionCaches<'a>,
        transaction: *mut T,
    ) -> ExecId {
        let input = build_read(arena, input, cache, transaction);
        arena.push(ExecNode::TopK(TopK {
            output: None,
            arena: Box::<Bump>::default(),
            sort_fields,
            limit,
            offset,
            input,
        }))
    }

    #[allow(clippy::mutable_key_type)]
    fn next_tuple(&mut self, arena: &mut ExecArena<'a, T>) -> Result<(), DatabaseError> {
        if self.output.is_none() {
            let keep_count = self.offset.unwrap_or(0) + self.limit;
            let mut set = BTreeSet::new();

            while arena.next_tuple(self.input)? {
                top_sort(
                    &self.arena,
                    &self.sort_fields,
                    &mut set,
                    arena.result_tuple().clone(),
                    keep_count,
                )?;
            }

            let offset = self.offset.unwrap_or(0);
            let rows = set.into_iter().skip(offset);
            // The arena lives at a stable boxed address, so we can keep the old set/key shape
            // and resume iteration across executor polls.
            self.output = Some(unsafe {
                transmute::<
                    std::iter::Skip<BTreeSetIntoIter<CmpItem<'_>>>,
                    std::iter::Skip<BTreeSetIntoIter<CmpItem<'static>>>,
                >(rows)
            });
        }

        if let Some(item) = self.output.as_mut().and_then(std::iter::Iterator::next) {
            arena.produce_tuple(item.tuple);
        } else {
            arena.finish();
        }
        Ok(())
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
                    position: 0,
                },
                asc,
                nulls_first,
            }]
        };
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
            &fn_sort_fields(true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Null]),
            2,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(0)]),
            2,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(1)]),
            2,
        )?;
        fn_asc_and_nulls_first_eq(indices);

        let mut indices = BTreeSet::new();

        top_sort(
            &arena,
            &fn_sort_fields(true, false),
            &mut indices,
            Tuple::new(None, vec![DataValue::Null]),
            2,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(true, false),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(0)]),
            2,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(true, false),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(1)]),
            2,
        )?;
        fn_asc_and_nulls_last_eq(indices);

        let mut indices = BTreeSet::new();

        top_sort(
            &arena,
            &fn_sort_fields(false, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Null]),
            2,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(false, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(0)]),
            2,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(false, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(1)]),
            2,
        )?;
        fn_desc_and_nulls_first_eq(indices);

        let mut indices = BTreeSet::new();

        top_sort(
            &arena,
            &fn_sort_fields(false, false),
            &mut indices,
            Tuple::new(None, vec![DataValue::Null]),
            2,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(false, false),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(0)]),
            2,
        )?;
        top_sort(
            &arena,
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
                        position: 0,
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
                        position: 1,
                    },
                    asc: asc_2,
                    nulls_first: nulls_first_2,
                },
            ]
        };
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
            &fn_sort_fields(true, true, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Null, DataValue::Null]),
            4,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(true, true, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Null]),
            4,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(true, true, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Null]),
            4,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(true, true, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Null, DataValue::Int32(0)]),
            4,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(true, true, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Int32(0)]),
            4,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(true, true, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Int32(0)]),
            4,
        )?;
        fn_asc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq(indices);

        let mut indices = BTreeSet::new();

        top_sort(
            &arena,
            &fn_sort_fields(true, false, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Null, DataValue::Null]),
            4,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(true, false, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Null]),
            4,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(true, false, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Null]),
            4,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(true, false, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Null, DataValue::Int32(0)]),
            4,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(true, false, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Int32(0)]),
            4,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(true, false, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Int32(0)]),
            4,
        )?;
        fn_asc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq(indices);

        let mut indices = BTreeSet::new();

        top_sort(
            &arena,
            &fn_sort_fields(false, true, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Null, DataValue::Null]),
            4,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(false, true, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Null]),
            4,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(false, true, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Null]),
            4,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(false, true, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Null, DataValue::Int32(0)]),
            4,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(false, true, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Int32(0)]),
            4,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(false, true, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Int32(0)]),
            4,
        )?;
        fn_desc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq(indices);

        let mut indices = BTreeSet::new();

        top_sort(
            &arena,
            &fn_sort_fields(false, false, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Null, DataValue::Null]),
            4,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(false, false, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Null]),
            4,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(false, false, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Null]),
            4,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(false, false, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Null, DataValue::Int32(0)]),
            4,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(false, false, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(0), DataValue::Int32(0)]),
            4,
        )?;
        top_sort(
            &arena,
            &fn_sort_fields(false, false, true, true),
            &mut indices,
            Tuple::new(None, vec![DataValue::Int32(1), DataValue::Int32(0)]),
            4,
        )?;
        fn_desc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq(indices);

        Ok(())
    }
}
