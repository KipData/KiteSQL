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
use crate::execution::{
    build_read, ExecArena, ExecId, ExecNode, ExecutionContext, ExecutorNode, ReadExecutor,
};
use crate::planner::operator::sort::SortField;
use crate::planner::operator::top_k::TopKOperator;
use crate::planner::LogicalPlan;
use crate::planner::{ExecMetaArena, MetaArena};
use crate::storage::table_codec::BumpBytes;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use bumpalo::Bump;
use std::cmp::Ordering;
use std::collections::{btree_set::IntoIter as BTreeSetIntoIter, BTreeSet};
use std::mem::transmute;

#[derive(Debug)]
struct CmpItem<'a> {
    key: BumpVec<'a, u8>,
    sequence: usize,
    tuple: Tuple,
}

impl PartialEq for CmpItem<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.sequence == other.sequence
    }
}

impl Eq for CmpItem<'_> {}

impl Ord for CmpItem<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.sequence.cmp(&other.sequence))
    }
}

impl PartialOrd for CmpItem<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[allow(clippy::mutable_key_type)]
fn top_sort<'a>(
    full_key: &mut BumpBytes<'a>,
    sort_fields: &[SortField],
    heap: &mut BTreeSet<CmpItem<'a>>,
    tuple: &mut Tuple,
    keep_count: usize,
    sequence: usize,
    plan_arena: &(dyn MetaArena + '_),
) -> Result<(), DatabaseError> {
    full_key.clear();
    for SortField {
        expr,
        nulls_first,
        asc,
    } in sort_fields
    {
        let start = full_key.len();
        plan_arena
            .expression(*expr)
            .eval(plan_arena, Some(&*tuple))?
            .memcomparable_encode_with_null_order(full_key, *nulls_first)?;
        if !asc {
            for byte in &mut full_key[start + 1..] {
                *byte ^= 0xFF;
            }
        }
    }

    if heap.len() < keep_count {
        heap.insert(CmpItem {
            key: std::mem::replace(full_key, BumpBytes::new_in(full_key.bump())),
            sequence,
            tuple: std::mem::take(tuple),
        });
    } else if let Some(mut cmp_item) = heap.pop_last() {
        if full_key.as_slice() < cmp_item.key.as_slice() {
            std::mem::swap(full_key, &mut cmp_item.key);
            cmp_item.sequence = sequence;
            cmp_item.tuple = std::mem::take(tuple);
        }
        heap.insert(cmp_item);
    }
    Ok(())
}

pub struct TopK<'a> {
    output: Option<std::iter::Skip<BTreeSetIntoIter<CmpItem<'static>>>>,
    arena: Box<Bump>,
    sort_fields: &'a [SortField],
    limit: usize,
    offset: Option<usize>,
    input: ExecId,
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for TopK<'a> {
    type Input = (&'a TopKOperator, &'a LogicalPlan);

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
        plan_arena: &mut (dyn MetaArena + 'a),
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let input = build_read(arena, plan_arena, input, cache, transaction);
        arena.push(ExecNode::TopK(TopK {
            output: None,
            arena: Box::<Bump>::default(),
            sort_fields,
            limit: *limit,
            offset: *offset,
            input,
        }))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for TopK<'a> {
    fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        if self.output.is_none() {
            let keep_count = self.offset.unwrap_or(0) + self.limit;
            #[allow(clippy::mutable_key_type)]
            let mut set = BTreeSet::new();

            let mut sequence = 0;
            let mut key_scratch = BumpBytes::new_in(&self.arena);
            while arena.next_tuple(self.input, plan_arena)? {
                top_sort(
                    &mut key_scratch,
                    self.sort_fields,
                    &mut set,
                    arena.result_tuple_mut(),
                    keep_count,
                    sequence,
                    plan_arena,
                )?;
                sequence += 1;
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
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::errors::DatabaseError;
    use crate::execution::dql::top_k::{top_sort, CmpItem};
    use crate::expression::ScalarExpression;
    use crate::planner::operator::sort::SortField;
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use bumpalo::Bump;
    use std::collections::BTreeSet;

    #[test]
    fn top_k_equal_keys_have_consistent_ordering() {
        let arena = Bump::new();
        let make_item = |sequence, value| {
            let mut key = crate::storage::table_codec::BumpBytes::new_in(&arena);
            key.push(1);
            CmpItem {
                key,
                sequence,
                tuple: Tuple::new(None, vec![DataValue::Int32(value)]),
            }
        };
        let first = make_item(0, 10);
        let same_key_and_sequence = make_item(0, 20);
        let second = make_item(1, 30);
        assert_eq!(first.cmp(&first), std::cmp::Ordering::Equal);
        assert_eq!(first, same_key_and_sequence);
        assert_eq!(first.cmp(&same_key_and_sequence), std::cmp::Ordering::Equal);
        assert_eq!(first.cmp(&second), std::cmp::Ordering::Less);
        assert_eq!(second.cmp(&first), std::cmp::Ordering::Greater);

        let mut set = BTreeSet::new();
        assert!(set.insert(second));
        assert!(set.insert(first));
        assert!(!set.insert(same_key_and_sequence));
        assert_eq!(set.pop_first().unwrap().sequence, 0);
        assert_eq!(set.pop_first().unwrap().sequence, 1);
    }

    #[test]
    fn test_top_k_sort() -> Result<(), DatabaseError> {
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
        let arena = Bump::new();
        let mut key_scratch = crate::storage::table_codec::BumpBytes::new_in(&arena);

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
            &mut key_scratch,
            &fn_sort_fields(true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Null]),
            2,
            0,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Int32(0)]),
            2,
            1,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Int32(1)]),
            2,
            2,
            &plan_arena,
        )?;
        fn_asc_and_nulls_first_eq(indices);

        let mut indices = BTreeSet::new();

        top_sort(
            &mut key_scratch,
            &fn_sort_fields(true, false),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Null]),
            2,
            3,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(true, false),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Int32(0)]),
            2,
            4,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(true, false),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Int32(1)]),
            2,
            5,
            &plan_arena,
        )?;
        fn_asc_and_nulls_last_eq(indices);

        let mut indices = BTreeSet::new();

        top_sort(
            &mut key_scratch,
            &fn_sort_fields(false, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Null]),
            2,
            6,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(false, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Int32(0)]),
            2,
            7,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(false, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Int32(1)]),
            2,
            8,
            &plan_arena,
        )?;
        fn_desc_and_nulls_first_eq(indices);

        let mut indices = BTreeSet::new();

        top_sort(
            &mut key_scratch,
            &fn_sort_fields(false, false),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Null]),
            2,
            9,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(false, false),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Int32(0)]),
            2,
            10,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(false, false),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Int32(1)]),
            2,
            11,
            &plan_arena,
        )?;
        fn_desc_and_nulls_last_eq(indices);

        Ok(())
    }

    #[test]
    fn test_top_k_sort_mix_values() -> Result<(), DatabaseError> {
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
        let arena = Bump::new();
        let mut key_scratch = crate::storage::table_codec::BumpBytes::new_in(&arena);

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
            &mut key_scratch,
            &fn_sort_fields(true, true, true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Null, DataValue::Null]),
            4,
            12,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(true, true, true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Int32(0), DataValue::Null]),
            4,
            13,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(true, true, true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Int32(1), DataValue::Null]),
            4,
            14,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(true, true, true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Null, DataValue::Int32(0)]),
            4,
            15,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(true, true, true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Int32(0), DataValue::Int32(0)]),
            4,
            16,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(true, true, true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Int32(1), DataValue::Int32(0)]),
            4,
            17,
            &plan_arena,
        )?;
        fn_asc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq(indices);

        let mut indices = BTreeSet::new();

        top_sort(
            &mut key_scratch,
            &fn_sort_fields(true, false, true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Null, DataValue::Null]),
            4,
            18,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(true, false, true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Int32(0), DataValue::Null]),
            4,
            19,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(true, false, true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Int32(1), DataValue::Null]),
            4,
            20,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(true, false, true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Null, DataValue::Int32(0)]),
            4,
            21,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(true, false, true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Int32(0), DataValue::Int32(0)]),
            4,
            22,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(true, false, true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Int32(1), DataValue::Int32(0)]),
            4,
            23,
            &plan_arena,
        )?;
        fn_asc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq(indices);

        let mut indices = BTreeSet::new();

        top_sort(
            &mut key_scratch,
            &fn_sort_fields(false, true, true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Null, DataValue::Null]),
            4,
            24,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(false, true, true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Int32(0), DataValue::Null]),
            4,
            25,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(false, true, true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Int32(1), DataValue::Null]),
            4,
            26,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(false, true, true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Null, DataValue::Int32(0)]),
            4,
            27,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(false, true, true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Int32(0), DataValue::Int32(0)]),
            4,
            28,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(false, true, true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Int32(1), DataValue::Int32(0)]),
            4,
            29,
            &plan_arena,
        )?;
        fn_desc_1_and_nulls_first_1_and_asc_2_and_nulls_first_2_eq(indices);

        let mut indices = BTreeSet::new();

        top_sort(
            &mut key_scratch,
            &fn_sort_fields(false, false, true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Null, DataValue::Null]),
            4,
            30,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(false, false, true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Int32(0), DataValue::Null]),
            4,
            31,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(false, false, true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Int32(1), DataValue::Null]),
            4,
            32,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(false, false, true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Null, DataValue::Int32(0)]),
            4,
            33,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(false, false, true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Int32(0), DataValue::Int32(0)]),
            4,
            34,
            &plan_arena,
        )?;
        top_sort(
            &mut key_scratch,
            &fn_sort_fields(false, false, true, true),
            &mut indices,
            &mut Tuple::new(None, vec![DataValue::Int32(1), DataValue::Int32(0)]),
            4,
            35,
            &plan_arena,
        )?;
        fn_desc_1_and_nulls_last_1_and_asc_2_and_nulls_first_2_eq(indices);

        Ok(())
    }
}
