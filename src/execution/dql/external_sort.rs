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
use crate::execution::dql::sort::compare_sort_keys;
use crate::execution::spill::{SegmentOffset, SegmentReader, SortRow, SpillReader, SpillVec};
use crate::execution::{
    build_read, ExecArena, ExecId, ExecNode, ExecutionContext, ExecutorNode, ReadExecutor,
};
use crate::planner::operator::sort::{SortField, SortOperator};
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use std::fs::File;
use std::io::BufReader;
use std::mem;

const MERGE_FAN_IN: usize = 16;

struct Run {
    first_segment: SegmentOffset,
    segment_count: usize,
}

impl Run {
    fn new(first_segment: SegmentOffset, segment_count: usize) -> Self {
        assert!(segment_count > 0, "spill run must contain a segment");
        Self {
            first_segment,
            segment_count,
        }
    }
}

pub struct ExternalSort {
    rows: Option<SpillReader<SortRow>>,
    sort_fields: Vec<SortField>,
    input: ExecId,
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for ExternalSort {
    type Input = (SortOperator, LogicalPlan);

    fn into_executor(
        (SortOperator { sort_fields }, input): Self::Input,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let input = build_read(arena, plan_arena, input, cache, transaction);
        arena.push(ExecNode::ExternalSort(ExternalSort {
            rows: None,
            sort_fields,
            input,
        }))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for ExternalSort {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        loop {
            if let Some(rows) = &mut self.rows {
                let Some(row) = rows.next().transpose()? else {
                    arena.finish();
                    return Ok(());
                };
                arena.produce_tuple(row.tuple);
                return Ok(());
            }

            // For R rows and B bytes of estimated SortRow data, sorting one run peaks at roughly
            // B + O(R * SortRow) for the stable-sort scratch space. B includes each Tuple and its
            // cached sort values, which are evaluated once here and spilled through every merge
            // pass. By default R <= 1,024 and B is about 1 MiB; one oversized row makes this a
            // soft bound.
            let sort_fields = &self.sort_fields;
            let mut rows = SpillVec::new().on_flush(move |rows| sort_segment(sort_fields, rows));
            let mut runs = Vec::new();
            while arena.next_tuple(self.input, plan_arena)? {
                let tuple = mem::take(arena.result_tuple_mut());
                if let Some(segment) = rows.push(SortRow::new(sort_fields, tuple)?)? {
                    runs.push(Run::new(segment, 1));
                }
            }
            self.rows = Some(finish_sort(rows, runs, &self.sort_fields, MERGE_FAN_IN)?);
        }
    }
}

#[inline]
fn sort_segment(sort_fields: &[SortField], rows: &mut [SortRow]) -> Result<(), DatabaseError> {
    rows.sort_by(|left, right| {
        compare_sort_keys(
            sort_fields,
            left.sort_values.iter(),
            right.sort_values.iter(),
        )
    });
    Ok(())
}

fn finish_sort<'on_flush>(
    mut spill: SpillVec<'on_flush, SortRow>,
    mut source_runs: Vec<Run>,
    sort_fields: &[SortField],
    fan_in: usize,
) -> Result<SpillReader<SortRow>, DatabaseError> {
    assert!(fan_in > 1, "sort merge fan-in must be greater than one");
    if !spill.is_spilled() {
        return Ok(spill.into_iter());
    }

    if let Some(segment) = spill.flush()? {
        source_runs.push(Run::new(segment, 1));
    }
    let mut target_runs = Vec::new();
    while source_runs.len() > 1 {
        let source = spill.into_iter();
        spill = merge_pass(
            source,
            &mut source_runs,
            &mut target_runs,
            sort_fields,
            fan_in,
        )?;
        mem::swap(&mut source_runs, &mut target_runs);
    }
    Ok(spill.into_iter())
}

struct RunCursor<'source> {
    remaining_segments: usize,
    reader: SegmentReader<'source, BufReader<File>, SortRow>,
    head: Option<SortRow>,
}

impl<'source> RunCursor<'source> {
    fn new(source: &'source SpillReader<SortRow>, run: &Run) -> Result<Self, DatabaseError> {
        let mut reader = source.open_segment_reader()?;
        reader.reset(run.first_segment)?;
        let mut cursor = Self {
            remaining_segments: run.segment_count - 1,
            reader,
            head: None,
        };
        let _ = cursor.next()?;
        Ok(cursor)
    }

    fn peek(&self) -> Option<&SortRow> {
        self.head.as_ref()
    }

    fn next(&mut self) -> Result<Option<SortRow>, DatabaseError> {
        let head = self.head.take();
        loop {
            if let Some(row) = self.reader.next() {
                self.head = Some(row?);
                return Ok(head);
            }
            if self.remaining_segments == 0 {
                return Ok(head);
            }
            if !self.reader.start_next_segment()? {
                return Err(DatabaseError::InvalidValue(
                    "spill run ended before its segment count".to_string(),
                ));
            }
            self.remaining_segments -= 1;
        }
    }
}

// One merge pass groups source runs by fan-in and produces fewer target runs:
//
// source: [R0][R1] ... [Rk-1] | [Rk][Rk+1] ...
//             \ k-way merge /          \ k-way merge /
// target:          [M0]                      [M1]
//
// Each cursor contributes one head tuple; the smallest head is appended to the target and then
// replaced from the same cursor. R/M is one logical run stored as contiguous spill segments. The
// target becomes the source of the next pass, until only one globally sorted run remains.
//
// For fan-in K, a merge keeps one output segment, K decoded SortRows, and K BufReader buffers:
// roughly 1 MiB + K * (SortRow + reader buffer). K = 16 therefore uses about 128 KiB for the
// current standard-library reader buffers. Run metadata adds one small entry per initial segment;
// no source segment is materialized in memory.
fn merge_pass<'on_flush>(
    source: SpillReader<SortRow>,
    source_runs: &mut Vec<Run>,
    target_runs: &mut Vec<Run>,
    sort_fields: &[SortField],
    fan_in: usize,
) -> Result<SpillVec<'on_flush, SortRow>, DatabaseError> {
    let mut target = SpillVec::new();
    target_runs.clear();
    target_runs.reserve(source_runs.len().div_ceil(fan_in));
    // Perf: a loser tree or binary heap would reduce head selection from O(K) to O(log K), but
    // K is deliberately small, so a linear scan keeps the merge state and update path simpler.
    let mut cursors = Vec::with_capacity(fan_in);

    for run_group in source_runs.chunks(fan_in) {
        cursors.clear();
        for run in run_group {
            cursors.push(RunCursor::new(&source, run)?);
        }
        let mut first_segment = None;
        let mut segment_count = 0;

        loop {
            let mut selected = None;
            for (index, cursor) in cursors.iter().enumerate() {
                let Some(row) = cursor.peek() else {
                    continue;
                };
                let Some(current) = selected else {
                    selected = Some(index);
                    continue;
                };
                let Some(current_row) = cursors[current].peek() else {
                    selected = Some(index);
                    continue;
                };
                if compare_sort_keys(
                    sort_fields,
                    row.sort_values.iter(),
                    current_row.sort_values.iter(),
                )
                .is_lt()
                {
                    selected = Some(index);
                }
            }
            let Some(selected) = selected else {
                break;
            };
            let Some(row) = cursors[selected].next()? else {
                return Err(DatabaseError::InvalidValue(
                    "sort merge selected an empty run".to_string(),
                ));
            };
            if let Some(segment) = target.push(row)? {
                first_segment.get_or_insert(segment);
                segment_count += 1;
            }
        }

        if let Some(segment) = target.flush()? {
            first_segment.get_or_insert(segment);
            segment_count += 1;
        }
        let Some(first_segment) = first_segment else {
            return Err(DatabaseError::InvalidValue(
                "sort merge produced an empty run".to_string(),
            ));
        };
        target_runs.push(Run::new(first_segment, segment_count));
    }
    source_runs.clear();

    Ok(target)
}

#[cfg(test)]
mod test {
    use super::{finish_sort, sort_segment, Run, MERGE_FAN_IN};
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::errors::DatabaseError;
    use crate::execution::spill::{SortRow, SpillVec};
    use crate::expression::ScalarExpression;
    use crate::planner::operator::sort::SortField;
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::cmp::Ordering;

    #[test]
    fn finish_sort_orders_in_memory_rows() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let sort_column = plan_arena.alloc_column(ColumnCatalog::new(
            String::new(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
        ));
        let sort_fields = vec![SortField {
            expr: ScalarExpression::ColumnRef {
                column: sort_column,
                position: 0,
            },
            asc: true,
            nulls_first: false,
        }];

        let mut rows = SpillVec::new()
            .limit(4, usize::MAX)
            .on_flush(|rows| sort_segment(&sort_fields, rows));
        for value in [DataValue::Int32(2), DataValue::Null, DataValue::Int32(1)] {
            let _ = rows.push(SortRow::new(&sort_fields, Tuple::new(None, vec![value]))?)?;
        }

        let values = finish_sort(rows, Vec::new(), &sort_fields, 2)?
            .map(|row| row.map(|row| row.tuple.values[0].clone()))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            values,
            vec![DataValue::Int32(1), DataValue::Int32(2), DataValue::Null,]
        );
        Ok(())
    }

    #[test]
    fn merges_spilled_runs_and_preserves_ties() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let sort_column = plan_arena.alloc_column(ColumnCatalog::new(
            String::new(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
        ));
        let sort_fields = vec![SortField {
            expr: ScalarExpression::ColumnRef {
                column: sort_column,
                position: 0,
            },
            asc: false,
            nulls_first: true,
        }];
        let values = [
            DataValue::Int32(0),
            DataValue::Null,
            DataValue::Int32(2),
            DataValue::Int32(1),
            DataValue::Int32(2),
            DataValue::Null,
            DataValue::Int32(-1),
            DataValue::Int32(3),
            DataValue::Int32(0),
        ];

        let mut rows = SpillVec::new()
            .limit(2, usize::MAX)
            .on_flush(|rows| sort_segment(&sort_fields, rows));
        let mut runs = Vec::new();
        for (sequence, value) in values.into_iter().enumerate() {
            let tuple = Tuple::new(
                Some(DataValue::Int32(sequence as i32)),
                vec![value, DataValue::Int32(sequence as i32)],
            );
            if let Some(segment) = rows.push(SortRow::new(&sort_fields, tuple)?)? {
                runs.push(Run::new(segment, 1));
            }
        }

        let tuples = finish_sort(rows, runs, &sort_fields, 2)?
            .map(|row| row.map(|row| row.tuple))
            .collect::<Result<Vec<_>, _>>()?;
        let positions = tuples
            .iter()
            .map(|tuple| tuple.values[1].clone())
            .collect::<Vec<_>>();
        assert_eq!(
            positions,
            vec![
                DataValue::Int32(1),
                DataValue::Int32(5),
                DataValue::Int32(7),
                DataValue::Int32(2),
                DataValue::Int32(4),
                DataValue::Int32(3),
                DataValue::Int32(0),
                DataValue::Int32(8),
                DataValue::Int32(6),
            ]
        );
        assert_eq!(tuples[0].pk, Some(DataValue::Int32(1)));
        assert_eq!(tuples[1].pk, Some(DataValue::Int32(5)));
        Ok(())
    }

    #[test]
    fn merges_large_dataset_across_multiple_passes() -> Result<(), DatabaseError> {
        const ROW_COUNT: usize = 20_000;

        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let key_column = plan_arena.alloc_column(ColumnCatalog::new(
            String::new(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
        ));
        let sequence_column = plan_arena.alloc_column(ColumnCatalog::new(
            String::new(),
            false,
            ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
        ));
        let sort_fields = vec![
            SortField {
                expr: ScalarExpression::ColumnRef {
                    column: key_column,
                    position: 0,
                },
                asc: false,
                nulls_first: true,
            },
            SortField {
                expr: ScalarExpression::ColumnRef {
                    column: sequence_column,
                    position: 1,
                },
                asc: true,
                nulls_first: false,
            },
        ];

        let mut rows = SpillVec::new().on_flush(|rows| sort_segment(&sort_fields, rows));
        let mut runs = Vec::new();
        for position in 0..ROW_COUNT {
            let sequence = (position * 7919) % ROW_COUNT;
            let key = if sequence % 113 == 0 {
                DataValue::Null
            } else {
                DataValue::Int32((sequence % 257) as i32 - 128)
            };
            let sequence = DataValue::Int32(sequence as i32);
            let tuple = Tuple::new(Some(sequence.clone()), vec![key, sequence]);
            if let Some(segment) = rows.push(SortRow::new(&sort_fields, tuple)?)? {
                runs.push(Run::new(segment, 1));
            }
        }
        assert!(runs.len() > MERGE_FAN_IN);

        let tuples = finish_sort(rows, runs, &sort_fields, MERGE_FAN_IN)?
            .map(|row| row.map(|row| row.tuple))
            .collect::<Result<Vec<_>, _>>()?;
        let mut expected = (0..ROW_COUNT).collect::<Vec<_>>();
        expected.sort_by(|left, right| {
            match (left % 113 == 0, right % 113 == 0) {
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
                (false, false) => (right % 257).cmp(&(left % 257)),
                (true, true) => Ordering::Equal,
            }
            .then_with(|| left.cmp(right))
        });
        let actual = tuples
            .iter()
            .map(|tuple| {
                assert_eq!(tuple.pk.as_ref(), tuple.values.get(1));
                tuple.values[1].clone()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            expected
                .into_iter()
                .map(|value| DataValue::Int32(value as i32))
                .collect::<Vec<_>>()
        );
        Ok(())
    }
}
