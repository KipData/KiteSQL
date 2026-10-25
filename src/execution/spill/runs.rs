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

use super::{FlushMode, SegmentOffset, SegmentReader, SpillCodec, SpillReader, SpillVec};
use crate::errors::DatabaseError;
use crate::execution::runs::{lower_bound, Runs, RunsBuilder, RunsCompare, RunsCursor};
use crate::types::value::DataValue;
use std::cmp::Ordering;
use std::fs::File;
use std::io::BufReader;
use std::mem::size_of;

pub(crate) struct SpillRuns {
    runs: Vec<SegmentOffset>,
    spill: SpillVec<'static, DataValue>,
}

/// Disk layout, entirely encoded as ordinary DataValues:
/// UInt32(N), N UInt32 offsets relative to the records area, N original values.
/// Extend and rotate the existing allocation; no scratch encoding or heap Vec.
fn prepare_disk_run(values: &mut Vec<DataValue>) -> Result<(), DatabaseError> {
    let rows = values.len();
    let count = u32::try_from(rows)?;
    let slots = rows
        .checked_add(1)
        .ok_or_else(|| DatabaseError::InvalidValue("run length overflow".into()))?;
    let len = rows
        .checked_add(slots)
        .ok_or_else(|| DatabaseError::InvalidValue("run length overflow".into()))?;
    values.resize(len, DataValue::Null);
    values.rotate_right(slots);
    values[0] = DataValue::UInt32(count);
    let mut offset = 0u32;
    for index in 0..rows {
        let bytes = u32::try_from(values[slots + index].encoded_len()?)?;
        values[index + 1] = DataValue::UInt32(offset);
        offset = offset.checked_add(bytes).ok_or_else(|| {
            DatabaseError::InvalidValue("run payload exceeds UInt32 offsets".into())
        })?;
    }
    Ok(())
}

impl SpillRuns {
    /// Ends the current run early; later values start a new one.
    #[cfg(test)]
    pub(crate) fn flush(&mut self) -> Result<(), DatabaseError> {
        if let Some(offset) = self.spill.flush()? {
            self.runs.push(offset);
        }
        Ok(())
    }
}

/// Every SpillVec flush sorts its bounded segment into one run.
impl RunsBuilder for SpillRuns {
    type Runs = SpillRunsReader;

    fn new(compare: RunsCompare) -> Self {
        Self {
            runs: Vec::new(),
            spill: SpillVec::new().on_flush(move |mode, values| {
                values.sort_by(compare);
                match mode {
                    FlushMode::Disk => prepare_disk_run(values),
                    FlushMode::Memory => Ok(()),
                }
            }),
        }
    }

    fn push(&mut self, value: DataValue) -> Result<(), DatabaseError> {
        if let Some(offset) = self.spill.push(value)? {
            self.runs.push(offset);
        }
        Ok(())
    }

    fn finish(self) -> Result<SpillRunsReader, DatabaseError> {
        Ok(SpillRunsReader {
            runs: self.runs,
            spill: self.spill.into_iter(),
        })
    }
}

pub(crate) struct SpillRunsReader {
    runs: Vec<SegmentOffset>,
    spill: SpillReader<DataValue>,
}

impl Runs for SpillRunsReader {
    type Cursor<'a> = SpillRunsCursor<'a>;

    fn cursor(&self, fixed_width: Option<usize>) -> Result<SpillRunsCursor<'_>, DatabaseError> {
        let memory = self.spill.memory_tail();
        if self.runs.is_empty() {
            return Ok(SpillRunsCursor::Memory(memory));
        }
        let directory_width = DataValue::UInt32(0).encoded_len()?;
        let reader = self
            .spill
            .open_segment_reader(fixed_width.map(|width| width.max(directory_width)))?;
        Ok(SpillRunsCursor::Spilled {
            runs: &self.runs,
            memory,
            reader,
        })
    }
}

pub(crate) enum SpillRunsCursor<'a> {
    Memory(&'a [DataValue]),
    Spilled {
        runs: &'a [SegmentOffset],
        memory: &'a [DataValue],
        reader: SegmentReader<'a, BufReader<File>, DataValue>,
    },
}

fn disk_value(
    reader: &mut SegmentReader<'_, BufReader<File>, DataValue>,
    index: usize,
    rows: usize,
    directory: u64,
) -> Result<DataValue, DatabaseError> {
    let width = DataValue::UInt32(0).encoded_len()? as u64;
    let DataValue::UInt32(offset) = reader.read_at(directory + index as u64 * width)? else {
        return Err(DatabaseError::InvalidValue("invalid run offset".into()));
    };
    // TODO: accept single-BufReader directory/record jumps for now. Profile
    // before caching the directory or introducing a second reader.
    reader.read_at(directory + rows as u64 * width + u64::from(offset))
}

fn disk_layout(
    reader: &mut SegmentReader<'_, BufReader<File>, DataValue>,
    offset: SegmentOffset,
) -> Result<(usize, u64), DatabaseError> {
    let offset = offset + size_of::<u64>() as u64;
    let DataValue::UInt32(rows) = reader.read_at(offset)? else {
        return Err(DatabaseError::InvalidValue("invalid run row count".into()));
    };
    Ok((
        rows as usize,
        offset + DataValue::UInt32(0).encoded_len()? as u64,
    ))
}

impl RunsCursor for SpillRunsCursor<'_> {
    fn run_count(&self) -> usize {
        match self {
            Self::Memory(memory) => memory.run_count(),
            Self::Spilled { runs, memory, .. } => runs.len() + memory.run_count(),
        }
    }

    fn lower_bound_by(
        &mut self,
        run: usize,
        mut compare: impl FnMut(&DataValue) -> Result<Ordering, DatabaseError>,
    ) -> Result<usize, DatabaseError> {
        match self {
            Self::Memory(memory) => memory.lower_bound_by(run, compare),
            Self::Spilled { runs, memory, .. } if run == runs.len() => {
                memory.lower_bound_by(0, compare)
            }
            Self::Spilled { runs, reader, .. } => {
                let (rows, directory) = disk_layout(reader, runs[run])?;
                lower_bound(rows, |index| {
                    compare(&disk_value(reader, index, rows, directory)?)
                })
            }
        }
    }

    fn visit_run<B>(
        &mut self,
        run: usize,
        start: usize,
        mut visit: impl FnMut(&DataValue) -> Result<Option<B>, DatabaseError>,
    ) -> Result<Option<B>, DatabaseError> {
        match self {
            Self::Memory(memory) => memory.visit_run(run, start, visit),
            Self::Spilled { runs, memory, .. } if run == runs.len() => {
                memory.visit_run(0, start, visit)
            }
            Self::Spilled { runs, reader, .. } => {
                let (rows, directory) = disk_layout(reader, runs[run])?;
                for index in start..rows {
                    if let Some(result) = visit(&disk_value(reader, index, rows, directory)?)? {
                        return Ok(Some(result));
                    }
                }
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compare(left: &DataValue, right: &DataValue) -> Ordering {
        match (left, right) {
            (DataValue::Null, DataValue::Null) => Ordering::Equal,
            (DataValue::Null, _) => Ordering::Less,
            (_, DataValue::Null) => Ordering::Greater,
            _ => left.partial_cmp(right).unwrap(),
        }
    }

    fn descending(left: &DataValue, right: &DataValue) -> Ordering {
        compare(right, left)
    }

    fn runs_of(
        compare: RunsCompare,
        batches: impl IntoIterator<Item = Vec<DataValue>>,
    ) -> Result<SpillRuns, DatabaseError> {
        let mut runs = SpillRuns::new(compare);
        for batch in batches {
            runs.flush()?;
            for value in batch {
                runs.push(value)?;
            }
        }
        Ok(runs)
    }

    #[test]
    fn original_memory_and_disk_values_match() -> Result<(), DatabaseError> {
        for values in [
            vec![
                DataValue::Int64(3),
                DataValue::Null,
                DataValue::Int64(-2),
                DataValue::Int64(3),
            ],
            vec![
                DataValue::new_utf8("中文".repeat(100)),
                DataValue::Null,
                DataValue::new_utf8("".into()),
                DataValue::new_utf8("abc".into()),
            ],
            vec![
                DataValue::Tuple(vec![DataValue::Int64(2), DataValue::Null]),
                DataValue::Null,
                DataValue::Tuple(vec![DataValue::Int64(1), DataValue::Null]),
            ],
            vec![DataValue::Null; 9],
        ] {
            let mut expected = values.clone();
            expected.sort_by(compare);
            for flushed in [false, true] {
                let mut runs = runs_of(compare, [values.clone()])?;
                if flushed {
                    runs.flush()?;
                }
                let source = runs.finish()?;
                if !flushed {
                    assert_eq!(source.spill.memory_tail(), expected);
                }
                for _ in 0..2 {
                    let mut cursor = source.cursor(None)?;
                    let mut actual = Vec::new();
                    cursor.visit_run::<()>(0, 0, |value| {
                        actual.push(value.clone());
                        Ok(None)
                    })?;
                    assert_eq!(actual, expected);
                    for target in &expected {
                        assert!(cursor.contains_by(|value| Ok(compare(value, target)))?);
                    }
                    assert_eq!(
                        cursor.lower_bound_by(0, |_| Ok(Ordering::Less))?,
                        expected.len()
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn disk_runs_and_original_memory_tail() -> Result<(), DatabaseError> {
        let source = runs_of(
            compare,
            [
                vec![DataValue::Int64(4), DataValue::Int64(2)],
                vec![DataValue::Int64(7), DataValue::Int64(1)],
                vec![DataValue::Int64(9)],
            ],
        )?
        .finish()?;
        let mut cursor = source.cursor(Some(9))?;
        assert_eq!(cursor.run_count(), 3);
        assert!(
            matches!(&cursor, SpillRunsCursor::Spilled { runs, memory, .. } if runs.len() == 2 && memory.len() == 1)
        );
        assert_eq!(source.spill.memory_tail(), &[DataValue::Int64(9)]);
        for target in 0..11 {
            assert_eq!(
                cursor.contains_by(|value| Ok(compare(value, &DataValue::Int64(target))))?,
                [1, 2, 4, 7, 9].contains(&target)
            );
        }
        let mut other = source.cursor(Some(9))?;
        assert_eq!(
            other.lower_bound_by(1, |value| Ok(compare(value, &DataValue::Int64(6))))?,
            1
        );
        assert!(cursor.contains_by(|value| Ok(compare(value, &DataValue::Int64(2))))?);
        Ok(())
    }

    #[test]
    fn automatic_spill_and_928_memory_keys() -> Result<(), DatabaseError> {
        let source = runs_of(
            compare,
            [(0..super::super::DEFAULT_MAX_ROWS + 1)
                .rev()
                .map(|value| DataValue::Int64(value as i64))
                .collect()],
        )?
        .finish()?;
        let mut cursor = source.cursor(Some(9))?;
        assert_eq!(cursor.run_count(), 2);
        for target in [0, 1, 1023, 1024] {
            assert!(cursor.contains_by(|value| Ok(compare(value, &DataValue::Int64(target))))?);
        }
        let runs = runs_of(compare, [(0..928).rev().map(DataValue::UInt32).collect()])?;
        assert!(!runs.spill.is_spilled());
        let source = runs.finish()?;
        let mut cursor = source.cursor(Some(5))?;
        assert!(matches!(&cursor, SpillRunsCursor::Memory(_)));
        let mut comparisons = 0;
        assert!(cursor.contains_by(|value| {
            comparisons += 1;
            Ok(compare(value, &DataValue::UInt32(900)))
        })?);
        assert!(comparisons <= 12);
        assert_eq!(
            SpillRuns::new(compare).finish()?.cursor(None)?.run_count(),
            0
        );
        Ok(())
    }

    #[test]
    fn cursor_uses_one_fixed_record_capped_at_default() -> Result<(), DatabaseError> {
        let default = super::super::DEFAULT_READ_BUFFER_SIZE;
        for (value, expected) in [
            (DataValue::Boolean(true), 5),
            (DataValue::Int64(42), 9),
            (DataValue::Tuple(vec![DataValue::Int32(1); 5]), 30),
            (DataValue::Tuple(vec![DataValue::Int64(1); 5]), 50),
        ] {
            let width = value.encoded_len()?;
            let mut runs = runs_of(
                compare,
                [vec![value.clone(), DataValue::Null, value.clone()]],
            )?;
            runs.flush()?;
            let source = runs.finish()?;
            let mut cursor = source.cursor(Some(width))?;
            let SpillRunsCursor::Spilled { reader, .. } = &cursor else {
                panic!("expected disk cursor");
            };
            assert_eq!(reader.reader.capacity(), expected);
            assert!(cursor.contains_by(|key| Ok(compare(key, &value)))?);
            assert!(cursor.contains_by(|key| Ok(compare(key, &DataValue::Null)))?);
            for (width, expected) in [(None, default), (Some(0), 5), (Some(default + 1), default)] {
                let cursor = source.cursor(width)?;
                let SpillRunsCursor::Spilled { reader, .. } = &cursor else {
                    panic!("expected disk cursor");
                };
                assert_eq!(reader.reader.capacity(), expected);
            }
        }
        Ok(())
    }

    #[test]
    fn directory_offsets_match_original_encoding() -> Result<(), DatabaseError> {
        let original = vec![
            DataValue::Null,
            DataValue::Int8(1),
            DataValue::Int64(9),
            DataValue::new_utf8("中文".into()),
            DataValue::Tuple(vec![DataValue::Null, DataValue::UInt32(4)]),
        ];
        let mut values = original.clone();
        prepare_disk_run(&mut values)?;
        assert_eq!(values.len(), original.len() * 2 + 1);
        assert_eq!(values[0], DataValue::UInt32(original.len() as u32));
        assert_eq!(&values[original.len() + 1..], original);
        let mut offset = 0;
        for (index, value) in original.iter().enumerate() {
            assert_eq!(values[index + 1], DataValue::UInt32(offset));
            let mut bytes = Vec::new();
            value.encode(&mut bytes)?;
            offset += bytes.len() as u32;
        }
        let mut empty = Vec::new();
        prepare_disk_run(&mut empty)?;
        assert_eq!(empty, [DataValue::UInt32(0)]);
        Ok(())
    }

    #[test]
    fn null_last_and_descending_runs() -> Result<(), DatabaseError> {
        for flushed in [false, true] {
            let mut runs = runs_of(
                descending,
                [vec![
                    DataValue::Int64(1),
                    DataValue::Null,
                    DataValue::Int64(5),
                ]],
            )?;
            if flushed {
                runs.flush()?;
            }
            let source = runs.finish()?;
            let mut cursor = source.cursor(Some(9))?;
            let mut actual = Vec::new();
            cursor.visit_run::<()>(0, 0, |value| {
                actual.push(value.clone());
                Ok(None)
            })?;
            assert_eq!(
                actual,
                [DataValue::Int64(5), DataValue::Int64(1), DataValue::Null]
            );
            for target in [DataValue::Int64(1), DataValue::Null, DataValue::Int64(5)] {
                assert!(cursor.contains_by(|value| Ok(compare(&target, value)))?);
            }
            assert!(!cursor.contains_by(|value| Ok(compare(&DataValue::Int64(3), value)))?);
        }
        Ok(())
    }

    #[test]
    fn directory_entries_stay_outside_search_range() -> Result<(), DatabaseError> {
        let mut runs = runs_of(
            compare,
            [vec![
                DataValue::new_utf8("a".into()),
                DataValue::new_utf8("c".into()),
            ]],
        )?;
        runs.flush()?;
        let source = runs.finish()?;
        let mut cursor = source.cursor(None)?;
        assert_eq!(cursor.lower_bound_by(0, |_| Ok(Ordering::Less))?, 2);
        assert!(!cursor.contains_by(|value| Ok(compare(value, &DataValue::new_utf8("z".into()))))?);
        Ok(())
    }

    #[test]
    fn spill_limits_cut_sorted_runs() -> Result<(), DatabaseError> {
        let max_rows = super::super::DEFAULT_MAX_ROWS;
        let rows = max_rows * 2 + 3;
        let mut runs = SpillRuns::new(compare);
        for value in (0..rows as i64).rev() {
            runs.push(DataValue::Int64(value))?;
        }
        let source = runs.finish()?;
        let mut cursor = source.cursor(Some(9))?;
        // Two full disk runs plus the sorted memory tail.
        assert_eq!(cursor.run_count(), 3);
        for run in 0..cursor.run_count() {
            let mut previous: Option<DataValue> = None;
            cursor.visit_run::<()>(run, 0, |value| {
                if let Some(previous) = &previous {
                    assert_eq!(compare(previous, value), Ordering::Less);
                }
                previous = Some(value.clone());
                Ok(None)
            })?;
        }
        for target in [0, 2, max_rows as i64, rows as i64 - 1] {
            assert!(cursor.contains_by(|value| Ok(compare(value, &DataValue::Int64(target))))?);
        }
        assert!(!cursor.contains_by(|value| Ok(compare(value, &DataValue::Int64(-1))))?);
        Ok(())
    }
}
