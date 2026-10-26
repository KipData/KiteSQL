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

//! Sorted runs of DataValues, shared by the in-memory `Vec` and spilled implementations.

use crate::errors::DatabaseError;
use crate::types::value::DataValue;
use std::cmp::Ordering;

pub(crate) type RunsCompare = fn(&DataValue, &DataValue) -> Ordering;

/// Accepts unsorted values; `finish` yields runs each sorted by `compare`.
pub(crate) trait RunsBuilder: Sized {
    type Runs: Runs;

    fn new(compare: RunsCompare) -> Self;

    fn push(&mut self, value: DataValue) -> Result<(), DatabaseError>;

    fn finish(self) -> Result<Self::Runs, DatabaseError>;
}

pub(crate) trait Runs {
    type Cursor<'a>: RunsCursor
    where
        Self: 'a;

    /// `fixed_width` is the encoded width shared by every value, sizing spilled reads.
    fn cursor(&self, fixed_width: Option<usize>) -> Result<Self::Cursor<'_>, DatabaseError>;
}

pub(crate) trait RunsCursor {
    fn run_count(&self) -> usize;

    /// First index in `run` whose value does not compare `Less` against the target.
    fn lower_bound_by(
        &mut self,
        run: usize,
        compare: impl FnMut(&DataValue) -> Result<Ordering, DatabaseError>,
    ) -> Result<usize, DatabaseError>;

    /// Visits `run` from `start`, stopping at the first `Some` result.
    fn visit_run<B>(
        &mut self,
        run: usize,
        start: usize,
        visit: impl FnMut(&DataValue) -> Result<Option<B>, DatabaseError>,
    ) -> Result<Option<B>, DatabaseError>;

    fn contains_by(
        &mut self,
        mut compare: impl FnMut(&DataValue) -> Result<Ordering, DatabaseError>,
    ) -> Result<bool, DatabaseError> {
        for run in 0..self.run_count() {
            let index = self.lower_bound_by(run, &mut compare)?;
            if self
                .visit_run(run, index, |value| {
                    Ok(Some(compare(value)? == Ordering::Equal))
                })?
                .unwrap_or(false)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Visits every value of every run, stopping at the first `Some` result.
    fn visit_all<B>(
        &mut self,
        mut visit: impl FnMut(&DataValue) -> Result<Option<B>, DatabaseError>,
    ) -> Result<Option<B>, DatabaseError> {
        for run in 0..self.run_count() {
            if let Some(result) = self.visit_run(run, 0, &mut visit)? {
                return Ok(Some(result));
            }
        }
        Ok(None)
    }
}

pub(crate) fn lower_bound(
    rows: usize,
    mut compare_at: impl FnMut(usize) -> Result<Ordering, DatabaseError>,
) -> Result<usize, DatabaseError> {
    let (mut low, mut high) = (0, rows);
    while low < high {
        let mid = low + (high - low) / 2;
        if compare_at(mid)? == Ordering::Less {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    Ok(low)
}

/// Builds a single in-memory run, sorted once by `finish`.
/// Spill builds use `SpillRuns` instead.
#[cfg(any(test, not(feature = "spill")))]
pub(crate) struct SortedVec {
    values: Vec<DataValue>,
    compare: RunsCompare,
}

#[cfg(any(test, not(feature = "spill")))]
impl RunsBuilder for SortedVec {
    type Runs = Vec<DataValue>;

    fn new(compare: RunsCompare) -> Self {
        Self {
            values: Vec::new(),
            compare,
        }
    }

    fn push(&mut self, value: DataValue) -> Result<(), DatabaseError> {
        self.values.push(value);
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<DataValue>, DatabaseError> {
        self.values.sort_by(self.compare);
        Ok(self.values)
    }
}

impl Runs for Vec<DataValue> {
    type Cursor<'a> = &'a [DataValue];

    fn cursor(&self, _: Option<usize>) -> Result<Self::Cursor<'_>, DatabaseError> {
        Ok(self.as_slice())
    }
}

/// A sorted slice is one run; an empty slice has none.
impl RunsCursor for &[DataValue] {
    fn run_count(&self) -> usize {
        usize::from(!self.is_empty())
    }

    fn lower_bound_by(
        &mut self,
        _: usize,
        mut compare: impl FnMut(&DataValue) -> Result<Ordering, DatabaseError>,
    ) -> Result<usize, DatabaseError> {
        lower_bound(self.len(), |index| compare(&self[index]))
    }

    fn visit_run<B>(
        &mut self,
        _: usize,
        start: usize,
        mut visit: impl FnMut(&DataValue) -> Result<Option<B>, DatabaseError>,
    ) -> Result<Option<B>, DatabaseError> {
        for value in self.iter().skip(start) {
            if let Some(result) = visit(value)? {
                return Ok(Some(result));
            }
        }
        Ok(None)
    }
}

// GRCOV_EXCL_START
#[cfg(test)]
mod tests {
    use super::*;

    fn compare(left: &DataValue, right: &DataValue) -> Ordering {
        left.partial_cmp(right).unwrap()
    }

    #[test]
    fn vec_runs_sort_once_and_search() -> Result<(), DatabaseError> {
        let mut builder = SortedVec::new(compare);
        for value in [5, 1, 3, 1] {
            builder.push(DataValue::Int32(value))?;
        }
        let runs = builder.finish()?;
        let mut cursor = runs.cursor(None)?;
        assert_eq!(cursor.run_count(), 1);
        for target in 0..7 {
            assert_eq!(
                cursor.contains_by(|value| Ok(compare(value, &DataValue::Int32(target))))?,
                [1, 3, 5].contains(&target)
            );
        }
        let mut visited = Vec::new();
        cursor.visit_all::<()>(|value| {
            visited.push(value.clone());
            Ok(None)
        })?;
        assert_eq!(visited, [1, 1, 3, 5].map(DataValue::Int32).to_vec());
        let empty = SortedVec::new(compare).finish()?;
        let mut cursor = empty.cursor(None)?;
        assert_eq!(cursor.run_count(), 0);
        assert!(!cursor.contains_by(|_| Ok(Ordering::Equal))?);
        Ok(())
    }
}
// GRCOV_EXCL_STOP
