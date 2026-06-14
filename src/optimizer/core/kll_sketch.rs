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
use crate::types::value::DataValue;
use std::cmp::Ordering;
use std::iter::Peekable;

#[derive(Debug, Clone, PartialEq, Eq)]
struct KllSketchItem {
    value: DataValue,
    ordinal: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KllWeightedItem {
    value: DataValue,
    ordinal: usize,
    weight: usize,
}

pub struct KllSketchBuilder {
    level_capacity: usize,
    len: usize,
    levels: Vec<Vec<KllSketchItem>>,
    min_value: Option<DataValue>,
    max_value: Option<DataValue>,
    compact_next_odd: bool,
}

impl KllSketchBuilder {
    fn new(level_capacity: usize) -> Result<Self, DatabaseError> {
        if level_capacity < 2 {
            return Err(DatabaseError::InvalidValue(format!(
                "KLL sketch level capacity must be at least 2, got {level_capacity}"
            )));
        }

        Ok(Self {
            level_capacity,
            len: 0,
            levels: vec![Vec::with_capacity(level_capacity)],
            min_value: None,
            max_value: None,
            compact_next_odd: false,
        })
    }

    pub fn with_relative_error(relative_error: f64) -> Result<Self, DatabaseError> {
        if relative_error <= 0.0 || relative_error.is_nan() {
            return Err(DatabaseError::InvalidValue(format!(
                "KLL sketch relative error must be greater than zero, got {relative_error}"
            )));
        }

        Self::new(Self::capacity_for_relative_error(relative_error)?)
    }

    pub fn insert(&mut self, value: DataValue) -> Result<(), DatabaseError> {
        self.update_bounds(&value)?;
        self.levels[0].push(KllSketchItem {
            value,
            ordinal: self.len,
        });
        self.len += 1;
        self.compact_if_needed(0)
    }

    pub fn build(self) -> Result<KllSketch, DatabaseError> {
        let Self {
            len,
            levels,
            min_value,
            max_value,
            ..
        } = self;
        let retained_len = levels.iter().map(Vec::len).sum();
        let mut items = Vec::with_capacity(retained_len);

        for (level_idx, level) in levels.into_iter().enumerate() {
            let weight = 1usize << level_idx;
            items.extend(level.into_iter().map(|item| KllWeightedItem {
                value: item.value,
                ordinal: item.ordinal,
                weight,
            }));
        }
        items.sort_by(|left, right| compare_values(&left.value, &right.value));

        Ok(KllSketch {
            len,
            items,
            min_value,
            max_value,
        })
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.len
    }

    #[cfg(test)]
    fn levels_len(&self) -> usize {
        self.levels.len()
    }

    #[cfg(test)]
    fn retained_len(&self) -> usize {
        self.levels.iter().map(Vec::len).sum()
    }

    fn update_bounds(&mut self, value: &DataValue) -> Result<(), DatabaseError> {
        if let Some(min_value) = &self.min_value {
            if compare_values(value, min_value) == Ordering::Less {
                self.min_value = Some(value.clone());
            }
        } else {
            self.min_value = Some(value.clone());
        }

        if let Some(max_value) = &self.max_value {
            if compare_values(value, max_value) == Ordering::Greater {
                self.max_value = Some(value.clone());
            }
        } else {
            self.max_value = Some(value.clone());
        }
        Ok(())
    }

    fn compact_if_needed(&mut self, level_idx: usize) -> Result<(), DatabaseError> {
        if self.levels[level_idx].len() <= self.level_capacity {
            return Ok(());
        }

        self.compact(level_idx)?;
        self.compact_if_needed(level_idx + 1)
    }

    fn compact(&mut self, level_idx: usize) -> Result<(), DatabaseError> {
        let level = &mut self.levels[level_idx];
        level.sort_by(|left, right| compare_values(&left.value, &right.value));

        let keep_odd = self.compact_next_odd;
        self.compact_next_odd = !self.compact_next_odd;

        let mut promoted = Vec::with_capacity(level.len().div_ceil(2));
        let mut retained = Vec::new();
        if level.len() % 2 == 1 {
            retained.push(level.pop().expect("odd level has a tail item"));
        }

        for (idx, item) in level.drain(..).enumerate() {
            if idx % 2 == usize::from(keep_odd) {
                promoted.push(item);
            }
        }
        *level = retained;

        if self.levels.len() == level_idx + 1 {
            self.levels.push(Vec::with_capacity(self.level_capacity));
        }
        self.levels[level_idx + 1].extend(promoted);
        Ok(())
    }

    fn capacity_for_relative_error(relative_error: f64) -> Result<usize, DatabaseError> {
        let capacity = (2.0 / relative_error).ceil();
        if !capacity.is_finite() || capacity > usize::MAX as f64 {
            return Err(DatabaseError::InvalidValue(format!(
                "KLL sketch relative error is too small: {relative_error}"
            )));
        }

        Ok(capacity as usize)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KllSketch {
    len: usize,
    items: Vec<KllWeightedItem>,
    min_value: Option<DataValue>,
    max_value: Option<DataValue>,
}

impl KllSketch {
    pub fn correlation(&self) -> f64 {
        if self.len == 0 {
            return 0.0;
        }

        let mut corr_xy_sum = 0.0;
        let mut rank = 0usize;
        for item in self.items.iter() {
            let next_rank = rank + item.weight;
            let rank_sum = (rank + next_rank - 1) as f64 * item.weight as f64 / 2.0;
            corr_xy_sum += rank_sum * item.ordinal as f64;
            rank = next_rank;
        }

        Self::calc_correlation(corr_xy_sum, self.len)
    }

    pub fn values_at_ranks<I>(self, ranks: I) -> impl Iterator<Item = Option<DataValue>>
    where
        I: IntoIterator<Item = usize>,
    {
        let Self {
            len,
            items,
            min_value,
            max_value,
        } = self;

        KllValuesAtRanks {
            len,
            ranks: ranks.into_iter().peekable(),
            items: items.into_iter(),
            current: None,
            accumulated: 0,
            last_rank: None,
            min_value,
            max_value,
        }
    }

    #[allow(dead_code)]
    fn len(&self) -> usize {
        self.len
    }

    // https://github.com/pingcap/tidb/blob/6957170f1147e96958e63db48148445a7670328e/pkg/statistics/builder.go#L210
    fn calc_correlation(corr_xy_sum: f64, values_len: usize) -> f64 {
        if values_len == 1 {
            return 1.0;
        }
        let item_count = values_len as f64;
        let corr_x_sum = (item_count - 1.0) * item_count / 2.0;
        let corr_x2_sum = (item_count - 1.0) * item_count * (2.0 * item_count - 1.0) / 6.0;
        (item_count * corr_xy_sum - corr_x_sum * corr_x_sum)
            / (item_count * corr_x2_sum - corr_x_sum * corr_x_sum)
    }
}

struct KllValuesAtRanks<I: Iterator<Item = usize>> {
    len: usize,
    ranks: Peekable<I>,
    items: std::vec::IntoIter<KllWeightedItem>,
    current: Option<KllWeightedItem>,
    accumulated: usize,
    last_rank: Option<usize>,
    min_value: Option<DataValue>,
    max_value: Option<DataValue>,
}

impl<I: Iterator<Item = usize>> Iterator for KllValuesAtRanks<I> {
    type Item = Option<DataValue>;

    fn next(&mut self) -> Option<Self::Item> {
        let rank = self.ranks.next()?;
        debug_assert!(
            self.last_rank.is_none_or(|last_rank| rank >= last_rank),
            "KLL ranks must be sorted"
        );
        self.last_rank = Some(rank);
        if self.len == 0 {
            return Some(None);
        }

        let ordinal = rank.saturating_add(1);
        let next_ordinal = self.ranks.peek().map(|rank| rank.saturating_add(1));
        if ordinal == 1 {
            return Some(Self::emit_value(&mut self.min_value, next_ordinal, 1));
        }
        if ordinal >= self.len {
            return Some(Self::emit_value(
                &mut self.max_value,
                next_ordinal,
                usize::MAX,
            ));
        }

        while self.accumulated < ordinal {
            if let Some(item) = self.items.next() {
                self.accumulated += item.weight;
                self.current = Some(item);
            } else {
                self.current = None;
                break;
            }
        }

        Some(if self.current.is_some() && self.accumulated >= ordinal {
            let upper_ordinal = self.accumulated.min(self.len - 1);
            self.emit_current(next_ordinal, upper_ordinal)
        } else {
            None
        })
    }
}

impl<I: Iterator<Item = usize>> KllValuesAtRanks<I> {
    fn emit_value(
        value: &mut Option<DataValue>,
        next_ordinal: Option<usize>,
        upper_ordinal: usize,
    ) -> Option<DataValue> {
        if next_ordinal.is_some_and(|next_ordinal| next_ordinal <= upper_ordinal) {
            value.clone()
        } else {
            value.take()
        }
    }

    fn emit_current(
        &mut self,
        next_ordinal: Option<usize>,
        upper_ordinal: usize,
    ) -> Option<DataValue> {
        if next_ordinal.is_some_and(|next_ordinal| next_ordinal <= upper_ordinal) {
            self.current.as_ref().map(|item| item.value.clone())
        } else {
            self.current.take().map(|item| item.value)
        }
    }
}

#[inline]
fn compare_values(left: &DataValue, right: &DataValue) -> Ordering {
    left.partial_cmp(right)
        .expect("KLL sketch values must be mutually comparable")
}

#[cfg(test)]
mod tests {
    use super::KllSketchBuilder;
    use crate::errors::DatabaseError;
    use crate::types::value::DataValue;

    fn build_builder(level_capacity: usize, len: i32) -> Result<KllSketchBuilder, DatabaseError> {
        let mut builder = KllSketchBuilder::new(level_capacity)?;
        for value in 0..len {
            builder.insert(DataValue::Int32(value))?;
        }
        Ok(builder)
    }

    #[test]
    fn kll_sketch_keeps_bounded_retained_items() -> Result<(), DatabaseError> {
        let builder = build_builder(32, 10_000)?;

        assert_eq!(builder.len(), 10_000);
        assert!(builder.levels_len() > 1);
        assert!(builder.retained_len() < 32 * 16);

        Ok(())
    }

    #[test]
    fn kll_sketch_estimates_quantiles() -> Result<(), DatabaseError> {
        let sketch = build_builder(256, 10_000)?.build()?;
        let values = sketch
            .values_at_ranks([0, 4_999, 8_999, 9_999])
            .collect::<Vec<_>>();

        assert_eq!(values[0], Some(DataValue::Int32(0)));
        assert_eq!(values[3], Some(DataValue::Int32(9999)));

        let p50 = match values[1].clone().unwrap() {
            DataValue::Int32(value) => value,
            value => panic!("unexpected value: {value:?}"),
        };
        let p90 = match values[2].clone().unwrap() {
            DataValue::Int32(value) => value,
            value => panic!("unexpected value: {value:?}"),
        };

        assert!((4_500..=5_500).contains(&p50), "p50={p50}");
        assert!((8_500..=9_500).contains(&p90), "p90={p90}");

        Ok(())
    }

    #[test]
    fn kll_sketch_handles_reversed_input() -> Result<(), DatabaseError> {
        let mut builder = KllSketchBuilder::new(64)?;
        for value in (0..1_000).rev() {
            builder.insert(DataValue::Int32(value))?;
        }
        let sketch = builder.build()?;

        let quantiles = sketch
            .values_at_ranks([0, 249, 499, 749, 999])
            .map(|value| value.expect("non-empty sketch"))
            .collect::<Vec<_>>();
        assert_eq!(quantiles.first(), Some(&DataValue::Int32(0)));
        assert_eq!(quantiles.last(), Some(&DataValue::Int32(999)));
        assert_eq!(quantiles.len(), 5);

        Ok(())
    }

    #[test]
    fn kll_sketch_correlation_tracks_original_order() -> Result<(), DatabaseError> {
        let ascending = build_builder(64, 15)?.build()?;
        assert_eq!(ascending.correlation(), 1.0);

        let mut descending = KllSketchBuilder::new(64)?;
        for value in (0..15).rev() {
            descending.insert(DataValue::Int32(value))?;
        }
        assert_eq!(descending.build()?.correlation(), -1.0);

        let mut mixed = KllSketchBuilder::new(64)?;
        for value in [14, 13, 12, 11, 10, 4, 3, 2, 1, 0, 9, 8, 7, 6, 5] {
            mixed.insert(DataValue::Int32(value))?;
        }
        assert!(mixed.build()?.correlation() < 0.0);

        Ok(())
    }

    #[test]
    fn kll_sketch_values_at_ranks_accepts_sorted_ranks() -> Result<(), DatabaseError> {
        let sketch = build_builder(64, 1_000)?.build()?;
        let values = sketch.values_at_ranks([0, 499, 999]).collect::<Vec<_>>();

        assert_eq!(values[0], Some(DataValue::Int32(0)));
        assert_eq!(values[2], Some(DataValue::Int32(999)));
        let mid = match values[1].clone().unwrap() {
            DataValue::Int32(value) => value,
            value => panic!("unexpected value: {value:?}"),
        };
        assert!((400..=600).contains(&mid), "mid={mid}");

        Ok(())
    }
}
