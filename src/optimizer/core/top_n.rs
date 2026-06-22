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

use crate::types::value::DataValue;
use kite_sql_serde_macros::ReferenceSerialization;
use std::cmp::Ordering;

pub(crate) const ANALYZE_STATISTICS_TOP_N_SIZE: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, ReferenceSerialization)]
pub struct ColumnTopNEntry {
    value: DataValue,
    count: usize,
    error: usize,
}

impl ColumnTopNEntry {
    pub fn value(&self) -> &DataValue {
        &self.value
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn error(&self) -> usize {
        self.error
    }
}

#[derive(Debug, Clone, Default, ReferenceSerialization)]
pub struct ColumnTopN {
    values: Vec<ColumnTopNEntry>,
    min_index: Option<usize>,
}

impl PartialEq for ColumnTopN {
    fn eq(&self, other: &Self) -> bool {
        self.values == other.values
    }
}

impl Eq for ColumnTopN {}

impl ColumnTopN {
    pub fn get(&self, value: &DataValue) -> Option<usize> {
        self.get_entry(value).map(ColumnTopNEntry::count)
    }

    pub fn get_entry(&self, value: &DataValue) -> Option<&ColumnTopNEntry> {
        if value.is_null() {
            return None;
        }

        self.find(value).ok().and_then(|idx| self.values.get(idx))
    }

    pub fn add_with_size(&mut self, top_n_size: usize, value: DataValue, count: usize) {
        self.add_with_options(top_n_size, value, count, 0);
    }

    pub fn finish_with_size(mut self, top_n_size: usize) -> Self {
        self.prune_to_capacity(top_n_size);
        self
    }

    pub fn values(&self) -> &[ColumnTopNEntry] {
        &self.values
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    fn add_with_options(&mut self, capacity: usize, value: DataValue, count: usize, error: usize) {
        if self.should_insert(&value, count, error) {
            self.insert_new_with_options(
                capacity,
                ColumnTopNEntry {
                    value,
                    count,
                    error,
                },
            );
        }
    }

    fn find(&self, value: &DataValue) -> Result<usize, usize> {
        self.values
            .binary_search_by(|entry| entry.value.partial_cmp(value).unwrap_or(Ordering::Equal))
    }

    fn should_insert(&mut self, value: &DataValue, count: usize, error: usize) -> bool {
        if count == 0 || value.is_null() {
            return false;
        }
        if let Ok(index) = self.find(value) {
            self.update_existing(index, count, error);
            return false;
        }
        true
    }

    fn update_existing(&mut self, index: usize, count: usize, error: usize) {
        let was_min = self.min_index == Some(index);
        let entry = &mut self.values[index];
        entry.count = entry.count.saturating_add(count);
        entry.error = entry.error.saturating_add(error);
        if was_min {
            self.min_index = None;
        }
    }

    fn insert_new_with_options(&mut self, capacity: usize, mut entry: ColumnTopNEntry) {
        if capacity == 0 {
            return;
        }

        if self.values.len() >= capacity {
            let Some(min_entry) = self.prune_min() else {
                return;
            };
            entry.count = min_entry.count.saturating_add(entry.count);
            entry.error = min_entry.count.saturating_add(entry.error);
        }

        let index = self.find(&entry.value).unwrap_or_else(|index| index);
        self.values.insert(index, entry);
        self.on_insert(index);
        if self.values.len() > capacity {
            self.prune_to_capacity(capacity);
        }
    }

    fn prune_to_capacity(&mut self, capacity: usize) {
        if self.values.len() <= capacity {
            return;
        }

        while self.values.len() > capacity {
            if self.prune_min().is_none() {
                break;
            }
        }
    }

    fn prune_min(&mut self) -> Option<ColumnTopNEntry> {
        let (min_index, next_min_index) = self.find_min_and_next_index()?;
        let entry = self.values.remove(min_index);
        self.min_index =
            next_min_index.map(|index| if index > min_index { index - 1 } else { index });
        Some(entry)
    }

    fn on_insert(&mut self, index: usize) {
        if let Some(min_index) = &mut self.min_index {
            if *min_index >= index {
                *min_index += 1;
            }
        }
        match self.min_index {
            None => self.min_index = Some(index),
            Some(min_index) => {
                let count = self.values[index].count;
                let min_count = self.values[min_index].count;
                if count < min_count || (count == min_count && index > min_index) {
                    self.min_index = Some(index);
                }
            }
        }
    }

    fn find_min_and_next_index(&self) -> Option<(usize, Option<usize>)> {
        let first = self.values.first()?;
        let mut min_index = 0;
        let mut min_count = first.count;
        let mut previous_min_index = None;
        let mut second_min: Option<(usize, usize)> = None;

        for (index, entry) in self.values.iter().enumerate().skip(1) {
            let count = entry.count;
            if count < min_count {
                second_min = Some((min_index, min_count));
                min_index = index;
                min_count = count;
                previous_min_index = None;
            } else if count == min_count {
                previous_min_index = Some(min_index);
                min_index = index;
            } else {
                match second_min {
                    None => second_min = Some((index, count)),
                    Some((second_index, second_count))
                        if count < second_count
                            || (count == second_count && index > second_index) =>
                    {
                        second_min = Some((index, count));
                    }
                    _ => {}
                }
            }
        }

        Some((
            min_index,
            previous_min_index.or(second_min.map(|(index, _)| index)),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::ColumnTopN;
    use crate::types::value::DataValue;

    #[test]
    fn top_n_replaces_min_counter_when_full() {
        let mut top_n = ColumnTopN::default();
        top_n.add_with_size(2, DataValue::Int32(1), 5);
        top_n.add_with_size(2, DataValue::Int32(2), 3);
        top_n.add_with_size(2, DataValue::Int32(3), 1);

        assert_eq!(top_n.len(), 2);
        assert_eq!(top_n.get(&DataValue::Int32(1)), Some(5));
        let entry = top_n.get_entry(&DataValue::Int32(3)).unwrap();
        assert_eq!(entry.count(), 4);
        assert_eq!(entry.error(), 3);
    }

    #[test]
    fn top_n_adds_existing_value() {
        let mut top_n = ColumnTopN::default();
        top_n.add_with_size(2, DataValue::Int32(2), 1);
        top_n.add_with_size(2, DataValue::Int32(1), 1);
        top_n.add_with_size(2, DataValue::Int32(1), 4);
        top_n.add_with_size(2, DataValue::Int32(3), 1);

        assert_eq!(top_n.len(), 2);
        assert!(top_n
            .values()
            .windows(2)
            .all(|pair| pair[0].value() < pair[1].value()));
        assert_eq!(top_n.get(&DataValue::Int32(1)), Some(5));
        let entry = top_n.get_entry(&DataValue::Int32(3)).unwrap();
        assert_eq!(entry.count(), 2);
        assert_eq!(entry.error(), 1);
    }

    #[test]
    fn top_n_skips_null() {
        let mut top_n = ColumnTopN::default();
        top_n.add_with_size(2, DataValue::Null, 1);
        top_n.add_with_size(2, DataValue::Int32(1), 1);

        assert_eq!(top_n.len(), 1);
        assert_eq!(top_n.get(&DataValue::Null), None);
    }
}
