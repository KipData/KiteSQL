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
use crate::expression::range_detacher::Range;
use crate::serdes::{ReferenceSerialization, ReferenceTables};
use crate::storage::{TableCache, Transaction};
use crate::types::value::DataValue;
use kite_sql_serde_macros::ReferenceSerialization;
use siphasher::sip::SipHasher13;
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::{cmp, mem};

pub(crate) type FastHasher = SipHasher13;
pub(crate) const COUNT_MIN_SKETCH_STORAGE_PAGE_LEN: usize = 16 * 1024;

#[derive(Debug, Clone, ReferenceSerialization)]
pub struct CountMinSketchMeta {
    width: usize,
    k_num: usize,
    page_len: usize,
    hasher_0: FastHasher,
    hasher_1: FastHasher,
}

impl CountMinSketchMeta {
    pub fn width(&self) -> usize {
        self.width
    }

    pub fn k_num(&self) -> usize {
        self.k_num
    }

    pub fn page_len(&self) -> usize {
        self.page_len
    }
}

impl CountMinSketchPage {
    pub fn row_idx(&self) -> usize {
        self.row_idx
    }

    pub fn page_idx(&self) -> usize {
        self.page_idx
    }

    pub fn counters(&self) -> &[usize] {
        &self.counters
    }
}

#[derive(Debug, Clone, ReferenceSerialization)]
pub struct CountMinSketchPage {
    row_idx: usize,
    page_idx: usize,
    counters: Vec<usize>,
}

// https://github.com/jedisct1/rust-count-min-sketch
#[derive(Debug, Clone)]
pub struct CountMinSketch<K> {
    counters: Vec<Vec<usize>>,
    offsets: Vec<usize>,
    hashers: [FastHasher; 2],
    mask: usize,
    k_num: usize,
    phantom_k: PhantomData<K>,
}

impl<K> CountMinSketch<K> {
    pub fn storage_page_count(&self, page_len: usize) -> usize {
        self.counters
            .iter()
            .map(|row| row.len().div_ceil(page_len))
            .sum()
    }

    pub fn into_storage_parts(
        self,
        page_len: usize,
    ) -> (CountMinSketchMeta, impl Iterator<Item = CountMinSketchPage>) {
        let CountMinSketch {
            counters,
            hashers,
            mask,
            k_num,
            ..
        } = self;
        let width = mask + 1;
        let meta = CountMinSketchMeta {
            width,
            k_num,
            page_len,
            hasher_0: hashers[0],
            hasher_1: hashers[1],
        };
        let pages = counters
            .into_iter()
            .enumerate()
            .flat_map(move |(row_idx, counters)| {
                let page_count = counters.len().div_ceil(page_len);
                (0..page_count).map(move |page_idx| {
                    let start = page_idx * page_len;
                    let end = ((page_idx + 1) * page_len).min(counters.len());

                    CountMinSketchPage {
                        row_idx,
                        page_idx,
                        counters: counters[start..end].to_vec(),
                    }
                })
            });

        (meta, pages)
    }

    pub fn from_storage_parts(
        meta: CountMinSketchMeta,
        pages: Vec<CountMinSketchPage>,
    ) -> Result<Self, DatabaseError> {
        let width = meta.width;
        let k_num = meta.k_num;
        let page_len = meta.page_len;
        if width == 0 || k_num == 0 || page_len == 0 {
            return Err(DatabaseError::InvalidValue(
                "count-min sketch storage meta is invalid".to_string(),
            ));
        }
        if !width.is_power_of_two() {
            return Err(DatabaseError::InvalidValue(
                "count-min sketch width must be a power of two".to_string(),
            ));
        }

        let mut counters = vec![Vec::with_capacity(width); k_num];
        let mut expected_page_idx = vec![0usize; k_num];

        for CountMinSketchPage {
            row_idx,
            page_idx,
            counters: page_counters,
        } in pages
        {
            if row_idx >= k_num {
                return Err(DatabaseError::InvalidValue(format!(
                    "count-min sketch row index out of bounds: {row_idx}"
                )));
            }
            if page_idx != expected_page_idx[row_idx] {
                return Err(DatabaseError::InvalidValue(format!(
                    "count-min sketch page sequence is invalid: row={row_idx}, page={page_idx}, expected={}",
                    expected_page_idx[row_idx]
                )));
            }
            if page_counters.len() > page_len {
                return Err(DatabaseError::InvalidValue(format!(
                    "count-min sketch page is too large: row={row_idx}, page={page_idx}"
                )));
            }

            counters[row_idx].extend(page_counters);
            expected_page_idx[row_idx] += 1;
        }

        for (row_idx, row) in counters.iter().enumerate() {
            if row.len() != width {
                return Err(DatabaseError::InvalidValue(format!(
                    "count-min sketch row width mismatch: row={row_idx}, expected={width}, actual={}",
                    row.len()
                )));
            }
        }

        Ok(CountMinSketch {
            counters,
            offsets: vec![0; k_num],
            hashers: [meta.hasher_0, meta.hasher_1],
            mask: width - 1,
            k_num,
            phantom_k: Default::default(),
        })
    }
}

impl CountMinSketch<DataValue> {
    pub fn collect_count(&self, ranges: &[Range]) -> usize {
        let mut count = 0;

        for range in ranges {
            count += match range {
                Range::Eq(value) => self.estimate(value),
                _ => 0,
            }
        }

        count
    }
}

impl<K: Hash> CountMinSketch<K> {
    pub fn new(capacity: usize, probability: f64, tolerance: f64) -> Self {
        let width = Self::optimal_width(capacity, tolerance);
        let k_num = Self::optimal_k_num(probability);
        let counters = vec![vec![0; width]; k_num];
        let offsets = vec![0; k_num];
        let hashers = [Self::sip_new(), Self::sip_new()];
        CountMinSketch {
            counters,
            offsets,
            hashers,
            mask: Self::mask(width),
            k_num,
            phantom_k: PhantomData,
        }
    }

    pub fn add<Q: ?Sized + Hash>(&mut self, key: &Q, value: usize)
    where
        K: Borrow<Q>,
    {
        let mut hashes = [0u64, 0u64];
        let lowest = (0..self.k_num)
            .map(|k_i| {
                let offset = self.offset(&mut hashes, key, k_i);
                self.offsets[k_i] = offset;
                self.counters[k_i][offset]
            })
            .min()
            .unwrap();
        for k_i in 0..self.k_num {
            let offset = self.offsets[k_i];
            if self.counters[k_i][offset] == lowest {
                self.counters[k_i][offset] = self.counters[k_i][offset].saturating_add(value);
            }
        }
    }

    pub fn increment<Q: ?Sized + Hash>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
    {
        self.add(key, 1)
    }

    pub fn estimate<Q: ?Sized + Hash>(&self, key: &Q) -> usize
    where
        K: Borrow<Q>,
    {
        let mut hashes = [0u64, 0u64];
        (0..self.k_num)
            .map(|k_i| {
                let offset = self.offset(&mut hashes, key, k_i);
                self.counters[k_i][offset]
            })
            .min()
            .unwrap()
    }

    #[allow(dead_code)]
    pub fn estimate_memory(
        capacity: usize,
        probability: f64,
        tolerance: f64,
    ) -> Result<usize, &'static str> {
        let width = Self::optimal_width(capacity, tolerance);
        let k_num = Self::optimal_k_num(probability);
        Ok(width * mem::size_of::<u64>() * k_num)
    }

    #[allow(dead_code)]
    pub fn clear(&mut self) {
        for k_i in 0..self.k_num {
            for counter in &mut self.counters[k_i] {
                *counter = 0
            }
        }
        self.hashers = [Self::sip_new(), Self::sip_new()];
    }

    fn optimal_width(capacity: usize, tolerance: f64) -> usize {
        let e = tolerance / (capacity as f64);
        let width = (2.0 / e).round() as usize;
        cmp::max(2, width)
            .checked_next_power_of_two()
            .expect("Width would be way too large")
    }

    fn mask(width: usize) -> usize {
        debug_assert!(width > 1);
        debug_assert_eq!(width & (width - 1), 0);
        width - 1
    }

    fn optimal_k_num(probability: f64) -> usize {
        cmp::max(1, ((1.0 - probability).ln() / 0.5f64.ln()) as usize)
    }

    fn sip_new() -> FastHasher {
        FastHasher::new_with_keys(0, 1)
    }

    fn offset<Q: ?Sized + Hash>(&self, hashes: &mut [u64; 2], key: &Q, k_i: usize) -> usize
    where
        K: Borrow<Q>,
    {
        if k_i < 2 {
            let sip = &mut self.hashers[k_i].clone();
            key.hash(sip);
            let hash = sip.finish();
            hashes[k_i] = hash;
            hash as usize & self.mask
        } else {
            hashes[0].wrapping_add((k_i as u64).wrapping_mul(hashes[1]) % 0xffffffffffffffc5)
                as usize
                & self.mask
        }
    }
}

impl<K> ReferenceSerialization for CountMinSketch<K> {
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        self.counters.encode(writer, is_direct, reference_tables)?;
        self.offsets.encode(writer, is_direct, reference_tables)?;
        self.hashers[0].encode(writer, is_direct, reference_tables)?;
        self.hashers[1].encode(writer, is_direct, reference_tables)?;
        self.mask.encode(writer, is_direct, reference_tables)?;
        self.k_num.encode(writer, is_direct, reference_tables)?;

        Ok(())
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        drive: Option<(&T, &TableCache)>,
        reference_tables: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        let counters = Vec::<Vec<usize>>::decode(reader, drive, reference_tables)?;
        let offsets = Vec::<usize>::decode(reader, drive, reference_tables)?;
        let hasher_0 = FastHasher::decode(reader, drive, reference_tables)?;
        let hasher_1 = FastHasher::decode(reader, drive, reference_tables)?;
        let mask = usize::decode(reader, drive, reference_tables)?;
        let k_num = usize::decode(reader, drive, reference_tables)?;

        Ok(CountMinSketch {
            counters,
            offsets,
            hashers: [hasher_0, hasher_1],
            mask,
            k_num,
            phantom_k: Default::default(),
        })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::expression::range_detacher::Range;
    use crate::optimizer::core::cm_sketch::CountMinSketch;
    use crate::types::value::DataValue;
    use std::collections::Bound;

    #[test]
    fn test_increment() {
        let mut cms = CountMinSketch::<&str>::new(100, 0.95, 10.0);
        for _ in 0..300 {
            cms.increment("key");
        }
        assert_eq!(cms.estimate("key"), 300);
    }

    #[test]
    fn test_increment_multi() {
        let mut cms = CountMinSketch::<u64>::new(100, 0.99, 2.0);
        for i in 0..1_000_000 {
            cms.increment(&(i % 100));
        }
        for key in 0..100 {
            assert!(cms.estimate(&key) >= 9_000);
        }
    }

    #[test]
    fn test_collect_count() {
        let mut cms = CountMinSketch::<DataValue>::new(100, 0.95, 10.0);
        for _ in 0..300 {
            cms.increment(&DataValue::Int32(300));
        }
        assert_eq!(
            cms.collect_count(&[
                Range::Eq(DataValue::Int32(300)),
                Range::Scope {
                    min: Bound::Unbounded,
                    max: Bound::Unbounded,
                }
            ]),
            300
        );
    }

    #[test]
    fn test_storage_parts_roundtrip() {
        let mut cms = CountMinSketch::<DataValue>::new(128, 0.95, 10.0);
        for i in 0..256 {
            cms.increment(&DataValue::Int32(i % 17));
        }

        let (meta, pages) = cms.clone().into_storage_parts(8);
        let rebuilt =
            CountMinSketch::<DataValue>::from_storage_parts(meta, pages.collect()).unwrap();

        assert_eq!(
            cms.estimate(&DataValue::Int32(3)),
            rebuilt.estimate(&DataValue::Int32(3))
        );
        assert_eq!(
            cms.estimate(&DataValue::Int32(9)),
            rebuilt.estimate(&DataValue::Int32(9))
        );
    }
}
