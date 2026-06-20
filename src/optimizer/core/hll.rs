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
use crate::serdes::stable_hash::{StableHasher, HLL_HASH_KEYS};
use std::cmp;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

const MIN_PRECISION: u8 = 4;
const MAX_PRECISION: u8 = 26;

#[derive(Debug, Clone)]
pub struct HyperLogLog<K> {
    registers: Vec<u8>,
    precision: u8,
    phantom_k: PhantomData<K>,
}

impl<K: Hash> HyperLogLog<K> {
    pub fn with_relative_error(relative_error: f64) -> Result<Self, DatabaseError> {
        if relative_error <= 0.0 || relative_error.is_nan() {
            return Err(DatabaseError::InvalidValue(format!(
                "hyperloglog relative error must be greater than zero, got {relative_error}"
            )));
        }

        let register_count = Self::optimal_register_count(relative_error)?;
        let precision = register_count.trailing_zeros() as u8;
        Ok(Self {
            registers: vec![0; register_count],
            precision,
            phantom_k: PhantomData,
        })
    }

    pub fn add<Q: ?Sized + Hash>(&mut self, value: &Q) {
        let hash = Self::hash(value);
        let idx = (hash >> (64 - self.precision)) as usize;
        let remaining = hash << self.precision;
        let rank = if remaining == 0 {
            64 - self.precision + 1
        } else {
            remaining.leading_zeros() as u8 + 1
        };
        self.registers[idx] = cmp::max(self.registers[idx], rank);
    }

    pub fn estimate(&self) -> usize {
        let register_count = self.registers.len() as f64;
        let zero_registers = self
            .registers
            .iter()
            .filter(|register| **register == 0)
            .count();
        let raw_estimate = self.alpha() * register_count * register_count
            / self
                .registers
                .iter()
                .map(|register| 2f64.powi(-i32::from(*register)))
                .sum::<f64>();

        let estimate = if raw_estimate <= 2.5 * register_count && zero_registers > 0 {
            register_count * (register_count / zero_registers as f64).ln()
        } else {
            raw_estimate
        };

        estimate.round() as usize
    }

    fn optimal_register_count(relative_error: f64) -> Result<usize, DatabaseError> {
        let count = (1.04 / relative_error).powi(2).ceil();
        if !count.is_finite() || count > usize::MAX as f64 {
            return Err(DatabaseError::InvalidValue(format!(
                "hyperloglog relative error is too small: {relative_error}"
            )));
        }

        let min_count = 1usize << MIN_PRECISION;
        let count = cmp::max(min_count, count as usize)
            .checked_next_power_of_two()
            .ok_or_else(|| {
                DatabaseError::InvalidValue(format!(
                    "hyperloglog register count overflow for relative error: {relative_error}"
                ))
            })?;
        let precision = count.trailing_zeros() as u8;
        if precision > MAX_PRECISION {
            return Err(DatabaseError::InvalidValue(format!(
                "hyperloglog relative error is too small: {relative_error}"
            )));
        }

        Ok(count)
    }

    fn hash<Q: ?Sized + Hash>(value: &Q) -> u64 {
        let (key0, key1) = HLL_HASH_KEYS;
        let mut hasher = StableHasher::new_with_keys(key0, key1);
        value.hash(&mut hasher);
        hasher.finish()
    }

    fn alpha(&self) -> f64 {
        match self.registers.len() {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            count => 0.7213 / (1.0 + 1.079 / count as f64),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::HyperLogLog;

    #[test]
    fn estimate_ignores_duplicates() {
        let mut hll = HyperLogLog::<u64>::with_relative_error(0.01).unwrap();
        for _ in 0..1000 {
            hll.add(&7);
        }

        assert_eq!(hll.estimate(), 1);
    }

    #[test]
    fn estimate_distinct_values_within_error_budget() {
        let mut hll = HyperLogLog::<u64>::with_relative_error(0.02).unwrap();
        for value in 0..10_000 {
            hll.add(&value);
        }

        let estimate = hll.estimate() as f64;
        let error = (estimate - 10_000.0).abs() / 10_000.0;
        assert!(error < 0.03, "estimate={estimate}, error={error}");
    }
}
