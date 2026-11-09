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

//! A fast hasher for the hash tables of execution operators. Unlike std's SipHash it does not
//! resist crafted collisions, which is acceptable for keys taken from the database's own rows.

use std::collections::HashSet;
use std::hash::{BuildHasherDefault, Hasher};

pub(crate) type FxBuildHasher = BuildHasherDefault<FxHasher>;
pub(crate) type FxHashSet<T> = HashSet<T, FxBuildHasher>;

const SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;

/// The multiply-rotate hash of rustc and Firefox, one 64-bit word at a time.
#[derive(Default, Clone, Copy)]
pub(crate) struct FxHasher {
    hash: u64,
}

impl FxHasher {
    #[inline]
    fn add(&mut self, word: u64) {
        self.hash = (self.hash.rotate_left(5) ^ word).wrapping_mul(SEED);
    }
}

impl Hasher for FxHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let (words, rest) = bytes.as_chunks::<8>();
        for word in words {
            self.add(u64::from_le_bytes(*word));
        }
        if !rest.is_empty() {
            let mut word = [0; 8];
            word[..rest.len()].copy_from_slice(rest);
            self.add(u64::from_le_bytes(word));
        }
    }

    #[inline]
    fn write_u8(&mut self, i: u8) {
        self.add(i as u64);
    }

    #[inline]
    fn write_u16(&mut self, i: u16) {
        self.add(i as u64);
    }

    #[inline]
    fn write_u32(&mut self, i: u32) {
        self.add(i as u64);
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.add(i);
    }

    #[inline]
    fn write_usize(&mut self, i: usize) {
        self.add(i as u64);
    }

    // The multiply leaves the best bits on top, while hash tables index with the low bits.
    #[inline]
    fn finish(&self) -> u64 {
        self.hash.rotate_left(26)
    }
}

// GRCOV_EXCL_START
#[cfg(test)]
#[crate::dual::dual]
mod tests {
    use super::FxBuildHasher;
    use std::hash::BuildHasher;

    #[test]
    fn hashes_every_byte() {
        let hasher = FxBuildHasher::default();
        assert_eq!(hasher.hash_one("kite_sql_1"), hasher.hash_one("kite_sql_1"));
        assert_ne!(hasher.hash_one("kite_sql_1"), hasher.hash_one("kite_sql_2"));
        assert_ne!(
            hasher.hash_one(&[1u8; 9][..]),
            hasher.hash_one(&[1u8; 8][..])
        );
    }
}
// GRCOV_EXCL_STOP
