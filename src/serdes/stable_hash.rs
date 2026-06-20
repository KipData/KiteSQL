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

use std::hash::Hasher;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

pub(crate) const TABLE_NAME_HASH_KEYS: (u64, u64) = (0x9e37_79b9_7f4a_7c15, 0xbf58_476d_1ce4_e5b9);
pub(crate) const CM_SKETCH_HASH_KEYS: [(u64, u64); 2] = [
    (0x94d0_49bb_1331_11eb, 0xd6e8_feb8_6659_fd93),
    (0xa076_1d64_78bd_642f, 0xe703_7ed1_a0b4_28db),
];
pub(crate) const HLL_HASH_KEYS: (u64, u64) = (0xd1b5_4a32_d192_ed03, 0xabc9_83a9_351f_3c2d);

#[derive(Debug, Clone, Copy)]
pub(crate) struct StableHasher {
    key0: u64,
    key1: u64,
    hash: u64,
}

impl StableHasher {
    pub(crate) fn new_with_keys(key0: u64, key1: u64) -> Self {
        StableHasher {
            key0,
            key1,
            hash: mix64(FNV_OFFSET ^ key0).wrapping_add(mix64(key1)),
        }
    }

    pub(crate) fn keys(&self) -> (u64, u64) {
        (self.key0, self.key1)
    }
}

impl Hasher for StableHasher {
    fn finish(&self) -> u64 {
        mix64(self.hash ^ self.key0.rotate_left(17) ^ self.key1.rotate_right(11))
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.hash ^= u64::from(*byte);
            self.hash = self.hash.wrapping_mul(FNV_PRIME);
        }
    }

    fn write_u8(&mut self, i: u8) {
        self.write(&[i]);
    }

    fn write_u16(&mut self, i: u16) {
        self.write(&i.to_le_bytes());
    }

    fn write_u32(&mut self, i: u32) {
        self.write(&i.to_le_bytes());
    }

    fn write_u64(&mut self, i: u64) {
        self.write(&i.to_le_bytes());
    }

    fn write_u128(&mut self, i: u128) {
        self.write(&i.to_le_bytes());
    }

    fn write_usize(&mut self, i: usize) {
        self.write_u64(i as u64);
    }

    fn write_i8(&mut self, i: i8) {
        self.write_u8(i as u8);
    }

    fn write_i16(&mut self, i: i16) {
        self.write(&i.to_le_bytes());
    }

    fn write_i32(&mut self, i: i32) {
        self.write(&i.to_le_bytes());
    }

    fn write_i64(&mut self, i: i64) {
        self.write(&i.to_le_bytes());
    }

    fn write_i128(&mut self, i: i128) {
        self.write(&i.to_le_bytes());
    }

    fn write_isize(&mut self, i: isize) {
        self.write_i64(i as i64);
    }
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::StableHasher;
    use std::hash::{Hash, Hasher};

    #[test]
    fn stable_hasher_is_deterministic_for_same_keys() {
        let mut left = StableHasher::new_with_keys(1, 2);
        let mut right = StableHasher::new_with_keys(1, 2);

        "kite".hash(&mut left);
        "kite".hash(&mut right);

        assert_eq!(left.finish(), right.finish());
        assert_eq!(left.keys(), (1, 2));
    }

    #[test]
    fn stable_hasher_keys_change_hash_output() {
        let mut left = StableHasher::new_with_keys(1, 2);
        let mut right = StableHasher::new_with_keys(2, 1);

        "kite".hash(&mut left);
        "kite".hash(&mut right);

        assert_ne!(left.finish(), right.finish());
    }

    #[test]
    fn stable_hasher_write_chunks_match_single_write() {
        let mut chunked = StableHasher::new_with_keys(0, 1);
        chunked.write(b"ki");
        chunked.write(b"te");

        let mut single = StableHasher::new_with_keys(0, 1);
        single.write(b"kite");

        assert_eq!(chunked.finish(), single.finish());
    }
}
