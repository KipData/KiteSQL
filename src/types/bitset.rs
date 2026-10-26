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

use kite_sql_serde_macros::ReferenceSerialization;

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, ReferenceSerialization)]
pub struct BitSet {
    blocks: Vec<usize>,
    len: usize,
}

impl BitSet {
    pub fn with_capacity(len: usize) -> Self {
        let bits_per_block = usize::BITS as usize;
        let blocks = len.div_ceil(bits_per_block);
        BitSet {
            blocks: vec![0; blocks],
            len,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn clear(&mut self) {
        self.blocks.fill(0);
    }

    pub fn insert(&mut self, index: usize) {
        assert!(
            index < self.len,
            "insert at index {index} exceeds bitmap size {}",
            self.len
        );
        let bits_per_block = usize::BITS as usize;
        self.blocks[index / bits_per_block] |= 1usize << (index % bits_per_block);
    }

    pub fn contains(&self, index: usize) -> bool {
        if index >= self.len {
            return false;
        }
        let bits_per_block = usize::BITS as usize;
        (self.blocks[index / bits_per_block] & (1usize << (index % bits_per_block))) != 0
    }
}

// GRCOV_EXCL_START
#[cfg(test)]
mod tests {
    use super::BitSet;

    #[test]
    fn bit_set_contains_false_for_empty_bitmap() {
        let bitmap = BitSet::with_capacity(0);

        assert!(!bitmap.contains(0));
        assert!(!bitmap.contains(usize::MAX));
    }

    #[test]
    fn bit_set_contains_false_for_unset_bits_within_capacity() {
        let bitmap = BitSet::with_capacity(usize::BITS as usize + 1);

        assert!(!bitmap.contains(0));
        assert!(!bitmap.contains(usize::BITS as usize));
    }

    #[test]
    fn bit_set_insert_marks_bits_across_blocks() {
        let boundary = usize::BITS as usize;
        let mut bitmap = BitSet::with_capacity(boundary + 2);

        bitmap.insert(0);
        bitmap.insert(boundary - 1);
        bitmap.insert(boundary);
        bitmap.insert(boundary + 1);

        assert!(bitmap.contains(0));
        assert!(bitmap.contains(boundary - 1));
        assert!(bitmap.contains(boundary));
        assert!(bitmap.contains(boundary + 1));
        assert!(!bitmap.contains(1));
    }

    #[test]
    fn bit_set_contains_returns_false_past_capacity() {
        let mut bitmap = BitSet::with_capacity(2);

        bitmap.insert(1);

        assert!(!bitmap.contains(2));
        assert!(!bitmap.contains(usize::MAX));
    }

    #[test]
    fn bit_set_clear_preserves_width_and_allocation() {
        let width = usize::BITS as usize + 2;
        let mut bitmap = BitSet::with_capacity(width);
        let blocks = bitmap.blocks.as_ptr();
        bitmap.insert(0);
        bitmap.insert(width - 1);

        bitmap.clear();
        assert_eq!(bitmap.len(), width);
        assert_eq!(bitmap.blocks.as_ptr(), blocks);
        assert!((0..width).all(|position| !bitmap.contains(position)));
        bitmap.insert(1);
        bitmap.clear();
        assert!(!bitmap.contains(1));

        let mut empty = BitSet::with_capacity(0);
        empty.clear();
        assert!(empty.is_empty());
    }

    #[test]
    #[should_panic(expected = "exceeds bitmap size")]
    fn bit_set_insert_panics_past_capacity() {
        let mut bitmap = BitSet::with_capacity(1);

        bitmap.insert(1);
    }
}
// GRCOV_EXCL_STOP
