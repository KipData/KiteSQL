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

use crate::planner::operator::join::JoinType;

mod hash;
pub(crate) mod hash_join;
pub(crate) mod nested_loop_join;

pub(crate) struct RowBitmap {
    blocks: Vec<usize>,
    len: usize,
}

impl RowBitmap {
    pub(crate) fn with_capacity(len: usize) -> Self {
        let bits_per_block = usize::BITS as usize;
        let blocks = len.div_ceil(bits_per_block);
        RowBitmap {
            blocks: vec![0; blocks],
            len,
        }
    }

    pub(crate) fn insert(&mut self, index: usize) {
        assert!(
            index < self.len,
            "insert at index {index} exceeds bitmap size {}",
            self.len
        );
        let bits_per_block = usize::BITS as usize;
        self.blocks[index / bits_per_block] |= 1usize << (index % bits_per_block);
    }

    pub(crate) fn contains(&self, index: usize) -> bool {
        if index >= self.len {
            return false;
        }
        let bits_per_block = usize::BITS as usize;
        (self.blocks[index / bits_per_block] & (1usize << (index % bits_per_block))) != 0
    }
}

pub fn joins_nullable(join_type: &JoinType) -> (bool, bool) {
    match join_type {
        JoinType::Inner => (false, false),
        JoinType::LeftOuter => (false, true),
        JoinType::RightOuter => (true, false),
        JoinType::Full => (true, true),
        JoinType::Cross => (true, true),
    }
}

#[cfg(test)]
mod tests {
    use super::RowBitmap;

    #[test]
    fn row_bitmap_contains_false_for_empty_bitmap() {
        let bitmap = RowBitmap::with_capacity(0);

        assert!(!bitmap.contains(0));
        assert!(!bitmap.contains(usize::MAX));
    }

    #[test]
    fn row_bitmap_contains_false_for_unset_bits_within_capacity() {
        let bitmap = RowBitmap::with_capacity(usize::BITS as usize + 1);

        assert!(!bitmap.contains(0));
        assert!(!bitmap.contains(usize::BITS as usize));
    }

    #[test]
    fn row_bitmap_insert_marks_bits_across_blocks() {
        let boundary = usize::BITS as usize;
        let mut bitmap = RowBitmap::with_capacity(boundary + 2);

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
    fn row_bitmap_contains_returns_false_past_capacity() {
        let mut bitmap = RowBitmap::with_capacity(2);

        bitmap.insert(1);

        assert!(!bitmap.contains(2));
        assert!(!bitmap.contains(usize::MAX));
    }

    #[test]
    #[should_panic(expected = "exceeds bitmap size")]
    fn row_bitmap_insert_panics_past_capacity() {
        let mut bitmap = RowBitmap::with_capacity(1);

        bitmap.insert(1);
    }
}
