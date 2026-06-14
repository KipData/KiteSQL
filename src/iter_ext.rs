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

use std::fmt::Display;

pub(crate) trait Itertools: Iterator + Sized {
    fn collect_vec(self) -> Vec<Self::Item> {
        self.collect()
    }

    fn join(self, separator: &str) -> String
    where
        Self::Item: Display,
    {
        let mut output = String::new();

        for (index, item) in self.enumerate() {
            if index > 0 {
                output.push_str(separator);
            }
            output.push_str(&item.to_string());
        }

        output
    }

    fn sorted_by_key<K, F>(self, f: F) -> std::vec::IntoIter<Self::Item>
    where
        K: Ord,
        F: FnMut(&Self::Item) -> K,
    {
        let mut values = self.collect_vec();
        values.sort_by_key(f);
        values.into_iter()
    }

    fn try_collect<T, E>(self) -> Result<Vec<T>, E>
    where
        Self: Iterator<Item = Result<T, E>>,
    {
        self.collect()
    }
}

impl<I> Itertools for I where I: Iterator {}
