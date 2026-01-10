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

pub(crate) mod aggregate;
pub(crate) mod describe;
pub(crate) mod dummy;
pub(crate) mod except;
pub(crate) mod explain;
pub(crate) mod filter;
pub(crate) mod function_scan;
pub(crate) mod index_scan;
pub(crate) mod join;
pub(crate) mod limit;
pub(crate) mod projection;
pub(crate) mod seq_scan;
pub(crate) mod show_table;
pub(crate) mod show_view;
pub(crate) mod sort;
pub(crate) mod top_k;
pub(crate) mod union;
pub(crate) mod values;

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) mod test {
    use crate::types::value::DataValue;
    use itertools::Itertools;

    pub(crate) fn build_integers(ints: Vec<Option<i32>>) -> Vec<DataValue> {
        ints.into_iter()
            .map(|i| i.map(DataValue::Int32).unwrap_or(DataValue::Null))
            .collect_vec()
    }
}
