// Copyright 2026 KipData/KiteSQL
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

macro_rules! bytes_binary {
    ($name:ident, $op:tt) => {
        pub fn $name(left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
            Ok(match (left, right) {
                (DataValue::Binary { value: left, .. }, DataValue::Binary { value: right, .. }) => {
                    DataValue::Boolean(left $op right)
                }
                (DataValue::Null, _) | (_, DataValue::Null) => DataValue::Null,
                _ => unreachable!(),
            })
        }
    };
}

bytes_binary!(bytes_eq_binary_eval, ==);
bytes_binary!(bytes_not_eq_binary_eval, !=);
bytes_binary!(bytes_gt_binary_eval, >);
bytes_binary!(bytes_gt_eq_binary_eval, >=);
bytes_binary!(bytes_lt_binary_eval, <);
bytes_binary!(bytes_lt_eq_binary_eval, <=);
