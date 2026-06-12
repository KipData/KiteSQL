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
use crate::types::evaluator::DataValue;

/// Tips:
/// - Null values operate as null values
pub fn null_binary_eval(_: &DataValue, _: &DataValue) -> Result<DataValue, DatabaseError> {
    Ok(DataValue::Null)
}
pub fn to_sql_null_cast_eval(_value: &DataValue) -> Result<DataValue, DatabaseError> {
    Ok(DataValue::Null)
}
pub fn null_cast_eval(_value: &DataValue) -> Result<DataValue, DatabaseError> {
    Ok(DataValue::Null)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use super::*;

    #[test]
    fn test_null_cast_evaluators() {
        assert_eq!(
            to_sql_null_cast_eval(&DataValue::Int32(1)).unwrap(),
            DataValue::Null
        );
        assert_eq!(null_cast_eval(&DataValue::Null).unwrap(), DataValue::Null);
    }
}
