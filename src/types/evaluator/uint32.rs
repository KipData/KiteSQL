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

use crate::numeric_binary_evaluator_definition;
use crate::types::evaluator::DataValue;
use crate::types::LogicalType;

numeric_binary_evaluator_definition!(UInt32, DataValue::UInt32);
crate::define_integer_cast_evaluators!(UInt32, UInt32, u32, LogicalType::UInteger);

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use super::*;
    use crate::types::evaluator::CastEvaluator;
    use crate::types::value::Utf8Type;
    use ordered_float::OrderedFloat;
    use rust_decimal::Decimal;
    use sqlparser::ast::CharLengthUnits;

    #[test]
    fn test_uint32_cast_evaluators() {
        let value = DataValue::UInt32(1);

        assert_eq!(UInt32ToBooleanCastEvaluator.eval_cast(&value).unwrap(), DataValue::Boolean(true));
        assert_eq!(UInt32ToTinyintCastEvaluator.eval_cast(&value).unwrap(), DataValue::Int8(1));
        assert_eq!(UInt32ToUTinyintCastEvaluator.eval_cast(&value).unwrap(), DataValue::UInt8(1));
        assert_eq!(UInt32ToSmallintCastEvaluator.eval_cast(&value).unwrap(), DataValue::Int16(1));
        assert_eq!(UInt32ToUSmallintCastEvaluator.eval_cast(&value).unwrap(), DataValue::UInt16(1));
        assert_eq!(UInt32ToIntegerCastEvaluator.eval_cast(&value).unwrap(), DataValue::Int32(1));
        assert_eq!(UInt32ToUIntegerCastEvaluator.eval_cast(&value).unwrap(), DataValue::UInt32(1));
        assert_eq!(UInt32ToBigintCastEvaluator.eval_cast(&value).unwrap(), DataValue::Int64(1));
        assert_eq!(UInt32ToUBigintCastEvaluator.eval_cast(&value).unwrap(), DataValue::UInt64(1));
        assert_eq!(
            UInt32ToFloatCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::Float32(OrderedFloat(1.0))
        );
        assert_eq!(
            UInt32ToDoubleCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::Float64(OrderedFloat(1.0))
        );
        assert_eq!(
            UInt32ToCharCastEvaluator {
                len: 1,
                unit: CharLengthUnits::Characters,
            }
            .eval_cast(&value)
            .unwrap(),
            DataValue::Utf8 {
                value: "1".to_string(),
                ty: Utf8Type::Fixed(1),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            UInt32ToVarcharCastEvaluator {
                len: Some(1),
                unit: CharLengthUnits::Characters,
            }
            .eval_cast(&value)
            .unwrap(),
            DataValue::Utf8 {
                value: "1".to_string(),
                ty: Utf8Type::Variable(Some(1)),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            UInt32ToDecimalCastEvaluator { scale: Some(2) }
                .eval_cast(&value)
                .unwrap(),
            DataValue::Decimal(Decimal::new(100, 2))
        );
    }
}
