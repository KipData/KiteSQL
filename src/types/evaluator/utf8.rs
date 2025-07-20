use crate::errors::DatabaseError;
use crate::types::evaluator::BinaryEvaluator;
use crate::types::evaluator::DataValue;
use crate::types::value::Utf8Type;
use regex::Regex;
use serde::{Deserialize, Serialize};
use sqlparser::ast::CharLengthUnits;
use std::hint;

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8GtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8GtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8LtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8LtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8EqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8NotEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8StringConcatBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8LikeBinaryEvaluator {
    pub(crate) escape_char: Option<char>,
}
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8NotLikeBinaryEvaluator {
    pub(crate) escape_char: Option<char>,
}

#[typetag::serde]
impl BinaryEvaluator for Utf8GtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Utf8 { value: v1, .. }, DataValue::Utf8 { value: v2, .. }) => {
                DataValue::Boolean(v1 > v2)
            }
            (DataValue::Utf8 { .. }, DataValue::Null)
            | (DataValue::Null, DataValue::Utf8 { .. })
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Utf8GtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Utf8 { value: v1, .. }, DataValue::Utf8 { value: v2, .. }) => {
                DataValue::Boolean(v1 >= v2)
            }
            (DataValue::Utf8 { .. }, DataValue::Null)
            | (DataValue::Null, DataValue::Utf8 { .. })
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Utf8LtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Utf8 { value: v1, .. }, DataValue::Utf8 { value: v2, .. }) => {
                DataValue::Boolean(v1 < v2)
            }
            (DataValue::Utf8 { .. }, DataValue::Null)
            | (DataValue::Null, DataValue::Utf8 { .. })
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Utf8LtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Utf8 { value: v1, .. }, DataValue::Utf8 { value: v2, .. }) => {
                DataValue::Boolean(v1 <= v2)
            }
            (DataValue::Utf8 { .. }, DataValue::Null)
            | (DataValue::Null, DataValue::Utf8 { .. })
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Utf8EqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Utf8 { value: v1, .. }, DataValue::Utf8 { value: v2, .. }) => {
                DataValue::Boolean(v1 == v2)
            }
            (DataValue::Utf8 { .. }, DataValue::Null)
            | (DataValue::Null, DataValue::Utf8 { .. })
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Utf8NotEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Utf8 { value: v1, .. }, DataValue::Utf8 { value: v2, .. }) => {
                DataValue::Boolean(v1 != v2)
            }
            (DataValue::Utf8 { .. }, DataValue::Null)
            | (DataValue::Null, DataValue::Utf8 { .. })
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Utf8StringConcatBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Utf8 { value: v1, .. }, DataValue::Utf8 { value: v2, .. }) => {
                DataValue::Utf8 {
                    value: v1.clone() + v2,
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                }
            }
            (DataValue::Utf8 { .. }, DataValue::Null)
            | (DataValue::Null, DataValue::Utf8 { .. })
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Utf8LikeBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Utf8 { value, .. }, DataValue::Utf8 { value: pattern, .. }) => {
                DataValue::Boolean(string_like(value, pattern, self.escape_char))
            }
            (DataValue::Utf8 { .. }, DataValue::Null)
            | (DataValue::Null, DataValue::Utf8 { .. })
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Utf8NotLikeBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Utf8 { value, .. }, DataValue::Utf8 { value: pattern, .. }) => {
                DataValue::Boolean(!string_like(value, pattern, self.escape_char))
            }
            (DataValue::Utf8 { .. }, DataValue::Null)
            | (DataValue::Null, DataValue::Utf8 { .. })
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}

fn string_like(value: &str, pattern: &str, escape_char: Option<char>) -> bool {
    let mut regex_pattern = String::new();
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        if matches!(escape_char.map(|escape_c| escape_c == c), Some(true)) {
            if let Some(next_char) = chars.next() {
                regex_pattern.push(next_char);
            }
        } else if c == '%' {
            regex_pattern.push_str(".*");
        } else if c == '_' {
            regex_pattern.push('.');
        } else {
            regex_pattern.push(c);
        }
    }
    Regex::new(&regex_pattern).unwrap().is_match(value)
}
