use crate::types::evaluator::BinaryEvaluator;
use crate::types::evaluator::DataValue;
use serde::{Deserialize, Serialize};
use std::hint;

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TimePlusBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TimeMinusBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TimeGtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TimeGtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TimeLtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TimeLtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TimeEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TimeNotEqBinaryEvaluator;

#[typetag::serde]
impl BinaryEvaluator for TimePlusBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        match (left, right) {
            (DataValue::Time(v1, p1, _), DataValue::Time(v2, p2, ..)) => {
                let (mut v1, n1) = DataValue::unpack(*v1, *p1);
                let (v2, n2) = DataValue::unpack(*v2, *p2);
                let mut n = n1 + n2;
                while n > 1_000_000_000 {
                    v1 += 1;
                    n -= 1_000_000_000;
                }
                let p = if p2 > p1 { *p2 } else { *p1 };
                if v1 + v2 > 86400 {
                    return DataValue::Null;
                }
                DataValue::Time(DataValue::pack(v1 + v2, n, p), p, false)
            }
            (DataValue::Time(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        }
    }
}
#[typetag::serde]
impl BinaryEvaluator for TimeMinusBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        match (left, right) {
            (DataValue::Time(v1, p1, _), DataValue::Time(v2, p2, ..)) => {
                let (mut v1, mut n1) = DataValue::unpack(*v1, *p1);
                let (v2, n2) = DataValue::unpack(*v2, *p2);
                while n1 < n2 {
                    v1 -= 1;
                    n1 += 1_000_000_000;
                }
                if v1 < v2 {
                    return DataValue::Null;
                }
                let p = if p2 > p1 { *p2 } else { *p1 };
                DataValue::Time(DataValue::pack(v1 - v2, n1 - n2, p), p, false)
            }
            (DataValue::Time(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        }
    }
}

#[typetag::serde]
impl BinaryEvaluator for TimeGtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        match (left, right) {
            (DataValue::Time(v1, p1, _), DataValue::Time(v2, p2, ..)) => {
                let (v1, n1) = DataValue::unpack(*v1, *p1);
                let (v2, n2) = DataValue::unpack(*v2, *p2);
                match v1 > v2 {
                    true => DataValue::Boolean(true),
                    false => match v1 < v2 {
                        true => DataValue::Boolean(false),
                        false => match n1 > n2 {
                            true => DataValue::Boolean(true),
                            false => DataValue::Boolean(false),
                        },
                    },
                }
            }
            (DataValue::Time(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        }
    }
}
#[typetag::serde]
impl BinaryEvaluator for TimeGtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        match (left, right) {
            (DataValue::Time(v1, p1, _), DataValue::Time(v2, p2, ..)) => {
                let (v1, n1) = DataValue::unpack(*v1, *p1);
                let (v2, n2) = DataValue::unpack(*v2, *p2);
                match v1 > v2 {
                    true => DataValue::Boolean(true),
                    false => match v1 < v2 {
                        true => DataValue::Boolean(false),
                        false => match n1 >= n2 {
                            true => DataValue::Boolean(true),
                            false => DataValue::Boolean(false),
                        },
                    },
                }
            }
            (DataValue::Time(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        }
    }
}
#[typetag::serde]
impl BinaryEvaluator for TimeLtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        match (left, right) {
            (DataValue::Time(v1, p1, _), DataValue::Time(v2, p2, ..)) => {
                let (v1, n1) = DataValue::unpack(*v1, *p1);
                let (v2, n2) = DataValue::unpack(*v2, *p2);
                match v1 < v2 {
                    true => DataValue::Boolean(true),
                    false => match v1 > v2 {
                        true => DataValue::Boolean(false),
                        false => match n1 < n2 {
                            true => DataValue::Boolean(true),
                            false => DataValue::Boolean(false),
                        },
                    },
                }
            }
            (DataValue::Time(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        }
    }
}
#[typetag::serde]
impl BinaryEvaluator for TimeLtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        match (left, right) {
            (DataValue::Time(v1, p1, _), DataValue::Time(v2, p2, ..)) => {
                let (v1, n1) = DataValue::unpack(*v1, *p1);
                let (v2, n2) = DataValue::unpack(*v2, *p2);
                match v1 < v2 {
                    true => DataValue::Boolean(true),
                    false => match v1 > v2 {
                        true => DataValue::Boolean(false),
                        false => match n1 <= n2 {
                            true => DataValue::Boolean(true),
                            false => DataValue::Boolean(false),
                        },
                    },
                }
            }
            (DataValue::Time(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        }
    }
}
#[typetag::serde]
impl BinaryEvaluator for TimeEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        match (left, right) {
            (DataValue::Time(v1, p1, _), DataValue::Time(v2, p2, ..)) => {
                let (v1, n1) = DataValue::unpack(*v1, *p1);
                let (v2, n2) = DataValue::unpack(*v2, *p2);
                match (v1 == v2, n1 == n2) {
                    (true, true) => DataValue::Boolean(true),
                    _ => DataValue::Boolean(false),
                }
            }
            (DataValue::Time(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        }
    }
}
#[typetag::serde]
impl BinaryEvaluator for TimeNotEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        match (left, right) {
            (DataValue::Time(v1, p1, _), DataValue::Time(v2, p2, ..)) => {
                let (v1, n1) = DataValue::unpack(*v1, *p1);
                let (v2, n2) = DataValue::unpack(*v2, *p2);
                match (v1 != v2, n1 != n2) {
                    (false, false) => DataValue::Boolean(false),
                    _ => DataValue::Boolean(true),
                }
            }
            (DataValue::Time(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        }
    }
}
