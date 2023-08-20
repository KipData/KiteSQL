use std::sync::Arc;
use arrow::array::ArrayRef;
use arrow::compute::{eq_dyn, gt_dyn, gt_eq_dyn, lt_dyn, lt_eq_dyn, neq_dyn};
use arrow::datatypes::DataType;
use arrow::array::*;
use arrow::compute::*;
use crate::execution_ap::ExecutorError;
use crate::expression::BinaryOperator;
use crate::types::LogicalType;
use crate::types::value::DataValue;
/// Copied from datafusion binary.rs
macro_rules! compute_op {
    // invoke binary operator
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        let ll = $LEFT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        let rr = $RIGHT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        Ok(Arc::new($OP(&ll, &rr)?))
    }};
    // invoke unary operator
    ($OPERAND:expr, $OP:ident, $DT:ident) => {{
        let operand = $OPERAND
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        Ok(Arc::new($OP(&operand)?))
    }};
}

macro_rules! arithmetic_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        match $LEFT.data_type() {
            DataType::Int32 => compute_op!($LEFT, $RIGHT, $OP, Int32Array),
            DataType::Int64 => compute_op!($LEFT, $RIGHT, $OP, Int64Array),
            DataType::Float64 => compute_op!($LEFT, $RIGHT, $OP, Float64Array),
            _ => todo!("unsupported data type"),
        }
    }};
}

macro_rules! boolean_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        if *$LEFT.data_type() != DataType::Boolean || *$RIGHT.data_type() != DataType::Boolean {
            return Err(ExecutorError::InternalError(format!(
                "Cannot evaluate binary expression with types {:?} and {:?}, only Boolean supported",
                $LEFT.data_type(),
                $RIGHT.data_type()
            )));
        }

        let ll = $LEFT
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean_op failed to downcast array");
        let rr = $RIGHT
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean_op failed to downcast array");
        Ok(Arc::new($OP(&ll, &rr)?))
    }};
}

pub fn binary_op(
    left: &ArrayRef,
    right: &ArrayRef,
    op: &BinaryOperator,
) -> Result<ArrayRef, ExecutorError> {
    match op {
        BinaryOperator::Plus => arithmetic_op!(left, right, add),
        BinaryOperator::Minus => arithmetic_op!(left, right, subtract),
        BinaryOperator::Multiply => arithmetic_op!(left, right, multiply),
        BinaryOperator::Divide => arithmetic_op!(left, right, divide),
        BinaryOperator::Gt => Ok(Arc::new(gt_dyn(left, right)?)),
        BinaryOperator::Lt => Ok(Arc::new(lt_dyn(left, right)?)),
        BinaryOperator::GtEq => Ok(Arc::new(gt_eq_dyn(left, right)?)),
        BinaryOperator::LtEq => Ok(Arc::new(lt_eq_dyn(left, right)?)),
        BinaryOperator::Eq => Ok(Arc::new(eq_dyn(left, right)?)),
        BinaryOperator::NotEq => Ok(Arc::new(neq_dyn(left, right)?)),
        BinaryOperator::And => boolean_op!(left, right, and_kleene),
        BinaryOperator::Or => boolean_op!(left, right, or_kleene),
        _ => todo!(),
    }
}

fn unpack_i32(value: DataValue) -> Option<i32> {
    match value {
        DataValue::Int32(inner) => inner,
        _ => None
    }
}

fn unpack_i64(value: DataValue) -> Option<i64> {
    match value {
        DataValue::Int64(inner) => inner,
        _ => None
    }
}

fn unpack_f64(value: DataValue) -> Option<f64> {
    match value {
        DataValue::Float64(inner) => inner,
        _ => None
    }
}

fn unpack_bool(value: DataValue) -> Option<bool> {
    match value {
        DataValue::Boolean(inner) => inner,
        _ => None
    }
}

/// Tips:
/// - Null values operate as null values
pub fn binary_op_tp(
    left: &DataValue,
    right: &DataValue,
    op: &BinaryOperator,
) -> DataValue {
    let main_type = left.logical_type();

    match &main_type {
        LogicalType::Integer => {
            let left_value = unpack_i32(left.clone());
            let right_value = unpack_i32(right.clone().cast(&main_type));

            match op {
                BinaryOperator::Plus => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 + v2)
                    } else {
                        None
                    };

                    DataValue::Int32(value)
                }
                BinaryOperator::Minus => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 - v2)
                    } else {
                        None
                    };

                    DataValue::Int32(value)
                }
                BinaryOperator::Multiply => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 * v2)
                    } else {
                        None
                    };

                    DataValue::Int32(value)
                }
                BinaryOperator::Divide => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 / v2)
                    } else {
                        None
                    };

                    DataValue::Int32(value)
                }

                BinaryOperator::Gt => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 > v2)
                    } else {
                        None
                    };

                    DataValue::Boolean(value)
                }
                BinaryOperator::Lt => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 < v2)
                    } else {
                        None
                    };

                    DataValue::Boolean(value)
                }
                BinaryOperator::GtEq => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 >= v2)
                    } else {
                        None
                    };

                    DataValue::Boolean(value)
                }
                BinaryOperator::LtEq => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 <= v2)
                    } else {
                        None
                    };

                    DataValue::Boolean(value)
                }
                BinaryOperator::Eq => {
                    let value = match (left_value, right_value) {
                        (Some(v1), Some(v2)) => {
                            Some(v1 == v2)
                        }
                        (None, None) => {
                            Some(true)
                        }
                        (_, _) => {
                            None
                        }
                    };

                    DataValue::Boolean(value)
                }
                BinaryOperator::NotEq => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 != v2)
                    } else {
                        None
                    };

                    DataValue::Boolean(value)
                }
                _ => todo!("unsupported operator")
            }
        },
        LogicalType::Bigint => {
            let left_value = unpack_i64(left.clone());
            let right_value = unpack_i64(right.clone().cast(&main_type));

            match op {
                BinaryOperator::Plus => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 + v2)
                    } else {
                        None
                    };

                    DataValue::Int64(value)
                }
                BinaryOperator::Minus => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 - v2)
                    } else {
                        None
                    };

                    DataValue::Int64(value)
                }
                BinaryOperator::Multiply => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 * v2)
                    } else {
                        None
                    };

                    DataValue::Int64(value)
                }
                BinaryOperator::Divide => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 / v2)
                    } else {
                        None
                    };

                    DataValue::Int64(value)
                }

                BinaryOperator::Gt => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 > v2)
                    } else {
                        None
                    };

                    DataValue::Boolean(value)
                }
                BinaryOperator::Lt => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 < v2)
                    } else {
                        None
                    };

                    DataValue::Boolean(value)
                }
                BinaryOperator::GtEq => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 >= v2)
                    } else {
                        None
                    };

                    DataValue::Boolean(value)
                }
                BinaryOperator::LtEq => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 <= v2)
                    } else {
                        None
                    };

                    DataValue::Boolean(value)
                }
                BinaryOperator::Eq => {
                    let value = match (left_value, right_value) {
                        (Some(v1), Some(v2)) => {
                            Some(v1 == v2)
                        }
                        (None, None) => {
                            Some(true)
                        }
                        (_, _) => {
                            None
                        }
                    };

                    DataValue::Boolean(value)
                }
                BinaryOperator::NotEq => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 != v2)
                    } else {
                        None
                    };

                    DataValue::Boolean(value)
                }
                _ => todo!("unsupported operator")
            }
        },
        LogicalType::Double => {
            let left_value = unpack_f64(left.clone());
            let right_value = unpack_f64(right.clone().cast(&main_type));

            match op {
                BinaryOperator::Plus => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 + v2)
                    } else {
                        None
                    };

                    DataValue::Float64(value)
                }
                BinaryOperator::Minus => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 - v2)
                    } else {
                        None
                    };

                    DataValue::Float64(value)
                }
                BinaryOperator::Multiply => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 * v2)
                    } else {
                        None
                    };

                    DataValue::Float64(value)
                }
                BinaryOperator::Divide => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 / v2)
                    } else {
                        None
                    };

                    DataValue::Float64(value)
                }

                BinaryOperator::Gt => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 > v2)
                    } else {
                        None
                    };

                    DataValue::Boolean(value)
                }
                BinaryOperator::Lt => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 < v2)
                    } else {
                        None
                    };

                    DataValue::Boolean(value)
                }
                BinaryOperator::GtEq => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 >= v2)
                    } else {
                        None
                    };

                    DataValue::Boolean(value)
                }
                BinaryOperator::LtEq => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 <= v2)
                    } else {
                        None
                    };

                    DataValue::Boolean(value)
                }
                BinaryOperator::Eq => {
                    let value = match (left_value, right_value) {
                        (Some(v1), Some(v2)) => {
                            Some(v1 == v2)
                        }
                        (None, None) => {
                            Some(true)
                        }
                        (_, _) => {
                            None
                        }
                    };

                    DataValue::Boolean(value)
                }
                BinaryOperator::NotEq => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 != v2)
                    } else {
                        None
                    };

                    DataValue::Boolean(value)
                }
                _ => todo!("unsupported operator")
            }
        },
        LogicalType::Boolean => {
            let left_value = unpack_bool(left.clone());
            let right_value = unpack_bool(right.clone().cast(&main_type));

            match op {
                BinaryOperator::And => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 && v2)
                    } else {
                        None
                    };

                    DataValue::Boolean(value)
                }
                BinaryOperator::Or => {
                    let value = if let (Some(v1), Some(v2)) = (left_value, right_value) {
                        Some(v1 || v2)
                    } else {
                        None
                    };

                    DataValue::Boolean(value)
                }
                _ => todo!("unsupported operator")
            }
        },
        _ => todo!("unsupported data type"),
    }
}

#[cfg(test)]
mod test {
    use crate::expression::array_compute::binary_op_tp;
    use crate::expression::BinaryOperator;
    use crate::types::value::DataValue;

    #[test]
    fn test_binary_op_tp_arithmetic_plus() {
        let plus_i32_1 = binary_op_tp(&DataValue::Int32(None), &DataValue::Int32(None), &BinaryOperator::Plus);
        let plus_i32_2 = binary_op_tp(&DataValue::Int32(Some(1)), &DataValue::Int32(None), &BinaryOperator::Plus);
        let plus_i32_3 = binary_op_tp(&DataValue::Int32(None), &DataValue::Int32(Some(1)), &BinaryOperator::Plus);
        let plus_i32_4 = binary_op_tp(&DataValue::Int32(Some(1)), &DataValue::Int32(Some(1)), &BinaryOperator::Plus);

        assert_eq!(plus_i32_1, plus_i32_2);
        assert_eq!(plus_i32_2, plus_i32_3);
        assert_eq!(plus_i32_4, DataValue::Int32(Some(2)));

        let plus_i64_1 = binary_op_tp(&DataValue::Int64(None), &DataValue::Int64(None), &BinaryOperator::Plus);
        let plus_i64_2 = binary_op_tp(&DataValue::Int64(Some(1)), &DataValue::Int64(None), &BinaryOperator::Plus);
        let plus_i64_3 = binary_op_tp(&DataValue::Int64(None), &DataValue::Int64(Some(1)), &BinaryOperator::Plus);
        let plus_i64_4 = binary_op_tp(&DataValue::Int64(Some(1)), &DataValue::Int64(Some(1)), &BinaryOperator::Plus);

        assert_eq!(plus_i64_1, plus_i64_2);
        assert_eq!(plus_i64_2, plus_i64_3);
        assert_eq!(plus_i64_4, DataValue::Int64(Some(2)));

        let plus_f64_1 = binary_op_tp(&DataValue::Float64(None), &DataValue::Float64(None), &BinaryOperator::Plus);
        let plus_f64_2 = binary_op_tp(&DataValue::Float64(Some(1.0)), &DataValue::Float64(None), &BinaryOperator::Plus);
        let plus_f64_3 = binary_op_tp(&DataValue::Float64(None), &DataValue::Float64(Some(1.0)), &BinaryOperator::Plus);
        let plus_f64_4 = binary_op_tp(&DataValue::Float64(Some(1.0)), &DataValue::Float64(Some(1.0)), &BinaryOperator::Plus);

        assert_eq!(plus_f64_1, plus_f64_2);
        assert_eq!(plus_f64_2, plus_f64_3);
        assert_eq!(plus_f64_4, DataValue::Float64(Some(2.0)));
    }

    #[test]
    fn test_binary_op_tp_arithmetic_minus() {
        let minus_i32_1 = binary_op_tp(&DataValue::Int32(None), &DataValue::Int32(None), &BinaryOperator::Minus);
        let minus_i32_2 = binary_op_tp(&DataValue::Int32(Some(1)), &DataValue::Int32(None), &BinaryOperator::Minus);
        let minus_i32_3 = binary_op_tp(&DataValue::Int32(None), &DataValue::Int32(Some(1)), &BinaryOperator::Minus);
        let minus_i32_4 = binary_op_tp(&DataValue::Int32(Some(1)), &DataValue::Int32(Some(1)), &BinaryOperator::Minus);

        assert_eq!(minus_i32_1, minus_i32_2);
        assert_eq!(minus_i32_2, minus_i32_3);
        assert_eq!(minus_i32_4, DataValue::Int32(Some(0)));

        let minus_i64_1 = binary_op_tp(&DataValue::Int64(None), &DataValue::Int64(None), &BinaryOperator::Minus);
        let minus_i64_2 = binary_op_tp(&DataValue::Int64(Some(1)), &DataValue::Int64(None), &BinaryOperator::Minus);
        let minus_i64_3 = binary_op_tp(&DataValue::Int64(None), &DataValue::Int64(Some(1)), &BinaryOperator::Minus);
        let minus_i64_4 = binary_op_tp(&DataValue::Int64(Some(1)), &DataValue::Int64(Some(1)), &BinaryOperator::Minus);

        assert_eq!(minus_i64_1, minus_i64_2);
        assert_eq!(minus_i64_2, minus_i64_3);
        assert_eq!(minus_i64_4, DataValue::Int64(Some(0)));

        let minus_f64_1 = binary_op_tp(&DataValue::Float64(None), &DataValue::Float64(None), &BinaryOperator::Minus);
        let minus_f64_2 = binary_op_tp(&DataValue::Float64(Some(1.0)), &DataValue::Float64(None), &BinaryOperator::Minus);
        let minus_f64_3 = binary_op_tp(&DataValue::Float64(None), &DataValue::Float64(Some(1.0)), &BinaryOperator::Minus);
        let minus_f64_4 = binary_op_tp(&DataValue::Float64(Some(1.0)), &DataValue::Float64(Some(1.0)), &BinaryOperator::Minus);

        assert_eq!(minus_f64_1, minus_f64_2);
        assert_eq!(minus_f64_2, minus_f64_3);
        assert_eq!(minus_f64_4, DataValue::Float64(Some(0.0)));
    }

    #[test]
    fn test_binary_op_tp_arithmetic_multiply() {
        let multiply_i32_1 = binary_op_tp(&DataValue::Int32(None), &DataValue::Int32(None), &BinaryOperator::Multiply);
        let multiply_i32_2 = binary_op_tp(&DataValue::Int32(Some(1)), &DataValue::Int32(None), &BinaryOperator::Multiply);
        let multiply_i32_3 = binary_op_tp(&DataValue::Int32(None), &DataValue::Int32(Some(1)), &BinaryOperator::Multiply);
        let multiply_i32_4 = binary_op_tp(&DataValue::Int32(Some(1)), &DataValue::Int32(Some(1)), &BinaryOperator::Multiply);

        assert_eq!(multiply_i32_1, multiply_i32_2);
        assert_eq!(multiply_i32_2, multiply_i32_3);
        assert_eq!(multiply_i32_4, DataValue::Int32(Some(1)));

        let multiply_i64_1 = binary_op_tp(&DataValue::Int64(None), &DataValue::Int64(None), &BinaryOperator::Multiply);
        let multiply_i64_2 = binary_op_tp(&DataValue::Int64(Some(1)), &DataValue::Int64(None), &BinaryOperator::Multiply);
        let multiply_i64_3 = binary_op_tp(&DataValue::Int64(None), &DataValue::Int64(Some(1)), &BinaryOperator::Multiply);
        let multiply_i64_4 = binary_op_tp(&DataValue::Int64(Some(1)), &DataValue::Int64(Some(1)), &BinaryOperator::Multiply);

        assert_eq!(multiply_i64_1, multiply_i64_2);
        assert_eq!(multiply_i64_2, multiply_i64_3);
        assert_eq!(multiply_i64_4, DataValue::Int64(Some(1)));

        let multiply_f64_1 = binary_op_tp(&DataValue::Float64(None), &DataValue::Float64(None), &BinaryOperator::Multiply);
        let multiply_f64_2 = binary_op_tp(&DataValue::Float64(Some(1.0)), &DataValue::Float64(None), &BinaryOperator::Multiply);
        let multiply_f64_3 = binary_op_tp(&DataValue::Float64(None), &DataValue::Float64(Some(1.0)), &BinaryOperator::Multiply);
        let multiply_f64_4 = binary_op_tp(&DataValue::Float64(Some(1.0)), &DataValue::Float64(Some(1.0)), &BinaryOperator::Multiply);

        assert_eq!(multiply_f64_1, multiply_f64_2);
        assert_eq!(multiply_f64_2, multiply_f64_3);
        assert_eq!(multiply_f64_4, DataValue::Float64(Some(1.0)));
    }

    #[test]
    fn test_binary_op_tp_arithmetic_divide() {
        let divide_i32_1 = binary_op_tp(&DataValue::Int32(None), &DataValue::Int32(None), &BinaryOperator::Divide);
        let divide_i32_2 = binary_op_tp(&DataValue::Int32(Some(1)), &DataValue::Int32(None), &BinaryOperator::Divide);
        let divide_i32_3 = binary_op_tp(&DataValue::Int32(None), &DataValue::Int32(Some(1)), &BinaryOperator::Divide);
        let divide_i32_4 = binary_op_tp(&DataValue::Int32(Some(1)), &DataValue::Int32(Some(1)), &BinaryOperator::Divide);

        assert_eq!(divide_i32_1, divide_i32_2);
        assert_eq!(divide_i32_2, divide_i32_3);
        assert_eq!(divide_i32_4, DataValue::Int32(Some(1)));

        let divide_i64_1 = binary_op_tp(&DataValue::Int64(None), &DataValue::Int64(None), &BinaryOperator::Divide);
        let divide_i64_2 = binary_op_tp(&DataValue::Int64(Some(1)), &DataValue::Int64(None), &BinaryOperator::Divide);
        let divide_i64_3 = binary_op_tp(&DataValue::Int64(None), &DataValue::Int64(Some(1)), &BinaryOperator::Divide);
        let divide_i64_4 = binary_op_tp(&DataValue::Int64(Some(1)), &DataValue::Int64(Some(1)), &BinaryOperator::Divide);

        assert_eq!(divide_i64_1, divide_i64_2);
        assert_eq!(divide_i64_2, divide_i64_3);
        assert_eq!(divide_i64_4, DataValue::Int64(Some(1)));

        let divide_f64_1 = binary_op_tp(&DataValue::Float64(None), &DataValue::Float64(None), &BinaryOperator::Divide);
        let divide_f64_2 = binary_op_tp(&DataValue::Float64(Some(1.0)), &DataValue::Float64(None), &BinaryOperator::Divide);
        let divide_f64_3 = binary_op_tp(&DataValue::Float64(None), &DataValue::Float64(Some(1.0)), &BinaryOperator::Divide);
        let divide_f64_4 = binary_op_tp(&DataValue::Float64(Some(1.0)), &DataValue::Float64(Some(1.0)), &BinaryOperator::Divide);

        assert_eq!(divide_f64_1, divide_f64_2);
        assert_eq!(divide_f64_2, divide_f64_3);
        assert_eq!(divide_f64_4, DataValue::Float64(Some(1.0)));
    }

    #[test]
    fn test_binary_op_tp_cast() {
        let i32_cast_1 = binary_op_tp(&DataValue::Int32(Some(1)), &DataValue::Int8(Some(1)), &BinaryOperator::Plus);
        let i32_cast_2 = binary_op_tp(&DataValue::Int32(Some(1)), &DataValue::Int16(Some(1)), &BinaryOperator::Plus);

        assert_eq!(i32_cast_1, i32_cast_2);

        let i64_cast_1 = binary_op_tp(&DataValue::Int64(Some(1)), &DataValue::Int8(Some(1)), &BinaryOperator::Plus);
        let i64_cast_2 = binary_op_tp(&DataValue::Int64(Some(1)), &DataValue::Int16(Some(1)), &BinaryOperator::Plus);
        let i64_cast_3 = binary_op_tp(&DataValue::Int64(Some(1)), &DataValue::Int32(Some(1)), &BinaryOperator::Plus);

        assert_eq!(i64_cast_1, i64_cast_2);
        assert_eq!(i64_cast_2, i64_cast_3);

        let f64_cast_1 = binary_op_tp(&DataValue::Float64(Some(1.0)), &DataValue::Float32(Some(1.0)), &BinaryOperator::Plus);
        assert_eq!(f64_cast_1, DataValue::Float64(Some(2.0)));
    }

    #[test]
    fn test_binary_op_tp_i32_compare() {
        assert_eq!(binary_op_tp(&DataValue::Int32(Some(1)), &DataValue::Int32(Some(0)), &BinaryOperator::Gt), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op_tp(&DataValue::Int32(Some(1)), &DataValue::Int32(Some(0)), &BinaryOperator::Lt), DataValue::Boolean(Some(false)));
        assert_eq!(binary_op_tp(&DataValue::Int32(Some(1)), &DataValue::Int32(Some(1)), &BinaryOperator::GtEq), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op_tp(&DataValue::Int32(Some(1)), &DataValue::Int32(Some(1)), &BinaryOperator::LtEq), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op_tp(&DataValue::Int32(Some(1)), &DataValue::Int32(Some(1)), &BinaryOperator::NotEq), DataValue::Boolean(Some(false)));
        assert_eq!(binary_op_tp(&DataValue::Int32(Some(1)), &DataValue::Int32(Some(1)), &BinaryOperator::Eq), DataValue::Boolean(Some(true)));

        assert_eq!(binary_op_tp(&DataValue::Int32(None), &DataValue::Int32(Some(0)), &BinaryOperator::Gt), DataValue::Boolean(None));
        assert_eq!(binary_op_tp(&DataValue::Int32(None), &DataValue::Int32(Some(0)), &BinaryOperator::Lt), DataValue::Boolean(None));
        assert_eq!(binary_op_tp(&DataValue::Int32(None), &DataValue::Int32(Some(1)), &BinaryOperator::GtEq), DataValue::Boolean(None));
        assert_eq!(binary_op_tp(&DataValue::Int32(None), &DataValue::Int32(Some(1)), &BinaryOperator::LtEq), DataValue::Boolean(None));
        assert_eq!(binary_op_tp(&DataValue::Int32(None), &DataValue::Int32(Some(1)), &BinaryOperator::NotEq), DataValue::Boolean(None));

        assert_eq!(binary_op_tp(&DataValue::Int32(None), &DataValue::Int32(Some(1)), &BinaryOperator::Eq), DataValue::Boolean(None));
        assert_eq!(binary_op_tp(&DataValue::Int32(None), &DataValue::Int32(None), &BinaryOperator::Eq), DataValue::Boolean(Some(true)));
    }

    #[test]
    fn test_binary_op_tp_i64_compare() {
        assert_eq!(binary_op_tp(&DataValue::Int64(Some(1)), &DataValue::Int64(Some(0)), &BinaryOperator::Gt), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op_tp(&DataValue::Int64(Some(1)), &DataValue::Int64(Some(0)), &BinaryOperator::Lt), DataValue::Boolean(Some(false)));
        assert_eq!(binary_op_tp(&DataValue::Int64(Some(1)), &DataValue::Int64(Some(1)), &BinaryOperator::GtEq), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op_tp(&DataValue::Int64(Some(1)), &DataValue::Int64(Some(1)), &BinaryOperator::LtEq), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op_tp(&DataValue::Int64(Some(1)), &DataValue::Int64(Some(1)), &BinaryOperator::NotEq), DataValue::Boolean(Some(false)));
        assert_eq!(binary_op_tp(&DataValue::Int64(Some(1)), &DataValue::Int64(Some(1)), &BinaryOperator::Eq), DataValue::Boolean(Some(true)));

        assert_eq!(binary_op_tp(&DataValue::Int64(None), &DataValue::Int64(Some(0)), &BinaryOperator::Gt), DataValue::Boolean(None));
        assert_eq!(binary_op_tp(&DataValue::Int64(None), &DataValue::Int64(Some(0)), &BinaryOperator::Lt), DataValue::Boolean(None));
        assert_eq!(binary_op_tp(&DataValue::Int64(None), &DataValue::Int64(Some(1)), &BinaryOperator::GtEq), DataValue::Boolean(None));
        assert_eq!(binary_op_tp(&DataValue::Int64(None), &DataValue::Int64(Some(1)), &BinaryOperator::LtEq), DataValue::Boolean(None));
        assert_eq!(binary_op_tp(&DataValue::Int64(None), &DataValue::Int64(Some(1)), &BinaryOperator::NotEq), DataValue::Boolean(None));

        assert_eq!(binary_op_tp(&DataValue::Int64(None), &DataValue::Int64(Some(1)), &BinaryOperator::Eq), DataValue::Boolean(None));
        assert_eq!(binary_op_tp(&DataValue::Int64(None), &DataValue::Int64(None), &BinaryOperator::Eq), DataValue::Boolean(Some(true)));
    }

    #[test]
    fn test_binary_op_tp_f64_compare() {
        assert_eq!(binary_op_tp(&DataValue::Float64(Some(1.0)), &DataValue::Float64(Some(0.0)), &BinaryOperator::Gt), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op_tp(&DataValue::Float64(Some(1.0)), &DataValue::Float64(Some(0.0)), &BinaryOperator::Lt), DataValue::Boolean(Some(false)));
        assert_eq!(binary_op_tp(&DataValue::Float64(Some(1.0)), &DataValue::Float64(Some(1.0)), &BinaryOperator::GtEq), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op_tp(&DataValue::Float64(Some(1.0)), &DataValue::Float64(Some(1.0)), &BinaryOperator::LtEq), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op_tp(&DataValue::Float64(Some(1.0)), &DataValue::Float64(Some(1.0)), &BinaryOperator::NotEq), DataValue::Boolean(Some(false)));
        assert_eq!(binary_op_tp(&DataValue::Float64(Some(1.0)), &DataValue::Float64(Some(1.0)), &BinaryOperator::Eq), DataValue::Boolean(Some(true)));

        assert_eq!(binary_op_tp(&DataValue::Float64(None), &DataValue::Float64(Some(0.0)), &BinaryOperator::Gt), DataValue::Boolean(None));
        assert_eq!(binary_op_tp(&DataValue::Float64(None), &DataValue::Float64(Some(0.0)), &BinaryOperator::Lt), DataValue::Boolean(None));
        assert_eq!(binary_op_tp(&DataValue::Float64(None), &DataValue::Float64(Some(1.0)), &BinaryOperator::GtEq), DataValue::Boolean(None));
        assert_eq!(binary_op_tp(&DataValue::Float64(None), &DataValue::Float64(Some(1.0)), &BinaryOperator::LtEq), DataValue::Boolean(None));
        assert_eq!(binary_op_tp(&DataValue::Float64(None), &DataValue::Float64(Some(1.0)), &BinaryOperator::NotEq), DataValue::Boolean(None));

        assert_eq!(binary_op_tp(&DataValue::Float64(None), &DataValue::Float64(Some(1.0)), &BinaryOperator::Eq), DataValue::Boolean(None));
        assert_eq!(binary_op_tp(&DataValue::Float64(None), &DataValue::Float64(None), &BinaryOperator::Eq), DataValue::Boolean(Some(true)));
    }

    #[test]
    fn test_binary_op_tp_bool_compare() {
        assert_eq!(binary_op_tp(&DataValue::Boolean(Some(true)), &DataValue::Boolean(Some(true)), &BinaryOperator::And), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op_tp(&DataValue::Boolean(Some(false)), &DataValue::Boolean(Some(true)), &BinaryOperator::And), DataValue::Boolean(Some(false)));
        assert_eq!(binary_op_tp(&DataValue::Boolean(Some(false)), &DataValue::Boolean(Some(false)), &BinaryOperator::And), DataValue::Boolean(Some(false)));

        assert_eq!(binary_op_tp(&DataValue::Boolean(None), &DataValue::Boolean(Some(true)), &BinaryOperator::And), DataValue::Boolean(None));

        assert_eq!(binary_op_tp(&DataValue::Boolean(Some(true)), &DataValue::Boolean(Some(true)), &BinaryOperator::Or), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op_tp(&DataValue::Boolean(Some(false)), &DataValue::Boolean(Some(true)), &BinaryOperator::Or), DataValue::Boolean(Some(true)));
        assert_eq!(binary_op_tp(&DataValue::Boolean(Some(false)), &DataValue::Boolean(Some(false)), &BinaryOperator::Or), DataValue::Boolean(Some(false)));

        assert_eq!(binary_op_tp(&DataValue::Boolean(None), &DataValue::Boolean(Some(true)), &BinaryOperator::And), DataValue::Boolean(None));
    }
}
