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
use crate::types::{CharLengthUnits, LogicalType};
use sqlparser::ast::{BinaryLength, CharacterLength, DataType, ExactNumberInfo, ObjectNamePart};
use std::cmp;

/// Maps sqlparser data types to KiteSQL types while statements are converted.
///
/// Return `Some(Ok(type))` to override the built-in mapping, `Some(Err(..))` to reject the
/// type, or `None` to use the built-in mapping.
pub trait DataTypeMapper {
    /// Maps a type inside an expression, such as `CAST(.. AS type)` and typed string literals.
    fn map_data_type(&self, _data_type: &DataType) -> Option<Result<LogicalType, DatabaseError>> {
        None
    }

    /// Maps the type of a column definition, in `CREATE TABLE` and `ALTER TABLE`.
    ///
    /// The default implementation delegates to [`map_data_type`](Self::map_data_type).
    fn map_column_type(&self, data_type: &DataType) -> Option<Result<LogicalType, DatabaseError>> {
        self.map_data_type(data_type)
    }
}

/// The built-in type mapping.
impl DataTypeMapper for () {}

fn char_length(
    length: Option<CharacterLength>,
    max_name: &str,
) -> Result<(Option<u64>, CharLengthUnits), DatabaseError> {
    match length {
        None => Ok((None, CharLengthUnits::Characters)),
        Some(CharacterLength::IntegerLength { length, unit }) => Ok((
            Some(length),
            match unit {
                Some(sqlparser::ast::CharLengthUnits::Octets) => CharLengthUnits::Octets,
                _ => CharLengthUnits::Characters,
            },
        )),
        Some(CharacterLength::Max) => Err(DatabaseError::UnsupportedStmt(format!(
            "{max_name}(MAX) is not supported"
        ))),
    }
}

#[cfg(not(feature = "time"))]
fn time_feature() -> Result<LogicalType, DatabaseError> {
    Err(DatabaseError::UnsupportedStmt(
        "time types require the `time` feature".to_string(),
    ))
}

/// The built-in mapping of a sqlparser data type.
pub(crate) fn logical_type(data_type: DataType) -> Result<LogicalType, DatabaseError> {
    Ok(match data_type {
        DataType::Char(length) | DataType::Character(length) => {
            let (len, unit) = char_length(length, "CHAR")?;
            LogicalType::Char(cmp::max(len.unwrap_or(1), 1) as u32, unit)
        }
        DataType::CharVarying(length)
        | DataType::CharacterVarying(length)
        | DataType::Varchar(length) => {
            let (len, unit) = char_length(length, "VARCHAR")?;
            LogicalType::Varchar(len.map(|len| len as u32), unit)
        }
        DataType::Varbinary(length) => LogicalType::Varbinary(match length {
            Some(BinaryLength::IntegerLength { length }) => Some(u32::try_from(length)?),
            None | Some(BinaryLength::Max) => None,
        }),
        DataType::Blob(length) | DataType::Bytes(length) => {
            LogicalType::Varbinary(length.map(u32::try_from).transpose()?)
        }
        DataType::Bytea => LogicalType::Varbinary(None),
        DataType::String(_) | DataType::Text => {
            LogicalType::Varchar(None, CharLengthUnits::Characters)
        }
        DataType::Float(_) | DataType::Float4 | DataType::Float32 | DataType::Real => {
            LogicalType::Float
        }
        DataType::Double(_) | DataType::DoublePrecision | DataType::Float8 | DataType::Float64 => {
            LogicalType::Double
        }
        DataType::TinyInt(_) => LogicalType::Tinyint,
        DataType::TinyIntUnsigned(_) | DataType::UTinyInt => LogicalType::UTinyint,
        DataType::SmallInt(_) | DataType::Int2(_) => LogicalType::Smallint,
        DataType::SmallIntUnsigned(_) | DataType::Int2Unsigned(_) | DataType::USmallInt => {
            LogicalType::USmallint
        }
        DataType::Int(_) | DataType::Integer(_) | DataType::Int4(_) | DataType::Int32 => {
            LogicalType::Integer
        }
        DataType::IntUnsigned(_)
        | DataType::IntegerUnsigned(_)
        | DataType::Int4Unsigned(_)
        | DataType::Unsigned
        | DataType::UnsignedInteger
        | DataType::UInt32 => LogicalType::UInteger,
        DataType::BigInt(_) | DataType::Int8(_) | DataType::Int64 => LogicalType::Bigint,
        DataType::BigIntUnsigned(_)
        | DataType::Int8Unsigned(_)
        | DataType::UBigInt
        | DataType::UInt64 => LogicalType::UBigint,
        DataType::Boolean => LogicalType::Boolean,
        #[cfg(feature = "time")]
        DataType::Date => LogicalType::Date,
        #[cfg(feature = "time")]
        DataType::Datetime(precision) => {
            if precision.is_some() {
                return Err(DatabaseError::UnsupportedStmt(
                    "time's precision".to_string(),
                ));
            }
            LogicalType::DateTime
        }
        #[cfg(feature = "time")]
        DataType::Time(precision, info) => {
            if !matches!(precision, Some(0..5) | None) {
                return Err(DatabaseError::UnsupportedStmt(
                    "time's precision must be less than 5".to_string(),
                ));
            }
            if !matches!(info, sqlparser::ast::TimezoneInfo::None) {
                return Err(DatabaseError::UnsupportedStmt(
                    "time's zone is not supported".to_string(),
                ));
            }
            LogicalType::Time(precision)
        }
        #[cfg(feature = "time")]
        DataType::Timestamp(precision, info) => {
            if !matches!(precision, Some(3 | 6 | 9) | None) {
                return Err(DatabaseError::UnsupportedStmt(
                    "timestamp's precision must be 3,6,9".to_string(),
                ));
            }
            let zone = matches!(info, sqlparser::ast::TimezoneInfo::WithTimeZone);
            LogicalType::TimeStamp(precision, zone)
        }
        #[cfg(not(feature = "time"))]
        DataType::Date | DataType::Datetime(_) | DataType::Time(..) | DataType::Timestamp(..) => {
            return time_feature()
        }
        DataType::Decimal(info)
        | DataType::DecimalUnsigned(info)
        | DataType::Dec(info)
        | DataType::DecUnsigned(info)
        | DataType::Numeric(info) => decimal(info)?,
        DataType::Custom(name, args) => {
            if name.0.len() == 1 {
                if let ObjectNamePart::Identifier(ident) = &name.0[0] {
                    if ident.value.eq_ignore_ascii_case("vector") {
                        if args.len() != 1 {
                            return Err(DatabaseError::UnsupportedStmt(
                                "VECTOR type requires exactly one dimension argument".to_string(),
                            ));
                        }
                        let dim = args[0].parse::<u32>().map_err(|_| {
                            DatabaseError::UnsupportedStmt(format!(
                                "VECTOR dimension must be a positive integer, got: {}",
                                args[0]
                            ))
                        })?;
                        if dim == 0 {
                            return Err(DatabaseError::UnsupportedStmt(
                                "VECTOR dimension must be greater than 0".to_string(),
                            ));
                        }
                        return Ok(LogicalType::Vector(dim));
                    }
                }
            }
            return Err(DatabaseError::UnsupportedStmt(format!(
                "unsupported custom data type: {name}"
            )));
        }
        other => {
            return Err(DatabaseError::UnsupportedStmt(format!(
                "unsupported data type: {other}"
            )))
        }
    })
}

#[cfg(feature = "decimal")]
fn decimal(info: ExactNumberInfo) -> Result<LogicalType, DatabaseError> {
    Ok(match info {
        ExactNumberInfo::None => LogicalType::Decimal(None, None),
        ExactNumberInfo::Precision(p) => LogicalType::Decimal(Some(p as u8), None),
        ExactNumberInfo::PrecisionAndScale(p, s) => {
            LogicalType::Decimal(Some(p as u8), Some(s as u8))
        }
    })
}

#[cfg(not(feature = "decimal"))]
fn decimal(_: ExactNumberInfo) -> Result<LogicalType, DatabaseError> {
    Err(DatabaseError::UnsupportedStmt(
        "DECIMAL requires the `decimal` feature".to_string(),
    ))
}

// GRCOV_EXCL_START
#[cfg(test)]
mod tests {
    use super::logical_type;
    use crate::errors::DatabaseError;
    use crate::types::LogicalType;
    #[cfg(feature = "decimal")]
    use sqlparser::ast::ExactNumberInfo;
    #[cfg(feature = "time")]
    use sqlparser::ast::TimezoneInfo;
    use sqlparser::ast::{CharacterLength, DataType};

    #[test]
    fn maps_unsigned_widths_and_validates_precision() -> Result<(), DatabaseError> {
        for (sql, expected) in [
            (DataType::TinyIntUnsigned(None), LogicalType::UTinyint),
            (DataType::SmallIntUnsigned(None), LogicalType::USmallint),
            (DataType::UnsignedInteger, LogicalType::UInteger),
            (DataType::UInt64, LogicalType::UBigint),
        ] {
            assert_eq!(logical_type(sql)?, expected);
        }
        for sql in [
            DataType::Char(Some(CharacterLength::Max)),
            DataType::Varchar(Some(CharacterLength::Max)),
        ] {
            assert!(matches!(
                logical_type(sql),
                Err(DatabaseError::UnsupportedStmt(_))
            ));
        }
        #[cfg(feature = "time")]
        {
            assert_eq!(
                logical_type(DataType::Time(Some(4), TimezoneInfo::None))?,
                LogicalType::Time(Some(4))
            );
            assert_eq!(
                logical_type(DataType::Timestamp(Some(6), TimezoneInfo::WithTimeZone))?,
                LogicalType::TimeStamp(Some(6), true)
            );
            for sql in [
                DataType::Time(Some(5), TimezoneInfo::None),
                DataType::Time(None, TimezoneInfo::WithTimeZone),
                DataType::Timestamp(Some(2), TimezoneInfo::None),
                DataType::Datetime(Some(3)),
            ] {
                assert!(matches!(
                    logical_type(sql),
                    Err(DatabaseError::UnsupportedStmt(_))
                ));
            }
        }
        #[cfg(feature = "decimal")]
        {
            assert_eq!(
                logical_type(DataType::Numeric(ExactNumberInfo::Precision(12)))?,
                LogicalType::Decimal(Some(12), None)
            );
            assert_eq!(
                logical_type(DataType::Dec(ExactNumberInfo::PrecisionAndScale(12, 3)))?,
                LogicalType::Decimal(Some(12), Some(3))
            );
        }
        {
            use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};
            let vector = DataType::Custom(
                ObjectName(vec![ObjectNamePart::Identifier(Ident::new("vector"))]),
                vec!["3".to_string()],
            );
            assert_eq!(logical_type(vector)?, LogicalType::Vector(3));
            let zero = DataType::Custom(
                ObjectName(vec![ObjectNamePart::Identifier(Ident::new("VECTOR"))]),
                vec!["0".to_string()],
            );
            assert!(matches!(
                logical_type(zero),
                Err(DatabaseError::UnsupportedStmt(_))
            ));
        }
        Ok(())
    }
}
// GRCOV_EXCL_STOP
