pub mod evaluator;
pub mod index;
pub mod tuple;
pub mod tuple_builder;
pub mod value;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::any::TypeId;
use std::cmp;

use crate::errors::DatabaseError;
use kite_sql_serde_macros::ReferenceSerialization;
use sqlparser::ast::{CharLengthUnits, ExactNumberInfo, TimezoneInfo};
use ulid::Ulid;

pub type ColumnId = Ulid;

/// Sqlrs type conversion:
/// sqlparser::ast::DataType -> LogicalType -> arrow::datatypes::DataType
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    ReferenceSerialization,
)]
pub enum LogicalType {
    SqlNull,
    Boolean,
    Tinyint,
    UTinyint,
    Smallint,
    USmallint,
    Integer,
    UInteger,
    Bigint,
    UBigint,
    Float,
    Double,
    Char(u32, CharLengthUnits),
    Varchar(Option<u32>, CharLengthUnits),
    Date,
    DateTime,
    Time(Option<u64>, bool),
    TimeStamp(Option<u64>, bool),
    // decimal (precision, scale)
    Decimal(Option<u8>, Option<u8>),
    Tuple(Vec<LogicalType>),
}

impl LogicalType {
    pub fn type_trans<T: 'static>() -> Option<LogicalType> {
        let type_id = TypeId::of::<T>();

        if type_id == TypeId::of::<i8>() {
            Some(LogicalType::Tinyint)
        } else if type_id == TypeId::of::<i16>() {
            Some(LogicalType::Smallint)
        } else if type_id == TypeId::of::<i32>() {
            Some(LogicalType::Integer)
        } else if type_id == TypeId::of::<i64>() {
            Some(LogicalType::Bigint)
        } else if type_id == TypeId::of::<u8>() {
            Some(LogicalType::UTinyint)
        } else if type_id == TypeId::of::<u16>() {
            Some(LogicalType::USmallint)
        } else if type_id == TypeId::of::<u32>() {
            Some(LogicalType::UInteger)
        } else if type_id == TypeId::of::<u64>() {
            Some(LogicalType::UBigint)
        } else if type_id == TypeId::of::<f32>() {
            Some(LogicalType::Float)
        } else if type_id == TypeId::of::<f64>() {
            Some(LogicalType::Double)
        } else if type_id == TypeId::of::<NaiveDate>() {
            Some(LogicalType::Date)
        } else if type_id == TypeId::of::<NaiveDateTime>() {
            Some(LogicalType::DateTime)
        } else if type_id == TypeId::of::<NaiveTime>() {
            Some(LogicalType::Time(Some(0), false))
        } else if type_id == TypeId::of::<Decimal>() {
            Some(LogicalType::Decimal(None, None))
        } else if type_id == TypeId::of::<String>() {
            Some(LogicalType::Varchar(None, CharLengthUnits::Characters))
        } else {
            None
        }
    }

    pub fn raw_len(&self) -> Option<usize> {
        match self {
            LogicalType::SqlNull => Some(0),
            LogicalType::Boolean => Some(1),
            LogicalType::Tinyint => Some(1),
            LogicalType::UTinyint => Some(1),
            LogicalType::Smallint => Some(2),
            LogicalType::USmallint => Some(2),
            LogicalType::Integer => Some(4),
            LogicalType::UInteger => Some(4),
            LogicalType::Bigint => Some(8),
            LogicalType::UBigint => Some(8),
            LogicalType::Float => Some(4),
            LogicalType::Double => Some(8),
            /// Note: The non-fixed length type's raw_len is None e.g. Varchar
            LogicalType::Varchar(_, _) => None,
            LogicalType::Char(len, unit) => match unit {
                CharLengthUnits::Characters => None,
                CharLengthUnits::Octets => Some(*len as usize),
            },
            LogicalType::Decimal(_, _) => Some(16),
            LogicalType::Date => Some(4),
            LogicalType::DateTime => Some(8),
            LogicalType::Time(_, _) => Some(4),
            LogicalType::TimeStamp(_, _) => Some(8),
            LogicalType::Tuple(_) => unreachable!(),
        }
    }

    pub fn numeric() -> Vec<LogicalType> {
        vec![
            LogicalType::Tinyint,
            LogicalType::UTinyint,
            LogicalType::Smallint,
            LogicalType::USmallint,
            LogicalType::Integer,
            LogicalType::UInteger,
            LogicalType::Bigint,
            LogicalType::UBigint,
            LogicalType::Float,
            LogicalType::Double,
        ]
    }

    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            LogicalType::Tinyint
                | LogicalType::UTinyint
                | LogicalType::Smallint
                | LogicalType::USmallint
                | LogicalType::Integer
                | LogicalType::UInteger
                | LogicalType::Bigint
                | LogicalType::UBigint
                | LogicalType::Float
                | LogicalType::Double
                | LogicalType::Decimal(_, _)
        )
    }

    pub fn is_signed_numeric(&self) -> bool {
        matches!(
            self,
            LogicalType::Tinyint
                | LogicalType::Smallint
                | LogicalType::Integer
                | LogicalType::Bigint
        )
    }

    pub fn is_unsigned_numeric(&self) -> bool {
        matches!(
            self,
            LogicalType::UTinyint
                | LogicalType::USmallint
                | LogicalType::UInteger
                | LogicalType::UBigint
        )
    }

    pub fn is_floating_point_numeric(&self) -> bool {
        matches!(self, LogicalType::Float | LogicalType::Double)
    }

    pub fn max_logical_type(
        left: &LogicalType,
        right: &LogicalType,
    ) -> Result<LogicalType, DatabaseError> {
        if left == right {
            return Ok(left.clone());
        }
        match (left, right) {
            // SqlNull type can be cast to anything
            (LogicalType::SqlNull, _) => return Ok(right.clone()),
            (_, LogicalType::SqlNull) => return Ok(left.clone()),
            (LogicalType::Tuple(types_0), LogicalType::Tuple(types_1)) => {
                if types_0.len() > types_1.len() {
                    return Ok(left.clone());
                } else {
                    return Ok(right.clone());
                }
            }
            _ => {}
        }
        if left.is_numeric() && right.is_numeric() {
            return LogicalType::combine_numeric_types(left, right);
        }
        if matches!(
            (left, right),
            (LogicalType::Date, LogicalType::Varchar(..))
                | (LogicalType::Varchar(..), LogicalType::Date)
        ) {
            return Ok(LogicalType::Date);
        }
        if matches!(
            (left, right),
            (LogicalType::Date, LogicalType::DateTime) | (LogicalType::DateTime, LogicalType::Date)
        ) {
            return Ok(LogicalType::DateTime);
        }
        if matches!(
            (left, right),
            (LogicalType::DateTime, LogicalType::Varchar(..))
                | (LogicalType::Varchar(..), LogicalType::DateTime)
        ) {
            return Ok(LogicalType::DateTime);
        }
        if let (LogicalType::Char(..), LogicalType::Varchar(..))
        | (LogicalType::Varchar(..), LogicalType::Char(..))
        | (LogicalType::Char(..), LogicalType::Char(..))
        | (LogicalType::Varchar(..), LogicalType::Varchar(..)) = (left, right)
        {
            return Ok(LogicalType::Varchar(None, CharLengthUnits::Characters));
        }
        Err(DatabaseError::Incomparable(left.clone(), right.clone()))
    }

    fn combine_numeric_types(
        left: &LogicalType,
        right: &LogicalType,
    ) -> Result<LogicalType, DatabaseError> {
        if left == right {
            return Ok(left.clone());
        }
        if left.is_signed_numeric() && right.is_unsigned_numeric() {
            // this method is symmetric
            // arrange it so the left type is smaller
            // to limit the number of options we need to check
            return LogicalType::combine_numeric_types(right, left);
        }

        if LogicalType::can_implicit_cast(left, right) {
            return Ok(right.clone());
        }
        if LogicalType::can_implicit_cast(right, left) {
            return Ok(left.clone());
        }
        // we can't cast implicitly either way and types are not equal
        // this happens when left is signed and right is unsigned
        // e.g. INTEGER and UINTEGER
        // in this case we need to upcast to make sure the types fit
        match (left, right) {
            (LogicalType::Bigint, _) | (_, LogicalType::UBigint) => Ok(LogicalType::Double),
            (LogicalType::Integer, _) | (_, LogicalType::UInteger) => Ok(LogicalType::Bigint),
            (LogicalType::Smallint, _) | (_, LogicalType::USmallint) => Ok(LogicalType::Integer),
            (LogicalType::Tinyint, _) | (_, LogicalType::UTinyint) => Ok(LogicalType::Smallint),
            (
                LogicalType::Decimal(precision_0, scale_0),
                LogicalType::Decimal(precision_1, scale_1),
            ) => {
                let fn_option = |num_0: &Option<u8>, num_1: &Option<u8>| match (num_0, num_1) {
                    (Some(num_0), Some(num_1)) => Some(*cmp::max(num_0, num_1)),
                    (Some(num), None) | (None, Some(num)) => Some(*num),
                    (None, None) => None,
                };
                Ok(LogicalType::Decimal(
                    fn_option(precision_0, precision_1),
                    fn_option(scale_0, scale_1),
                ))
            }
            _ => Err(DatabaseError::Incomparable(left.clone(), right.clone())),
        }
    }

    pub fn can_implicit_cast(from: &LogicalType, to: &LogicalType) -> bool {
        if from == to {
            return true;
        }
        match from {
            LogicalType::SqlNull => true,
            LogicalType::Boolean => false,
            LogicalType::Tinyint => matches!(
                to,
                LogicalType::Smallint
                    | LogicalType::Integer
                    | LogicalType::Bigint
                    | LogicalType::Float
                    | LogicalType::Double
                    | LogicalType::Decimal(_, _)
            ),
            LogicalType::UTinyint => matches!(
                to,
                LogicalType::USmallint
                    | LogicalType::UInteger
                    | LogicalType::UBigint
                    | LogicalType::Smallint
                    | LogicalType::Integer
                    | LogicalType::Bigint
                    | LogicalType::Float
                    | LogicalType::Double
                    | LogicalType::Decimal(_, _)
            ),
            LogicalType::Smallint => matches!(
                to,
                LogicalType::Integer
                    | LogicalType::Bigint
                    | LogicalType::Float
                    | LogicalType::Double
                    | LogicalType::Decimal(_, _)
            ),
            LogicalType::USmallint => matches!(
                to,
                LogicalType::UInteger
                    | LogicalType::UBigint
                    | LogicalType::Integer
                    | LogicalType::Bigint
                    | LogicalType::Float
                    | LogicalType::Double
                    | LogicalType::Decimal(_, _)
            ),
            LogicalType::Integer => matches!(
                to,
                LogicalType::Bigint
                    | LogicalType::Float
                    | LogicalType::Double
                    | LogicalType::Decimal(_, _)
            ),
            LogicalType::UInteger => matches!(
                to,
                LogicalType::UBigint
                    | LogicalType::Bigint
                    | LogicalType::Float
                    | LogicalType::Double
                    | LogicalType::Decimal(_, _)
            ),
            LogicalType::Bigint => matches!(
                to,
                LogicalType::Float | LogicalType::Double | LogicalType::Decimal(_, _)
            ),
            LogicalType::UBigint => matches!(
                to,
                LogicalType::Float | LogicalType::Double | LogicalType::Decimal(_, _)
            ),
            LogicalType::Float => matches!(to, LogicalType::Double | LogicalType::Decimal(_, _)),
            LogicalType::Double => matches!(to, LogicalType::Decimal(_, _)),
            LogicalType::Char(..) => false,
            LogicalType::Varchar(..) => false,
            LogicalType::Date => matches!(
                to,
                LogicalType::DateTime | LogicalType::Varchar(..) | LogicalType::Char(..)
            ),
            LogicalType::DateTime | LogicalType::TimeStamp(_, _) => matches!(
                to,
                LogicalType::Date
                    | LogicalType::Time(..)
                    | LogicalType::Varchar(..)
                    | LogicalType::Char(..)
            ),
            LogicalType::Time(..) => {
                matches!(to, LogicalType::Varchar(..) | LogicalType::Char(..))
            }
            LogicalType::Decimal(_, _) | LogicalType::Tuple(_) => false,
        }
    }
}

/// sqlparser datatype to logical type
impl TryFrom<sqlparser::ast::DataType> for LogicalType {
    type Error = DatabaseError;

    fn try_from(value: sqlparser::ast::DataType) -> Result<Self, Self::Error> {
        match value {
            sqlparser::ast::DataType::Char(char_len)
            | sqlparser::ast::DataType::Character(char_len) => {
                let mut len = 1;
                let mut char_unit = None;
                if let Some(sqlparser::ast::CharacterLength { length, unit }) = char_len {
                    len = cmp::max(len, length);
                    char_unit = unit;
                }
                Ok(LogicalType::Char(
                    len as u32,
                    char_unit.unwrap_or(CharLengthUnits::Characters),
                ))
            }
            sqlparser::ast::DataType::CharVarying(varchar_len)
            | sqlparser::ast::DataType::CharacterVarying(varchar_len)
            | sqlparser::ast::DataType::Varchar(varchar_len) => {
                let mut len = None;
                let mut char_unit = None;
                if let Some(sqlparser::ast::CharacterLength { length, unit }) = varchar_len {
                    len = Some(length as u32);
                    char_unit = unit;
                }
                Ok(LogicalType::Varchar(
                    len,
                    char_unit.unwrap_or(CharLengthUnits::Characters),
                ))
            }
            sqlparser::ast::DataType::String | sqlparser::ast::DataType::Text => {
                Ok(LogicalType::Varchar(None, CharLengthUnits::Characters))
            }
            sqlparser::ast::DataType::Float(_) => Ok(LogicalType::Float),
            sqlparser::ast::DataType::Real => Ok(LogicalType::Float),
            sqlparser::ast::DataType::Double | sqlparser::ast::DataType::DoublePrecision => {
                Ok(LogicalType::Double)
            }
            sqlparser::ast::DataType::TinyInt(_) => Ok(LogicalType::Tinyint),
            sqlparser::ast::DataType::UnsignedTinyInt(_) => Ok(LogicalType::UTinyint),
            sqlparser::ast::DataType::SmallInt(_) => Ok(LogicalType::Smallint),
            sqlparser::ast::DataType::UnsignedSmallInt(_) => Ok(LogicalType::USmallint),
            sqlparser::ast::DataType::Int(_) | sqlparser::ast::DataType::Integer(_) => {
                Ok(LogicalType::Integer)
            }
            sqlparser::ast::DataType::UnsignedInt(_)
            | sqlparser::ast::DataType::UnsignedInteger(_) => Ok(LogicalType::UInteger),
            sqlparser::ast::DataType::BigInt(_) => Ok(LogicalType::Bigint),
            sqlparser::ast::DataType::UnsignedBigInt(_) => Ok(LogicalType::UBigint),
            sqlparser::ast::DataType::Boolean => Ok(LogicalType::Boolean),
            sqlparser::ast::DataType::Date => Ok(LogicalType::Date),
            sqlparser::ast::DataType::Datetime(precision) => {
                if precision.is_some() {
                    return Err(DatabaseError::UnsupportedStmt(
                        "time's precision".to_string(),
                    ));
                }
                Ok(LogicalType::DateTime)
            }
            sqlparser::ast::DataType::Time(precision, info) => {
                match precision {
                    Some(0..5) | None => (),
                    _ => {
                        return Err(DatabaseError::UnsupportedStmt(
                            "time's precision must less than 5".to_string(),
                        ))
                    }
                }
                if !matches!(info, TimezoneInfo::None) {
                    return Err(DatabaseError::UnsupportedStmt(
                        "time's zone is not supported".to_string(),
                    ));
                }
                Ok(LogicalType::Time(precision, false))
            }
            sqlparser::ast::DataType::Timestamp(precision, info) => {
                let mut zone = false;
                match precision {
                    Some(3 | 6 | 9) | None => (),
                    _ => {
                        return Err(DatabaseError::UnsupportedStmt(
                            "timestamp's precision must 3,6,9".to_string(),
                        ))
                    }
                }
                if matches!(info, TimezoneInfo::WithTimeZone) {
                    zone = true;
                }
                Ok(LogicalType::TimeStamp(precision, zone))
            }
            sqlparser::ast::DataType::Decimal(info) | sqlparser::ast::DataType::Dec(info) => {
                match info {
                    ExactNumberInfo::None => Ok(Self::Decimal(None, None)),
                    ExactNumberInfo::Precision(p) => Ok(Self::Decimal(Some(p as u8), None)),
                    ExactNumberInfo::PrecisionAndScale(p, s) => {
                        Ok(Self::Decimal(Some(p as u8), Some(s as u8)))
                    }
                }
            }
            other => Err(DatabaseError::UnsupportedStmt(format!(
                "unsupported data type: {other}"
            ))),
        }
    }
}

impl std::fmt::Display for LogicalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogicalType::SqlNull => write!(f, "SqlNull")?,
            LogicalType::Boolean => write!(f, "Boolean")?,
            LogicalType::Tinyint => write!(f, "Tinyint")?,
            LogicalType::UTinyint => write!(f, "UTinyint")?,
            LogicalType::Smallint => write!(f, "Smallint")?,
            LogicalType::USmallint => write!(f, "USmallint")?,
            LogicalType::Integer => write!(f, "Integer")?,
            LogicalType::UInteger => write!(f, "UInteger")?,
            LogicalType::Bigint => write!(f, "Bigint")?,
            LogicalType::UBigint => write!(f, "UBigint")?,
            LogicalType::Float => write!(f, "Float")?,
            LogicalType::Double => write!(f, "Double")?,
            LogicalType::Char(len, units) => write!(f, "Char({}, {})", len, units)?,
            LogicalType::Varchar(len, units) => write!(f, "Varchar({:?}, {})", len, units)?,
            LogicalType::Date => write!(f, "Date")?,
            LogicalType::DateTime => write!(f, "DateTime")?,
            LogicalType::TimeStamp(precision, zone) => {
                write!(f, "TimeStamp({:?}, {:?})", precision, zone)?
            }
            LogicalType::Time(precision, zone) => write!(f, "Time({:?}, {:?})", precision, zone)?,
            LogicalType::Decimal(precision, scale) => {
                write!(f, "Decimal({:?}, {:?})", precision, scale)?
            }
            LogicalType::Tuple(types) => {
                write!(f, "(")?;
                let mut first = true;
                for ty in types {
                    if !first {
                        write!(f, ", ")?;
                    }
                    first = false;
                    write!(f, "{}", ty)?;
                }
                write!(f, ")")?
            }
        }

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod test {
    use crate::errors::DatabaseError;
    use crate::serdes::{ReferenceSerialization, ReferenceTables};
    use crate::storage::rocksdb::RocksTransaction;
    use crate::types::LogicalType;
    use sqlparser::ast::CharLengthUnits;
    use std::io::{Cursor, Seek, SeekFrom};

    #[test]
    fn test_logic_type_serialization() -> Result<(), DatabaseError> {
        fn fn_assert(
            cursor: &mut Cursor<Vec<u8>>,
            reference_tables: &mut ReferenceTables,
            logical_type: LogicalType,
        ) -> Result<(), DatabaseError> {
            logical_type.encode(cursor, false, reference_tables)?;

            cursor.seek(SeekFrom::Start(0))?;
            assert_eq!(
                LogicalType::decode::<RocksTransaction, _>(cursor, None, reference_tables)?,
                logical_type
            );
            cursor.seek(SeekFrom::Start(0))?;

            Ok(())
        }

        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();

        fn_assert(&mut cursor, &mut reference_tables, LogicalType::SqlNull)?;
        fn_assert(&mut cursor, &mut reference_tables, LogicalType::Boolean)?;
        fn_assert(&mut cursor, &mut reference_tables, LogicalType::Tinyint)?;
        fn_assert(&mut cursor, &mut reference_tables, LogicalType::UTinyint)?;
        fn_assert(&mut cursor, &mut reference_tables, LogicalType::Smallint)?;
        fn_assert(&mut cursor, &mut reference_tables, LogicalType::USmallint)?;
        fn_assert(&mut cursor, &mut reference_tables, LogicalType::Integer)?;
        fn_assert(&mut cursor, &mut reference_tables, LogicalType::UInteger)?;
        fn_assert(&mut cursor, &mut reference_tables, LogicalType::Bigint)?;
        fn_assert(&mut cursor, &mut reference_tables, LogicalType::UBigint)?;
        fn_assert(&mut cursor, &mut reference_tables, LogicalType::Float)?;
        fn_assert(&mut cursor, &mut reference_tables, LogicalType::Double)?;
        fn_assert(
            &mut cursor,
            &mut reference_tables,
            LogicalType::Char(42, CharLengthUnits::Characters),
        )?;
        fn_assert(
            &mut cursor,
            &mut reference_tables,
            LogicalType::Char(42, CharLengthUnits::Octets),
        )?;
        fn_assert(
            &mut cursor,
            &mut reference_tables,
            LogicalType::Varchar(Some(42), CharLengthUnits::Characters),
        )?;
        fn_assert(
            &mut cursor,
            &mut reference_tables,
            LogicalType::Varchar(None, CharLengthUnits::Characters),
        )?;
        fn_assert(
            &mut cursor,
            &mut reference_tables,
            LogicalType::Varchar(Some(42), CharLengthUnits::Octets),
        )?;
        fn_assert(
            &mut cursor,
            &mut reference_tables,
            LogicalType::Varchar(None, CharLengthUnits::Octets),
        )?;
        fn_assert(&mut cursor, &mut reference_tables, LogicalType::Date)?;
        fn_assert(&mut cursor, &mut reference_tables, LogicalType::DateTime)?;
        fn_assert(
            &mut cursor,
            &mut reference_tables,
            LogicalType::Time(Some(3), true),
        )?;
        fn_assert(
            &mut cursor,
            &mut reference_tables,
            LogicalType::Time(Some(3), false),
        )?;
        fn_assert(
            &mut cursor,
            &mut reference_tables,
            LogicalType::Time(None, true),
        )?;
        fn_assert(
            &mut cursor,
            &mut reference_tables,
            LogicalType::Time(None, false),
        )?;
        fn_assert(
            &mut cursor,
            &mut reference_tables,
            LogicalType::TimeStamp(Some(3), true),
        )?;
        fn_assert(
            &mut cursor,
            &mut reference_tables,
            LogicalType::TimeStamp(Some(3), false),
        )?;
        fn_assert(
            &mut cursor,
            &mut reference_tables,
            LogicalType::TimeStamp(None, true),
        )?;
        fn_assert(
            &mut cursor,
            &mut reference_tables,
            LogicalType::TimeStamp(None, false),
        )?;
        fn_assert(
            &mut cursor,
            &mut reference_tables,
            LogicalType::Decimal(Some(4), Some(2)),
        )?;
        fn_assert(
            &mut cursor,
            &mut reference_tables,
            LogicalType::Decimal(Some(4), None),
        )?;
        fn_assert(
            &mut cursor,
            &mut reference_tables,
            LogicalType::Decimal(None, Some(2)),
        )?;
        fn_assert(
            &mut cursor,
            &mut reference_tables,
            LogicalType::Decimal(None, None),
        )?;
        fn_assert(
            &mut cursor,
            &mut reference_tables,
            LogicalType::Tuple(vec![LogicalType::Integer]),
        )?;

        Ok(())
    }
}
