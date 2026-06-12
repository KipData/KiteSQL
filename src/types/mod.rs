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

pub mod evaluator;
pub mod index;
pub mod serialize;
pub mod tuple;
pub mod tuple_builder;
pub mod value;

#[cfg(feature = "time")]
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
#[cfg(feature = "decimal")]
use rust_decimal::Decimal;
use std::any::TypeId;
use std::borrow::Cow;
use std::cmp;

use crate::errors::DatabaseError;
use kite_sql_serde_macros::ReferenceSerialization;
use ulid::Ulid;

pub type ColumnId = Ulid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum CharLengthUnits {
    Characters,
    Octets,
}

impl std::fmt::Display for CharLengthUnits {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Characters => write!(f, "CHARACTERS"),
            Self::Octets => write!(f, "OCTETS"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, ReferenceSerialization)]
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
    Time(Option<u64>),
    TimeStamp(Option<u64>, bool),
    // decimal (precision, scale)
    Decimal(Option<u8>, Option<u8>),
    Tuple(Vec<LogicalType>),
}

impl LogicalType {
    pub fn type_trans<T: 'static>() -> Option<LogicalType> {
        let type_id = TypeId::of::<T>();

        #[cfg(feature = "decimal")]
        if type_id == TypeId::of::<Decimal>() {
            return Some(LogicalType::Decimal(None, None));
        }

        if type_id == TypeId::of::<bool>() {
            Some(LogicalType::Boolean)
        } else if type_id == TypeId::of::<i8>() {
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
        } else if type_id == TypeId::of::<String>() {
            Some(LogicalType::Varchar(None, CharLengthUnits::Characters))
        } else {
            #[cfg(feature = "time")]
            {
                if type_id == TypeId::of::<NaiveDate>() {
                    return Some(LogicalType::Date);
                }
                if type_id == TypeId::of::<NaiveDateTime>() {
                    return Some(LogicalType::DateTime);
                }
                if type_id == TypeId::of::<NaiveTime>() {
                    return Some(LogicalType::Time(Some(0)));
                }
            }
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
            LogicalType::Time(_) => Some(4),
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

    pub fn max_logical_type<'a>(
        left: &'a LogicalType,
        right: &'a LogicalType,
    ) -> Result<Cow<'a, LogicalType>, DatabaseError> {
        if left == right {
            return Ok(Cow::Borrowed(left));
        }
        match (left, right) {
            // SqlNull type can be cast to anything
            (LogicalType::SqlNull, _) => return Ok(Cow::Borrowed(right)),
            (_, LogicalType::SqlNull) => return Ok(Cow::Borrowed(left)),
            (LogicalType::Tuple(types_0), LogicalType::Tuple(types_1)) => {
                if types_0.len() > types_1.len() {
                    return Ok(Cow::Borrowed(left));
                } else {
                    return Ok(Cow::Borrowed(right));
                }
            }
            _ => {}
        }
        if left.is_numeric() && right.is_numeric() {
            return LogicalType::combine_numeric_types(left, right);
        }
        #[cfg(feature = "time")]
        {
            if matches!(
                (left, right),
                (LogicalType::Date, LogicalType::Varchar(..))
                    | (LogicalType::Varchar(..), LogicalType::Date)
            ) {
                return Ok(Cow::Owned(LogicalType::Date));
            }
            if matches!(
                (left, right),
                (LogicalType::Date, LogicalType::DateTime)
                    | (LogicalType::DateTime, LogicalType::Date)
            ) {
                return Ok(Cow::Owned(LogicalType::DateTime));
            }
            if matches!(
                (left, right),
                (LogicalType::DateTime, LogicalType::Varchar(..))
                    | (LogicalType::Varchar(..), LogicalType::DateTime)
            ) {
                return Ok(Cow::Owned(LogicalType::DateTime));
            }
        }
        if let (LogicalType::Char(..), LogicalType::Varchar(..))
        | (LogicalType::Varchar(..), LogicalType::Char(..))
        | (LogicalType::Char(..), LogicalType::Char(..))
        | (LogicalType::Varchar(..), LogicalType::Varchar(..)) = (left, right)
        {
            return Ok(Cow::Owned(LogicalType::Varchar(
                None,
                CharLengthUnits::Characters,
            )));
        }
        Err(DatabaseError::Incomparable(left.clone(), right.clone()))
    }

    fn combine_numeric_types<'a>(
        left: &'a LogicalType,
        right: &'a LogicalType,
    ) -> Result<Cow<'a, LogicalType>, DatabaseError> {
        if left == right {
            return Ok(Cow::Borrowed(left));
        }
        if left.is_signed_numeric() && right.is_unsigned_numeric() {
            // this method is symmetric
            // arrange it so the left type is smaller
            // to limit the number of options we need to check
            return LogicalType::combine_numeric_types(right, left);
        }

        if LogicalType::can_implicit_cast(left, right) {
            return Ok(Cow::Borrowed(right));
        }
        if LogicalType::can_implicit_cast(right, left) {
            return Ok(Cow::Borrowed(left));
        }
        // we can't cast implicitly either way and types are not equal
        // this happens when left is signed and right is unsigned
        // e.g. INTEGER and UINTEGER
        // in this case we need to upcast to make sure the types fit
        match (left, right) {
            (LogicalType::Bigint, _) | (_, LogicalType::UBigint) => {
                Ok(Cow::Owned(LogicalType::Double))
            }
            (LogicalType::Integer, _) | (_, LogicalType::UInteger) => {
                Ok(Cow::Owned(LogicalType::Bigint))
            }
            (LogicalType::Smallint, _) | (_, LogicalType::USmallint) => {
                Ok(Cow::Owned(LogicalType::Integer))
            }
            (LogicalType::Tinyint, _) | (_, LogicalType::UTinyint) => {
                Ok(Cow::Owned(LogicalType::Smallint))
            }
            (
                LogicalType::Decimal(precision_0, scale_0),
                LogicalType::Decimal(precision_1, scale_1),
            ) => {
                let fn_option = |num_0: &Option<u8>, num_1: &Option<u8>| match (num_0, num_1) {
                    (Some(num_0), Some(num_1)) => Some(*cmp::max(num_0, num_1)),
                    (Some(num), None) | (None, Some(num)) => Some(*num),
                    (None, None) => None,
                };
                Ok(Cow::Owned(LogicalType::Decimal(
                    fn_option(precision_0, precision_1),
                    fn_option(scale_0, scale_1),
                )))
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
            LogicalType::Char(len, units) => write!(f, "Char({len}, {units})")?,
            LogicalType::Varchar(len, units) => write!(f, "Varchar({len:?}, {units})")?,
            LogicalType::Date => write!(f, "Date")?,
            LogicalType::DateTime => write!(f, "DateTime")?,
            LogicalType::TimeStamp(precision, zone) => {
                write!(f, "TimeStamp({precision:?}, {zone:?})")?
            }
            LogicalType::Time(precision) => write!(f, "Time({precision:?})")?,
            LogicalType::Decimal(precision, scale) => {
                write!(f, "Decimal({precision:?}, {scale:?})")?
            }
            LogicalType::Tuple(types) => {
                write!(f, "(")?;
                let mut first = true;
                for ty in types {
                    if !first {
                        write!(f, ", ")?;
                    }
                    first = false;
                    write!(f, "{ty}")?;
                }
                write!(f, ")")?
            }
        }

        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) mod test {
    use crate::errors::DatabaseError;
    use crate::serdes::{ReferenceSerialization, ReferenceTables};
    use crate::storage::rocksdb::RocksTransaction;
    use crate::types::CharLengthUnits;
    use crate::types::LogicalType;
    use std::io::{Cursor, Seek, SeekFrom};

    #[test]
    fn test_logic_type_serialization() -> Result<(), DatabaseError> {
        fn fn_assert(
            cursor: &mut Cursor<Vec<u8>>,
            reference_tables: &mut ReferenceTables,
            logical_type: LogicalType,
        ) -> Result<(), DatabaseError> {
            let mut arena = crate::planner::TableArena::default();
            logical_type.encode(cursor, false, reference_tables, &arena)?;

            cursor.seek(SeekFrom::Start(0))?;
            assert_eq!(
                LogicalType::decode::<RocksTransaction, _, _>(
                    cursor,
                    None,
                    reference_tables,
                    &mut arena,
                )?,
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
            LogicalType::Time(Some(3)),
        )?;
        fn_assert(&mut cursor, &mut reference_tables, LogicalType::Time(None))?;
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
