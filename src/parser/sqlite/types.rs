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

use super::unsupported;
use crate::errors::DatabaseError;
use crate::types::{CharLengthUnits, LogicalType};
use sqlite3_parser::ast::{Expr, Type, TypeSize};

/// The KiteSQL type of a SQLite type name, such as `VARCHAR(10)` or `UNSIGNED BIG INT`.
///
/// SQLite accepts any type name; KiteSQL needs one it knows.
pub(super) fn logical_type(ty: &Type<'_>) -> Result<LogicalType, DatabaseError> {
    let name = ty.name.to_ascii_uppercase();
    #[cfg_attr(not(feature = "decimal"), allow(unused_variables))]
    let (first, second) = match &ty.size {
        None => (None, None),
        Some(TypeSize::MaxSize(size)) => (Some(size_value(size)?), None),
        Some(TypeSize::TypeSize(first, second)) => {
            (Some(size_value(first)?), Some(size_value(second)?))
        }
    };
    let characters = CharLengthUnits::Characters;
    Ok(match name.as_str() {
        "CHAR" | "CHARACTER" | "NCHAR" | "NATIVE CHARACTER" => {
            LogicalType::Char(first.unwrap_or(1).max(1), characters)
        }
        "VARCHAR" | "VARYING CHARACTER" | "NVARCHAR" | "CHARACTER VARYING" => {
            LogicalType::Varchar(first, characters)
        }
        "TEXT" | "CLOB" | "STRING" => LogicalType::Varchar(None, characters),
        "BLOB" | "BYTEA" => LogicalType::Varbinary(None),
        "VARBINARY" => LogicalType::Varbinary(first),
        "TINYINT" => LogicalType::Tinyint,
        "SMALLINT" | "INT2" => LogicalType::Smallint,
        "INT" | "INTEGER" | "MEDIUMINT" | "INT4" => LogicalType::Integer,
        "BIGINT" | "INT8" | "UNSIGNED BIG INT" => LogicalType::Bigint,
        "REAL" | "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" => LogicalType::Double,
        "FLOAT" | "FLOAT4" => LogicalType::Float,
        "BOOLEAN" | "BOOL" => LogicalType::Boolean,
        #[cfg(feature = "time")]
        "DATE" => LogicalType::Date,
        #[cfg(feature = "time")]
        "DATETIME" => LogicalType::DateTime,
        #[cfg(feature = "time")]
        "TIME" => LogicalType::Time(first.map(u64::from)),
        #[cfg(feature = "time")]
        "TIMESTAMP" => LogicalType::TimeStamp(first.map(u64::from), false),
        #[cfg(feature = "decimal")]
        "DECIMAL" | "NUMERIC" => LogicalType::Decimal(
            first.map(u8::try_from).transpose()?,
            second.map(u8::try_from).transpose()?,
        ),
        "VECTOR" => {
            let Some(dim) = first else {
                return Err(unsupported(
                    "VECTOR type requires exactly one dimension argument",
                ));
            };
            if second.is_some() {
                return Err(unsupported(
                    "VECTOR type requires exactly one dimension argument",
                ));
            }
            if dim == 0 {
                return Err(unsupported("VECTOR dimension must be greater than 0"));
            }
            LogicalType::Vector(dim)
        }
        _ => return Err(unsupported(format!("unsupported data type: {}", ty.name))),
    })
}

fn size_value(size: &Expr<'_>) -> Result<u32, DatabaseError> {
    size.is_integer()
        .and_then(|size| u32::try_from(size).ok())
        .ok_or_else(|| DatabaseError::InvalidValue(format!("invalid type size: {size}")))
}
