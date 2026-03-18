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

//! KiteSQL is a high-performance SQL database
//! that can be embedded in Rust code (based on RocksDB by default),
//! making it possible to call SQL just like calling a function.
//! It supports most of the syntax of SQL 2016.
//!
//! KiteSQL provides thread-safe API: [`DataBase::run`](db::Database::run) for running SQL
//!
//! KiteSQL uses [`DataBaseBuilder`](db::DataBaseBuilder) for instance construction,
//! configuration in builder mode
//!
//! Support type
//! - SqlNull
//! - Boolean
//! - Tinyint
//! - UTinyint
//! - Smallint
//! - USmallint
//! - Integer
//! - UInteger
//! - Bigint
//! - UBigint
//! - Float
//! - Double
//! - Char
//! - Varchar
//! - Date
//! - DateTime
//! - Time
//! - Tuple
//!
//! support optimistic transaction with the
//! [`Database::new_transaction`](db::Database::new_transaction) method.
//!
//! support UDF (User-Defined Function) so that users can customize internal calculation functions
//! with the [`DataBaseBuilder::register_function`](db::DataBaseBuilder::register_scala_function)
//!
//! # Examples
//!
//! ```ignore
//! use kite_sql::db::{DataBaseBuilder, ResultIter};
//! use kite_sql::errors::DatabaseError;
//! use kite_sql::Model;
//!
//! #[derive(Default, Debug, PartialEq, Model)]
//! #[model(table = "my_struct")]
//! struct MyStruct {
//!     #[model(primary_key)]
//!     pub c1: i32,
//!     pub c2: String,
//! }
//!
//! #[cfg(feature = "orm")]
//! fn main() -> Result<(), DatabaseError> {
//!     let database = DataBaseBuilder::path("./hello_world").build()?;
//!
//!     database.create_table_if_not_exists::<MyStruct>()?;
//!     database.insert(&MyStruct {
//!         c1: 0,
//!         c2: "zero".to_string(),
//!     })?;
//!     database.insert(&MyStruct {
//!         c1: 1,
//!         c2: "one".to_string(),
//!     })?;
//!
//!     for row in database.fetch::<MyStruct>()? {
//!         println!("{:?}", row?);
//!     }
//!     database.drop_table::<MyStruct>()?;
//!
//!     Ok(())
//! }
//! ```
#![allow(unused_doc_comments)]
extern crate core;

pub mod binder;
pub mod catalog;
pub mod db;
pub mod errors;
pub mod execution;
pub mod expression;
mod function;
#[cfg(feature = "macros")]
pub mod macros;
mod optimizer;
#[cfg(feature = "orm")]
pub mod orm;
pub mod parser;
pub mod planner;
#[cfg(all(not(target_arch = "wasm32"), feature = "python"))]
pub mod python;
pub mod serdes;
pub mod storage;
pub mod types;
pub(crate) mod utils;
#[cfg(target_arch = "wasm32")]
pub mod wasm;

#[cfg(feature = "orm")]
pub use kite_sql_serde_macros::Model;
#[cfg(feature = "orm")]
pub use kite_sql_serde_macros::Projection;
