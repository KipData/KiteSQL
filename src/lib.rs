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
//! use kite_sql::implement_from_tuple;
//! use kite_sql::types::value::DataValue;
//!
//! #[derive(Default, Debug, PartialEq)]
//! struct MyStruct {
//!     pub c1: i32,
//!     pub c2: String,
//! }
//!
//! implement_from_tuple!(
//!     MyStruct, (
//!         c1: i32 => |inner: &mut MyStruct, value| {
//!             if let DataValue::Int32(Some(val)) = value {
//!                 inner.c1 = val;
//!             }
//!         },
//!         c2: String => |inner: &mut MyStruct, value| {
//!             if let DataValue::Utf8 { value: Some(val), .. } = value {
//!                 inner.c2 = val;
//!             }
//!         }
//!     )
//! );
//!
//! #[cfg(feature = "macros")]
//! fn main() -> Result<(), DatabaseError> {
//!     let database = DataBaseBuilder::path("./hello_world").build()?;
//!
//!     database
//!         .run("create table if not exists my_struct (c1 int primary key, c2 int)")?
//!         .done()?;
//!     database
//!         .run("insert into my_struct values(0, 0), (1, 1)")?
//!         .done()?;
//!
//!     let iter = database.run("select * from my_struct")?;
//!     let schema = iter.schema().clone();
//!
//!     for tuple in iter {
//!         println!("{:?}", MyStruct::from((&schema, tuple?)));
//!     }
//!     database.run("drop table my_struct")?.done()?;
//!
//!     Ok(())
//! }
//! ```
#![feature(error_generic_member_access)]
#![allow(unused_doc_comments)]
#![feature(result_flattening)]
#![feature(coroutines)]
#![feature(coroutine_trait)]
#![feature(iterator_try_collect)]
#![feature(slice_pattern)]
#![feature(stmt_expr_attributes)]
#![feature(random)]
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
pub mod parser;
pub mod planner;
pub mod serdes;
pub mod storage;
pub mod types;
pub(crate) mod utils;
