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
use crate::expression::function::scala::ArcScalarFunctionImpl;
use crate::expression::function::table::ArcTableFunctionImpl;
use crate::expression::function::FunctionSummary;
use crate::planner::MetaArena;
use crate::serdes::{ReferenceDecodeContext, ReferenceSerialization};
use crate::storage::StorageTransaction;
use std::io::{Read, Write};

impl ReferenceSerialization for ArcScalarFunctionImpl {
    fn encode<W: Write, A: MetaArena + ?Sized>(
        &self,
        writer: &mut W,
        is_direct: bool,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        self.summary().encode(writer, is_direct, arena)
    }

    fn decode<T: StorageTransaction, R: Read, A: MetaArena + ?Sized>(
        reader: &mut R,
        mut context: Option<&mut ReferenceDecodeContext<'_, T>>,
        arena: &mut A,
    ) -> Result<Self, DatabaseError> {
        let summary = FunctionSummary::decode(reader, context.as_deref_mut(), arena)?;
        let Some(functions) = context
            .as_deref()
            .and_then(ReferenceDecodeContext::scala_functions)
        else {
            return Err(DatabaseError::InvalidValue(format!(
                "scalar function decode context missing for {}",
                summary.name
            )));
        };
        let Some(function) = functions.get(&summary) else {
            return Err(DatabaseError::InvalidValue(format!(
                "scalar function not found when decoding: {}",
                summary.name
            )));
        };

        Ok(Self(function.clone()))
    }
}

impl ReferenceSerialization for ArcTableFunctionImpl {
    fn encode<W: Write, A: MetaArena + ?Sized>(
        &self,
        writer: &mut W,
        is_direct: bool,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        self.summary().encode(writer, is_direct, arena)
    }

    fn decode<T: StorageTransaction, R: Read, A: MetaArena + ?Sized>(
        reader: &mut R,
        mut context: Option<&mut ReferenceDecodeContext<'_, T>>,
        arena: &mut A,
    ) -> Result<Self, DatabaseError> {
        let summary = FunctionSummary::decode(reader, context.as_deref_mut(), arena)?;
        let Some(functions) = context
            .as_deref()
            .and_then(ReferenceDecodeContext::table_functions)
        else {
            return Err(DatabaseError::InvalidValue(format!(
                "table function decode context missing for {}",
                summary.name
            )));
        };
        let Some(function) = functions.get(&summary) else {
            return Err(DatabaseError::InvalidValue(format!(
                "table function not found when decoding: {}",
                summary.name
            )));
        };

        Ok(function.inner.clone())
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::db::{ScalaFunctions, TableFunctions};
    use crate::planner::TableArena;
    use crate::serdes::ReferenceDecodeContext;
    use crate::storage::rocksdb::RocksTransaction;
    use crate::types::LogicalType;
    use std::io::Cursor;

    fn encoded_summary(name: &str) -> Vec<u8> {
        let summary = FunctionSummary {
            name: name.into(),
            arg_types: vec![LogicalType::Integer],
        };
        let mut bytes = Vec::new();
        summary
            .encode(&mut bytes, false, &TableArena::default())
            .unwrap();
        bytes
    }

    #[test]
    fn scalar_function_decode_requires_context_and_registration() {
        let bytes = encoded_summary("missing_scalar");
        let mut arena = TableArena::default();

        let err = ArcScalarFunctionImpl::decode::<RocksTransaction, _, _>(
            &mut Cursor::new(&bytes),
            None,
            &mut arena,
        )
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("scalar function decode context missing"));

        let scalars = ScalaFunctions::default();
        let table_functions = TableFunctions::default();
        let mut context = ReferenceDecodeContext::with_functions(&scalars, &table_functions);
        let err = ArcScalarFunctionImpl::decode::<RocksTransaction, _, _>(
            &mut Cursor::new(&bytes),
            Some(&mut context),
            &mut arena,
        )
        .unwrap_err();
        assert!(err.to_string().contains("scalar function not found"));
    }

    #[test]
    fn table_function_decode_requires_context_and_registration() {
        let bytes = encoded_summary("missing_table");
        let mut arena = TableArena::default();

        let err = ArcTableFunctionImpl::decode::<RocksTransaction, _, _>(
            &mut Cursor::new(&bytes),
            None,
            &mut arena,
        )
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("table function decode context missing"));

        let scalars = ScalaFunctions::default();
        let table_functions = TableFunctions::default();
        let mut context = ReferenceDecodeContext::with_functions(&scalars, &table_functions);
        let err = ArcTableFunctionImpl::decode::<RocksTransaction, _, _>(
            &mut Cursor::new(&bytes),
            Some(&mut context),
            &mut arena,
        )
        .unwrap_err();
        assert!(err.to_string().contains("table function not found"));
    }
}
// GRCOV_EXCL_STOP
