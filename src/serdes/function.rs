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
use crate::serdes::{ReferenceDecodeContext, ReferenceSerialization, ReferenceTables};
use crate::storage::Transaction;
use std::io::{Read, Write};

impl ReferenceSerialization for ArcScalarFunctionImpl {
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        self.summary().encode(writer, is_direct, reference_tables)
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        context: Option<&ReferenceDecodeContext<'_, T>>,
        reference_tables: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        let summary = FunctionSummary::decode(reader, context, reference_tables)?;
        let Some(functions) = context.and_then(ReferenceDecodeContext::scala_functions) else {
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
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        self.summary().encode(writer, is_direct, reference_tables)
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        context: Option<&ReferenceDecodeContext<'_, T>>,
        reference_tables: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        let summary = FunctionSummary::decode(reader, context, reference_tables)?;
        let Some(functions) = context.and_then(ReferenceDecodeContext::table_functions) else {
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

        Ok(Self(function.clone()))
    }
}
