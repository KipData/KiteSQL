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
use crate::expression::{BinaryOperator, UnaryOperator};
use crate::serdes::{ReferenceDecodeContext, ReferenceSerialization, ReferenceTables};
use crate::storage::Transaction;
use crate::types::evaluator::{
    binary_create, cast_create, unary_create, BinaryEvaluatorBox, CastEvaluatorBox,
    UnaryEvaluatorBox,
};
use crate::types::LogicalType;
use std::borrow::Cow;
use std::io::{Read, Write};

impl ReferenceSerialization for UnaryEvaluatorBox {
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        self.ty.encode(writer, is_direct, reference_tables)?;
        self.op.encode(writer, is_direct, reference_tables)
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        context: Option<&ReferenceDecodeContext<'_, T>>,
        reference_tables: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        let ty = LogicalType::decode(reader, context, reference_tables)?;
        let op = UnaryOperator::decode(reader, context, reference_tables)?;
        unary_create(Cow::Owned(ty), op)
    }
}

impl ReferenceSerialization for BinaryEvaluatorBox {
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        self.ty.encode(writer, is_direct, reference_tables)?;
        self.op.encode(writer, is_direct, reference_tables)
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        context: Option<&ReferenceDecodeContext<'_, T>>,
        reference_tables: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        let ty = LogicalType::decode(reader, context, reference_tables)?;
        let op = BinaryOperator::decode(reader, context, reference_tables)?;
        binary_create(Cow::Owned(ty), op)
    }
}

impl ReferenceSerialization for CastEvaluatorBox {
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        self.from.encode(writer, is_direct, reference_tables)?;
        self.to.encode(writer, is_direct, reference_tables)
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        context: Option<&ReferenceDecodeContext<'_, T>>,
        reference_tables: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        let from = LogicalType::decode(reader, context, reference_tables)?;
        let to = LogicalType::decode(reader, context, reference_tables)?;
        cast_create(Cow::Owned(from), Cow::Owned(to))
    }
}
