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

use crate::serdes::DatabaseError;
use crate::serdes::Transaction;
use crate::serdes::{ReferenceSerialization, ReferenceTables};
use std::io::{Read, Write};
use std::sync::Arc;

#[macro_export]
macro_rules! implement_ptr_serialization {
    ($struct_name:ident) => {
        impl<V> ReferenceSerialization for $struct_name<V>
        where
            V: ReferenceSerialization,
        {
            fn encode<W: Write, A: crate::planner::MetaArena>(
                &self,
                writer: &mut W,
                is_direct: bool,
                reference_tables: &mut ReferenceTables,
                arena: &A,
            ) -> Result<(), DatabaseError> {
                self.as_ref()
                    .encode(writer, is_direct, reference_tables, arena)
            }

            fn decode<T: Transaction, R: Read, A: crate::planner::MetaArena>(
                reader: &mut R,
                drive: Option<&crate::serdes::ReferenceDecodeContext<'_, T>>,
                reference_tables: &ReferenceTables,
                arena: &mut A,
            ) -> Result<Self, DatabaseError>
            where
                Self: Sized,
            {
                Ok($struct_name::from(V::decode(
                    reader,
                    drive,
                    reference_tables,
                    arena,
                )?))
            }
        }
    };
}

implement_ptr_serialization!(Arc);
implement_ptr_serialization!(Box);
