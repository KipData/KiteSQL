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
use crate::planner::MetaArena;
use crate::serdes::ReferenceSerialization;
use crate::storage::StorageTransaction;
use std::io::{Read, Write};
use std::marker::PhantomData;

impl<V> ReferenceSerialization for PhantomData<V> {
    fn encode<W: Write, A: MetaArena + ?Sized>(
        &self,
        _: &mut W,
        _: bool,
        _: &A,
    ) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn decode<T: StorageTransaction, R: Read, A: MetaArena + ?Sized>(
        _: &mut R,
        _: Option<&mut crate::serdes::ReferenceDecodeContext<'_, T>>,
        _: &mut A,
    ) -> Result<Self, DatabaseError> {
        Ok(PhantomData)
    }
}
