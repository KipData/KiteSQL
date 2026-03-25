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
use crate::execution::{ExecArena, ExecId, ExecNode, ExecutionCaches, ReadExecutor};
use crate::planner::operator::values::ValuesOperator;
use crate::storage::Transaction;
use crate::types::tuple::SchemaRef;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use std::mem;

pub struct Values {
    rows: std::vec::IntoIter<Vec<DataValue>>,
    schema_ref: SchemaRef,
}

impl From<ValuesOperator> for Values {
    fn from(ValuesOperator { rows, schema_ref }: ValuesOperator) -> Self {
        Values {
            rows: rows.into_iter(),
            schema_ref,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Values {
    fn into_executor(
        self,
        arena: &mut ExecArena<'a, T>,
        _: ExecutionCaches<'a>,
        _: *mut T,
    ) -> ExecId {
        arena.push(ExecNode::Values(self))
    }
}

impl Values {
    pub(crate) fn next_tuple<'a, T: Transaction + 'a>(
        &mut self,
        _: &mut ExecArena<'a, T>,
    ) -> Result<Option<Tuple>, DatabaseError> {
        let Some(mut values) = self.rows.next() else {
            return Ok(None);
        };

        for (i, value) in values.iter_mut().enumerate() {
            let ty = self.schema_ref[i].datatype().clone();

            if value.logical_type() != ty {
                *value = mem::replace(value, DataValue::Null).cast(&ty)?;
            }
        }

        Ok(Some(Tuple::new(None, values)))
    }
}
