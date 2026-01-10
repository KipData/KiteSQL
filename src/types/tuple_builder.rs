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

use crate::catalog::PrimaryKeyIndices;
use crate::errors::DatabaseError;
use crate::types::tuple::{Schema, Tuple};
use crate::types::value::{DataValue, Utf8Type};
use sqlparser::ast::CharLengthUnits;

pub struct TupleBuilder<'a> {
    schema: &'a Schema,
    pk_indices: Option<&'a PrimaryKeyIndices>,
}

impl<'a> TupleBuilder<'a> {
    pub fn new(schema: &'a Schema, pk_indices: Option<&'a PrimaryKeyIndices>) -> Self {
        TupleBuilder { schema, pk_indices }
    }

    pub fn build_result(message: String) -> Tuple {
        let values = vec![DataValue::Utf8 {
            value: message,
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        }];

        Tuple::new(None, values)
    }

    pub fn build_with_row<'b>(
        &self,
        row: impl IntoIterator<Item = &'b str>,
    ) -> Result<Tuple, DatabaseError> {
        let mut values = Vec::with_capacity(self.schema.len());

        for (i, value) in row.into_iter().enumerate() {
            values.push(
                DataValue::Utf8 {
                    value: value.to_string(),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                }
                .cast(self.schema[i].datatype())?,
            );
        }
        if values.len() != self.schema.len() {
            return Err(DatabaseError::MisMatch("types", "values"));
        }

        let pk = self
            .pk_indices
            .map(|indices| Tuple::primary_projection(indices, &values));

        Ok(Tuple::new(pk, values))
    }
}
