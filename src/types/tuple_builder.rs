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
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, Utf8Type};
use crate::types::CharLengthUnits;
use crate::types::LogicalType;

pub struct TupleBuilder<'a> {
    column_types: Vec<LogicalType>,
    pk_indices: Option<&'a PrimaryKeyIndices>,
}

impl<'a> TupleBuilder<'a> {
    pub fn new(column_types: Vec<LogicalType>, pk_indices: Option<&'a PrimaryKeyIndices>) -> Self {
        TupleBuilder {
            column_types,
            pk_indices,
        }
    }

    pub fn build_result(message: String) -> Tuple {
        let values = vec![DataValue::Utf8 {
            value: message,
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        }];

        Tuple::new(None, values)
    }

    pub fn build_result_into(tuple: &mut Tuple, message: String) {
        tuple.pk = None;
        tuple.values.clear();
        tuple.values.push(DataValue::Utf8 {
            value: message,
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        });
    }

    pub fn build_with_row<'b>(
        &self,
        row: impl IntoIterator<Item = &'b str>,
    ) -> Result<Tuple, DatabaseError> {
        let mut values = Vec::with_capacity(self.column_types.len());

        for (i, value) in row.into_iter().enumerate() {
            values.push(
                DataValue::Utf8 {
                    value: value.to_string(),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                }
                .cast(&self.column_types[i])?,
            );
        }
        if values.len() != self.column_types.len() {
            return Err(DatabaseError::MisMatch("types", "values"));
        }

        let pk = self
            .pk_indices
            .map(|indices| Tuple::primary_projection(indices, &values));

        Ok(Tuple::new(pk, values))
    }
}
