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
