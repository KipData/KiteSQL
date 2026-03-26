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

use crate::catalog::ColumnRef;
use crate::db::ResultIter;
use crate::errors::DatabaseError;
use crate::storage::table_codec::BumpBytes;
use crate::types::serialize::{TupleValueSerializable, TupleValueSerializableImpl};
use crate::types::value::DataValue;
use bumpalo::Bump;
use comfy_table::{Cell, Table};
use itertools::Itertools;
use std::io::Cursor;
use std::sync::Arc;

const BITS_MAX_INDEX: usize = 8;

pub type TupleId = DataValue;
pub type Schema = Vec<ColumnRef>;
pub type SchemaRef = Arc<Schema>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Tuple {
    pub pk: Option<TupleId>,
    pub values: Vec<DataValue>,
}

impl Default for Tuple {
    fn default() -> Self {
        Self {
            pk: None,
            values: Vec::new(),
        }
    }
}

impl<'a> From<&'a Tuple> for &'a [DataValue] {
    fn from(val: &'a Tuple) -> Self {
        val.values.as_slice()
    }
}

impl Tuple {
    pub fn new(pk: Option<TupleId>, values: Vec<DataValue>) -> Self {
        Tuple { pk, values }
    }

    #[inline]
    pub fn deserialize_from_into(
        &mut self,
        deserializers: &[TupleValueSerializableImpl],
        bytes: &[u8],
        total_len: usize,
    ) -> Result<(), DatabaseError> {
        fn is_null(bits: u8, i: usize) -> bool {
            bits & (1 << (7 - i)) > 0
        }

        let bits_len = (total_len + BITS_MAX_INDEX) / BITS_MAX_INDEX;
        self.values.clear();
        self.values.reserve(deserializers.len());

        let mut cursor = Cursor::new(&bytes[bits_len..]);

        for (i, deserializer) in deserializers.iter().enumerate() {
            if is_null(bytes[i / BITS_MAX_INDEX], i % BITS_MAX_INDEX) {
                self.values.push(DataValue::Null);
                continue;
            }
            deserializer.filling_value(&mut cursor, &mut self.values)?;
        }
        Ok(())
    }

    /// e.g.: bits(u8)..|data_0(len for utf8_1)|utf8_0|data_1|
    /// Tips: all len is u32
    pub fn serialize_to<'a>(
        &self,
        serializers: &[TupleValueSerializableImpl],
        arena: &'a Bump,
    ) -> Result<BumpBytes<'a>, DatabaseError> {
        debug_assert_eq!(self.values.len(), serializers.len());

        fn flip_bit(bits: u8, i: usize) -> u8 {
            bits | (1 << (7 - i))
        }

        let values_len = self.values.len();
        let bits_len = (values_len + BITS_MAX_INDEX) / BITS_MAX_INDEX;
        let mut bytes = BumpBytes::new_in(arena);
        bytes.resize(bits_len, 0u8);
        let null_bytes: *mut BumpBytes = &mut bytes;

        debug_assert_eq!(self.values.len(), serializers.len());
        for (i, (value, serializer)) in self.values.iter().zip(serializers.iter()).enumerate() {
            if value.is_null() {
                let null_bytes = unsafe { &mut *null_bytes };
                null_bytes[i / BITS_MAX_INDEX] =
                    flip_bit(null_bytes[i / BITS_MAX_INDEX], i % BITS_MAX_INDEX);
            } else {
                serializer.to_raw(value, &mut bytes)?;
            }
        }
        Ok(bytes)
    }

    pub fn primary_projection(pk_indices: &[usize], values: &[DataValue]) -> TupleId {
        if pk_indices.len() > 1 {
            DataValue::Tuple(
                pk_indices.iter().map(|i| values[*i].clone()).collect_vec(),
                false,
            )
        } else {
            values[pk_indices[0]].clone()
        }
    }
}

pub fn create_table<I: ResultIter>(iter: I) -> Result<Table, DatabaseError> {
    let mut table = Table::new();
    let mut header = Vec::new();
    let schema = iter.schema().clone();

    for col in schema.iter() {
        header.push(Cell::new(col.full_name()));
    }
    table.set_header(header);

    for tuple in iter {
        let tuple = tuple?;
        debug_assert_eq!(schema.len(), tuple.values.len());

        let cells = tuple
            .values
            .iter()
            .map(|value| Cell::new(format!("{value}")))
            .collect_vec();

        table.add_row(cells);
    }

    Ok(table)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::types::tuple::Tuple;
    use crate::types::value::{DataValue, Utf8Type};
    use crate::types::LogicalType;
    use bumpalo::Bump;
    use itertools::Itertools;
    use ordered_float::OrderedFloat;
    use rust_decimal::Decimal;
    use sqlparser::ast::CharLengthUnits;
    use std::sync::Arc;

    #[test]
    fn test_tuple_serialize_to_and_deserialize_from() {
        let columns = Arc::new(vec![
            ColumnRef::from(ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, Some(0), false, None).unwrap(),
            )),
            ColumnRef::from(ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::UInteger, None, false, None).unwrap(),
            )),
            ColumnRef::from(ColumnCatalog::new(
                "c3".to_string(),
                false,
                ColumnDesc::new(
                    LogicalType::Varchar(Some(2), CharLengthUnits::Characters),
                    None,
                    false,
                    None,
                )
                .unwrap(),
            )),
            ColumnRef::from(ColumnCatalog::new(
                "c4".to_string(),
                false,
                ColumnDesc::new(LogicalType::Smallint, None, false, None).unwrap(),
            )),
            ColumnRef::from(ColumnCatalog::new(
                "c5".to_string(),
                false,
                ColumnDesc::new(LogicalType::USmallint, None, false, None).unwrap(),
            )),
            ColumnRef::from(ColumnCatalog::new(
                "c6".to_string(),
                false,
                ColumnDesc::new(LogicalType::Float, None, false, None).unwrap(),
            )),
            ColumnRef::from(ColumnCatalog::new(
                "c7".to_string(),
                false,
                ColumnDesc::new(LogicalType::Double, None, false, None).unwrap(),
            )),
            ColumnRef::from(ColumnCatalog::new(
                "c8".to_string(),
                false,
                ColumnDesc::new(LogicalType::Tinyint, None, false, None).unwrap(),
            )),
            ColumnRef::from(ColumnCatalog::new(
                "c9".to_string(),
                false,
                ColumnDesc::new(LogicalType::UTinyint, None, false, None).unwrap(),
            )),
            ColumnRef::from(ColumnCatalog::new(
                "c10".to_string(),
                false,
                ColumnDesc::new(LogicalType::Boolean, None, false, None).unwrap(),
            )),
            ColumnRef::from(ColumnCatalog::new(
                "c11".to_string(),
                false,
                ColumnDesc::new(LogicalType::DateTime, None, false, None).unwrap(),
            )),
            ColumnRef::from(ColumnCatalog::new(
                "c12".to_string(),
                false,
                ColumnDesc::new(LogicalType::Date, None, false, None).unwrap(),
            )),
            ColumnRef::from(ColumnCatalog::new(
                "c13".to_string(),
                false,
                ColumnDesc::new(LogicalType::Decimal(None, None), None, false, None).unwrap(),
            )),
            ColumnRef::from(ColumnCatalog::new(
                "c14".to_string(),
                false,
                ColumnDesc::new(
                    LogicalType::Char(1, CharLengthUnits::Characters),
                    None,
                    false,
                    None,
                )
                .unwrap(),
            )),
            ColumnRef::from(ColumnCatalog::new(
                "c15".to_string(),
                false,
                ColumnDesc::new(
                    LogicalType::Varchar(Some(2), CharLengthUnits::Octets),
                    None,
                    false,
                    None,
                )
                .unwrap(),
            )),
            ColumnRef::from(ColumnCatalog::new(
                "c16".to_string(),
                false,
                ColumnDesc::new(
                    LogicalType::Char(10, CharLengthUnits::Octets),
                    None,
                    false,
                    None,
                )
                .unwrap(),
            )),
        ]);

        let tuples = [
            Tuple::new(
                Some(DataValue::Int32(0)),
                vec![
                    DataValue::Int32(0),
                    DataValue::UInt32(1),
                    DataValue::Utf8 {
                        value: "LOL".to_string(),
                        ty: Utf8Type::Variable(Some(2)),
                        unit: CharLengthUnits::Characters,
                    },
                    DataValue::Int16(1),
                    DataValue::UInt16(1),
                    DataValue::Float32(OrderedFloat(0.1)),
                    DataValue::Float64(OrderedFloat(0.1)),
                    DataValue::Int8(1),
                    DataValue::UInt8(1),
                    DataValue::Boolean(true),
                    DataValue::Date64(0),
                    DataValue::Date32(0),
                    DataValue::Decimal(Decimal::new(0, 3)),
                    DataValue::Utf8 {
                        value: "K".to_string(),
                        ty: Utf8Type::Fixed(1),
                        unit: CharLengthUnits::Characters,
                    },
                    DataValue::Utf8 {
                        value: "LOL".to_string(),
                        ty: Utf8Type::Variable(Some(2)),
                        unit: CharLengthUnits::Octets,
                    },
                    DataValue::Utf8 {
                        value: "K".to_string(),
                        ty: Utf8Type::Fixed(10),
                        unit: CharLengthUnits::Octets,
                    },
                ],
            ),
            Tuple::new(
                Some(DataValue::Int32(1)),
                vec![
                    DataValue::Int32(1),
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                ],
            ),
        ];
        let serializers = columns
            .iter()
            .map(|column| column.datatype().serializable())
            .collect_vec();
        let columns = Arc::new(columns);
        let arena = Bump::new();
        {
            let mut tuple_0 = Tuple {
                pk: tuples[0].pk.clone(),
                values: Vec::with_capacity(serializers.len()),
            };
            tuple_0
                .deserialize_from_into(
                    &serializers,
                    &tuples[0].serialize_to(&serializers, &arena).unwrap(),
                    columns.len(),
                )
                .unwrap();

            assert_eq!(tuples[0], tuple_0);
        }
        {
            let mut tuple_1 = Tuple {
                pk: tuples[1].pk.clone(),
                values: Vec::with_capacity(serializers.len()),
            };
            tuple_1
                .deserialize_from_into(
                    &serializers,
                    &tuples[1].serialize_to(&serializers, &arena).unwrap(),
                    columns.len(),
                )
                .unwrap();

            assert_eq!(tuples[1], tuple_1);
        }
        // projection
        {
            let projection_serializers = vec![
                columns[0].datatype().serializable(),
                columns[1].datatype().skip_serializable(),
                columns[2].datatype().skip_serializable(),
                columns[3].datatype().serializable(),
            ];
            let mut tuple_2 = Tuple {
                pk: tuples[0].pk.clone(),
                values: Vec::with_capacity(2),
            };
            tuple_2
                .deserialize_from_into(
                    &projection_serializers,
                    &tuples[0].serialize_to(&serializers, &arena).unwrap(),
                    columns.len(),
                )
                .unwrap();

            assert_eq!(
                tuple_2,
                Tuple {
                    pk: Some(DataValue::Int32(0)),
                    values: vec![DataValue::Int32(0), DataValue::Int16(1)],
                }
            );
        }
        // multiple pk
        {
            let multiple_pk_serializers = columns
                .iter()
                .take(5)
                .map(|column| column.datatype().serializable())
                .collect_vec();
            let mut multi_pk_tuple = tuples[0].clone();
            multi_pk_tuple.pk = Some(DataValue::Tuple(
                vec![
                    multi_pk_tuple.values[4].clone(),
                    multi_pk_tuple.values[2].clone(),
                ],
                false,
            ));

            let mut tuple_3 = Tuple {
                pk: multi_pk_tuple.pk.clone(),
                values: Vec::with_capacity(serializers.len()),
            };
            tuple_3
                .deserialize_from_into(
                    &multiple_pk_serializers,
                    &multi_pk_tuple.serialize_to(&serializers, &arena).unwrap(),
                    columns.len(),
                )
                .unwrap();

            assert_eq!(
                tuple_3,
                Tuple {
                    pk: multi_pk_tuple.pk.clone(),
                    values: vec![
                        DataValue::Int32(0),
                        DataValue::UInt32(1),
                        DataValue::Utf8 {
                            value: "LOL".to_string(),
                            ty: Utf8Type::Variable(Some(2)),
                            unit: CharLengthUnits::Characters,
                        },
                        DataValue::Int16(1),
                        DataValue::UInt16(1),
                    ],
                }
            );
        }
    }
}
