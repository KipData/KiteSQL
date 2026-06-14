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

use crate::catalog::{ColumnCatalog, ColumnRef};
use crate::errors::DatabaseError;
use crate::planner::PlanArena;
use crate::types::serialize::{TupleValueSerializable, TupleValueSerializableImpl};
use crate::types::value::DataValue;
use itertools::Itertools;
use std::borrow::Borrow;
use std::io::Cursor;

const BITS_MAX_INDEX: usize = 8;

pub type TupleId = DataValue;
pub type Schema = Vec<ColumnRef>;

pub struct SchemaView<'a, 'p> {
    schema: &'a Schema,
    arena: &'a PlanArena<'p>,
}

pub struct SchemaColumnIter<'a, 'p, 's> {
    columns: std::slice::Iter<'s, ColumnRef>,
    arena: &'a PlanArena<'p>,
}

impl<'a> Iterator for SchemaColumnIter<'a, '_, '_> {
    type Item = &'a ColumnCatalog;

    fn next(&mut self) -> Option<Self::Item> {
        self.columns.next().map(|column| self.arena.column(*column))
    }
}

impl<'a, 'p> SchemaView<'a, 'p> {
    pub fn new(schema: &'a Schema, arena: &'a PlanArena<'p>) -> Self {
        Self { schema, arena }
    }

    pub fn iter(&self) -> SchemaColumnIter<'a, 'p, '_> {
        SchemaColumnIter {
            columns: self.schema.iter(),
            arena: self.arena,
        }
    }

    pub fn len(&self) -> usize {
        self.schema.len()
    }

    pub fn is_empty(&self) -> bool {
        self.schema.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<&'a ColumnCatalog> {
        self.schema
            .get(index)
            .map(|column| self.arena.column(*column))
    }

    pub fn position(&self, name: &str) -> Option<usize> {
        self.iter().position(|column| column.name() == name)
    }
}

pub trait TupleLike {
    fn value_at(&self, index: usize) -> &DataValue;

    #[inline]
    fn as_slice(&self) -> Option<&[DataValue]> {
        None
    }
}

#[derive(Clone, Copy)]
pub struct SplitTupleRef<'a> {
    left: &'a [DataValue],
    right: &'a [DataValue],
    left_len: usize,
}

impl<'a> SplitTupleRef<'a> {
    pub fn new(left: &'a Tuple, right: &'a Tuple) -> Self {
        Self::from_slices(left.values.as_slice(), right.values.as_slice())
    }

    pub fn from_slices(left: &'a [DataValue], right: &'a [DataValue]) -> Self {
        SplitTupleRef {
            left,
            right,
            left_len: left.len(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct Tuple {
    pub pk: Option<TupleId>,
    pub values: Vec<DataValue>,
}

impl TupleLike for Tuple {
    #[inline]
    fn value_at(&self, index: usize) -> &DataValue {
        &self.values[index]
    }

    #[inline]
    fn as_slice(&self) -> Option<&[DataValue]> {
        Some(self.values.as_slice())
    }
}

impl TupleLike for [DataValue] {
    #[inline]
    fn value_at(&self, index: usize) -> &DataValue {
        &self[index]
    }

    #[inline]
    fn as_slice(&self) -> Option<&[DataValue]> {
        Some(self)
    }
}

impl TupleLike for &Tuple {
    #[inline]
    fn value_at(&self, index: usize) -> &DataValue {
        &self.values[index]
    }

    #[inline]
    fn as_slice(&self) -> Option<&[DataValue]> {
        Some(self.values.as_slice())
    }
}

impl TupleLike for &[DataValue] {
    #[inline]
    fn value_at(&self, index: usize) -> &DataValue {
        &self[index]
    }

    #[inline]
    fn as_slice(&self) -> Option<&[DataValue]> {
        Some(self)
    }
}

impl TupleLike for &dyn TupleLike {
    #[inline]
    fn value_at(&self, index: usize) -> &DataValue {
        (*self).value_at(index)
    }

    #[inline]
    fn as_slice(&self) -> Option<&[DataValue]> {
        (*self).as_slice()
    }
}

impl TupleLike for SplitTupleRef<'_> {
    #[inline]
    fn value_at(&self, index: usize) -> &DataValue {
        if index < self.left_len {
            &self.left[index]
        } else {
            &self.right[index - self.left_len]
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
    pub fn deserialize_from_into<I, S>(
        &mut self,
        deserializers: I,
        bytes: &[u8],
        total_len: usize,
    ) -> Result<(), DatabaseError>
    where
        I: IntoIterator<Item = S>,
        S: Borrow<TupleValueSerializableImpl>,
    {
        fn is_null(bits: u8, i: usize) -> bool {
            bits & (1 << (7 - i)) > 0
        }

        let bits_len = (total_len + BITS_MAX_INDEX) / BITS_MAX_INDEX;
        self.values.clear();
        self.values.reserve(total_len);

        let mut cursor = Cursor::new(&bytes[bits_len..]);

        for (i, deserializer) in deserializers.into_iter().enumerate() {
            if is_null(bytes[i / BITS_MAX_INDEX], i % BITS_MAX_INDEX) {
                self.values.push(DataValue::Null);
                continue;
            }
            deserializer
                .borrow()
                .filling_value(&mut cursor, &mut self.values)?;
        }
        Ok(())
    }

    /// e.g.: bits(u8)..|data_0(len for utf8_1)|utf8_0|data_1|
    /// Tips: all len is u32
    pub fn serialize_to<I, S>(
        &self,
        serializers: I,
        bytes: &mut Vec<u8>,
    ) -> Result<(), DatabaseError>
    where
        I: IntoIterator<Item = S>,
        S: Borrow<TupleValueSerializableImpl>,
    {
        fn flip_bit(bits: u8, i: usize) -> u8 {
            bits | (1 << (7 - i))
        }

        let values_len = self.values.len();
        let bits_len = (values_len + BITS_MAX_INDEX) / BITS_MAX_INDEX;
        let values_bytes_len = self
            .values
            .iter()
            .map(DataValue::serialized_len_hint)
            .sum::<usize>();
        bytes.clear();
        bytes.reserve(bits_len + values_bytes_len);
        bytes.resize(bits_len, 0u8);

        for (i, (value, serializer)) in self.values.iter().zip(serializers).enumerate() {
            if value.is_null() {
                bytes[i / BITS_MAX_INDEX] = flip_bit(bytes[i / BITS_MAX_INDEX], i % BITS_MAX_INDEX);
            } else {
                serializer.borrow().to_raw(value, bytes)?;
            }
        }

        Ok(())
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::types::tuple::Tuple;
    use crate::types::value::{DataValue, Utf8Type};
    use crate::types::CharLengthUnits;
    use crate::types::LogicalType;
    use itertools::Itertools;
    use ordered_float::OrderedFloat;
    use rust_decimal::Decimal;
    use std::sync::Arc;

    #[test]
    fn test_tuple_serialize_to_and_deserialize_from() {
        let columns = Arc::new(vec![
            ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, Some(0), false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::UInteger, None, false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c3".to_string(),
                false,
                ColumnDesc::new(
                    LogicalType::Varchar(Some(2), CharLengthUnits::Characters),
                    None,
                    false,
                    None,
                )
                .unwrap(),
            ),
            ColumnCatalog::new(
                "c4".to_string(),
                false,
                ColumnDesc::new(LogicalType::Smallint, None, false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c5".to_string(),
                false,
                ColumnDesc::new(LogicalType::USmallint, None, false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c6".to_string(),
                false,
                ColumnDesc::new(LogicalType::Float, None, false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c7".to_string(),
                false,
                ColumnDesc::new(LogicalType::Double, None, false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c8".to_string(),
                false,
                ColumnDesc::new(LogicalType::Tinyint, None, false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c9".to_string(),
                false,
                ColumnDesc::new(LogicalType::UTinyint, None, false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c10".to_string(),
                false,
                ColumnDesc::new(LogicalType::Boolean, None, false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c11".to_string(),
                false,
                ColumnDesc::new(LogicalType::DateTime, None, false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c12".to_string(),
                false,
                ColumnDesc::new(LogicalType::Date, None, false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c13".to_string(),
                false,
                ColumnDesc::new(LogicalType::Decimal(None, None), None, false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c14".to_string(),
                false,
                ColumnDesc::new(
                    LogicalType::Char(1, CharLengthUnits::Characters),
                    None,
                    false,
                    None,
                )
                .unwrap(),
            ),
            ColumnCatalog::new(
                "c15".to_string(),
                false,
                ColumnDesc::new(
                    LogicalType::Varchar(Some(2), CharLengthUnits::Octets),
                    None,
                    false,
                    None,
                )
                .unwrap(),
            ),
            ColumnCatalog::new(
                "c16".to_string(),
                false,
                ColumnDesc::new(
                    LogicalType::Char(10, CharLengthUnits::Octets),
                    None,
                    false,
                    None,
                )
                .unwrap(),
            ),
            ColumnCatalog::new(
                "c17".to_string(),
                false,
                ColumnDesc::new(
                    LogicalType::Char(3, CharLengthUnits::Characters),
                    None,
                    false,
                    None,
                )
                .unwrap(),
            ),
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
                    DataValue::Utf8 {
                        value: "你".to_string(),
                        ty: Utf8Type::Fixed(3),
                        unit: CharLengthUnits::Characters,
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
                    DataValue::Null,
                ],
            ),
        ];
        let serializers = columns
            .iter()
            .map(|column| column.datatype().serializable())
            .collect_vec();
        let mut bytes = Vec::new();
        {
            let mut tuple_0 = Tuple {
                pk: tuples[0].pk.clone(),
                values: Vec::with_capacity(serializers.len()),
            };
            tuples[0].serialize_to(&serializers, &mut bytes).unwrap();
            tuple_0
                .deserialize_from_into(&serializers, &bytes, columns.len())
                .unwrap();

            assert_eq!(tuples[0], tuple_0);
        }
        {
            let mut tuple_1 = Tuple {
                pk: tuples[1].pk.clone(),
                values: Vec::with_capacity(serializers.len()),
            };
            tuples[1].serialize_to(&serializers, &mut bytes).unwrap();
            tuple_1
                .deserialize_from_into(&serializers, &bytes, columns.len())
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
            tuples[0].serialize_to(&serializers, &mut bytes).unwrap();
            tuple_2
                .deserialize_from_into(&projection_serializers, &bytes, columns.len())
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
            multi_pk_tuple
                .serialize_to(&serializers, &mut bytes)
                .unwrap();
            tuple_3
                .deserialize_from_into(&multiple_pk_serializers, &bytes, columns.len())
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
