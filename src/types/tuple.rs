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

impl Tuple {
    pub fn new(pk: Option<TupleId>, values: Vec<DataValue>) -> Self {
        Tuple { pk, values }
    }

    #[inline]
    pub fn deserialize_from(
        deserializers: &[TupleValueSerializableImpl],
        pk_indices: Option<&[usize]>,
        bytes: &[u8],
        values_len: usize,
        total_len: usize,
    ) -> Result<Self, DatabaseError> {
        fn is_null(bits: u8, i: usize) -> bool {
            bits & (1 << (7 - i)) > 0
        }

        let bits_len = (total_len + BITS_MAX_INDEX) / BITS_MAX_INDEX;
        let mut values = Vec::with_capacity(values_len);

        let mut cursor = Cursor::new(&bytes[bits_len..]);

        for (i, deserializer) in deserializers.iter().enumerate() {
            if is_null(bytes[i / BITS_MAX_INDEX], i % BITS_MAX_INDEX) {
                values.push(DataValue::Null);
                continue;
            }
            deserializer.filling_value(&mut cursor, &mut values)?;
        }

        Ok(Tuple {
            pk: pk_indices.map(|pk_indices| Tuple::primary_projection(pk_indices, &values)),
            values,
        })
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

#[cfg(test)]
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

        let tuples = vec![
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
            let tuple_0 = Tuple::deserialize_from(
                &serializers,
                Some(vec![0]).as_deref(),
                &tuples[0].serialize_to(&serializers, &arena).unwrap(),
                serializers.len(),
                columns.len(),
            )
            .unwrap();

            assert_eq!(tuples[0], tuple_0);
        }
        {
            let tuple_1 = Tuple::deserialize_from(
                &serializers,
                Some(vec![0]).as_deref(),
                &tuples[1].serialize_to(&serializers, &arena).unwrap(),
                serializers.len(),
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
            let tuple_2 = Tuple::deserialize_from(
                &projection_serializers,
                Some(vec![0]).as_deref(),
                &tuples[0].serialize_to(&serializers, &arena).unwrap(),
                2,
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

            let tuple_3 = Tuple::deserialize_from(
                &multiple_pk_serializers,
                Some(vec![4, 2]).as_deref(),
                &tuples[0].serialize_to(&serializers, &arena).unwrap(),
                serializers.len(),
                columns.len(),
            )
            .unwrap();

            assert_eq!(
                tuple_3,
                Tuple {
                    pk: Some(DataValue::Tuple(
                        vec![
                            DataValue::UInt16(1),
                            DataValue::Utf8 {
                                value: "LOL".to_string(),
                                ty: Utf8Type::Variable(Some(2)),
                                unit: CharLengthUnits::Octets,
                            },
                        ],
                        false
                    )),
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
