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
use crate::serdes::{ReferenceDecodeContext, ReferenceSerialization};
use crate::storage::StorageTransaction;
use crate::types::evaluator::{
    BinaryEvaluatorParams, BinaryEvaluatorRef, CastEvaluatorParams, CastEvaluatorRef,
    UnaryEvaluatorRef,
};
use std::io::{Read, Write};

impl ReferenceSerialization for BinaryEvaluatorParams {
    fn encode<W: Write, A: MetaArena + ?Sized>(
        &self,
        writer: &mut W,
        is_direct: bool,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        match self {
            BinaryEvaluatorParams::Unit => 0u8.encode(writer, is_direct, arena),
            BinaryEvaluatorParams::Like { escape_char } => {
                1u8.encode(writer, is_direct, arena)?;
                escape_char.encode(writer, is_direct, arena)
            }
        }
    }

    fn decode<T: StorageTransaction, R: Read, A: MetaArena + ?Sized>(
        reader: &mut R,
        mut context: Option<&mut ReferenceDecodeContext<'_, T>>,
        arena: &mut A,
    ) -> Result<Self, DatabaseError> {
        Ok(match u8::decode(reader, context.as_deref_mut(), arena)? {
            0 => BinaryEvaluatorParams::Unit,
            1 => BinaryEvaluatorParams::Like {
                escape_char: Option::<char>::decode(reader, context, arena)?,
            },
            _ => unreachable!(),
        })
    }
}

impl ReferenceSerialization for CastEvaluatorParams {
    fn encode<W: Write, A: MetaArena + ?Sized>(
        &self,
        writer: &mut W,
        is_direct: bool,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        match self {
            CastEvaluatorParams::Identity => 0u8.encode(writer, is_direct, arena),
            CastEvaluatorParams::Unit => 1u8.encode(writer, is_direct, arena),
            CastEvaluatorParams::String { len, unit } => {
                2u8.encode(writer, is_direct, arena)?;
                len.encode(writer, is_direct, arena)?;
                unit.encode(writer, is_direct, arena)
            }
            #[cfg(feature = "decimal")]
            CastEvaluatorParams::Decimal { precision, scale } => {
                3u8.encode(writer, is_direct, arena)?;
                precision.encode(writer, is_direct, arena)?;
                scale.encode(writer, is_direct, arena)
            }
            CastEvaluatorParams::Precision { precision } => {
                4u8.encode(writer, is_direct, arena)?;
                precision.encode(writer, is_direct, arena)
            }
            CastEvaluatorParams::Timestamp { precision, zone } => {
                5u8.encode(writer, is_direct, arena)?;
                precision.encode(writer, is_direct, arena)?;
                zone.encode(writer, is_direct, arena)
            }
            CastEvaluatorParams::Binary { len } => {
                7u8.encode(writer, is_direct, arena)?;
                len.encode(writer, is_direct, arena)
            }
            CastEvaluatorParams::Tuple { evaluators } => {
                6u8.encode(writer, is_direct, arena)?;
                evaluators.encode(writer, is_direct, arena)
            }
        }
    }

    fn decode<T: StorageTransaction, R: Read, A: MetaArena + ?Sized>(
        reader: &mut R,
        mut context: Option<&mut ReferenceDecodeContext<'_, T>>,
        arena: &mut A,
    ) -> Result<Self, DatabaseError> {
        Ok(match u8::decode(reader, context.as_deref_mut(), arena)? {
            0 => CastEvaluatorParams::Identity,
            1 => CastEvaluatorParams::Unit,
            2 => CastEvaluatorParams::String {
                len: Option::<u32>::decode(reader, context.as_deref_mut(), arena)?,
                unit: crate::types::CharLengthUnits::decode(reader, context.as_deref_mut(), arena)?,
            },
            #[cfg(feature = "decimal")]
            3 => CastEvaluatorParams::Decimal {
                precision: Option::<u8>::decode(reader, context.as_deref_mut(), arena)?,
                scale: Option::<u8>::decode(reader, context.as_deref_mut(), arena)?,
            },
            #[cfg(not(feature = "decimal"))]
            3 => {
                let _ = Option::<u8>::decode(reader, context.as_deref_mut(), arena)?;
                let _ = Option::<u8>::decode(reader, context.as_deref_mut(), arena)?;
                return Err(DatabaseError::UnsupportedStmt(
                    "DECIMAL requires the `decimal` feature".to_string(),
                ));
            }
            4 => CastEvaluatorParams::Precision {
                precision: Option::<u64>::decode(reader, context.as_deref_mut(), arena)?,
            },
            5 => CastEvaluatorParams::Timestamp {
                precision: Option::<u64>::decode(reader, context.as_deref_mut(), arena)?,
                zone: bool::decode(reader, context.as_deref_mut(), arena)?,
            },
            7 => CastEvaluatorParams::Binary {
                len: Option::<u32>::decode(reader, context.as_deref_mut(), arena)?,
            },
            6 => CastEvaluatorParams::Tuple {
                evaluators: Vec::<CastEvaluatorRef>::decode(reader, context, arena)?,
            },
            _ => unreachable!(),
        })
    }
}

impl ReferenceSerialization for UnaryEvaluatorRef {
    fn encode<W: Write, A: MetaArena + ?Sized>(
        &self,
        writer: &mut W,
        is_direct: bool,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        self.pos.encode(writer, is_direct, arena)
    }

    fn decode<T: StorageTransaction, R: Read, A: MetaArena + ?Sized>(
        reader: &mut R,
        context: Option<&mut ReferenceDecodeContext<'_, T>>,
        arena: &mut A,
    ) -> Result<Self, DatabaseError> {
        Ok(UnaryEvaluatorRef::new(u16::decode(reader, context, arena)?))
    }
}

impl ReferenceSerialization for BinaryEvaluatorRef {
    fn encode<W: Write, A: MetaArena + ?Sized>(
        &self,
        writer: &mut W,
        is_direct: bool,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        self.pos.encode(writer, is_direct, arena)?;
        self.params.encode(writer, is_direct, arena)
    }

    fn decode<T: StorageTransaction, R: Read, A: MetaArena + ?Sized>(
        reader: &mut R,
        mut context: Option<&mut ReferenceDecodeContext<'_, T>>,
        arena: &mut A,
    ) -> Result<Self, DatabaseError> {
        Ok(BinaryEvaluatorRef::new(
            u16::decode(reader, context.as_deref_mut(), arena)?,
            BinaryEvaluatorParams::decode(reader, context, arena)?,
        ))
    }
}

impl ReferenceSerialization for CastEvaluatorRef {
    fn encode<W: Write, A: MetaArena + ?Sized>(
        &self,
        writer: &mut W,
        is_direct: bool,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        self.pos.encode(writer, is_direct, arena)?;
        self.params.encode(writer, is_direct, arena)
    }

    fn decode<T: StorageTransaction, R: Read, A: MetaArena + ?Sized>(
        reader: &mut R,
        mut context: Option<&mut ReferenceDecodeContext<'_, T>>,
        arena: &mut A,
    ) -> Result<Self, DatabaseError> {
        Ok(CastEvaluatorRef::new(
            u16::decode(reader, context.as_deref_mut(), arena)?,
            CastEvaluatorParams::decode(reader, context, arena)?,
        ))
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::storage::rocksdb::RocksTransaction;
    use crate::types::CharLengthUnits;
    use std::io::{Cursor, Seek, SeekFrom};

    fn roundtrip_cast_params(
        params: CastEvaluatorParams,
        expected_tag: u8,
    ) -> Result<(), DatabaseError> {
        let mut cursor = Cursor::new(Vec::new());
        let mut arena = crate::planner::TableArena::default();

        params.encode(&mut cursor, false, &arena)?;
        assert_eq!(cursor.get_ref()[0], expected_tag);
        cursor.seek(SeekFrom::Start(0))?;

        assert_eq!(
            CastEvaluatorParams::decode::<RocksTransaction, _, _>(&mut cursor, None, &mut arena,)?,
            params
        );

        Ok(())
    }

    fn roundtrip_cast_ref(evaluator: CastEvaluatorRef) -> Result<(), DatabaseError> {
        let mut cursor = Cursor::new(Vec::new());
        let mut arena = crate::planner::TableArena::default();

        evaluator.encode(&mut cursor, false, &arena)?;
        cursor.seek(SeekFrom::Start(0))?;

        assert_eq!(
            CastEvaluatorRef::decode::<RocksTransaction, _, _>(&mut cursor, None, &mut arena,)?,
            evaluator
        );

        Ok(())
    }

    #[test]
    fn cast_evaluator_params_serialization_roundtrips_all_variants() -> Result<(), DatabaseError> {
        roundtrip_cast_params(CastEvaluatorParams::Identity, 0)?;
        roundtrip_cast_params(CastEvaluatorParams::Unit, 1)?;
        roundtrip_cast_params(CastEvaluatorParams::Binary { len: Some(8) }, 7)?;
        roundtrip_cast_params(CastEvaluatorParams::Binary { len: None }, 7)?;
        roundtrip_cast_params(
            CastEvaluatorParams::String {
                len: Some(12),
                unit: CharLengthUnits::Octets,
            },
            2,
        )?;
        roundtrip_cast_params(
            CastEvaluatorParams::String {
                len: None,
                unit: CharLengthUnits::Characters,
            },
            2,
        )?;
        #[cfg(feature = "decimal")]
        roundtrip_cast_params(
            CastEvaluatorParams::Decimal {
                precision: Some(10),
                scale: Some(2),
            },
            3,
        )?;
        #[cfg(feature = "decimal")]
        roundtrip_cast_params(
            CastEvaluatorParams::Decimal {
                precision: None,
                scale: None,
            },
            3,
        )?;
        roundtrip_cast_params(CastEvaluatorParams::Precision { precision: Some(6) }, 4)?;
        roundtrip_cast_params(CastEvaluatorParams::Precision { precision: None }, 4)?;
        roundtrip_cast_params(
            CastEvaluatorParams::Timestamp {
                precision: Some(3),
                zone: true,
            },
            5,
        )?;
        roundtrip_cast_params(
            CastEvaluatorParams::Timestamp {
                precision: None,
                zone: false,
            },
            5,
        )?;
        roundtrip_cast_params(
            CastEvaluatorParams::Tuple {
                evaluators: vec![
                    CastEvaluatorRef::new(1, CastEvaluatorParams::Unit),
                    CastEvaluatorRef::new(
                        2,
                        CastEvaluatorParams::String {
                            len: Some(8),
                            unit: CharLengthUnits::Characters,
                        },
                    ),
                ],
            },
            6,
        )?;

        Ok(())
    }

    #[test]
    fn cast_evaluator_ref_serialization_roundtrips_nested_tuple_params() -> Result<(), DatabaseError>
    {
        roundtrip_cast_ref(CastEvaluatorRef::new(
            42,
            CastEvaluatorParams::Tuple {
                evaluators: vec![
                    #[cfg(feature = "decimal")]
                    CastEvaluatorRef::new(
                        3,
                        CastEvaluatorParams::Decimal {
                            precision: Some(18),
                            scale: Some(4),
                        },
                    ),
                    CastEvaluatorRef::new(
                        4,
                        CastEvaluatorParams::Timestamp {
                            precision: Some(9),
                            zone: true,
                        },
                    ),
                ],
            },
        ))
    }
}
// GRCOV_EXCL_STOP
