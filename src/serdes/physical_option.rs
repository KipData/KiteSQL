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
use crate::planner::operator::{PhysicalOption, PlanImpl, RowCountOption, SortOption};
use crate::planner::MetaArena;
use crate::serdes::{ReferenceDecodeContext, ReferenceSerialization, ReferenceTables};
use crate::storage::Transaction;
use std::io::{Read, Write};

impl ReferenceSerialization for RowCountOption {
    fn encode<W: Write, A: MetaArena + ?Sized>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        match self {
            Self::Follow => writer.write_all(&[0])?,
            Self::Unknown => writer.write_all(&[1])?,
            Self::Estimated { rows, .. } => {
                writer.write_all(&[2])?;
                rows.encode(writer, is_direct, reference_tables, arena)?;
            }
        }
        Ok(())
    }

    fn decode<T: Transaction, R: Read, A: MetaArena + ?Sized>(
        reader: &mut R,
        context: Option<&ReferenceDecodeContext<'_, T>>,
        reference_tables: &ReferenceTables,
        arena: &mut A,
    ) -> Result<Self, DatabaseError> {
        let mut tag = [0];
        reader.read_exact(&mut tag)?;
        Ok(match tag[0] {
            0 => Self::Follow,
            1 => Self::Unknown,
            2 => Self::Estimated {
                rows: usize::decode(reader, context, reference_tables, arena)?,
                ndv: None,
            },
            _ => return Err(DatabaseError::InvalidValue("invalid row count tag".into())),
        })
    }
}

impl ReferenceSerialization for PhysicalOption {
    fn encode<W: Write, A: MetaArena + ?Sized>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        self.plan
            .encode(writer, is_direct, reference_tables, arena)?;
        self.sort_option
            .encode(writer, is_direct, reference_tables, arena)?;
        self.row_count
            .encode(writer, is_direct, reference_tables, arena)
    }

    fn decode<T: Transaction, R: Read, A: MetaArena + ?Sized>(
        reader: &mut R,
        context: Option<&ReferenceDecodeContext<'_, T>>,
        reference_tables: &ReferenceTables,
        arena: &mut A,
    ) -> Result<Self, DatabaseError> {
        Ok(Self {
            plan: PlanImpl::decode(reader, context, reference_tables, arena)?,
            sort_option: SortOption::decode(reader, context, reference_tables, arena)?,
            row_count: RowCountOption::decode(reader, context, reference_tables, arena)?,
            startup_work: None,
        })
    }
}

// GRCOV_EXCL_START
#[cfg(all(
    test,
    not(target_arch = "wasm32"),
    any(feature = "rocksdb", feature = "lmdb")
))]
mod tests {
    use super::*;
    use crate::expression::ScalarExpression;
    use crate::planner::operator::sort::SortField;
    use crate::planner::{PlanArena, TableArenaCell};
    #[cfg(all(not(feature = "rocksdb"), feature = "lmdb"))]
    use crate::storage::lmdb::LmdbTransaction as TestTransaction;
    #[cfg(feature = "rocksdb")]
    use crate::storage::rocksdb::OptimisticRocksTransaction as TestTransaction;
    use crate::types::value::DataValue;

    #[test]
    fn physical_option_preserves_fields_in_roundtrip() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        for option in [
            PhysicalOption::new(
                PlanImpl::SeqScan,
                SortOption::None,
                RowCountOption::Estimated {
                    rows: 12,
                    ndv: None,
                },
            ),
            PhysicalOption::new(
                PlanImpl::Project,
                SortOption::Follow,
                RowCountOption::Follow,
            ),
            PhysicalOption::new(PlanImpl::Sort, SortOption::None, RowCountOption::Unknown),
            PhysicalOption::new(
                PlanImpl::Values,
                SortOption::None,
                RowCountOption::Estimated { rows: 0, ndv: None },
            ),
        ] {
            for is_direct in [false, true] {
                let mut encoded = Vec::new();
                let mut references = ReferenceTables::new();
                option.encode(&mut encoded, is_direct, &mut references, &arena)?;
                let mut reader = encoded.as_slice();
                let decoded = PhysicalOption::decode::<TestTransaction<'_>, _, _>(
                    &mut reader,
                    None,
                    &references,
                    &mut arena,
                )?;
                assert_eq!(decoded, option);
                assert_eq!(decoded.startup_work, None);
                assert!(reader.is_empty());
            }
        }
        Ok(())
    }

    #[test]
    fn physical_option_keeps_wire_order_and_discards_startup_cache() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let key = arena.alloc_expression(ScalarExpression::Constant(DataValue::Int32(1)));
        let mut option = PhysicalOption::new(
            PlanImpl::Sort,
            SortOption::OrderBy {
                fields: vec![SortField {
                    expr: key,
                    asc: false,
                    nulls_first: true,
                }],
                ignore_prefix_len: 1,
            },
            RowCountOption::Estimated {
                rows: 42,
                ndv: None,
            },
        );
        option.row_count = RowCountOption::Estimated {
            rows: 42,
            ndv: Some(37),
        };
        for is_direct in [false, true] {
            // This is the pre-cache wire layout: exactly three ordered fields.
            let mut expected = Vec::new();
            let mut reference_tables = ReferenceTables::new();
            option
                .plan
                .encode(&mut expected, is_direct, &mut reference_tables, &arena)?;
            option
                .sort_option
                .encode(&mut expected, is_direct, &mut reference_tables, &arena)?;
            option
                .row_count
                .encode(&mut expected, is_direct, &mut reference_tables, &arena)?;
            for cache in [None, Some(None), Some(Some(192))] {
                option.startup_work = cache;
                let mut encoded = Vec::new();
                let mut references = ReferenceTables::new();
                option.encode(&mut encoded, is_direct, &mut references, &arena)?;
                assert_eq!(encoded, expected);
                assert_eq!(references, reference_tables);
                let mut reader = encoded.as_slice();
                let decoded = PhysicalOption::decode::<TestTransaction<'_>, _, _>(
                    &mut reader,
                    None,
                    &references,
                    &mut arena,
                )?;
                assert_eq!(decoded.plan, PlanImpl::Sort);
                assert_eq!(
                    decoded.row_count,
                    RowCountOption::Estimated {
                        rows: 42,
                        ndv: None
                    }
                );
                assert_eq!(decoded.startup_work, None);
                let SortOption::OrderBy {
                    fields,
                    ignore_prefix_len,
                } = decoded.sort_option
                else {
                    unreachable!()
                };
                assert_eq!(ignore_prefix_len, 1);
                assert_eq!(fields.len(), 1);
                assert!(!fields[0].asc);
                assert!(fields[0].nulls_first);
                assert_eq!(
                    arena.expression(fields[0].expr),
                    &ScalarExpression::Constant(DataValue::Int32(1))
                );
                assert!(reader.is_empty());
            }
        }
        Ok(())
    }
}
// GRCOV_EXCL_STOP
