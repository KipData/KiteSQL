// Copyright 2024 KipData/KiteSQL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::catalog::ColumnRef;
use crate::expression::window::WindowFunction;
use crate::iter_ext::Itertools;
use crate::planner::operator::sort::SortField;
use crate::planner::operator::SortOption;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct WindowOperator {
    pub sort_fields: Vec<SortField>,
    pub partition_by_len: usize,
    pub functions: Vec<WindowFunction>,
    pub output_columns: Vec<ColumnRef>,
}

impl WindowOperator {
    pub(crate) fn sort_option(&self) -> SortOption {
        if self.sort_fields.is_empty() {
            SortOption::Follow
        } else {
            SortOption::OrderBy {
                fields: self.sort_fields.clone(),
                ignore_prefix_len: 0,
            }
        }
    }
}

impl fmt::Display for WindowOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (partition_by, order_by) = self.sort_fields.split_at(self.partition_by_len);
        write!(
            f,
            "Window [{}]",
            self.functions
                .iter()
                .map(|expr| format!("{expr:?}"))
                .join(", ")
        )?;
        if !self.sort_fields.is_empty() {
            write!(f, " ->")?;
        }
        if !partition_by.is_empty() {
            write!(
                f,
                " Partition By [{}]",
                partition_by
                    .iter()
                    .map(|field| field.expr.to_string())
                    .join(", ")
            )?;
        }
        if !order_by.is_empty() {
            write!(
                f,
                " Order By [{}]",
                order_by.iter().map(ToString::to_string).join(", ")
            )?;
        }
        Ok(())
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::expression::window::WindowFunctionKind;
    use crate::expression::ScalarExpression;
    use crate::planner::TableArena;
    use crate::serdes::{ReferenceSerialization, ReferenceTables};
    use crate::storage::rocksdb::RocksTransaction;
    use crate::types::LogicalType;
    use std::io::{Cursor, Seek, SeekFrom};

    fn operator(partition_by: Vec<ScalarExpression>, order_by: Vec<SortField>) -> WindowOperator {
        let partition_by_len = partition_by.len();
        WindowOperator {
            sort_fields: partition_by
                .into_iter()
                .map(SortField::from)
                .chain(order_by)
                .collect(),
            partition_by_len,
            functions: vec![WindowFunction {
                kind: WindowFunctionKind::RowNumber,
                args: Vec::new(),
                ty: LogicalType::Bigint,
            }],
            output_columns: Vec::new(),
        }
    }

    #[test]
    fn display_window_spec() {
        let function = "Window [WindowFunction { kind: RowNumber, args: [], ty: Bigint }]";
        assert_eq!(operator(Vec::new(), Vec::new()).to_string(), function);
        assert_eq!(
            operator(vec![1.into()], Vec::new()).to_string(),
            format!("{function} -> Partition By [1]")
        );
        assert_eq!(
            operator(Vec::new(), vec![ScalarExpression::from(2).desc()]).to_string(),
            format!("{function} -> Order By [2 Desc Nulls Last]")
        );
        assert_eq!(
            operator(vec![1.into()], vec![ScalarExpression::from(2).desc()]).to_string(),
            format!("{function} -> Partition By [1] Order By [2 Desc Nulls Last]")
        );
        assert_eq!(
            operator(Vec::new(), Vec::new()).sort_option(),
            SortOption::Follow
        );
        assert_eq!(
            operator(vec![1.into()], vec![ScalarExpression::from(2).desc()]).sort_option(),
            SortOption::OrderBy {
                fields: vec![
                    ScalarExpression::from(1).asc(),
                    ScalarExpression::from(2).desc(),
                ],
                ignore_prefix_len: 0,
            }
        );
    }

    #[test]
    fn serialization_roundtrip() -> Result<(), crate::errors::DatabaseError> {
        let source = operator(vec![1.into()], vec![ScalarExpression::from(2).desc()]);
        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();
        let arena = TableArena::default();
        source.encode(&mut cursor, false, &mut reference_tables, &arena)?;
        cursor.seek(SeekFrom::Start(0))?;

        assert_eq!(
            WindowOperator::decode::<RocksTransaction, _, _>(
                &mut cursor,
                None,
                &reference_tables,
                &mut TableArena::default(),
            )?,
            source
        );
        Ok(())
    }
}
// GRCOV_EXCL_STOP
