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
use crate::expression::ScalarExpression;
use crate::iter_ext::Itertools;
use crate::planner::operator::sort::SortField;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct WindowOperator {
    pub partition_by: Vec<ScalarExpression>,
    pub order_by: Vec<SortField>,
    pub functions: Vec<WindowFunction>,
    pub output_columns: Vec<ColumnRef>,
}

impl fmt::Display for WindowOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Window [{}]",
            self.functions
                .iter()
                .map(|expr| format!("{expr:?}"))
                .join(", ")
        )?;
        if !self.partition_by.is_empty() || !self.order_by.is_empty() {
            write!(f, " ->")?;
        }
        if !self.partition_by.is_empty() {
            write!(
                f,
                " Partition By [{}]",
                self.partition_by.iter().map(ToString::to_string).join(", ")
            )?;
        }
        if !self.order_by.is_empty() {
            write!(
                f,
                " Order By [{}]",
                self.order_by.iter().map(ToString::to_string).join(", ")
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
    use crate::planner::TableArena;
    use crate::serdes::{ReferenceSerialization, ReferenceTables};
    use crate::storage::rocksdb::RocksTransaction;
    use crate::types::LogicalType;
    use std::io::{Cursor, Seek, SeekFrom};

    fn operator(partition_by: Vec<ScalarExpression>, order_by: Vec<SortField>) -> WindowOperator {
        WindowOperator {
            partition_by,
            order_by,
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
