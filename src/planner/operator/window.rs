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
use crate::planner::operator::sort::SortField;
use crate::planner::operator::SortOption;
use crate::planner::{fmt_explain_list, Explain, PlanArena};
use kite_sql_serde_macros::ReferenceSerialization;

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

impl Explain for WindowOperator {
    fn fmt(&self, arena: &PlanArena<'_>, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (partition_by, order_by) = self.sort_fields.split_at(self.partition_by_len);
        f.write_str("Window [")?;
        for (index, function) in self.functions.iter().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            write!(f, "WindowFunction {{ kind: {:?}, args: [", function.kind)?;
            fmt_explain_list(&function.args, ", ", arena, f)?;
            write!(f, "], ty: {:?} }}", function.ty)?;
        }
        f.write_str("]")?;
        if !self.sort_fields.is_empty() {
            f.write_str(" ->")?;
        }
        if !partition_by.is_empty() {
            f.write_str(" Partition By [")?;
            for (index, field) in partition_by.iter().enumerate() {
                if index > 0 {
                    f.write_str(", ")?;
                }
                field.expr.fmt(arena, f)?;
            }
            f.write_str("]")?;
        }
        if !order_by.is_empty() {
            f.write_str(" Order By [")?;
            fmt_explain_list(order_by, ", ", arena, f)?;
            f.write_str("]")?;
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
    use crate::planner::{ExprRef, PlanArena, TableArena, TableArenaCell};
    use crate::serdes::{ReferenceSerialization, ReferenceTables};
    use crate::storage::rocksdb::RocksTransaction;
    use crate::types::LogicalType;
    use std::io::{Cursor, Seek, SeekFrom};

    fn operator(partition_by: Vec<ExprRef>, order_by: Vec<SortField>) -> WindowOperator {
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
    fn explain_window_spec() {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let one = arena.alloc_expression(ScalarExpression::from(1));
        let two = arena.alloc_expression(ScalarExpression::from(2));
        let function = "Window [WindowFunction { kind: RowNumber, args: [], ty: Bigint }]";
        assert_eq!(
            operator(Vec::new(), Vec::new()).explain(&arena).to_string(),
            function
        );
        assert_eq!(
            operator(vec![one], Vec::new()).explain(&arena).to_string(),
            function.to_owned() + " -> Partition By [1]"
        );
        assert_eq!(
            operator(Vec::new(), vec![SortField::from(two).desc()])
                .explain(&arena)
                .to_string(),
            function.to_owned() + " -> Order By [2 Desc Nulls Last]"
        );
        assert_eq!(
            operator(vec![one], vec![SortField::from(two).desc()])
                .explain(&arena)
                .to_string(),
            function.to_owned() + " -> Partition By [1] Order By [2 Desc Nulls Last]"
        );
        assert_eq!(
            operator(Vec::new(), Vec::new()).sort_option(),
            SortOption::Follow
        );
        assert_eq!(
            operator(vec![one], vec![SortField::from(two).desc()]).sort_option(),
            SortOption::OrderBy {
                fields: vec![SortField::from(one).asc(), SortField::from(two).desc()],
                ignore_prefix_len: 0,
            }
        );
    }

    #[test]
    fn serialization_roundtrip() -> Result<(), crate::errors::DatabaseError> {
        let mut arena = TableArena::default();
        let source = operator(
            vec![arena.alloc_expression(ScalarExpression::from(1))],
            vec![SortField::from(arena.alloc_expression(ScalarExpression::from(2))).desc()],
        );
        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();
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
