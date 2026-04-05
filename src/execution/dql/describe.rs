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

use crate::catalog::{ColumnCatalog, ColumnRef, TableName};
use crate::errors::DatabaseError;
use crate::execution::{ExecArena, ExecId, ExecNode, ExecutionCaches, ExecutorNode, ReadExecutor};
use crate::planner::operator::describe::DescribeOperator;
use crate::storage::Transaction;
use crate::types::value::{DataValue, Utf8Type};
use sqlparser::ast::CharLengthUnits;
use std::sync::LazyLock;

static PRIMARY_KEY_TYPE: LazyLock<DataValue> = LazyLock::new(|| DataValue::Utf8 {
    value: String::from("PRIMARY"),
    ty: Utf8Type::Variable(None),
    unit: CharLengthUnits::Characters,
});

static UNIQUE_KEY_TYPE: LazyLock<DataValue> = LazyLock::new(|| DataValue::Utf8 {
    value: String::from("UNIQUE"),
    ty: Utf8Type::Variable(None),
    unit: CharLengthUnits::Characters,
});

static EMPTY_KEY_TYPE: LazyLock<DataValue> = LazyLock::new(|| DataValue::Utf8 {
    value: String::from("EMPTY"),
    ty: Utf8Type::Variable(None),
    unit: CharLengthUnits::Characters,
});

pub struct Describe {
    table_name: TableName,
    columns: Option<Vec<ColumnRef>>,
    cursor: usize,
}

impl From<DescribeOperator> for Describe {
    fn from(op: DescribeOperator) -> Self {
        Describe {
            table_name: op.table_name,
            columns: None,
            cursor: 0,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Describe {
    fn into_executor(
        self,
        arena: &mut ExecArena<'a, T>,
        _: ExecutionCaches<'a>,
        _: *mut T,
    ) -> ExecId {
        arena.push(ExecNode::Describe(self))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for Describe {
    type Input = DescribeOperator;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        _: ExecutionCaches<'a>,
        _: *mut T,
    ) -> ExecId {
        arena.push(ExecNode::Describe(Describe::from(input)))
    }

    fn next_tuple(&mut self, arena: &mut ExecArena<'a, T>) -> Result<(), DatabaseError> {
        Describe::next_tuple(self, arena)
    }
}

impl Describe {
    pub(crate) fn next_tuple<'a, T: Transaction + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
    ) -> Result<(), DatabaseError> {
        if self.columns.is_none() {
            let table = arena
                .transaction_mut()
                .table(arena.table_cache(), self.table_name.clone())?
                .ok_or(DatabaseError::TableNotFound)?;
            self.columns = Some(table.columns().cloned().collect());
        }

        let Some(column) = self
            .columns
            .as_ref()
            .and_then(|columns| columns.get(self.cursor))
            .cloned()
        else {
            arena.finish();
            return Ok(());
        };

        self.cursor += 1;

        let output = arena.result_tuple_mut();
        output.pk = None;
        output.values.clear();
        fill_describe_row(&mut output.values, &column);

        arena.resume();
        Ok(())
    }
}

fn fill_describe_row(values: &mut Vec<DataValue>, column: &ColumnCatalog) {
    let datatype = column.datatype();
    let default = column
        .desc()
        .default
        .as_ref()
        .map(|expr| format!("{expr}"))
        .unwrap_or_else(|| "null".to_string());

    values.push(DataValue::Utf8 {
        value: column.name().to_string(),
        ty: Utf8Type::Variable(None),
        unit: CharLengthUnits::Characters,
    });
    values.push(DataValue::Utf8 {
        value: datatype.to_string(),
        ty: Utf8Type::Variable(None),
        unit: CharLengthUnits::Characters,
    });
    values.push(DataValue::Utf8 {
        value: datatype
            .raw_len()
            .map(|len| len.to_string())
            .unwrap_or_else(|| "variable".to_string()),
        ty: Utf8Type::Variable(None),
        unit: CharLengthUnits::Characters,
    });
    values.push(DataValue::Utf8 {
        value: column.nullable().to_string(),
        ty: Utf8Type::Variable(None),
        unit: CharLengthUnits::Characters,
    });
    values.push(key_value(column));
    values.push(DataValue::Utf8 {
        value: default,
        ty: Utf8Type::Variable(None),
        unit: CharLengthUnits::Characters,
    });
}

fn key_value(column: &ColumnCatalog) -> DataValue {
    if column.desc().is_primary() {
        PRIMARY_KEY_TYPE.clone()
    } else if column.desc().is_unique() {
        UNIQUE_KEY_TYPE.clone()
    } else {
        EMPTY_KEY_TYPE.clone()
    }
}
