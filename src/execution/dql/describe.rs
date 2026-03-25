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

use crate::catalog::{ColumnCatalog, TableName};
use crate::errors::DatabaseError;
use crate::execution::{ExecArena, ExecId, ExecNode, ExecutionCaches, ReadExecutor};
use crate::planner::operator::describe::DescribeOperator;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
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
    rows: Option<std::vec::IntoIter<Tuple>>,
}

impl From<DescribeOperator> for Describe {
    fn from(op: DescribeOperator) -> Self {
        Describe {
            table_name: op.table_name,
            rows: None,
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

impl Describe {
    pub(crate) fn next_tuple<'a, T: Transaction + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
    ) -> Result<Option<Tuple>, DatabaseError> {
        if self.rows.is_none() {
            let table = arena
                .transaction_mut()
                .table(arena.table_cache(), self.table_name.clone())?
                .ok_or(DatabaseError::TableNotFound)?;
            let key_fn = |column: &ColumnCatalog| {
                if column.desc().is_primary() {
                    PRIMARY_KEY_TYPE.clone()
                } else if column.desc().is_unique() {
                    UNIQUE_KEY_TYPE.clone()
                } else {
                    EMPTY_KEY_TYPE.clone()
                }
            };

            let rows = table
                .columns()
                .map(|column| {
                    let datatype = column.datatype();
                    let default = column
                        .desc()
                        .default
                        .as_ref()
                        .map(|expr| format!("{expr}"))
                        .unwrap_or_else(|| "null".to_string());
                    let values = vec![
                        DataValue::Utf8 {
                            value: column.name().to_string(),
                            ty: Utf8Type::Variable(None),
                            unit: CharLengthUnits::Characters,
                        },
                        DataValue::Utf8 {
                            value: datatype.to_string(),
                            ty: Utf8Type::Variable(None),
                            unit: CharLengthUnits::Characters,
                        },
                        DataValue::Utf8 {
                            value: datatype
                                .raw_len()
                                .map(|len| len.to_string())
                                .unwrap_or_else(|| "variable".to_string()),
                            ty: Utf8Type::Variable(None),
                            unit: CharLengthUnits::Characters,
                        },
                        DataValue::Utf8 {
                            value: column.nullable().to_string(),
                            ty: Utf8Type::Variable(None),
                            unit: CharLengthUnits::Characters,
                        },
                        key_fn(column),
                        DataValue::Utf8 {
                            value: default,
                            ty: Utf8Type::Variable(None),
                            unit: CharLengthUnits::Characters,
                        },
                    ];
                    Tuple::new(None, values)
                })
                .collect::<Vec<_>>();
            self.rows = Some(rows.into_iter());
        }

        Ok(self.rows.as_mut().and_then(std::iter::Iterator::next))
    }
}
