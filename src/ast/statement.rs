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

use super::{Expr, Ident, ObjectName, Query, Span, TableWithJoins};
use crate::types::LogicalType;

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Query(Box<Query>),
    Insert(Box<Insert>),
    Update(Box<Update>),
    Delete(Box<Delete>),
    CreateTable(Box<CreateTable>),
    CreateIndex(Box<CreateIndex>),
    CreateView(Box<CreateView>),
    AlterTable(Box<AlterTable>),
    Drop {
        object_type: ObjectType,
        name: ObjectName,
        if_exists: bool,
        /// The table of `DROP INDEX name ON table`.
        table: Option<ObjectName>,
    },
    Truncate {
        table_name: ObjectName,
    },
    Analyze {
        table_name: ObjectName,
    },
    Copy(Box<Copy>),
    Explain(Box<Statement>),
    /// `DESCRIBE table`
    Describe {
        table_name: ObjectName,
    },
    ShowTables,
    ShowViews,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Insert {
    pub table_name: ObjectName,
    pub columns: Vec<Ident>,
    /// `INSERT OVERWRITE` or `REPLACE INTO`: rows replace those with the same primary key.
    pub overwrite: bool,
    pub source: Box<Query>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Update {
    /// The target table, which is a plain table, and the tables joined to it.
    pub table: TableWithJoins,
    pub assignments: Vec<Assignment>,
    pub selection: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub target: AssignmentTarget,
    pub value: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AssignmentTarget {
    ColumnName(ObjectName),
    /// `(a, b) = (..)`
    Tuple(Vec<ObjectName>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Delete {
    /// The target table, which is a plain table.
    pub table: TableWithJoins,
    pub selection: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTable {
    pub name: ObjectName,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: Ident,
    pub data_type: LogicalType,
    pub options: Vec<ColumnOption>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnOption {
    Null,
    NotNull,
    PrimaryKey,
    Unique,
    Default(Expr),
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
    PrimaryKey(Vec<KeyColumn>),
    Unique(Vec<KeyColumn>),
}

/// A column of a key constraint, with the prefix length of `col(n)`.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyColumn {
    pub name: Ident,
    pub prefix_len: Option<u32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndex {
    pub name: ObjectName,
    pub table_name: ObjectName,
    pub columns: Vec<IndexColumn>,
    pub unique: bool,
    pub if_not_exists: bool,
}

/// A key of `CREATE INDEX`, with the prefix length of `col(n)`.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexColumn {
    pub expr: Expr,
    pub prefix_len: Option<u32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateView {
    pub or_replace: bool,
    pub name: ObjectName,
    pub columns: Vec<Ident>,
    pub query: Box<Query>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectType {
    Table,
    View,
    Index,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterTable {
    pub name: ObjectName,
    pub operation: AlterTableOperation,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableOperation {
    AddColumn {
        if_not_exists: bool,
        column_def: ColumnDef,
    },
    DropColumn {
        column_name: Ident,
        if_exists: bool,
    },
    RenameColumn {
        old_column_name: Ident,
        new_column_name: Ident,
    },
    /// `ALTER COLUMN name ..`
    AlterColumn {
        column_name: Ident,
        op: AlterColumnOperation,
    },
    /// `MODIFY COLUMN name type options`
    ModifyColumn {
        column_name: Ident,
        data_type: LogicalType,
        options: Vec<ChangeColumnOption>,
    },
    /// `CHANGE COLUMN old new type options`
    ChangeColumn {
        old_name: Ident,
        new_name: Ident,
        data_type: LogicalType,
        options: Vec<ChangeColumnOption>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterColumnOperation {
    SetDataType(LogicalType),
    SetDefault(Expr),
    DropDefault,
    SetNotNull,
    DropNotNull,
}

/// A column option of `MODIFY COLUMN` and `CHANGE COLUMN`.
#[derive(Debug, Clone, PartialEq)]
pub enum ChangeColumnOption {
    Null,
    NotNull,
    Default(Expr),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Copy {
    pub source: CopySource,
    /// `COPY .. TO`; otherwise `COPY .. FROM`.
    pub to: bool,
    /// The file read or written.
    pub path: String,
    pub format: CopyFormat,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CopySource {
    Table(ObjectName),
    Query(Box<Query>),
}

/// CSV options of `COPY`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CopyFormat {
    pub delimiter: char,
    pub quote: char,
    pub escape: Option<char>,
    pub header: bool,
}

impl Default for CopyFormat {
    fn default() -> Self {
        Self {
            delimiter: ',',
            quote: '"',
            escape: None,
            header: false,
        }
    }
}
