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

use super::expr::expr;
use super::query::select;
use super::{ident, name, object_name, qualified_name, types, unsupported};
use crate::ast;
use crate::errors::DatabaseError;
use sqlite3_parser::ast::{
    AlterTableBody, Cmd, ColumnConstraint, ColumnDefinition, CreateTableBody, Expr, InsertBody,
    QualifiedName, ResolveType, SortOrder, SortedColumn, Stmt, TableConstraint,
};

pub(super) fn cmd(cmd: &Cmd<'_>) -> Result<ast::Statement, DatabaseError> {
    match cmd {
        Cmd::Explain(stmt) | Cmd::ExplainQueryPlan(stmt) => {
            statement(stmt, cmd).map(|stmt| ast::Statement::Explain(Box::new(stmt)))
        }
        Cmd::Stmt(stmt) => statement(stmt, cmd),
    }
}

fn statement(stmt: &Stmt<'_>, cmd: &Cmd<'_>) -> Result<ast::Statement, DatabaseError> {
    Ok(match stmt {
        Stmt::Select(query) => ast::Statement::Query(select(query).map(Box::new)?),
        Stmt::Insert {
            with,
            or_conflict,
            tbl_name,
            columns,
            body,
            returning,
        } => {
            if with.is_some() {
                return Err(unsupported("WITH before INSERT is not supported"));
            }
            if returning.is_some() {
                return Err(unsupported("INSERT RETURNING is not supported"));
            }
            let overwrite = match or_conflict {
                None | Some(ResolveType::Abort) => false,
                Some(ResolveType::Replace) => true,
                Some(resolve) => {
                    return Err(unsupported(format!(
                        "INSERT OR {resolve:?} is not supported"
                    )))
                }
            };
            let source = match body {
                InsertBody::Select(_, Some(_)) => {
                    return Err(unsupported("INSERT ON CONFLICT is not supported"))
                }
                InsertBody::Select(query, None) => select(query)?,
                InsertBody::DefaultValues => {
                    return Err(unsupported("INSERT DEFAULT VALUES is not supported"))
                }
            };
            ast::Statement::Insert(Box::new(ast::Insert {
                table_name: qualified_name(tbl_name),
                columns: columns
                    .iter()
                    .flat_map(|names| names.iter())
                    .map(name)
                    .collect(),
                overwrite,
                source: Box::new(source),
            }))
        }
        Stmt::Update {
            with,
            or_conflict,
            tbl_name,
            sets,
            from,
            where_clause,
            returning,
            order_by,
            limit,
            ..
        } => {
            if with.is_some()
                || !matches!(or_conflict, None | Some(ResolveType::Abort))
                || from.is_some()
                || returning.is_some()
                || order_by.is_some()
                || limit.is_some()
            {
                return Err(unsupported("UPDATE supports only SET and WHERE"));
            }
            let assignments = sets
                .iter()
                .map(|set| {
                    let mut names = set.col_names.iter().map(object_name);
                    let target = match set.col_names.len() {
                        1 => ast::AssignmentTarget::ColumnName(names.next().unwrap()),
                        _ => ast::AssignmentTarget::Tuple(names.collect()),
                    };
                    Ok(ast::Assignment {
                        target,
                        value: expr(&set.expr)?,
                    })
                })
                .collect::<Result<_, DatabaseError>>()?;
            ast::Statement::Update(Box::new(ast::Update {
                table: target_table(tbl_name),
                assignments,
                selection: where_clause.as_ref().map(expr).transpose()?,
            }))
        }
        Stmt::Delete {
            with,
            tbl_name,
            where_clause,
            returning,
            order_by,
            limit,
            ..
        } => {
            if with.is_some() || returning.is_some() || order_by.is_some() || limit.is_some() {
                return Err(unsupported("DELETE supports only WHERE"));
            }
            ast::Statement::Delete(Box::new(ast::Delete {
                table: target_table(tbl_name),
                selection: where_clause.as_ref().map(expr).transpose()?,
            }))
        }
        Stmt::CreateTable {
            temporary,
            if_not_exists,
            tbl_name,
            body,
        } => {
            if *temporary {
                return Err(unsupported("temporary tables are not supported"));
            }
            let CreateTableBody::ColumnsAndConstraints {
                columns,
                constraints,
                ..
            } = body
            else {
                return Err(unsupported("CREATE TABLE AS is not supported"));
            };
            ast::Statement::CreateTable(Box::new(ast::CreateTable {
                name: qualified_name(tbl_name),
                columns: columns.iter().map(column_def).collect::<Result<_, _>>()?,
                constraints: constraints
                    .unwrap_or_default()
                    .iter()
                    .map(|constraint| table_constraint(&constraint.constraint))
                    .collect::<Result<_, _>>()?,
                if_not_exists: *if_not_exists,
            }))
        }
        Stmt::CreateIndex {
            unique,
            if_not_exists,
            idx_name,
            tbl_name,
            columns,
            where_clause,
        } => {
            if where_clause.is_some() {
                return Err(unsupported("CREATE INDEX options"));
            }
            let columns = columns
                .iter()
                .map(|column| {
                    if column.order == Some(SortOrder::Desc) || column.nulls.is_some() {
                        return Err(unsupported("CREATE INDEX column options"));
                    }
                    Ok(ast::IndexColumn {
                        expr: expr(&column.expr)?,
                        prefix_len: None,
                    })
                })
                .collect::<Result<_, DatabaseError>>()?;
            ast::Statement::CreateIndex(Box::new(ast::CreateIndex {
                name: qualified_name(idx_name),
                table_name: object_name(tbl_name),
                columns,
                unique: *unique,
                if_not_exists: *if_not_exists,
            }))
        }
        Stmt::CreateView {
            temporary,
            if_not_exists,
            view_name,
            columns,
            select: query,
        } => {
            if *temporary || *if_not_exists {
                return Err(unsupported(
                    "TEMPORARY and IF NOT EXISTS views are not supported",
                ));
            }
            ast::Statement::CreateView(Box::new(ast::CreateView {
                or_replace: false,
                name: qualified_name(view_name),
                columns: columns
                    .unwrap_or_default()
                    .iter()
                    .map(|column| name(&column.col_name))
                    .collect(),
                query: select(query).map(Box::new)?,
            }))
        }
        Stmt::DropTable {
            if_exists,
            tbl_name,
        } => drop_object(ast::ObjectType::Table, tbl_name, *if_exists),
        Stmt::DropView {
            if_exists,
            view_name,
        } => drop_object(ast::ObjectType::View, view_name, *if_exists),
        Stmt::DropIndex {
            if_exists,
            idx_name,
        } => drop_object(ast::ObjectType::Index, idx_name, *if_exists),
        Stmt::AlterTable(table_name, body) => {
            ast::Statement::AlterTable(Box::new(ast::AlterTable {
                name: qualified_name(table_name),
                operation: alter_table(body)?,
            }))
        }
        Stmt::Analyze(Some(table_name)) => ast::Statement::Analyze {
            table_name: qualified_name(table_name),
        },
        Stmt::Analyze(None) => return Err(unsupported("ANALYZE without table is not supported")),
        _ => return Err(unsupported(cmd.to_string())),
    })
}

fn target_table(name: &QualifiedName<'_>) -> ast::TableWithJoins {
    ast::TableWithJoins {
        relation: ast::TableFactor::Table {
            name: qualified_name(name),
            alias: name.alias.as_ref().map(|alias| ast::TableAlias {
                name: self::name(alias),
                columns: Vec::new(),
            }),
        },
        joins: Vec::new(),
    }
}

fn drop_object(
    object_type: ast::ObjectType,
    name: &QualifiedName<'_>,
    if_exists: bool,
) -> ast::Statement {
    ast::Statement::Drop {
        object_type,
        name: qualified_name(name),
        if_exists,
        table: None,
    }
}

fn column_def(column: &ColumnDefinition<'_>) -> Result<ast::ColumnDef, DatabaseError> {
    let data_type = match &column.col_type {
        Some(ty) => types::logical_type(ty)?,
        None => {
            return Err(unsupported(format!(
                "column `{}` needs a type",
                column.col_name
            )))
        }
    };
    let options = column
        .constraints
        .iter()
        .map(|constraint| {
            Ok(match &constraint.constraint {
                ColumnConstraint::PrimaryKey {
                    conflict_clause: None,
                    auto_increment: false,
                    ..
                } => ast::ColumnOption::PrimaryKey,
                ColumnConstraint::NotNull {
                    nullable,
                    conflict_clause: None,
                } => {
                    if *nullable {
                        ast::ColumnOption::Null
                    } else {
                        ast::ColumnOption::NotNull
                    }
                }
                ColumnConstraint::Unique(None) => ast::ColumnOption::Unique,
                ColumnConstraint::Default(value) => ast::ColumnOption::Default(expr(value)?),
                option => {
                    return Err(unsupported(format!(
                        "`Column` does not currently support this option: {option:?}"
                    )))
                }
            })
        })
        .collect::<Result<_, DatabaseError>>()?;

    Ok(ast::ColumnDef {
        name: name(&column.col_name),
        data_type,
        options,
        span: ast::Span::empty(),
    })
}

fn table_constraint(
    constraint: &TableConstraint<'_>,
) -> Result<ast::TableConstraint, DatabaseError> {
    Ok(match constraint {
        TableConstraint::PrimaryKey {
            columns,
            auto_increment: false,
            conflict_clause: None,
        } => ast::TableConstraint::PrimaryKey(key_columns(columns)?),
        TableConstraint::Unique {
            columns,
            conflict_clause: None,
        } => ast::TableConstraint::Unique(key_columns(columns)?),
        constraint => {
            return Err(unsupported(format!(
                "`CreateTable` does not currently support this constraint: {constraint:?}"
            )))
        }
    })
}

fn key_columns(columns: &[SortedColumn<'_>]) -> Result<Vec<ast::KeyColumn>, DatabaseError> {
    columns
        .iter()
        .map(|column| {
            let name = match &column.expr {
                Expr::Id(id) => ident(id.0),
                Expr::Name(column) => name(column),
                _ => {
                    return Err(unsupported(
                        "only identifier columns are supported in `PRIMARY KEY/UNIQUE`",
                    ))
                }
            };
            Ok(ast::KeyColumn {
                name,
                prefix_len: None,
            })
        })
        .collect()
}

fn alter_table(body: &AlterTableBody<'_>) -> Result<ast::AlterTableOperation, DatabaseError> {
    Ok(match body {
        AlterTableBody::AddColumn(column) => ast::AlterTableOperation::AddColumn {
            if_not_exists: false,
            column_def: column_def(column)?,
        },
        AlterTableBody::DropColumn(column) => ast::AlterTableOperation::DropColumn {
            column_name: name(column),
            if_exists: false,
        },
        AlterTableBody::RenameColumn { old, new } => ast::AlterTableOperation::RenameColumn {
            old_column_name: name(old),
            new_column_name: name(new),
        },
        AlterTableBody::DropColumnNotNull(column) => ast::AlterTableOperation::AlterColumn {
            column_name: name(column),
            op: ast::AlterColumnOperation::DropNotNull,
        },
        AlterTableBody::SetColumnNotNull(column, None) => ast::AlterTableOperation::AlterColumn {
            column_name: name(column),
            op: ast::AlterColumnOperation::SetNotNull,
        },
        body => return Err(unsupported(format!("AlertOperation: {body:?}"))),
    })
}
