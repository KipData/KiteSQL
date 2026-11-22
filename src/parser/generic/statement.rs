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

use super::{span, unsupported, Lower};
use crate::ast;
use crate::errors::DatabaseError;
use sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, Assignment, AssignmentTarget, ColumnDef,
    ColumnOption, ContextModifier, CopyOption, CopySource, CopyTarget, CreateIndex, CreateTable,
    DescribeAlias, Expr, FromTable, FunctionArg, FunctionArgExpr, FunctionArguments, Ident,
    IndexColumn, IndexOption, IndexType, Insert, NullsDistinctOption, ObjectNamePart, ObjectType,
    OnInsert, Set, Spanned, SqliteOnConflict, Statement, TableConstraint, TableFactor, TableObject,
    TableWithJoins, Value,
};

impl Lower<'_> {
    pub(super) fn statement(&self, statement: Statement) -> Result<ast::Statement, DatabaseError> {
        Ok(match statement {
            Statement::Query(query) => ast::Statement::Query(self.query(*query).map(Box::new)?),
            Statement::Insert(insert) => ast::Statement::Insert(self.insert(insert).map(Box::new)?),
            Statement::Update(update) => ast::Statement::Update(Box::new(ast::Update {
                table: self.target_table(update.table, "UPDATE")?,
                assignments: update
                    .assignments
                    .into_iter()
                    .map(|assignment| self.assignment(assignment))
                    .collect::<Result<_, _>>()?,
                selection: update.selection.map(|expr| self.expr(expr)).transpose()?,
            })),
            Statement::Delete(delete) => {
                let (FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from)) =
                    delete.from;
                let table = from
                    .into_iter()
                    .next()
                    .ok_or_else(|| DatabaseError::invalid_table("DELETE without FROM"))?;
                ast::Statement::Delete(Box::new(ast::Delete {
                    table: self.target_table(table, "DELETE")?,
                    selection: delete.selection.map(|expr| self.expr(expr)).transpose()?,
                }))
            }
            Statement::CreateTable(create) => {
                ast::Statement::CreateTable(self.create_table(create).map(Box::new)?)
            }
            Statement::CreateIndex(create) => {
                ast::Statement::CreateIndex(self.create_index(create).map(Box::new)?)
            }
            Statement::CreateView(create) => {
                ast::Statement::CreateView(Box::new(ast::CreateView {
                    or_replace: create.or_replace,
                    name: self.object_name(create.name)?,
                    columns: create
                        .columns
                        .into_iter()
                        .map(|column| self.ident(column.name))
                        .collect(),
                    query: self.query(*create.query).map(Box::new)?,
                }))
            }
            Statement::AlterTable(alter) => {
                if alter.operations.len() != 1 {
                    return Err(unsupported(
                        "only a single ALTER TABLE operation is supported",
                    ));
                }
                let operation = alter.operations.into_iter().next().unwrap();
                ast::Statement::AlterTable(Box::new(ast::AlterTable {
                    name: self.object_name(alter.name)?,
                    operation: self.alter_table_operation(operation)?,
                }))
            }
            Statement::Drop {
                object_type,
                names,
                if_exists,
                table,
                ..
            } => {
                if names.len() != 1 {
                    return Err(unsupported(
                        "only Drop a single `Table`, `View` or `Index` is allowed",
                    ));
                }
                if table.is_some() && object_type != ObjectType::Index {
                    return Err(unsupported("DROP ON is only supported for INDEX"));
                }
                let object_type = match object_type {
                    ObjectType::Table => ast::ObjectType::Table,
                    ObjectType::View => ast::ObjectType::View,
                    ObjectType::Index => ast::ObjectType::Index,
                    _ => {
                        return Err(unsupported(
                            "only `Table`, `View` and `Index` are allowed to be Dropped",
                        ))
                    }
                };
                ast::Statement::Drop {
                    object_type,
                    name: self.object_name(names.into_iter().next().unwrap())?,
                    if_exists,
                    table: table.map(|table| self.object_name(table)).transpose()?,
                }
            }
            Statement::Truncate(truncate) => {
                if truncate.table_names.len() != 1 {
                    return Err(unsupported("only truncate a single table is supported"));
                }
                let target = truncate.table_names.into_iter().next().unwrap();
                ast::Statement::Truncate {
                    table_name: self.object_name(target.name)?,
                }
            }
            Statement::Analyze(analyze) => {
                let table_name = analyze
                    .table_name
                    .ok_or_else(|| unsupported("ANALYZE without table is not supported"))?;
                ast::Statement::Analyze {
                    table_name: self.object_name(table_name)?,
                }
            }
            Statement::Copy {
                source,
                to,
                target,
                options,
                ..
            } => {
                let source = match source {
                    CopySource::Table { table_name, .. } => {
                        ast::CopySource::Table(self.object_name(table_name)?)
                    }
                    CopySource::Query(query) => {
                        ast::CopySource::Query(self.query(*query).map(Box::new)?)
                    }
                };
                let path = match target {
                    CopyTarget::File { filename } => filename,
                    target => return Err(unsupported(format!("copy target: {target:?}"))),
                };
                ast::Statement::Copy(Box::new(ast::Copy {
                    source,
                    to,
                    path,
                    format: copy_format(options)?,
                }))
            }
            Statement::Explain { statement, .. } => {
                ast::Statement::Explain(self.statement(*statement).map(Box::new)?)
            }
            Statement::ExplainTable {
                describe_alias: DescribeAlias::Describe | DescribeAlias::Desc,
                table_name,
                ..
            } => ast::Statement::Describe {
                table_name: self.object_name(table_name)?,
            },
            Statement::ShowTables { .. } => ast::Statement::ShowTables,
            Statement::ShowViews { .. } => ast::Statement::ShowViews,
            Statement::StartTransaction {
                modes,
                modifier: None,
                statements,
                exception: None,
                ..
            } if modes.is_empty() && statements.is_empty() => ast::Statement::StartTransaction,
            Statement::Commit {
                chain: false,
                end: false,
                modifier: None,
            } => ast::Statement::Commit,
            Statement::Rollback {
                chain: false,
                savepoint: None,
            } => ast::Statement::Rollback,
            Statement::Set(Set::SingleAssignment {
                scope: None | Some(ContextModifier::Session),
                hivevar: false,
                variable,
                values,
            }) if self.set_autocommit
                && variable.to_string().eq_ignore_ascii_case("autocommit") =>
            {
                ast::Statement::SetAutocommit(autocommit_value(&values)?)
            }
            statement => return Err(unsupported(statement.to_string())),
        })
    }

    fn insert(&self, insert: Insert) -> Result<ast::Insert, DatabaseError> {
        let Insert {
            table,
            columns,
            overwrite,
            or,
            replace_into,
            source,
            returning,
            on,
            ..
        } = insert;
        if returning.is_some() {
            return Err(unsupported("INSERT RETURNING is not supported"));
        }
        if let Some(on) = on {
            let clause = match on {
                OnInsert::OnConflict(_) => "ON CONFLICT",
                OnInsert::DuplicateKeyUpdate(_) => "ON DUPLICATE KEY UPDATE",
                _ => "conflict clause",
            };
            return Err(unsupported(format!("INSERT {clause} is not supported")));
        }
        let TableObject::TableName(table_name) = table else {
            return Err(unsupported("insert into table function is not supported"));
        };
        let source = source.ok_or_else(|| unsupported("insert without source is not supported"))?;

        Ok(ast::Insert {
            table_name: self.object_name(table_name)?,
            columns: self.idents(columns),
            overwrite: overwrite || replace_into || matches!(or, Some(SqliteOnConflict::Replace)),
            source: self.query(*source).map(Box::new)?,
        })
    }

    fn target_table(
        &self,
        table: TableWithJoins,
        statement: &str,
    ) -> Result<ast::TableWithJoins, DatabaseError> {
        if !matches!(table.relation, TableFactor::Table { .. }) {
            return Err(unsupported(format!(
                "{statement} target must be a table: {:?}",
                table.relation
            )));
        }
        self.table_with_joins(table)
    }

    fn assignment(&self, assignment: Assignment) -> Result<ast::Assignment, DatabaseError> {
        Ok(ast::Assignment {
            target: match assignment.target {
                AssignmentTarget::ColumnName(name) => {
                    ast::AssignmentTarget::ColumnName(self.object_name(name)?)
                }
                AssignmentTarget::Tuple(names) => ast::AssignmentTarget::Tuple(
                    names
                        .into_iter()
                        .map(|name| self.object_name(name))
                        .collect::<Result<_, _>>()?,
                ),
            },
            value: self.expr(assignment.value)?,
        })
    }

    fn create_table(&self, create: CreateTable) -> Result<ast::CreateTable, DatabaseError> {
        let columns = create
            .columns
            .into_iter()
            .map(|column| self.column_def(column))
            .collect::<Result<_, _>>()?;
        let constraints = create
            .constraints
            .into_iter()
            .map(|constraint| match constraint {
                TableConstraint::PrimaryKey(primary) => self
                    .key_columns(primary.columns)
                    .map(ast::TableConstraint::PrimaryKey),
                TableConstraint::Unique(unique) => {
                    check_unique_nulls(unique.nulls_distinct)?;
                    self.key_columns(unique.columns)
                        .map(ast::TableConstraint::Unique)
                }
                constraint => Err(unsupported(format!(
                    "`CreateTable` does not currently support this constraint: {constraint:?}"
                ))),
            })
            .collect::<Result<_, _>>()?;

        Ok(ast::CreateTable {
            name: self.object_name(create.name)?,
            columns,
            constraints,
            if_not_exists: create.if_not_exists,
        })
    }

    fn column_def(&self, column: ColumnDef) -> Result<ast::ColumnDef, DatabaseError> {
        let column_span = span(column.span());
        let options = column
            .options
            .into_iter()
            .map(|option| {
                Ok(match option.option {
                    ColumnOption::Null => ast::ColumnOption::Null,
                    ColumnOption::NotNull => ast::ColumnOption::NotNull,
                    ColumnOption::PrimaryKey(_) => ast::ColumnOption::PrimaryKey,
                    ColumnOption::Unique(unique) => {
                        check_unique_nulls(unique.nulls_distinct)?;
                        ast::ColumnOption::Unique
                    }
                    ColumnOption::Default(expr) => ast::ColumnOption::Default(self.expr(expr)?),
                    option => {
                        return Err(unsupported(format!(
                            "`Column` does not currently support this option: {option:?}"
                        )))
                    }
                })
            })
            .collect::<Result<_, DatabaseError>>()?;

        Ok(ast::ColumnDef {
            name: self.ident(column.name),
            data_type: self.column_type(column.data_type)?,
            options,
            span: column_span,
        })
    }

    fn change_column_options(
        &self,
        options: Vec<ColumnOption>,
    ) -> Result<Vec<ast::ChangeColumnOption>, DatabaseError> {
        options
            .into_iter()
            .map(|option| {
                Ok(match option {
                    ColumnOption::Null => ast::ChangeColumnOption::Null,
                    ColumnOption::NotNull => ast::ChangeColumnOption::NotNull,
                    ColumnOption::Default(expr) => {
                        ast::ChangeColumnOption::Default(self.expr(expr)?)
                    }
                    option => {
                        return Err(unsupported(format!(
                        "CHANGE/MODIFY COLUMN does not currently support this option: {option:?}"
                    )))
                    }
                })
            })
            .collect()
    }

    fn alter_table_operation(
        &self,
        operation: AlterTableOperation,
    ) -> Result<ast::AlterTableOperation, DatabaseError> {
        Ok(match operation {
            AlterTableOperation::AddColumn {
                if_not_exists,
                column_def,
                ..
            } => ast::AlterTableOperation::AddColumn {
                if_not_exists,
                column_def: self.column_def(column_def)?,
            },
            AlterTableOperation::DropColumn {
                column_names,
                if_exists,
                ..
            } => {
                let [column_name] = <[Ident; 1]>::try_from(column_names)
                    .map_err(|_| unsupported("only dropping a single column is supported"))?;
                ast::AlterTableOperation::DropColumn {
                    column_name: self.ident(column_name),
                    if_exists,
                }
            }
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => ast::AlterTableOperation::RenameColumn {
                old_column_name: self.ident(old_column_name),
                new_column_name: self.ident(new_column_name),
            },
            AlterTableOperation::AlterColumn { column_name, op } => {
                let op = match op {
                    AlterColumnOperation::SetDataType {
                        data_type, using, ..
                    } => {
                        if using.is_some() {
                            return Err(unsupported("ALTER COLUMN TYPE USING is not supported"));
                        }
                        ast::AlterColumnOperation::SetDataType(self.column_type(data_type)?)
                    }
                    AlterColumnOperation::SetDefault { value } => {
                        ast::AlterColumnOperation::SetDefault(self.expr(value)?)
                    }
                    AlterColumnOperation::DropDefault => ast::AlterColumnOperation::DropDefault,
                    AlterColumnOperation::SetNotNull => ast::AlterColumnOperation::SetNotNull,
                    AlterColumnOperation::DropNotNull => ast::AlterColumnOperation::DropNotNull,
                    op => {
                        return Err(unsupported(format!(
                            "unsupported alter column operation: {op:?}"
                        )))
                    }
                };
                ast::AlterTableOperation::AlterColumn {
                    column_name: self.ident(column_name),
                    op,
                }
            }
            AlterTableOperation::ModifyColumn {
                col_name,
                data_type,
                options,
                column_position,
            } => {
                if column_position.is_some() {
                    return Err(unsupported(
                        "MODIFY COLUMN does not currently support column positions",
                    ));
                }
                ast::AlterTableOperation::ModifyColumn {
                    column_name: self.ident(col_name),
                    data_type: self.column_type(data_type)?,
                    options: self.change_column_options(options)?,
                }
            }
            AlterTableOperation::ChangeColumn {
                old_name,
                new_name,
                data_type,
                options,
                column_position,
            } => {
                if column_position.is_some() {
                    return Err(unsupported(
                        "CHANGE COLUMN does not currently support column positions",
                    ));
                }
                ast::AlterTableOperation::ChangeColumn {
                    old_name: self.ident(old_name),
                    new_name: self.ident(new_name),
                    data_type: self.column_type(data_type)?,
                    options: self.change_column_options(options)?,
                }
            }
            op => return Err(unsupported(format!("AlertOperation: {op:?}"))),
        })
    }

    fn create_index(&self, create: CreateIndex) -> Result<ast::CreateIndex, DatabaseError> {
        if create.concurrently
            || !create.include.is_empty()
            || create.nulls_distinct.is_some()
            || !create.with.is_empty()
            || create.predicate.is_some()
            || !create.alter_options.is_empty()
            || create
                .using
                .as_ref()
                .is_some_and(|ty| !matches!(ty, IndexType::BTree))
            || create
                .index_options
                .iter()
                .any(|option| !matches!(option, IndexOption::Using(IndexType::BTree)))
        {
            return Err(unsupported("CREATE INDEX options"));
        }
        let table_name = self.object_name(create.table_name)?;
        let name = create.name.ok_or(DatabaseError::InvalidIndex)?;
        let columns = create
            .columns
            .into_iter()
            .map(|column| {
                let options = &column.column.options;
                if column.operator_class.is_some()
                    || options.asc == Some(false)
                    || options.nulls_first.is_some()
                    || column.column.with_fill.is_some()
                {
                    return Err(unsupported("CREATE INDEX column options"));
                }
                Ok(match prefix_key_part(&column.column.expr) {
                    Some((ident, prefix_len)) => ast::IndexColumn {
                        expr: ast::Expr::Identifier(self.ident(ident.clone())),
                        prefix_len: Some(prefix_len),
                    },
                    None => ast::IndexColumn {
                        expr: self.expr(column.column.expr)?,
                        prefix_len: None,
                    },
                })
            })
            .collect::<Result<_, _>>()?;

        Ok(ast::CreateIndex {
            name: self.object_name(name)?,
            table_name,
            columns,
            unique: create.unique,
            if_not_exists: create.if_not_exists,
        })
    }

    fn key_columns(&self, columns: Vec<IndexColumn>) -> Result<Vec<ast::KeyColumn>, DatabaseError> {
        columns
            .into_iter()
            .map(|column| {
                let (name, prefix_len) = match column.column.expr {
                    Expr::Identifier(ident) => (ident, None),
                    expr => match prefix_key_part(&expr) {
                        Some((ident, prefix_len)) => (ident.clone(), Some(prefix_len)),
                        None => {
                            return Err(unsupported(
                                "only identifier columns are supported in `PRIMARY KEY/UNIQUE`",
                            ))
                        }
                    },
                };
                Ok(ast::KeyColumn {
                    name: self.ident(name),
                    prefix_len,
                })
            })
            .collect()
    }
}

fn check_unique_nulls(option: NullsDistinctOption) -> Result<(), DatabaseError> {
    if option != NullsDistinctOption::None {
        return Err(unsupported("UNIQUE NULLS options"));
    }
    Ok(())
}

/// A prefix key part `col(n)` parses as a call.
fn prefix_key_part(expr: &Expr) -> Option<(&Ident, u32)> {
    let Expr::Function(func) = expr else {
        return None;
    };
    let [ObjectNamePart::Identifier(ident)] = func.name.0.as_slice() else {
        return None;
    };
    let FunctionArguments::List(list) = &func.args else {
        return None;
    };
    let [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value)))] = list.args.as_slice()
    else {
        return None;
    };
    let Value::Number(length, false) = &value.value else {
        return None;
    };
    let length = length.parse::<u32>().ok().filter(|len| *len > 0)?;
    let is_plain_call = !func.uses_odbc_syntax
        && matches!(func.parameters, FunctionArguments::None)
        && list.duplicate_treatment.is_none()
        && list.clauses.is_empty()
        && func.filter.is_none()
        && func.null_treatment.is_none()
        && func.over.is_none()
        && func.within_group.is_empty();
    is_plain_call.then_some((ident, length))
}

fn copy_format(options: Vec<CopyOption>) -> Result<ast::CopyFormat, DatabaseError> {
    let mut format = ast::CopyFormat::default();
    for option in options {
        match option {
            CopyOption::Format(name) => {
                debug_assert_eq!(name.value.to_lowercase(), "csv", "only support CSV format")
            }
            CopyOption::Delimiter(delimiter) => format.delimiter = delimiter,
            CopyOption::Header(header) => format.header = header,
            CopyOption::Quote(quote) => format.quote = quote,
            CopyOption::Escape(escape) => format.escape = Some(escape),
            option => return Err(unsupported(format!("copy option: {option:?}"))),
        }
    }
    Ok(format)
}

fn autocommit_value(values: &[Expr]) -> Result<bool, DatabaseError> {
    match values {
        [Expr::Value(value)] => match &value.value {
            Value::Number(n, _) if n == "0" => Some(false),
            Value::Number(n, _) if n == "1" => Some(true),
            Value::Boolean(value) => Some(*value),
            _ => None,
        },
        [Expr::Identifier(ident)] if ident.value.eq_ignore_ascii_case("on") => Some(true),
        [Expr::Identifier(ident)] if ident.value.eq_ignore_ascii_case("off") => Some(false),
        _ => None,
    }
    .ok_or_else(|| unsupported("autocommit must be 0, 1, ON or OFF"))
}

// GRCOV_EXCL_START
#[cfg(test)]
mod tests {
    use super::copy_format;
    use crate::ast;
    use crate::errors::DatabaseError;
    use crate::parser::generic::{parse_sql, Lower};
    use sqlparser::ast::{AlterTableOperation, CopyOption, Ident};

    fn assert_unsupported(err: DatabaseError, expected: &str) {
        let DatabaseError::UnsupportedStmt(message) = err else {
            panic!("expected unsupported statement, got {err:?}");
        };
        assert!(message.contains(expected), "{message}");
    }

    #[test]
    fn copy_format_reads_csv_options() -> Result<(), DatabaseError> {
        let format = copy_format(vec![
            CopyOption::Delimiter('|'),
            CopyOption::Header(true),
            CopyOption::Quote('\''),
            CopyOption::Escape('\\'),
        ])?;
        assert_eq!(
            format,
            ast::CopyFormat {
                delimiter: '|',
                quote: '\'',
                escape: Some('\\'),
                header: true,
            }
        );
        assert!(copy_format(vec![CopyOption::Null("NULL".to_string())]).is_err());
        assert_unsupported(parse_sql("copy t to stdout").unwrap_err(), "copy target");
        Ok(())
    }

    #[test]
    fn alter_table_accepts_a_single_operation() {
        assert_unsupported(
            parse_sql("alter table t1 add column c5 int null, add column c6 int null").unwrap_err(),
            "only a single ALTER TABLE operation",
        );
        let operation = AlterTableOperation::DropColumn {
            has_column_keyword: true,
            column_names: vec![Ident::new("c1"), Ident::new("c2")],
            if_exists: false,
            drop_behavior: None,
        };
        assert_unsupported(
            Lower {
                types: &(),
                set_autocommit: false,
            }
            .alter_table_operation(operation)
            .unwrap_err(),
            "only dropping a single column",
        );
    }
}
// GRCOV_EXCL_STOP
