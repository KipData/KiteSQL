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

pub mod aggregate;
mod alter_table;
mod analyze;
pub mod copy;
mod create_index;
mod create_table;
mod create_view;
mod delete;
mod describe;
mod distinct;
mod drop_index;
mod drop_table;
mod drop_view;
mod explain;
pub mod expr;
mod insert;
mod select;
mod show_table;
mod show_view;
mod truncate;
mod update;

use sqlparser::ast::{
    DescribeAlias, FromTable, Ident, ObjectName, ObjectNamePart, ObjectType, SetExpr, Spanned,
    Statement, TableObject,
};
use sqlparser::tokenizer::Span;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::catalog::view::View;
use crate::catalog::{ColumnRef, TableCatalog, TableName};
use crate::db::{ScalaFunctions, TableFunctions};
use crate::errors::{DatabaseError, SqlErrorSpan};
use crate::expression::ScalarExpression;
use crate::planner::operator::join::JoinType;
use crate::planner::{LogicalPlan, SchemaOutput};
use crate::storage::{TableCache, Transaction, ViewCache};
use crate::types::tuple::SchemaRef;
use crate::types::value::DataValue;

pub enum InputRefType {
    AggCall,
    GroupBy,
}

pub enum CommandType {
    DQL,
    DML,
    DDL,
}

fn annotate_bind_error(stmt: &Statement, err: DatabaseError) -> DatabaseError {
    attach_span_if_absent(err, stmt)
}

pub(crate) fn attach_span_from_sqlparser_span_if_absent(
    err: DatabaseError,
    span: Span,
) -> DatabaseError {
    if err.sql_error_span().is_some() {
        return err;
    }

    match sqlparser_span_to_sql_error_span(span) {
        Some(span) => err.with_span(span),
        None => err,
    }
}

pub(crate) fn attach_span_if_absent<T: Spanned + ?Sized>(
    err: DatabaseError,
    node: &T,
) -> DatabaseError {
    attach_span_from_sqlparser_span_if_absent(err, node.span())
}

pub(crate) fn sqlparser_span_to_sql_error_span(span: Span) -> Option<SqlErrorSpan> {
    if span == Span::empty() {
        return None;
    }

    let start = span.start.column as usize;
    let mut end = span.end.column as usize;
    if end <= start {
        end = start.saturating_add(1);
    }

    Some(SqlErrorSpan {
        start,
        end,
        line: span.start.line as usize,
        near: None,
    })
}

pub fn command_type(stmt: &Statement) -> Result<CommandType, DatabaseError> {
    match stmt {
        Statement::CreateTable(_)
        | Statement::CreateIndex(_)
        | Statement::CreateView(_)
        | Statement::AlterTable(_)
        | Statement::Drop { .. } => Ok(CommandType::DDL),
        Statement::Query(_)
        | Statement::Explain { .. }
        | Statement::ExplainTable { .. }
        | Statement::ShowTables { .. }
        | Statement::ShowViews { .. } => Ok(CommandType::DQL),
        Statement::Analyze(_)
        | Statement::Truncate(_)
        | Statement::Update(_)
        | Statement::Delete(_)
        | Statement::Insert(_)
        | Statement::Copy { .. } => Ok(CommandType::DML),
        stmt => Err(DatabaseError::UnsupportedStmt(stmt.to_string())),
    }
}

// Tips: only query now!
#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
pub enum QueryBindStep {
    From,
    Join,
    Where,
    Agg,
    Having,
    Distinct,
    Sort,
    Project,
    Limit,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum SubQueryType {
    SubQuery(LogicalPlan),
    ExistsSubQuery(bool, LogicalPlan),
    InSubQuery(bool, LogicalPlan),
}

#[derive(Debug, Clone)]
pub enum Source<'a> {
    Table(&'a TableCatalog),
    View(&'a View),
}

#[derive(Clone)]
pub struct BinderContext<'a, T: Transaction> {
    pub(crate) scala_functions: &'a ScalaFunctions,
    pub(crate) table_functions: &'a TableFunctions,
    pub(crate) table_cache: &'a TableCache,
    pub(crate) view_cache: &'a ViewCache,
    pub(crate) transaction: &'a T,
    // Tips: When there are multiple tables and Wildcard, use BTreeMap to ensure that the order of the output tables is certain.
    pub(crate) bind_table: BTreeMap<(TableName, Option<TableName>, Option<JoinType>), Source<'a>>,
    // alias
    expr_aliases: BTreeMap<(Option<String>, String), ScalarExpression>,
    table_aliases: HashMap<TableName, TableName>,
    // agg
    group_by_exprs: Vec<ScalarExpression>,
    pub(crate) agg_calls: Vec<ScalarExpression>,
    // join
    using: HashSet<ColumnRef>,

    bind_step: QueryBindStep,
    sub_queries: HashMap<QueryBindStep, Vec<SubQueryType>>,

    temp_table_id: Arc<AtomicUsize>,
    pub(crate) allow_default: bool,
}

impl Source<'_> {
    pub(crate) fn column(
        &self,
        name: &str,
        schema_buf: &mut Option<SchemaOutput>,
    ) -> Option<ColumnRef> {
        match self {
            Source::Table(table) => table.get_column_by_name(name),
            Source::View(view) => schema_buf
                .get_or_insert_with(|| view.plan.output_schema_direct())
                .columns()
                .find(|column| column.name() == name),
        }
        .cloned()
    }

    pub(crate) fn columns<'a>(
        &'a self,
        schema_buf: &'a mut Option<SchemaOutput>,
    ) -> Box<dyn Iterator<Item = &'a ColumnRef> + 'a> {
        match self {
            Source::Table(table) => Box::new(table.columns()),
            Source::View(view) => Box::new(
                schema_buf
                    .get_or_insert_with(|| view.plan.output_schema_direct())
                    .columns(),
            ),
        }
    }

    pub(crate) fn schema_ref(&self, schema_buf: &mut Option<SchemaOutput>) -> SchemaRef {
        match self {
            Source::Table(table) => table.schema_ref().clone(),
            Source::View(view) => {
                match schema_buf.get_or_insert_with(|| view.plan.output_schema_direct()) {
                    SchemaOutput::Schema(schema) => Arc::new(schema.clone()),
                    SchemaOutput::SchemaRef(schema_ref) => schema_ref.clone(),
                }
            }
        }
    }
}

impl<'a, T: Transaction> BinderContext<'a, T> {
    pub fn new(
        table_cache: &'a TableCache,
        view_cache: &'a ViewCache,
        transaction: &'a T,
        scala_functions: &'a ScalaFunctions,
        table_functions: &'a TableFunctions,
        temp_table_id: Arc<AtomicUsize>,
    ) -> Self {
        BinderContext {
            scala_functions,
            table_functions,
            table_cache,
            view_cache,
            transaction,
            bind_table: Default::default(),
            expr_aliases: Default::default(),
            table_aliases: Default::default(),
            group_by_exprs: vec![],
            agg_calls: Default::default(),
            using: Default::default(),
            bind_step: QueryBindStep::From,
            sub_queries: Default::default(),
            temp_table_id,
            allow_default: false,
        }
    }

    pub fn temp_table(&mut self) -> TableName {
        format!(
            "_temp_table_{}_",
            self.temp_table_id.fetch_add(1, Ordering::SeqCst)
        )
        .into()
    }

    pub fn step(&mut self, bind_step: QueryBindStep) {
        self.bind_step = bind_step;
    }

    pub fn is_step(&self, bind_step: &QueryBindStep) -> bool {
        &self.bind_step == bind_step
    }

    pub fn step_now(&self) -> QueryBindStep {
        self.bind_step
    }

    pub fn sub_query(&mut self, sub_query: SubQueryType) {
        self.sub_queries
            .entry(self.bind_step)
            .or_default()
            .push(sub_query)
    }

    pub fn sub_queries_at_now(&mut self) -> Option<Vec<SubQueryType>> {
        self.sub_queries.remove(&self.bind_step)
    }

    pub fn table(&self, table_name: TableName) -> Result<Option<&TableCatalog>, DatabaseError> {
        if let Some(real_name) = self.table_aliases.get(table_name.as_ref()) {
            self.transaction.table(self.table_cache, real_name.clone())
        } else {
            self.transaction.table(self.table_cache, table_name)
        }
    }

    pub fn view(&self, view_name: TableName) -> Result<Option<&View>, DatabaseError> {
        if let Some(real_name) = self.table_aliases.get(view_name.as_ref()) {
            self.transaction
                .view(self.table_cache, self.view_cache, real_name.clone())
        } else {
            self.transaction
                .view(self.table_cache, self.view_cache, view_name.clone())
        }
    }

    #[allow(unused_assignments)]
    pub fn source_and_bind(
        &mut self,
        table_name: TableName,
        alias: Option<&TableName>,
        join_type: Option<JoinType>,
        only_table: bool,
    ) -> Result<Option<Source<'_>>, DatabaseError> {
        let mut source = None;

        source = if let Some(real_name) = self.table_aliases.get(table_name.as_ref()) {
            self.transaction.table(self.table_cache, real_name.clone())
        } else {
            self.transaction.table(self.table_cache, table_name.clone())
        }?
        .map(Source::Table);

        if source.is_none() && !only_table {
            source = if let Some(real_name) = self.table_aliases.get(table_name.as_ref()) {
                self.transaction
                    .view(self.table_cache, self.view_cache, real_name.clone())
            } else {
                self.transaction
                    .view(self.table_cache, self.view_cache, table_name.clone())
            }?
            .map(Source::View);
        }
        if let Some(source) = &source {
            self.bind_table.insert(
                (table_name.clone(), alias.cloned(), join_type),
                source.clone(),
            );
        }
        Ok(source)
    }

    pub fn bind_source<'b: 'a>(&self, table_name: &str) -> Result<&Source<'_>, DatabaseError> {
        if let Some(source) = self.bind_table.iter().find(|((t, alias, _), _)| {
            t.as_ref() == table_name
                || matches!(alias.as_ref().map(|a| a.as_ref() == table_name), Some(true))
        }) {
            Ok(source.1)
        } else {
            Err(DatabaseError::invalid_table(table_name))
        }
    }

    // Tips: The order of this index is based on Aggregate being bound first.
    pub fn input_ref_index(&self, ty: InputRefType) -> usize {
        match ty {
            InputRefType::AggCall => self.agg_calls.len(),
            InputRefType::GroupBy => self.agg_calls.len() + self.group_by_exprs.len(),
        }
    }

    pub fn add_using(
        &mut self,
        join_type: JoinType,
        left_expr: &ColumnRef,
        right_expr: &ColumnRef,
    ) {
        self.using.insert(if join_type.is_right() {
            left_expr.clone()
        } else {
            right_expr.clone()
        });
    }

    pub fn add_alias(
        &mut self,
        alias_table: Option<String>,
        alias_column: String,
        expr: ScalarExpression,
    ) {
        self.expr_aliases.insert((alias_table, alias_column), expr);
    }

    pub fn add_table_alias(&mut self, alias: TableName, table: TableName) {
        self.table_aliases.insert(alias.clone(), table.clone());
    }

    pub fn has_agg_call(&self, expr: &ScalarExpression) -> bool {
        self.group_by_exprs.contains(expr)
    }
}

pub struct Binder<'a, 'b, T: Transaction, A: AsRef<[(&'static str, DataValue)]>> {
    context: BinderContext<'a, T>,
    table_schema_buf: HashMap<TableName, Option<SchemaOutput>>,
    args: &'a A,
    with_pk: Option<TableName>,
    pub(crate) parent: Option<&'b Binder<'a, 'b, T, A>>,
}

impl<'a, 'b, T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'a, 'b, T, A> {
    pub fn new(
        context: BinderContext<'a, T>,
        args: &'a A,
        parent: Option<&'b Binder<'a, 'b, T, A>>,
    ) -> Self {
        Binder {
            context,
            table_schema_buf: Default::default(),
            args,
            with_pk: None,
            parent,
        }
    }

    pub fn with_pk(&mut self, table_name: TableName) {
        self.with_pk = Some(table_name);
    }

    pub fn is_scan_with_pk(&self, table_name: &TableName) -> bool {
        if let Some(with_pk_table) = self.with_pk.as_ref() {
            return with_pk_table == table_name;
        }
        false
    }

    fn bind_inner(&mut self, stmt: &Statement) -> Result<LogicalPlan, DatabaseError> {
        let plan = match stmt {
            Statement::Query(query) => self.bind_query(query)?,
            Statement::AlterTable(alter) => {
                if alter.operations.len() != 1 {
                    return Err(DatabaseError::UnsupportedStmt(
                        "only a single ALTER TABLE operation is supported".to_string(),
                    ));
                }
                self.bind_alter_table(&alter.name, &alter.operations[0])?
            }
            Statement::CreateTable(create) => self.bind_create_table(
                &create.name,
                &create.columns,
                &create.constraints,
                create.if_not_exists,
            )?,
            Statement::Drop {
                object_type,
                names,
                if_exists,
                ..
            } => {
                if names.len() > 1 {
                    return Err(DatabaseError::UnsupportedStmt(
                        "only Drop a single `Table` or `View` is allowed".to_string(),
                    ));
                }
                match object_type {
                    ObjectType::Table => self.bind_drop_table(&names[0], if_exists)?,
                    ObjectType::View => self.bind_drop_view(&names[0], if_exists)?,
                    ObjectType::Index => self.bind_drop_index(&names[0], if_exists)?,
                    _ => {
                        return Err(DatabaseError::UnsupportedStmt(
                            "only `Table` and `View` are allowed to be Dropped".to_string(),
                        ))
                    }
                }
            }
            Statement::Insert(insert) => {
                let table_name = match &insert.table {
                    TableObject::TableName(table_name) => table_name,
                    TableObject::TableFunction(_) => {
                        return Err(DatabaseError::UnsupportedStmt(
                            "insert into table function is not supported".to_string(),
                        ))
                    }
                };
                let source = insert.source.as_ref().ok_or_else(|| {
                    DatabaseError::UnsupportedStmt(
                        "insert without source is not supported".to_string(),
                    )
                })?;
                // TODO: support body on Insert
                if let SetExpr::Values(values) = source.body.as_ref() {
                    self.bind_insert(
                        table_name,
                        &insert.columns,
                        &values.rows,
                        insert.overwrite,
                        false,
                    )?
                } else {
                    return Err(DatabaseError::UnsupportedStmt(format!(
                        "insert body: {:#?}",
                        source.body
                    )));
                }
            }
            Statement::Update(update) => {
                let table = &update.table;
                if !table.joins.is_empty() {
                    unimplemented!()
                } else {
                    self.bind_update(table, &update.selection, &update.assignments)?
                }
            }
            Statement::Delete(delete) => {
                let from = match &delete.from {
                    FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from) => from,
                };
                let table = &from[0];

                if !table.joins.is_empty() {
                    unimplemented!()
                } else {
                    self.bind_delete(table, &delete.selection)?
                }
            }
            Statement::Analyze(analyze) => {
                let table_name = analyze.table_name.as_ref().ok_or_else(|| {
                    DatabaseError::UnsupportedStmt(
                        "ANALYZE without table is not supported".to_string(),
                    )
                })?;
                self.bind_analyze(table_name)?
            }
            Statement::Truncate(truncate) => {
                if truncate.table_names.len() != 1 {
                    return Err(DatabaseError::UnsupportedStmt(
                        "only truncate a single table is supported".to_string(),
                    ));
                }
                self.bind_truncate(&truncate.table_names[0].name)?
            }
            Statement::ShowTables { .. } => self.bind_show_tables()?,
            Statement::ShowViews { .. } => self.bind_show_views()?,
            Statement::Copy {
                source,
                to,
                target,
                options,
                ..
            } => self.bind_copy(source.clone(), *to, target.clone(), options)?,
            Statement::Explain { statement, .. } => {
                let plan = self.bind_inner(statement)?;

                self.bind_explain(plan)?
            }
            Statement::ExplainTable {
                describe_alias: DescribeAlias::Describe | DescribeAlias::Desc,
                table_name,
                ..
            } => self.bind_describe(table_name)?,
            Statement::CreateIndex(create) => self.bind_create_index(
                &create.table_name,
                create.name.as_ref(),
                &create.columns,
                create.if_not_exists,
                create.unique,
            )?,
            Statement::CreateView(create) => self.bind_create_view(
                &create.or_replace,
                &create.name,
                &create.columns,
                &create.query,
            )?,
            _ => return Err(DatabaseError::UnsupportedStmt(stmt.to_string())),
        };
        Ok(plan)
    }

    pub fn bind(&mut self, stmt: &Statement) -> Result<LogicalPlan, DatabaseError> {
        self.bind_inner(stmt)
            .map_err(|err| annotate_bind_error(stmt, err))
    }

    pub fn bind_set_expr(&mut self, set_expr: &SetExpr) -> Result<LogicalPlan, DatabaseError> {
        match set_expr {
            SetExpr::Select(select) => self.bind_select(select, &[]),
            SetExpr::Query(query) => self.bind_query(query),
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => self.bind_set_operation(op, set_quantifier, left, right),
            expr => Err(DatabaseError::UnsupportedStmt(format!(
                "set expression: {expr:?}"
            ))),
        }
    }

    fn extend(&mut self, context: BinderContext<'a, T>) {
        for (key, table) in context.bind_table {
            self.context.bind_table.insert(key, table);
        }
        for (key, expr) in context.expr_aliases {
            self.context.expr_aliases.insert(key, expr);
        }
        for (key, table_name) in context.table_aliases {
            self.context.table_aliases.insert(key, table_name);
        }
    }
}

fn lower_ident(ident: &Ident) -> String {
    ident.value.to_lowercase()
}

fn lower_name_part(part: &ObjectNamePart) -> Result<String, DatabaseError> {
    part.as_ident()
        .map(lower_ident)
        .ok_or_else(|| attach_span_if_absent(DatabaseError::invalid_table(part.to_string()), part))
}

/// Convert an object name into lower case
fn lower_case_name(name: &ObjectName) -> Result<String, DatabaseError> {
    if name.0.len() == 1 {
        return lower_name_part(&name.0[0]);
    }
    Err(attach_span_if_absent(
        DatabaseError::invalid_table(name.to_string()),
        name,
    ))
}

pub(crate) fn is_valid_identifier(s: &str) -> bool {
    s.chars().all(|c| c.is_alphanumeric() || c == '_')
        && !s.chars().next().unwrap_or_default().is_numeric()
        && !s.chars().all(|c| c == '_')
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod test {
    use crate::binder::{is_valid_identifier, Binder, BinderContext};
    use crate::catalog::{ColumnCatalog, ColumnDesc, TableCatalog};
    use crate::errors::DatabaseError;
    use crate::planner::LogicalPlan;
    use crate::storage::rocksdb::RocksStorage;
    use crate::storage::{Storage, TableCache, Transaction, ViewCache};
    use crate::types::ColumnId;
    use crate::types::LogicalType::Integer;
    use crate::utils::lru::SharedLruCache;
    use std::hash::RandomState;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use tempfile::TempDir;

    pub(crate) struct TableState<S: Storage> {
        pub(crate) table: TableCatalog,
        pub(crate) table_cache: Arc<TableCache>,
        pub(crate) view_cache: Arc<ViewCache>,
        pub(crate) storage: S,
    }

    impl<S: Storage> TableState<S> {
        pub(crate) fn plan<T: AsRef<str>>(&self, sql: T) -> Result<LogicalPlan, DatabaseError> {
            let scala_functions = Default::default();
            let table_functions = Default::default();
            let transaction = self.storage.transaction()?;
            let mut binder = Binder::new(
                BinderContext::new(
                    &self.table_cache,
                    &self.view_cache,
                    &transaction,
                    &scala_functions,
                    &table_functions,
                    Arc::new(AtomicUsize::new(0)),
                ),
                &[],
                None,
            );
            let stmt = crate::parser::parse_sql(sql)?;

            binder.bind(&stmt[0])
        }

        pub(crate) fn column_id_by_name(&self, name: &str) -> &ColumnId {
            self.table.get_column_id_by_name(name).unwrap()
        }
    }

    pub(crate) fn build_t1_table() -> Result<TableState<RocksStorage>, DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let view_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let storage = build_test_catalog(&table_cache, temp_dir.path())?;
        let table = {
            let transaction = storage.transaction()?;
            transaction
                .table(&table_cache, "t1".to_string().into())?
                .unwrap()
                .clone()
        };

        Ok(TableState {
            table,
            table_cache,
            view_cache,
            storage,
        })
    }

    pub(crate) fn build_test_catalog(
        table_cache: &TableCache,
        path: impl Into<PathBuf> + Send,
    ) -> Result<RocksStorage, DatabaseError> {
        let storage = RocksStorage::new(path)?;
        let mut transaction = storage.transaction()?;

        let _ = transaction.create_table(
            table_cache,
            "t1".to_string().into(),
            vec![
                ColumnCatalog::new(
                    "c1".to_string(),
                    false,
                    ColumnDesc::new(Integer, Some(0), false, None)?,
                ),
                ColumnCatalog::new(
                    "c2".to_string(),
                    false,
                    ColumnDesc::new(Integer, None, true, None)?,
                ),
            ],
            false,
        )?;

        let _ = transaction.create_table(
            table_cache,
            "t2".to_string().into(),
            vec![
                ColumnCatalog::new(
                    "c3".to_string(),
                    false,
                    ColumnDesc::new(Integer, Some(0), false, None)?,
                ),
                ColumnCatalog::new(
                    "c4".to_string(),
                    false,
                    ColumnDesc::new(Integer, None, false, None)?,
                ),
            ],
            false,
        )?;

        transaction.commit()?;

        Ok(storage)
    }

    #[test]
    pub fn test_valid_identifier() {
        assert!(is_valid_identifier("valid_table"));
        assert!(is_valid_identifier("valid_column"));
        assert!(is_valid_identifier("_valid_column"));
        assert!(is_valid_identifier("valid_column_1"));

        assert!(!is_valid_identifier("invalid_name&"));
        assert!(!is_valid_identifier("1_invalid_name"));
        assert!(!is_valid_identifier("____"));
    }
}
