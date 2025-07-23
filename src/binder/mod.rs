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

use sqlparser::ast::{Ident, ObjectName, ObjectType, SetExpr, Statement};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::catalog::view::View;
use crate::catalog::{ColumnRef, TableCatalog, TableName};
use crate::db::{ScalaFunctions, TableFunctions};
use crate::errors::DatabaseError;
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

pub fn command_type(stmt: &Statement) -> Result<CommandType, DatabaseError> {
    match stmt {
        Statement::CreateTable { .. }
        | Statement::CreateIndex { .. }
        | Statement::CreateView { .. }
        | Statement::AlterTable { .. }
        | Statement::Drop { .. } => Ok(CommandType::DDL),
        Statement::Query(_)
        | Statement::Explain { .. }
        | Statement::ExplainTable { .. }
        | Statement::ShowTables { .. }
        | Statement::ShowVariable { .. } => Ok(CommandType::DQL),
        Statement::Analyze { .. }
        | Statement::Truncate { .. }
        | Statement::Update { .. }
        | Statement::Delete { .. }
        | Statement::Insert { .. }
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
    using: HashSet<String>,

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
        Arc::new(format!(
            "_temp_table_{}_",
            self.temp_table_id.fetch_add(1, Ordering::SeqCst)
        ))
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
    ) -> Result<Option<Source>, DatabaseError> {
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

    pub fn bind_source<'b: 'a>(&self, table_name: &str) -> Result<&Source, DatabaseError> {
        if let Some(source) = self.bind_table.iter().find(|((t, alias, _), _)| {
            t.as_str() == table_name
                || matches!(alias.as_ref().map(|a| a.as_str() == table_name), Some(true))
        }) {
            Ok(source.1)
        } else {
            Err(DatabaseError::InvalidTable(table_name.into()))
        }
    }

    // Tips: The order of this index is based on Aggregate being bound first.
    pub fn input_ref_index(&self, ty: InputRefType) -> usize {
        match ty {
            InputRefType::AggCall => self.agg_calls.len(),
            InputRefType::GroupBy => self.agg_calls.len() + self.group_by_exprs.len(),
        }
    }

    pub fn add_using(&mut self, name: String) {
        self.using.insert(name);
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

    pub fn bind(&mut self, stmt: &Statement) -> Result<LogicalPlan, DatabaseError> {
        let plan = match stmt {
            Statement::Query(query) => self.bind_query(query)?,
            Statement::AlterTable { name, operation } => self.bind_alter_table(name, operation)?,
            Statement::CreateTable {
                name,
                columns,
                constraints,
                if_not_exists,
                ..
            } => self.bind_create_table(name, columns, constraints, *if_not_exists)?,
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
            Statement::Insert {
                table_name,
                columns,
                source,
                overwrite,
                ..
            } => {
                // TODO: support body on Insert
                if let SetExpr::Values(values) = source.body.as_ref() {
                    self.bind_insert(table_name, columns, &values.rows, *overwrite, false)?
                } else {
                    return Err(DatabaseError::UnsupportedStmt(format!(
                        "insert body: {:#?}",
                        source.body
                    )));
                }
            }
            Statement::Update {
                table,
                selection,
                assignments,
                ..
            } => {
                if !table.joins.is_empty() {
                    unimplemented!()
                } else {
                    self.bind_update(table, selection, assignments)?
                }
            }
            Statement::Delete {
                from, selection, ..
            } => {
                let table = &from[0];

                if !table.joins.is_empty() {
                    unimplemented!()
                } else {
                    self.bind_delete(table, selection)?
                }
            }
            Statement::Analyze { table_name, .. } => self.bind_analyze(table_name)?,
            Statement::Truncate { table_name, .. } => self.bind_truncate(table_name)?,
            Statement::ShowTables { .. } => self.bind_show_tables()?,
            Statement::ShowVariable { variable } => match &variable[0].value.to_lowercase()[..] {
                "views" => self.bind_show_views()?,
                _ => return Err(DatabaseError::UnsupportedStmt(stmt.to_string())),
            },
            Statement::Copy {
                source,
                to,
                target,
                options,
                ..
            } => self.bind_copy(source.clone(), *to, target.clone(), options)?,
            Statement::Explain { statement, .. } => {
                let plan = self.bind(statement)?;

                self.bind_explain(plan)?
            }
            Statement::ExplainTable {
                describe_alias: true,
                table_name,
            } => self.bind_describe(table_name)?,
            Statement::CreateIndex {
                table_name,
                name,
                columns,
                if_not_exists,
                unique,
                ..
            } => self.bind_create_index(table_name, name, columns, *if_not_exists, *unique)?,
            Statement::CreateView {
                or_replace,
                name,
                columns,
                query,
                ..
            } => self.bind_create_view(or_replace, name, columns, query)?,
            _ => return Err(DatabaseError::UnsupportedStmt(stmt.to_string())),
        };
        Ok(plan)
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
                "set expression: {:?}",
                expr
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

/// Convert an object name into lower case
fn lower_case_name(name: &ObjectName) -> Result<String, DatabaseError> {
    if name.0.len() == 1 {
        return Ok(lower_ident(&name.0[0]));
    }
    Err(DatabaseError::InvalidTable(name.to_string()))
}

pub(crate) fn is_valid_identifier(s: &str) -> bool {
    s.chars().all(|c| c.is_alphanumeric() || c == '_')
        && !s.chars().next().unwrap_or_default().is_numeric()
        && !s.chars().all(|c| c == '_')
}

#[cfg(test)]
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

            Ok(binder.bind(&stmt[0])?)
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
                .table(&table_cache, Arc::new("t1".to_string()))?
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
            Arc::new("t1".to_string()),
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
            Arc::new("t2".to_string()),
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
