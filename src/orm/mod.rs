#![doc = include_str!("README.md")]

use crate::catalog::{ColumnRef, TableCatalog};
use crate::db::{
    DBTransaction, Database, DatabaseIter, OrmIter, ResultIter, Statement, TransactionIter,
};
use crate::errors::DatabaseError;
use crate::storage::{Storage, Transaction};
use crate::types::tuple::{SchemaRef, Tuple};
use crate::types::value::DataValue;
use crate::types::LogicalType;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    AlterColumnOperation, AlterTable, AlterTableOperation, Analyze, Assignment, AssignmentTarget,
    BinaryOperator as SqlBinaryOperator, CharLengthUnits, ColumnDef, ColumnOption, ColumnOptionDef,
    CreateIndex, CreateTable, DataType, Delete, Expr, FromTable, Function, FunctionArg,
    FunctionArgExpr, FunctionArgumentList, FunctionArguments, GroupByExpr, HiveDistributionStyle,
    Ident, IndexColumn, Insert, KeyOrIndexDisplay, LimitClause, NullsDistinctOption, ObjectName,
    ObjectType, Offset, OffsetRows, OrderBy, OrderByExpr, OrderByKind, OrderByOptions,
    PrimaryKeyConstraint, Query, Select, SelectFlavor, SelectItem, SetExpr, TableFactor,
    TableObject, TableWithJoins, TimezoneInfo, UniqueConstraint, Update, Value, Values,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

mod ddl;
mod dml;
mod dql;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Static metadata about a single model field.
///
/// This type is primarily consumed by code generated from `#[derive(Model)]`.
pub struct OrmField {
    pub column: &'static str,
    pub placeholder: &'static str,
    pub primary_key: bool,
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Static metadata about a single persisted model column.
///
/// This is primarily consumed by the built-in ORM migration helper.
pub struct OrmColumn {
    pub name: &'static str,
    pub ddl_type: String,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub default_expr: Option<&'static str>,
}

impl OrmColumn {
    /// Renders the SQL column definition used by `CREATE TABLE` and migrations.
    pub fn definition_sql(&self) -> String {
        let mut column_def = ::std::format!("{} {}", self.name, self.ddl_type);

        if self.primary_key {
            column_def.push_str(" primary key");
        } else {
            if !self.nullable {
                column_def.push_str(" not null");
            }
            if self.unique {
                column_def.push_str(" unique");
            }
        }
        if let Some(default_expr) = self.default_expr {
            column_def.push_str(" default ");
            column_def.push_str(default_expr);
        }

        column_def
    }

    fn column_def(&self) -> Result<ColumnDef, DatabaseError> {
        let mut options = Vec::new();

        if self.primary_key {
            options.push(column_option(ColumnOption::PrimaryKey(
                PrimaryKeyConstraint {
                    name: None,
                    index_name: None,
                    index_type: None,
                    columns: vec![],
                    index_options: vec![],
                    characteristics: None,
                },
            )));
        } else {
            if !self.nullable {
                options.push(column_option(ColumnOption::NotNull));
            }
            if self.unique {
                options.push(column_option(ColumnOption::Unique(UniqueConstraint {
                    name: None,
                    index_name: None,
                    index_type_display: KeyOrIndexDisplay::None,
                    index_type: None,
                    columns: vec![],
                    index_options: vec![],
                    characteristics: None,
                    nulls_distinct: NullsDistinctOption::None,
                })));
            }
        }
        if let Some(default_expr) = self.default_expr {
            options.push(column_option(ColumnOption::Default(parse_expr_fragment(
                default_expr,
            )?)));
        }

        Ok(ColumnDef {
            name: ident(self.name),
            data_type: parse_data_type_fragment(&self.ddl_type)?,
            options,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Typed column handle generated for `#[derive(Model)]` query builders.
pub struct Field<M, T> {
    table: &'static str,
    column: &'static str,
    _marker: PhantomData<(M, T)>,
}

impl<M, T> Field<M, T> {
    pub const fn new(table: &'static str, column: &'static str) -> Self {
        Self {
            table,
            column,
            _marker: PhantomData,
        }
    }

    fn value(self) -> QueryValue {
        QueryValue::Column {
            table: self.table,
            column: self.column,
        }
    }

    pub fn eq<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        self.value().eq(value)
    }

    pub fn ne<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        self.value().ne(value)
    }

    pub fn gt<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        self.value().gt(value)
    }

    pub fn gte<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        self.value().gte(value)
    }

    pub fn lt<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        self.value().lt(value)
    }

    pub fn lte<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        self.value().lte(value)
    }

    pub fn is_null(self) -> QueryExpr {
        self.value().is_null()
    }

    pub fn is_not_null(self) -> QueryExpr {
        self.value().is_not_null()
    }

    pub fn like<V: Into<QueryValue>>(self, pattern: V) -> QueryExpr {
        self.value().like(pattern)
    }

    pub fn not_like<V: Into<QueryValue>>(self, pattern: V) -> QueryExpr {
        self.value().not_like(pattern)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueryValue {
    Column {
        table: &'static str,
        column: &'static str,
    },
    Param(DataValue),
    Function {
        name: String,
        args: Vec<QueryValue>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueryExpr {
    Compare {
        left: QueryValue,
        op: CompareOp,
        right: QueryValue,
    },
    IsNull {
        value: QueryValue,
        negated: bool,
    },
    Like {
        value: QueryValue,
        pattern: QueryValue,
        negated: bool,
    },
    And(Box<QueryExpr>, Box<QueryExpr>),
    Or(Box<QueryExpr>, Box<QueryExpr>),
    Not(Box<QueryExpr>),
}

pub fn func<N, I, V>(name: N, args: I) -> QueryValue
where
    N: Into<String>,
    I: IntoIterator<Item = V>,
    V: Into<QueryValue>,
{
    QueryValue::function(name, args)
}

impl QueryExpr {
    pub fn and(self, rhs: QueryExpr) -> QueryExpr {
        QueryExpr::And(Box::new(self), Box::new(rhs))
    }

    pub fn or(self, rhs: QueryExpr) -> QueryExpr {
        QueryExpr::Or(Box::new(self), Box::new(rhs))
    }

    pub fn not(self) -> QueryExpr {
        QueryExpr::Not(Box::new(self))
    }

    fn to_ast(&self) -> Expr {
        match self {
            QueryExpr::Compare { left, op, right } => Expr::BinaryOp {
                left: Box::new(left.to_ast()),
                op: op.as_ast(),
                right: Box::new(right.to_ast()),
            },
            QueryExpr::IsNull { value, negated } => {
                if *negated {
                    Expr::IsNotNull(Box::new(value.to_ast()))
                } else {
                    Expr::IsNull(Box::new(value.to_ast()))
                }
            }
            QueryExpr::Like {
                value,
                pattern,
                negated,
            } => Expr::Like {
                negated: *negated,
                expr: Box::new(value.to_ast()),
                pattern: Box::new(pattern.to_ast()),
                escape_char: None,
                any: false,
            },
            QueryExpr::And(left, right) => Expr::BinaryOp {
                left: Box::new(nested_expr(left.to_ast())),
                op: SqlBinaryOperator::And,
                right: Box::new(nested_expr(right.to_ast())),
            },
            QueryExpr::Or(left, right) => Expr::BinaryOp {
                left: Box::new(nested_expr(left.to_ast())),
                op: SqlBinaryOperator::Or,
                right: Box::new(nested_expr(right.to_ast())),
            },
            QueryExpr::Not(inner) => Expr::UnaryOp {
                op: sqlparser::ast::UnaryOperator::Not,
                expr: Box::new(nested_expr(inner.to_ast())),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct SortExpr {
    value: QueryValue,
    desc: bool,
}

impl SortExpr {
    fn new(value: QueryValue, desc: bool) -> Self {
        Self { value, desc }
    }

    fn to_ast(&self) -> OrderByExpr {
        OrderByExpr {
            expr: self.value.to_ast(),
            options: OrderByOptions {
                asc: Some(!self.desc),
                nulls_first: None,
            },
            with_fill: None,
        }
    }
}

impl QueryValue {
    pub fn function<N, I, V>(name: N, args: I) -> Self
    where
        N: Into<String>,
        I: IntoIterator<Item = V>,
        V: Into<QueryValue>,
    {
        Self::Function {
            name: name.into(),
            args: args.into_iter().map(Into::into).collect(),
        }
    }

    pub fn eq<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        QueryExpr::Compare {
            left: self,
            op: CompareOp::Eq,
            right: value.into(),
        }
    }

    pub fn ne<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        QueryExpr::Compare {
            left: self,
            op: CompareOp::Ne,
            right: value.into(),
        }
    }

    pub fn gt<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        QueryExpr::Compare {
            left: self,
            op: CompareOp::Gt,
            right: value.into(),
        }
    }

    pub fn gte<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        QueryExpr::Compare {
            left: self,
            op: CompareOp::Gte,
            right: value.into(),
        }
    }

    pub fn lt<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        QueryExpr::Compare {
            left: self,
            op: CompareOp::Lt,
            right: value.into(),
        }
    }

    pub fn lte<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        QueryExpr::Compare {
            left: self,
            op: CompareOp::Lte,
            right: value.into(),
        }
    }

    pub fn is_null(self) -> QueryExpr {
        QueryExpr::IsNull {
            value: self,
            negated: false,
        }
    }

    pub fn is_not_null(self) -> QueryExpr {
        QueryExpr::IsNull {
            value: self,
            negated: true,
        }
    }

    pub fn like<V: Into<QueryValue>>(self, pattern: V) -> QueryExpr {
        QueryExpr::Like {
            value: self,
            pattern: pattern.into(),
            negated: false,
        }
    }

    pub fn not_like<V: Into<QueryValue>>(self, pattern: V) -> QueryExpr {
        QueryExpr::Like {
            value: self,
            pattern: pattern.into(),
            negated: true,
        }
    }

    fn asc(self) -> SortExpr {
        SortExpr::new(self, false)
    }

    fn desc(self) -> SortExpr {
        SortExpr::new(self, true)
    }

    fn to_ast(&self) -> Expr {
        match self {
            QueryValue::Column { table, column } => {
                Expr::CompoundIdentifier(vec![ident(*table), ident(*column)])
            }
            QueryValue::Param(value) => data_value_to_ast_expr(value),
            QueryValue::Function { name, args } => Expr::Function(Function {
                name: object_name(name),
                uses_odbc_syntax: false,
                parameters: FunctionArguments::None,
                args: FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    args: args
                        .iter()
                        .map(QueryValue::to_ast)
                        .map(FunctionArgExpr::Expr)
                        .map(FunctionArg::Unnamed)
                        .collect(),
                    clauses: vec![],
                }),
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
            }),
        }
    }
}

impl<M, T> From<Field<M, T>> for QueryValue {
    fn from(value: Field<M, T>) -> Self {
        value.value()
    }
}

impl<T: ToDataValue> From<T> for QueryValue {
    fn from(value: T) -> Self {
        QueryValue::Param(value.to_data_value())
    }
}

impl CompareOp {
    fn as_ast(&self) -> SqlBinaryOperator {
        match self {
            CompareOp::Eq => SqlBinaryOperator::Eq,
            CompareOp::Ne => SqlBinaryOperator::NotEq,
            CompareOp::Gt => SqlBinaryOperator::Gt,
            CompareOp::Gte => SqlBinaryOperator::GtEq,
            CompareOp::Lt => SqlBinaryOperator::Lt,
            CompareOp::Lte => SqlBinaryOperator::LtEq,
        }
    }
}

pub trait StatementSource {
    type Iter: ResultIter;

    fn execute_statement<A: AsRef<[(&'static str, DataValue)]>>(
        self,
        statement: &Statement,
        params: A,
    ) -> Result<Self::Iter, DatabaseError>;
}

impl<'a, S: Storage> StatementSource for &'a Database<S> {
    type Iter = DatabaseIter<'a, S>;

    fn execute_statement<A: AsRef<[(&'static str, DataValue)]>>(
        self,
        statement: &Statement,
        params: A,
    ) -> Result<Self::Iter, DatabaseError> {
        self.execute(statement, params)
    }
}

impl<'a, 'tx, S: Storage> StatementSource for &'a mut DBTransaction<'tx, S> {
    type Iter = TransactionIter<'a>;

    fn execute_statement<A: AsRef<[(&'static str, DataValue)]>>(
        self,
        statement: &Statement,
        params: A,
    ) -> Result<Self::Iter, DatabaseError> {
        self.execute(statement, params)
    }
}

/// Lightweight single-table query builder for ORM models.
pub struct SelectBuilder<Q: StatementSource, M: Model> {
    source: Q,
    filter: Option<QueryExpr>,
    order_bys: Vec<SortExpr>,
    limit: Option<usize>,
    offset: Option<usize>,
    _marker: PhantomData<M>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterMode {
    Replace,
    And,
    Or,
}

macro_rules! impl_select_builder_compare_methods {
    ($($name:ident),+ $(,)?) => {
        $(
            pub fn $name<L: Into<QueryValue>, R: Into<QueryValue>>(self, left: L, right: R) -> Self {
                self.push_filter(left.into().$name(right), FilterMode::Replace)
            }
        )+
    };
}

macro_rules! impl_select_builder_null_methods {
    ($($name:ident),+ $(,)?) => {
        $(
            pub fn $name<V: Into<QueryValue>>(self, value: V) -> Self {
                self.push_filter(value.into().$name(), FilterMode::Replace)
            }
        )+
    };
}

macro_rules! impl_select_builder_like_methods {
    ($($name:ident),+ $(,)?) => {
        $(
            pub fn $name<L: Into<QueryValue>, R: Into<QueryValue>>(self, value: L, pattern: R) -> Self {
                self.push_filter(value.into().$name(pattern), FilterMode::Replace)
            }
        )+
    };
}

impl<Q: StatementSource, M: Model> SelectBuilder<Q, M> {
    fn new(source: Q) -> Self {
        Self {
            source,
            filter: None,
            order_bys: Vec::new(),
            limit: None,
            offset: None,
            _marker: PhantomData,
        }
    }

    fn push_filter(mut self, expr: QueryExpr, mode: FilterMode) -> Self {
        self.filter = Some(match (mode, self.filter.take()) {
            (FilterMode::Replace, _) => expr,
            (FilterMode::And, Some(current)) => current.and(expr),
            (FilterMode::Or, Some(current)) => current.or(expr),
            (_, None) => expr,
        });
        self
    }

    pub fn filter(mut self, expr: QueryExpr) -> Self {
        self.filter = Some(expr);
        self
    }

    pub fn and(self, left: QueryExpr, right: QueryExpr) -> Self {
        self.push_filter(left.and(right), FilterMode::And)
    }

    pub fn or(self, left: QueryExpr, right: QueryExpr) -> Self {
        self.push_filter(left.or(right), FilterMode::Or)
    }

    pub fn not(self, expr: QueryExpr) -> Self {
        self.push_filter(expr.not(), FilterMode::Replace)
    }

    fn push_order(mut self, order: SortExpr) -> Self {
        self.order_bys.push(order);
        self
    }

    pub fn asc<V: Into<QueryValue>>(self, value: V) -> Self {
        self.push_order(value.into().asc())
    }

    pub fn desc<V: Into<QueryValue>>(self, value: V) -> Self {
        self.push_order(value.into().desc())
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    fn statement(&self) -> Statement {
        orm_select_query_statement(
            M::table_name(),
            M::fields(),
            self.filter.as_ref(),
            &self.order_bys,
            self.limit,
            self.offset,
        )
    }

    impl_select_builder_compare_methods!(eq, ne, gt, gte, lt, lte);

    impl_select_builder_null_methods!(is_null, is_not_null);

    impl_select_builder_like_methods!(like, not_like);

    pub fn raw(self) -> Result<Q::Iter, DatabaseError> {
        let statement = self.statement();
        self.source.execute_statement(&statement, &[])
    }

    pub fn fetch(self) -> Result<OrmIter<Q::Iter, M>, DatabaseError> {
        Ok(self.raw()?.orm::<M>())
    }

    pub fn get(self) -> Result<Option<M>, DatabaseError> {
        extract_optional_model(self.limit(1).raw()?)
    }

    pub fn exists(self) -> Result<bool, DatabaseError> {
        let mut iter = self.limit(1).raw()?;
        Ok(iter.next().transpose()?.is_some())
    }

    pub fn count(self) -> Result<usize, DatabaseError> {
        let statement = orm_count_statement(M::table_name(), self.filter.as_ref());
        let mut iter = self.source.execute_statement(&statement, &[])?;
        let count = match iter.next().transpose()? {
            Some(tuple) => match tuple.values.first() {
                Some(DataValue::Int32(value)) => *value as usize,
                Some(DataValue::Int64(value)) => *value as usize,
                Some(DataValue::UInt32(value)) => *value as usize,
                Some(DataValue::UInt64(value)) => *value as usize,
                other => {
                    return Err(DatabaseError::InvalidValue(format!(
                        "unexpected count result: {:?}",
                        other
                    )))
                }
            },
            None => 0,
        };
        iter.done()?;
        Ok(count)
    }
}

fn ident(value: impl Into<String>) -> Ident {
    Ident::new(value)
}

fn object_name(value: &str) -> ObjectName {
    value.split('.').map(ident).collect::<Vec<_>>().into()
}

fn nested_expr(expr: Expr) -> Expr {
    Expr::Nested(Box::new(expr))
}

fn number_expr(value: impl ToString) -> Expr {
    Expr::Value(Value::Number(value.to_string(), false).with_empty_span())
}

fn string_expr(value: impl Into<String>) -> Expr {
    Expr::Value(Value::SingleQuotedString(value.into()).with_empty_span())
}

fn placeholder_expr(value: &str) -> Expr {
    Expr::Value(Value::Placeholder(value.to_string()).with_empty_span())
}

fn typed_string_expr(data_type: DataType, value: impl Into<String>) -> Expr {
    Expr::TypedString(sqlparser::ast::TypedString {
        data_type,
        value: Value::SingleQuotedString(value.into()).with_empty_span(),
        uses_odbc_syntax: false,
    })
}

fn column_option(option: ColumnOption) -> ColumnOptionDef {
    ColumnOptionDef { name: None, option }
}

fn table_factor(table_name: &str) -> TableFactor {
    TableFactor::Table {
        name: object_name(table_name),
        alias: None,
        args: None,
        with_hints: vec![],
        version: None,
        with_ordinality: false,
        partitions: vec![],
        json_path: None,
        sample: None,
        index_hints: vec![],
    }
}

fn table_with_joins(table_name: &str) -> TableWithJoins {
    TableWithJoins {
        relation: table_factor(table_name),
        joins: vec![],
    }
}

fn select_projection(fields: &[OrmField]) -> Vec<SelectItem> {
    fields
        .iter()
        .map(|field| SelectItem::UnnamedExpr(Expr::Identifier(ident(field.column))))
        .collect()
}

fn select_query(
    table_name: &str,
    projection: Vec<SelectItem>,
    filter: Option<&QueryExpr>,
    order_bys: &[SortExpr],
    limit: Option<usize>,
    offset: Option<usize>,
) -> Query {
    Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(Select {
            select_token: AttachedToken::empty(),
            optimizer_hint: None,
            distinct: None,
            select_modifiers: None,
            top: None,
            top_before_distinct: false,
            projection,
            exclude: None,
            into: None,
            from: vec![table_with_joins(table_name)],
            lateral_views: vec![],
            prewhere: None,
            selection: filter.map(QueryExpr::to_ast),
            connect_by: vec![],
            group_by: GroupByExpr::Expressions(vec![], vec![]),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            named_window: vec![],
            qualify: None,
            window_before_qualify: false,
            value_table_mode: None,
            flavor: SelectFlavor::Standard,
        }))),
        order_by: (!order_bys.is_empty()).then(|| OrderBy {
            kind: OrderByKind::Expressions(order_bys.iter().map(SortExpr::to_ast).collect()),
            interpolate: None,
        }),
        limit_clause: if limit.is_some() || offset.is_some() {
            Some(LimitClause::LimitOffset {
                limit: limit.map(number_expr),
                offset: offset.map(|offset| Offset {
                    value: number_expr(offset),
                    rows: OffsetRows::None,
                }),
                limit_by: vec![],
            })
        } else {
            None
        },
        fetch: None,
        locks: vec![],
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: vec![],
    }
}

fn values_query(values: Vec<Expr>) -> Query {
    Query {
        with: None,
        body: Box::new(SetExpr::Values(Values {
            explicit_row: false,
            value_keyword: false,
            rows: vec![values],
        })),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: vec![],
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: vec![],
    }
}

fn parse_expr_fragment(value: &str) -> Result<Expr, DatabaseError> {
    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect).try_with_sql(value)?;
    parser.parse_expr().map_err(Into::into)
}

fn parse_data_type_fragment(value: &str) -> Result<DataType, DatabaseError> {
    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect).try_with_sql(value)?;
    parser.parse_data_type().map_err(Into::into)
}

fn data_value_to_ast_expr(value: &DataValue) -> Expr {
    match value {
        DataValue::Null => Expr::Value(Value::Null.with_empty_span()),
        DataValue::Boolean(value) => Expr::Value(Value::Boolean(*value).with_empty_span()),
        DataValue::Float32(value) => number_expr(value),
        DataValue::Float64(value) => number_expr(value),
        DataValue::Int8(value) => number_expr(value),
        DataValue::Int16(value) => number_expr(value),
        DataValue::Int32(value) => number_expr(value),
        DataValue::Int64(value) => number_expr(value),
        DataValue::UInt8(value) => number_expr(value),
        DataValue::UInt16(value) => number_expr(value),
        DataValue::UInt32(value) => number_expr(value),
        DataValue::UInt64(value) => number_expr(value),
        DataValue::Utf8 { value, .. } => string_expr(value),
        DataValue::Date32(_) => typed_string_expr(DataType::Date, value.to_string()),
        DataValue::Date64(_) => typed_string_expr(DataType::Datetime(None), value.to_string()),
        DataValue::Time32(..) => {
            typed_string_expr(DataType::Time(None, TimezoneInfo::None), value.to_string())
        }
        DataValue::Time64(_, _, zone) => typed_string_expr(
            DataType::Timestamp(
                None,
                if *zone {
                    TimezoneInfo::WithTimeZone
                } else {
                    TimezoneInfo::None
                },
            ),
            value.to_string(),
        ),
        DataValue::Decimal(value) => number_expr(value),
        DataValue::Tuple(values, ..) => {
            Expr::Tuple(values.iter().map(data_value_to_ast_expr).collect())
        }
    }
}

#[doc(hidden)]
pub fn orm_select_statement(table_name: &str, fields: &[OrmField]) -> Statement {
    Statement::Query(Box::new(select_query(
        table_name,
        select_projection(fields),
        None,
        &[],
        None,
        None,
    )))
}

#[doc(hidden)]
pub fn orm_insert_statement(table_name: &str, fields: &[OrmField]) -> Statement {
    Statement::Insert(Insert {
        insert_token: AttachedToken::empty(),
        optimizer_hint: None,
        or: None,
        ignore: false,
        into: true,
        table: TableObject::TableName(object_name(table_name)),
        table_alias: None,
        columns: fields.iter().map(|field| ident(field.column)).collect(),
        overwrite: false,
        source: Some(Box::new(values_query(
            fields
                .iter()
                .map(|field| placeholder_expr(field.placeholder))
                .collect(),
        ))),
        assignments: vec![],
        partitioned: None,
        after_columns: vec![],
        has_table_keyword: false,
        on: None,
        returning: None,
        replace_into: false,
        priority: None,
        insert_alias: None,
        settings: None,
        format_clause: None,
    })
}

#[doc(hidden)]
pub fn orm_update_statement(
    table_name: &str,
    fields: &[OrmField],
    primary_key: &OrmField,
) -> Statement {
    Statement::Update(Update {
        update_token: AttachedToken::empty(),
        optimizer_hint: None,
        table: table_with_joins(table_name),
        assignments: fields
            .iter()
            .filter(|field| !field.primary_key)
            .map(|field| Assignment {
                target: AssignmentTarget::ColumnName(object_name(field.column)),
                value: placeholder_expr(field.placeholder),
            })
            .collect(),
        from: None,
        selection: Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(ident(primary_key.column))),
            op: SqlBinaryOperator::Eq,
            right: Box::new(placeholder_expr(primary_key.placeholder)),
        }),
        returning: None,
        or: None,
        limit: None,
    })
}

#[doc(hidden)]
pub fn orm_delete_statement(table_name: &str, primary_key: &OrmField) -> Statement {
    Statement::Delete(Delete {
        delete_token: AttachedToken::empty(),
        optimizer_hint: None,
        tables: vec![],
        from: FromTable::WithFromKeyword(vec![table_with_joins(table_name)]),
        using: None,
        selection: Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(ident(primary_key.column))),
            op: SqlBinaryOperator::Eq,
            right: Box::new(placeholder_expr(primary_key.placeholder)),
        }),
        returning: None,
        order_by: vec![],
        limit: None,
    })
}

#[doc(hidden)]
pub fn orm_find_statement(
    table_name: &str,
    fields: &[OrmField],
    primary_key: &OrmField,
) -> Statement {
    Statement::Query(Box::new(Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(Select {
            select_token: AttachedToken::empty(),
            optimizer_hint: None,
            distinct: None,
            select_modifiers: None,
            top: None,
            top_before_distinct: false,
            projection: select_projection(fields),
            exclude: None,
            into: None,
            from: vec![table_with_joins(table_name)],
            lateral_views: vec![],
            prewhere: None,
            selection: Some(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(ident(primary_key.column))),
                op: SqlBinaryOperator::Eq,
                right: Box::new(placeholder_expr(primary_key.placeholder)),
            }),
            connect_by: vec![],
            group_by: GroupByExpr::Expressions(vec![], vec![]),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            named_window: vec![],
            qualify: None,
            window_before_qualify: false,
            value_table_mode: None,
            flavor: SelectFlavor::Standard,
        }))),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: vec![],
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: vec![],
    }))
}

#[doc(hidden)]
pub fn orm_create_table_statement(
    table_name: &str,
    columns: &[OrmColumn],
    if_not_exists: bool,
) -> Result<Statement, DatabaseError> {
    Ok(Statement::CreateTable(CreateTable {
        or_replace: false,
        temporary: false,
        external: false,
        dynamic: false,
        global: None,
        if_not_exists,
        transient: false,
        volatile: false,
        iceberg: false,
        name: object_name(table_name),
        columns: columns
            .iter()
            .map(OrmColumn::column_def)
            .collect::<Result<Vec<_>, _>>()?,
        constraints: vec![],
        hive_distribution: HiveDistributionStyle::NONE,
        hive_formats: None,
        table_options: Default::default(),
        file_format: None,
        location: None,
        query: None,
        without_rowid: false,
        like: None,
        clone: None,
        version: None,
        comment: None,
        on_commit: None,
        on_cluster: None,
        primary_key: None,
        order_by: None,
        partition_by: None,
        cluster_by: None,
        clustered_by: None,
        inherits: None,
        partition_of: None,
        for_values: None,
        strict: false,
        copy_grants: false,
        enable_schema_evolution: None,
        change_tracking: None,
        data_retention_time_in_days: None,
        max_data_extension_time_in_days: None,
        default_ddl_collation: None,
        with_aggregation_policy: None,
        with_row_access_policy: None,
        with_tags: None,
        external_volume: None,
        base_location: None,
        catalog: None,
        catalog_sync: None,
        storage_serialization_policy: None,
        target_lag: None,
        warehouse: None,
        refresh_mode: None,
        initialize: None,
        require_user: false,
    }))
}

#[doc(hidden)]
pub fn orm_create_index_statement(
    table_name: &str,
    index_name: &str,
    columns: &[&str],
    unique: bool,
    if_not_exists: bool,
) -> Statement {
    Statement::CreateIndex(CreateIndex {
        name: Some(object_name(index_name)),
        table_name: object_name(table_name),
        using: None,
        columns: columns.iter().copied().map(IndexColumn::from).collect(),
        unique,
        concurrently: false,
        if_not_exists,
        include: vec![],
        nulls_distinct: None,
        with: vec![],
        predicate: None,
        index_options: vec![],
        alter_options: vec![],
    })
}

#[doc(hidden)]
pub fn orm_drop_table_statement(table_name: &str, if_exists: bool) -> Statement {
    Statement::Drop {
        object_type: ObjectType::Table,
        if_exists,
        names: vec![object_name(table_name)],
        cascade: false,
        restrict: false,
        purge: false,
        temporary: false,
        table: None,
    }
}

#[doc(hidden)]
pub fn orm_drop_index_statement(table_name: &str, index_name: &str, if_exists: bool) -> Statement {
    Statement::Drop {
        object_type: ObjectType::Index,
        if_exists,
        names: vec![object_name(&format!("{table_name}.{index_name}"))],
        cascade: false,
        restrict: false,
        purge: false,
        temporary: false,
        table: None,
    }
}

#[doc(hidden)]
pub fn orm_analyze_statement(table_name: &str) -> Statement {
    Statement::Analyze(Analyze {
        table_name: Some(object_name(table_name)),
        partitions: None,
        for_columns: false,
        columns: vec![],
        cache_metadata: false,
        noscan: false,
        compute_statistics: false,
        has_table_keyword: true,
    })
}

fn orm_select_query_statement(
    table_name: &str,
    fields: &[OrmField],
    filter: Option<&QueryExpr>,
    order_bys: &[SortExpr],
    limit: Option<usize>,
    offset: Option<usize>,
) -> Statement {
    Statement::Query(Box::new(select_query(
        table_name,
        select_projection(fields),
        filter,
        order_bys,
        limit,
        offset,
    )))
}

fn orm_count_statement(table_name: &str, filter: Option<&QueryExpr>) -> Statement {
    Statement::Query(Box::new(select_query(
        table_name,
        vec![SelectItem::UnnamedExpr(Expr::Function(Function {
            name: object_name("count"),
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Wildcard)],
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
        }))],
        filter,
        &[],
        None,
        None,
    )))
}

fn orm_alter_table_statement(table_name: &str, operation: AlterTableOperation) -> Statement {
    Statement::AlterTable(AlterTable {
        name: object_name(table_name),
        if_exists: false,
        only: false,
        operations: vec![operation],
        location: None,
        on_cluster: None,
        table_type: None,
        end_token: AttachedToken::empty(),
    })
}

fn orm_alter_column_type_statement(
    table_name: &str,
    column_name: &str,
    ddl_type: &str,
) -> Result<Statement, DatabaseError> {
    Ok(orm_alter_table_statement(
        table_name,
        AlterTableOperation::AlterColumn {
            column_name: ident(column_name),
            op: AlterColumnOperation::SetDataType {
                data_type: parse_data_type_fragment(ddl_type)?,
                using: None,
                had_set: false,
            },
        },
    ))
}

fn orm_alter_column_default_statement(
    table_name: &str,
    column_name: &str,
    default_expr: Option<&str>,
) -> Result<Statement, DatabaseError> {
    Ok(orm_alter_table_statement(
        table_name,
        AlterTableOperation::AlterColumn {
            column_name: ident(column_name),
            op: match default_expr {
                Some(default_expr) => AlterColumnOperation::SetDefault {
                    value: parse_expr_fragment(default_expr)?,
                },
                None => AlterColumnOperation::DropDefault,
            },
        },
    ))
}

fn orm_alter_column_nullability_statement(
    table_name: &str,
    column_name: &str,
    nullable: bool,
) -> Statement {
    orm_alter_table_statement(
        table_name,
        AlterTableOperation::AlterColumn {
            column_name: ident(column_name),
            op: if nullable {
                AlterColumnOperation::DropNotNull
            } else {
                AlterColumnOperation::SetNotNull
            },
        },
    )
}

fn orm_rename_column_statement(table_name: &str, old_name: &str, new_name: &str) -> Statement {
    orm_alter_table_statement(
        table_name,
        AlterTableOperation::RenameColumn {
            old_column_name: ident(old_name),
            new_column_name: ident(new_name),
        },
    )
}

fn orm_drop_column_statement(table_name: &str, column_name: &str) -> Statement {
    orm_alter_table_statement(
        table_name,
        AlterTableOperation::DropColumn {
            has_column_keyword: true,
            column_names: vec![ident(column_name)],
            if_exists: false,
            drop_behavior: None,
        },
    )
}

fn orm_add_column_statement(
    table_name: &str,
    column: &OrmColumn,
) -> Result<Statement, DatabaseError> {
    Ok(orm_alter_table_statement(
        table_name,
        AlterTableOperation::AddColumn {
            column_keyword: true,
            if_not_exists: false,
            column_def: column.column_def()?,
            column_position: None,
        },
    ))
}

/// Trait implemented by ORM models.
///
/// In normal usage you should derive this trait with `#[derive(Model)]` rather
/// than implementing it by hand. The derive macro generates tuple mapping,
/// cached CRUD/DDL statements and model metadata.
pub trait Model: Sized + for<'a> From<(&'a SchemaRef, Tuple)> {
    /// Rust type used as the model primary key.
    ///
    /// This associated type lets APIs such as
    /// [`Database::get`](crate::orm::Database::get) and
    /// [`Database::delete_by_id`](crate::orm::Database::delete_by_id)
    /// infer the key type directly from the model, so callers only need to
    /// write `database.get::<User>(&id)`.
    type PrimaryKey: ToDataValue;

    /// Returns the backing table name for the model.
    fn table_name() -> &'static str;

    /// Returns metadata for every persisted field on the model.
    fn fields() -> &'static [OrmField];

    /// Returns persisted column definitions for the model.
    ///
    /// `#[derive(Model)]` generates this automatically. Manual implementations
    /// can override it to opt into [`Database::migrate`](crate::orm::Database::migrate).
    fn columns() -> &'static [OrmColumn] {
        &[]
    }

    /// Converts the model into named query parameters.
    fn params(&self) -> Vec<(&'static str, DataValue)>;

    /// Returns a reference to the current primary-key value.
    fn primary_key(&self) -> &Self::PrimaryKey;

    /// Returns the cached `SELECT` statement used by [`Database::fetch`](crate::orm::Database::fetch).
    fn select_statement() -> &'static Statement;

    /// Returns the cached `INSERT` statement for the model.
    fn insert_statement() -> &'static Statement;

    /// Returns the cached `UPDATE` statement for the model.
    fn update_statement() -> &'static Statement;

    /// Returns the cached `DELETE` statement for the model.
    fn delete_statement() -> &'static Statement;

    /// Returns the cached `SELECT .. WHERE primary_key = ...` statement.
    fn find_statement() -> &'static Statement;

    /// Returns the cached `CREATE TABLE` statement for the model.
    fn create_table_statement() -> &'static Statement;

    /// Returns the cached `CREATE TABLE IF NOT EXISTS` statement for the model.
    fn create_table_if_not_exists_statement() -> &'static Statement;

    /// Returns cached `CREATE INDEX` statements declared by the model.
    ///
    /// `#[derive(Model)]` generates these from fields annotated with
    /// `#[model(index)]`. Manual implementations can override this to provide
    /// custom secondary indexes.
    fn create_index_statements() -> &'static [Statement] {
        &[]
    }

    /// Returns cached `CREATE INDEX IF NOT EXISTS` statements declared by the model.
    fn create_index_if_not_exists_statements() -> &'static [Statement] {
        &[]
    }

    /// Returns the cached `DROP TABLE` statement for the model.
    fn drop_table_statement() -> &'static Statement;

    /// Returns the cached `DROP TABLE IF EXISTS` statement for the model.
    fn drop_table_if_exists_statement() -> &'static Statement;

    /// Returns the cached `ANALYZE TABLE` statement for the model.
    fn analyze_statement() -> &'static Statement;

    fn primary_key_field() -> &'static OrmField {
        Self::fields()
            .iter()
            .find(|field| field.primary_key)
            .expect("ORM model must define exactly one primary key field")
    }
}

/// Conversion trait from [`DataValue`] into Rust values for ORM mapping.
///
/// This trait is mainly intended for framework internals and derive-generated
/// code.
pub trait FromDataValue: Sized {
    fn logical_type() -> Option<LogicalType>;

    fn from_data_value(value: DataValue) -> Option<Self>;
}

/// Conversion trait from Rust values into [`DataValue`] for ORM parameters.
///
/// This trait is mainly intended for framework internals and derive-generated
/// code.
pub trait ToDataValue {
    fn to_data_value(&self) -> DataValue;
}

/// Maps a Rust field type to the SQL column type used by ORM DDL helpers.
///
/// `#[derive(Model)]` relies on this trait to build `CREATE TABLE` statements.
/// Most built-in scalar types already implement it, and custom types can opt in
/// by implementing this trait together with [`FromDataValue`] and [`ToDataValue`].
pub trait ModelColumnType {
    fn ddl_type() -> String;

    fn nullable() -> bool {
        false
    }
}

/// Marker trait for string-like model fields that support `#[model(varchar = N)]`
/// and `#[model(char = N)]`.
pub trait StringType {}

/// Marker trait for decimal-like model fields that support precision/scale DDL attributes.
pub trait DecimalType {}

/// Extracts and converts a named field from a tuple using the given schema.
///
/// This helper is used by code generated from `#[derive(Model)]` and by the
/// lower-level `from_tuple!` macro.
pub fn try_get<T: FromDataValue>(
    tuple: &mut Tuple,
    schema: &SchemaRef,
    field_name: &str,
) -> Option<T> {
    let ty = T::logical_type()?;
    let (idx, _) = schema
        .iter()
        .enumerate()
        .find(|(_, col)| col.name() == field_name)?;

    let value = std::mem::replace(&mut tuple.values[idx], DataValue::Null)
        .cast(&ty)
        .ok()?;

    T::from_data_value(value)
}

macro_rules! impl_from_data_value_by_method {
    ($ty:ty, $method:ident) => {
        impl FromDataValue for $ty {
            fn logical_type() -> Option<LogicalType> {
                LogicalType::type_trans::<Self>()
            }

            fn from_data_value(value: DataValue) -> Option<Self> {
                value.$method()
            }
        }
    };
}

macro_rules! impl_to_data_value_by_clone {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl ToDataValue for $ty {
                fn to_data_value(&self) -> DataValue {
                    DataValue::from(self.clone())
                }
            }
        )+
    };
}

impl_from_data_value_by_method!(bool, bool);
impl_from_data_value_by_method!(i8, i8);
impl_from_data_value_by_method!(i16, i16);
impl_from_data_value_by_method!(i32, i32);
impl_from_data_value_by_method!(i64, i64);
impl_from_data_value_by_method!(u8, u8);
impl_from_data_value_by_method!(u16, u16);
impl_from_data_value_by_method!(u32, u32);
impl_from_data_value_by_method!(u64, u64);
impl_from_data_value_by_method!(f32, float);
impl_from_data_value_by_method!(f64, double);
impl_from_data_value_by_method!(NaiveDate, date);
impl_from_data_value_by_method!(NaiveDateTime, datetime);
impl_from_data_value_by_method!(NaiveTime, time);
impl_from_data_value_by_method!(Decimal, decimal);

impl_to_data_value_by_clone!(bool, i8, i16, i32, i64, u8, u16, u32, u64, f32, f64, Decimal, String);

macro_rules! impl_model_column_type {
    ($sql:expr; $($ty:ty),+ $(,)?) => {
        $(
            impl ModelColumnType for $ty {
                fn ddl_type() -> String {
                    $sql.to_string()
                }
            }
        )+
    };
}

impl_model_column_type!("boolean"; bool);
impl_model_column_type!("tinyint"; i8);
impl_model_column_type!("smallint"; i16);
impl_model_column_type!("int"; i32);
impl_model_column_type!("bigint"; i64);
impl_model_column_type!("utinyint"; u8);
impl_model_column_type!("usmallint"; u16);
impl_model_column_type!("unsigned integer"; u32);
impl_model_column_type!("ubigint"; u64);
impl_model_column_type!("float"; f32);
impl_model_column_type!("double"; f64);
impl_model_column_type!("date"; NaiveDate);
impl_model_column_type!("datetime"; NaiveDateTime);
impl_model_column_type!("time"; NaiveTime);
impl_model_column_type!("decimal"; Decimal);
impl_model_column_type!("varchar"; String, Arc<str>);

impl StringType for String {}
impl StringType for Arc<str> {}
impl DecimalType for Decimal {}

impl FromDataValue for String {
    fn logical_type() -> Option<LogicalType> {
        LogicalType::type_trans::<Self>()
    }

    fn from_data_value(value: DataValue) -> Option<Self> {
        if let DataValue::Utf8 { value, .. } = value {
            Some(value)
        } else {
            None
        }
    }
}

impl FromDataValue for Arc<str> {
    fn logical_type() -> Option<LogicalType> {
        Some(LogicalType::Varchar(None, CharLengthUnits::Characters))
    }

    fn from_data_value(value: DataValue) -> Option<Self> {
        if let DataValue::Utf8 { value, .. } = value {
            Some(value.into())
        } else {
            None
        }
    }
}

impl ToDataValue for Arc<str> {
    fn to_data_value(&self) -> DataValue {
        DataValue::from(self.to_string())
    }
}

impl ToDataValue for str {
    fn to_data_value(&self) -> DataValue {
        DataValue::from(self.to_string())
    }
}

impl ToDataValue for &str {
    fn to_data_value(&self) -> DataValue {
        DataValue::from((*self).to_string())
    }
}

impl ToDataValue for NaiveDate {
    fn to_data_value(&self) -> DataValue {
        DataValue::from(self)
    }
}

impl ToDataValue for NaiveDateTime {
    fn to_data_value(&self) -> DataValue {
        DataValue::from(self)
    }
}

impl ToDataValue for NaiveTime {
    fn to_data_value(&self) -> DataValue {
        DataValue::from(self)
    }
}

impl<T: FromDataValue> FromDataValue for Option<T> {
    fn logical_type() -> Option<LogicalType> {
        T::logical_type()
    }

    fn from_data_value(value: DataValue) -> Option<Self> {
        if matches!(value, DataValue::Null) {
            Some(None)
        } else {
            T::from_data_value(value).map(Some)
        }
    }
}

impl<T: ToDataValue> ToDataValue for Option<T> {
    fn to_data_value(&self) -> DataValue {
        match self {
            Some(value) => value.to_data_value(),
            None => DataValue::Null,
        }
    }
}

impl<T: ModelColumnType> ModelColumnType for Option<T> {
    fn ddl_type() -> String {
        T::ddl_type()
    }

    fn nullable() -> bool {
        true
    }
}

impl<T: StringType> StringType for Option<T> {}
impl<T: DecimalType> DecimalType for Option<T> {}

fn normalize_sql_fragment(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn canonicalize_model_type(value: &str) -> String {
    let normalized = normalize_sql_fragment(value);

    match normalized.as_str() {
        "boolean" => "boolean".to_string(),
        "tinyint" => "tinyint".to_string(),
        "smallint" => "smallint".to_string(),
        "int" | "integer" => "integer".to_string(),
        "bigint" => "bigint".to_string(),
        "utinyint" => "utinyint".to_string(),
        "usmallint" => "usmallint".to_string(),
        "unsigned integer" | "uinteger" => "uinteger".to_string(),
        "ubigint" => "ubigint".to_string(),
        "float" => "float".to_string(),
        "double" => "double".to_string(),
        "date" => "date".to_string(),
        "datetime" => "datetime".to_string(),
        "time" => "time(some(0))".to_string(),
        "varchar" => "varchar(none, characters)".to_string(),
        "decimal" => "decimal(none, none)".to_string(),
        _ => {
            if let Some(inner) = normalized
                .strip_prefix("varchar(")
                .and_then(|value| value.strip_suffix(')'))
            {
                return ::std::format!("varchar(some({}), characters)", inner.trim());
            }
            if let Some(inner) = normalized
                .strip_prefix("char(")
                .and_then(|value| value.strip_suffix(')'))
            {
                return ::std::format!("char({}, characters)", inner.trim());
            }
            if let Some(inner) = normalized
                .strip_prefix("decimal(")
                .and_then(|value| value.strip_suffix(')'))
            {
                let parts = inner.split(',').map(str::trim).collect::<Vec<_>>();
                return match parts.as_slice() {
                    [precision] => ::std::format!("decimal(some({precision}), none)"),
                    [precision, scale] => {
                        ::std::format!("decimal(some({precision}), some({scale}))")
                    }
                    _ => normalized,
                };
            }
            normalized
        }
    }
}

fn model_column_default(model: &OrmColumn) -> Option<String> {
    model.default_expr.map(normalize_sql_fragment)
}

fn catalog_column_default(column: &ColumnRef) -> Option<String> {
    column
        .desc()
        .default
        .as_ref()
        .map(|expr| normalize_sql_fragment(&expr.to_string()))
}

fn model_column_type_matches_catalog(model: &OrmColumn, column: &ColumnRef) -> bool {
    canonicalize_model_type(&model.ddl_type)
        == normalize_sql_fragment(&column.datatype().to_string())
}

fn model_column_matches_catalog(model: &OrmColumn, column: &ColumnRef) -> bool {
    model.primary_key == column.desc().is_primary()
        && model.unique == column.desc().is_unique()
        && model.nullable == column.nullable()
        && model_column_type_matches_catalog(model, column)
        && model_column_default(model) == catalog_column_default(column)
}

fn model_column_rename_compatible(model: &OrmColumn, column: &ColumnRef) -> bool {
    model.primary_key == column.desc().is_primary()
        && model.unique == column.desc().is_unique()
        && model.nullable == column.nullable()
        && model_column_type_matches_catalog(model, column)
        && model_column_default(model) == catalog_column_default(column)
}

fn extract_optional_model<I, M>(mut iter: I) -> Result<Option<M>, DatabaseError>
where
    I: ResultIter,
    M: Model,
{
    let schema = iter.schema().clone();

    Ok(match iter.next() {
        Some(tuple) => Some(M::from((&schema, tuple?))),
        None => None,
    })
}

fn orm_analyze<E: StatementSource, M: Model>(executor: E) -> Result<(), DatabaseError> {
    executor
        .execute_statement(M::analyze_statement(), &[])?
        .done()
}

fn orm_insert<E: StatementSource, M: Model>(executor: E, model: &M) -> Result<(), DatabaseError> {
    executor
        .execute_statement(M::insert_statement(), model.params())?
        .done()
}

fn orm_update<E: StatementSource, M: Model>(executor: E, model: &M) -> Result<(), DatabaseError> {
    executor
        .execute_statement(M::update_statement(), model.params())?
        .done()
}

fn orm_delete_by_id<E: StatementSource, M: Model>(
    executor: E,
    key: &M::PrimaryKey,
) -> Result<(), DatabaseError> {
    let params = [(M::primary_key_field().placeholder, key.to_data_value())];
    executor
        .execute_statement(M::delete_statement(), params)?
        .done()
}

fn orm_get<E: StatementSource, M: Model>(
    executor: E,
    key: &M::PrimaryKey,
) -> Result<Option<M>, DatabaseError> {
    let params = [(M::primary_key_field().placeholder, key.to_data_value())];
    extract_optional_model(executor.execute_statement(M::find_statement(), params)?)
}

fn orm_list<E: StatementSource, M: Model>(
    executor: E,
) -> Result<OrmIter<E::Iter, M>, DatabaseError> {
    Ok(executor
        .execute_statement(M::select_statement(), &[])?
        .orm::<M>())
}
