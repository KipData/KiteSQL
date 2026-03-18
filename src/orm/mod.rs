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
    BinaryOperator as SqlBinaryOperator, CaseWhen, CastKind, CharLengthUnits, ColumnDef,
    ColumnOption, ColumnOptionDef, CreateIndex, CreateTable, DataType, Delete, Expr, FromTable,
    Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, GroupByExpr,
    HiveDistributionStyle, Ident, IndexColumn, Insert, KeyOrIndexDisplay, LimitClause,
    NullsDistinctOption, ObjectName, ObjectType, Offset, OffsetRows, OrderBy, OrderByExpr,
    OrderByKind, OrderByOptions, PrimaryKeyConstraint, Query, Select, SelectFlavor, SelectItem,
    SetExpr, TableFactor, TableObject, TableWithJoins, TimezoneInfo, UniqueConstraint, Update,
    Value, Values,
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
        QueryValue::from_ast(Expr::CompoundIdentifier(vec![
            ident(self.table),
            ident(self.column),
        ]))
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

    pub fn in_list<I, V>(self, values: I) -> QueryExpr
    where
        I: IntoIterator<Item = V>,
        V: Into<QueryValue>,
    {
        self.value().in_list(values)
    }

    pub fn not_in_list<I, V>(self, values: I) -> QueryExpr
    where
        I: IntoIterator<Item = V>,
        V: Into<QueryValue>,
    {
        self.value().not_in_list(values)
    }

    pub fn between<L: Into<QueryValue>, H: Into<QueryValue>>(self, low: L, high: H) -> QueryExpr {
        self.value().between(low, high)
    }

    pub fn not_between<L: Into<QueryValue>, H: Into<QueryValue>>(
        self,
        low: L,
        high: H,
    ) -> QueryExpr {
        self.value().not_between(low, high)
    }

    pub fn cast(self, data_type: &str) -> Result<QueryValue, DatabaseError> {
        self.value().cast(data_type)
    }

    pub fn cast_to(self, data_type: DataType) -> QueryValue {
        self.value().cast_to(data_type)
    }

    pub fn alias(self, alias: &str) -> ProjectedValue {
        self.value().alias(alias)
    }

    pub fn in_subquery<S: SubquerySource>(self, subquery: S) -> QueryExpr {
        self.value().in_subquery(subquery)
    }

    pub fn not_in_subquery<S: SubquerySource>(self, subquery: S) -> QueryExpr {
        self.value().not_in_subquery(subquery)
    }
}

#[derive(Debug, Clone, PartialEq)]
/// A lightweight ORM expression wrapper for value-producing SQL AST nodes.
pub struct QueryValue {
    expr: Expr,
}

#[derive(Debug, Clone, PartialEq)]
/// A projected ORM expression, optionally carrying a select-list alias.
pub struct ProjectedValue {
    item: SelectItem,
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
/// A lightweight ORM expression wrapper for predicate-oriented SQL AST nodes.
pub struct QueryExpr {
    expr: Expr,
}

pub fn func<N, I, V>(name: N, args: I) -> QueryValue
where
    N: Into<String>,
    I: IntoIterator<Item = V>,
    V: Into<QueryValue>,
{
    QueryValue::function(name, args)
}

pub fn count<V: Into<QueryValue>>(value: V) -> QueryValue {
    QueryValue::aggregate("count", [value.into()])
}

pub fn count_all() -> QueryValue {
    QueryValue::aggregate_all("count")
}

pub fn sum<V: Into<QueryValue>>(value: V) -> QueryValue {
    QueryValue::aggregate("sum", [value.into()])
}

pub fn avg<V: Into<QueryValue>>(value: V) -> QueryValue {
    QueryValue::aggregate("avg", [value.into()])
}

pub fn min<V: Into<QueryValue>>(value: V) -> QueryValue {
    QueryValue::aggregate("min", [value.into()])
}

pub fn max<V: Into<QueryValue>>(value: V) -> QueryValue {
    QueryValue::aggregate("max", [value.into()])
}

pub fn case_when<I, C, R, E>(conditions: I, else_result: E) -> QueryValue
where
    I: IntoIterator<Item = (C, R)>,
    C: Into<QueryExpr>,
    R: Into<QueryValue>,
    E: Into<QueryValue>,
{
    QueryValue::searched_case(conditions, else_result)
}

pub fn case_value<O, I, W, R, E>(operand: O, conditions: I, else_result: E) -> QueryValue
where
    O: Into<QueryValue>,
    I: IntoIterator<Item = (W, R)>,
    W: Into<QueryValue>,
    R: Into<QueryValue>,
    E: Into<QueryValue>,
{
    QueryValue::simple_case(operand, conditions, else_result)
}

impl QueryExpr {
    pub fn from_ast(expr: Expr) -> QueryExpr {
        Self { expr }
    }

    pub fn as_ast(&self) -> &Expr {
        &self.expr
    }

    pub fn into_ast(self) -> Expr {
        self.expr
    }

    pub fn and(self, rhs: QueryExpr) -> QueryExpr {
        QueryExpr::from_ast(Expr::BinaryOp {
            left: Box::new(nested_expr(self.into_ast())),
            op: SqlBinaryOperator::And,
            right: Box::new(nested_expr(rhs.into_ast())),
        })
    }

    pub fn or(self, rhs: QueryExpr) -> QueryExpr {
        QueryExpr::from_ast(Expr::BinaryOp {
            left: Box::new(nested_expr(self.into_ast())),
            op: SqlBinaryOperator::Or,
            right: Box::new(nested_expr(rhs.into_ast())),
        })
    }

    pub fn not(self) -> QueryExpr {
        QueryExpr::from_ast(Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Not,
            expr: Box::new(nested_expr(self.into_ast())),
        })
    }

    pub fn exists<S: SubquerySource>(subquery: S) -> QueryExpr {
        QueryExpr::from_ast(Expr::Exists {
            subquery: Box::new(subquery.into_subquery()),
            negated: false,
        })
    }

    pub fn not_exists<S: SubquerySource>(subquery: S) -> QueryExpr {
        QueryExpr::from_ast(Expr::Exists {
            subquery: Box::new(subquery.into_subquery()),
            negated: true,
        })
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

    fn into_ast(self) -> OrderByExpr {
        OrderByExpr {
            expr: self.value.into_ast(),
            options: OrderByOptions {
                asc: Some(!self.desc),
                nulls_first: None,
            },
            with_fill: None,
        }
    }
}

impl QueryValue {
    pub fn from_ast(expr: Expr) -> Self {
        Self { expr }
    }

    pub fn as_ast(&self) -> &Expr {
        &self.expr
    }

    pub fn into_ast(self) -> Expr {
        self.expr
    }

    pub fn function<N, I, V>(name: N, args: I) -> Self
    where
        N: Into<String>,
        I: IntoIterator<Item = V>,
        V: Into<QueryValue>,
    {
        Self::function_with_args(
            name,
            args.into_iter()
                .map(Into::into)
                .map(QueryValue::into_ast)
                .map(FunctionArgExpr::Expr),
        )
    }

    pub fn aggregate<N, I, V>(name: N, args: I) -> Self
    where
        N: Into<String>,
        I: IntoIterator<Item = V>,
        V: Into<QueryValue>,
    {
        Self::function(name, args)
    }

    pub fn aggregate_all(name: impl Into<String>) -> Self {
        Self::function_with_args(name, [FunctionArgExpr::Wildcard])
    }

    pub fn alias(self, alias: &str) -> ProjectedValue {
        ProjectedValue {
            item: SelectItem::ExprWithAlias {
                expr: self.into_ast(),
                alias: ident(alias),
            },
        }
    }

    pub fn searched_case<I, C, R, E>(conditions: I, else_result: E) -> Self
    where
        I: IntoIterator<Item = (C, R)>,
        C: Into<QueryExpr>,
        R: Into<QueryValue>,
        E: Into<QueryValue>,
    {
        Self::from_ast(Expr::Case {
            case_token: AttachedToken::empty(),
            end_token: AttachedToken::empty(),
            operand: None,
            conditions: conditions
                .into_iter()
                .map(|(condition, result)| CaseWhen {
                    condition: condition.into().into_ast(),
                    result: result.into().into_ast(),
                })
                .collect(),
            else_result: Some(Box::new(else_result.into().into_ast())),
        })
    }

    pub fn simple_case<O, I, W, R, E>(operand: O, conditions: I, else_result: E) -> Self
    where
        O: Into<QueryValue>,
        I: IntoIterator<Item = (W, R)>,
        W: Into<QueryValue>,
        R: Into<QueryValue>,
        E: Into<QueryValue>,
    {
        Self::from_ast(Expr::Case {
            case_token: AttachedToken::empty(),
            end_token: AttachedToken::empty(),
            operand: Some(Box::new(operand.into().into_ast())),
            conditions: conditions
                .into_iter()
                .map(|(condition, result)| CaseWhen {
                    condition: condition.into().into_ast(),
                    result: result.into().into_ast(),
                })
                .collect(),
            else_result: Some(Box::new(else_result.into().into_ast())),
        })
    }

    pub fn eq<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        QueryExpr::from_ast(Expr::BinaryOp {
            left: Box::new(self.into_ast()),
            op: CompareOp::Eq.as_ast(),
            right: Box::new(value.into().into_ast()),
        })
    }

    pub fn ne<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        QueryExpr::from_ast(Expr::BinaryOp {
            left: Box::new(self.into_ast()),
            op: CompareOp::Ne.as_ast(),
            right: Box::new(value.into().into_ast()),
        })
    }

    pub fn gt<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        QueryExpr::from_ast(Expr::BinaryOp {
            left: Box::new(self.into_ast()),
            op: CompareOp::Gt.as_ast(),
            right: Box::new(value.into().into_ast()),
        })
    }

    pub fn gte<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        QueryExpr::from_ast(Expr::BinaryOp {
            left: Box::new(self.into_ast()),
            op: CompareOp::Gte.as_ast(),
            right: Box::new(value.into().into_ast()),
        })
    }

    pub fn lt<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        QueryExpr::from_ast(Expr::BinaryOp {
            left: Box::new(self.into_ast()),
            op: CompareOp::Lt.as_ast(),
            right: Box::new(value.into().into_ast()),
        })
    }

    pub fn lte<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        QueryExpr::from_ast(Expr::BinaryOp {
            left: Box::new(self.into_ast()),
            op: CompareOp::Lte.as_ast(),
            right: Box::new(value.into().into_ast()),
        })
    }

    pub fn is_null(self) -> QueryExpr {
        QueryExpr::from_ast(Expr::IsNull(Box::new(self.into_ast())))
    }

    pub fn is_not_null(self) -> QueryExpr {
        QueryExpr::from_ast(Expr::IsNotNull(Box::new(self.into_ast())))
    }

    pub fn like<V: Into<QueryValue>>(self, pattern: V) -> QueryExpr {
        QueryExpr::from_ast(Expr::Like {
            negated: false,
            expr: Box::new(self.into_ast()),
            pattern: Box::new(pattern.into().into_ast()),
            escape_char: None,
            any: false,
        })
    }

    pub fn not_like<V: Into<QueryValue>>(self, pattern: V) -> QueryExpr {
        QueryExpr::from_ast(Expr::Like {
            negated: true,
            expr: Box::new(self.into_ast()),
            pattern: Box::new(pattern.into().into_ast()),
            escape_char: None,
            any: false,
        })
    }

    pub fn in_list<I, V>(self, values: I) -> QueryExpr
    where
        I: IntoIterator<Item = V>,
        V: Into<QueryValue>,
    {
        QueryExpr::from_ast(Expr::InList {
            expr: Box::new(self.into_ast()),
            list: values
                .into_iter()
                .map(Into::into)
                .map(QueryValue::into_ast)
                .collect(),
            negated: false,
        })
    }

    pub fn not_in_list<I, V>(self, values: I) -> QueryExpr
    where
        I: IntoIterator<Item = V>,
        V: Into<QueryValue>,
    {
        QueryExpr::from_ast(Expr::InList {
            expr: Box::new(self.into_ast()),
            list: values
                .into_iter()
                .map(Into::into)
                .map(QueryValue::into_ast)
                .collect(),
            negated: true,
        })
    }

    pub fn between<L: Into<QueryValue>, H: Into<QueryValue>>(self, low: L, high: H) -> QueryExpr {
        QueryExpr::from_ast(Expr::Between {
            expr: Box::new(self.into_ast()),
            negated: false,
            low: Box::new(low.into().into_ast()),
            high: Box::new(high.into().into_ast()),
        })
    }

    pub fn not_between<L: Into<QueryValue>, H: Into<QueryValue>>(
        self,
        low: L,
        high: H,
    ) -> QueryExpr {
        QueryExpr::from_ast(Expr::Between {
            expr: Box::new(self.into_ast()),
            negated: true,
            low: Box::new(low.into().into_ast()),
            high: Box::new(high.into().into_ast()),
        })
    }

    pub fn cast(self, data_type: &str) -> Result<QueryValue, DatabaseError> {
        Ok(self.cast_to(parse_data_type_fragment(data_type)?))
    }

    pub fn cast_to(self, data_type: DataType) -> QueryValue {
        QueryValue::from_ast(Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(self.into_ast()),
            data_type,
            array: false,
            format: None,
        })
    }

    pub fn subquery<S: SubquerySource>(query: S) -> QueryValue {
        QueryValue::from_ast(Expr::Subquery(Box::new(query.into_subquery())))
    }

    pub fn in_subquery<S: SubquerySource>(self, subquery: S) -> QueryExpr {
        QueryExpr::from_ast(Expr::InSubquery {
            expr: Box::new(self.into_ast()),
            subquery: Box::new(subquery.into_subquery()),
            negated: false,
        })
    }

    pub fn not_in_subquery<S: SubquerySource>(self, subquery: S) -> QueryExpr {
        QueryExpr::from_ast(Expr::InSubquery {
            expr: Box::new(self.into_ast()),
            subquery: Box::new(subquery.into_subquery()),
            negated: true,
        })
    }

    fn asc(self) -> SortExpr {
        SortExpr::new(self, false)
    }

    fn desc(self) -> SortExpr {
        SortExpr::new(self, true)
    }

    fn function_with_args<N, I>(name: N, args: I) -> Self
    where
        N: Into<String>,
        I: IntoIterator<Item = FunctionArgExpr>,
    {
        let name = name.into();
        Self::from_ast(Expr::Function(Function {
            name: object_name(&name),
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: args.into_iter().map(FunctionArg::Unnamed).collect(),
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
        }))
    }
}

impl<M, T> From<Field<M, T>> for QueryValue {
    fn from(value: Field<M, T>) -> Self {
        value.value()
    }
}

impl ProjectedValue {
    fn into_select_item(self) -> SelectItem {
        self.item
    }
}

impl<V: Into<QueryValue>> From<V> for ProjectedValue {
    fn from(value: V) -> Self {
        Self {
            item: SelectItem::UnnamedExpr(value.into().into_ast()),
        }
    }
}

impl From<Expr> for QueryValue {
    fn from(value: Expr) -> Self {
        QueryValue::from_ast(value)
    }
}

impl From<Expr> for QueryExpr {
    fn from(value: Expr) -> Self {
        QueryExpr::from_ast(value)
    }
}

impl<T: ToDataValue> From<T> for QueryValue {
    fn from(value: T) -> Self {
        QueryValue::from_ast(data_value_to_ast_expr(&value.to_data_value()))
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

mod private {
    pub trait Sealed {}
}

#[doc(hidden)]
pub trait SubquerySource: private::Sealed {
    fn into_subquery(self) -> Query;
}

/// Lightweight single-table query builder for ORM models.
pub struct SelectBuilder<Q: StatementSource, M: Model, P = ModelProjection> {
    state: BuilderState<Q, M>,
    projection: P,
}

/// Lightweight single-table query builder for scalar value projections.
pub type ProjectValueBuilder<Q, M> = SelectBuilder<Q, M, ValueProjection>;

pub struct ModelProjection;

pub struct ValueProjection {
    value: ProjectedValue,
}

struct BuilderState<Q: StatementSource, M: Model> {
    source: Q,
    filter: Option<QueryExpr>,
    group_bys: Vec<QueryValue>,
    having: Option<QueryExpr>,
    order_bys: Vec<SortExpr>,
    limit: Option<usize>,
    offset: Option<usize>,
    _marker: PhantomData<M>,
}

impl<Q: StatementSource, M: Model> BuilderState<Q, M> {
    fn new(source: Q) -> Self {
        Self {
            source,
            filter: None,
            group_bys: Vec::new(),
            having: None,
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

    fn push_order(mut self, order: SortExpr) -> Self {
        self.order_bys.push(order);
        self
    }

    fn push_group_by(mut self, expr: QueryValue) -> Self {
        self.group_bys.push(expr);
        self
    }
}

#[doc(hidden)]
pub trait ProjectionSpec<M: Model> {
    fn into_select_items(self) -> Vec<SelectItem>;
}

impl<M: Model> ProjectionSpec<M> for ModelProjection {
    fn into_select_items(self) -> Vec<SelectItem> {
        select_projection(M::fields())
    }
}

impl<M: Model> ProjectionSpec<M> for ValueProjection {
    fn into_select_items(self) -> Vec<SelectItem> {
        vec![self.value.into_select_item()]
    }
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

impl<Q: StatementSource, M: Model> SelectBuilder<Q, M, ModelProjection> {
    fn new(source: Q) -> Self {
        Self {
            state: BuilderState::new(source),
            projection: ModelProjection,
        }
    }

    pub fn fetch(self) -> Result<OrmIter<Q::Iter, M>, DatabaseError> {
        Ok(self.raw()?.orm::<M>())
    }

    pub fn get(self) -> Result<Option<M>, DatabaseError> {
        extract_optional_model(self.limit(1).raw()?)
    }
}

impl<Q: StatementSource, M: Model> SelectBuilder<Q, M, ValueProjection> {
    fn new_value<V: Into<ProjectedValue>>(source: Q, value: V) -> Self {
        Self {
            state: BuilderState::new(source),
            projection: ValueProjection {
                value: value.into(),
            },
        }
    }

    pub fn fetch<T: FromDataValue>(self) -> Result<ProjectValueIter<Q::Iter, T>, DatabaseError> {
        Ok(ProjectValueIter::new(self.raw()?))
    }

    pub fn get<T: FromDataValue>(self) -> Result<Option<T>, DatabaseError> {
        extract_optional_value(self.limit(1).raw()?)
    }
}

impl<Q: StatementSource, M: Model, P: ProjectionSpec<M>> SelectBuilder<Q, M, P> {
    fn push_filter(self, expr: QueryExpr, mode: FilterMode) -> Self {
        Self {
            state: self.state.push_filter(expr, mode),
            projection: self.projection,
        }
    }

    pub fn filter(mut self, expr: QueryExpr) -> Self {
        self.state.filter = Some(expr);
        self
    }

    pub fn and(self, left: QueryExpr, right: QueryExpr) -> Self {
        Self {
            state: self.state.push_filter(left.and(right), FilterMode::And),
            projection: self.projection,
        }
    }

    pub fn or(self, left: QueryExpr, right: QueryExpr) -> Self {
        Self {
            state: self.state.push_filter(left.or(right), FilterMode::Or),
            projection: self.projection,
        }
    }

    pub fn not(self, expr: QueryExpr) -> Self {
        Self {
            state: self.state.push_filter(expr.not(), FilterMode::Replace),
            projection: self.projection,
        }
    }

    pub fn where_exists<S: SubquerySource>(self, subquery: S) -> Self {
        Self {
            state: self
                .state
                .push_filter(QueryExpr::exists(subquery), FilterMode::Replace),
            projection: self.projection,
        }
    }

    pub fn where_not_exists<S: SubquerySource>(self, subquery: S) -> Self {
        Self {
            state: self
                .state
                .push_filter(QueryExpr::not_exists(subquery), FilterMode::Replace),
            projection: self.projection,
        }
    }

    pub fn group_by<V: Into<QueryValue>>(self, value: V) -> Self {
        Self {
            state: self.state.push_group_by(value.into()),
            projection: self.projection,
        }
    }

    pub fn having(mut self, expr: QueryExpr) -> Self {
        self.state.having = Some(expr);
        self
    }

    pub fn asc<V: Into<QueryValue>>(self, value: V) -> Self {
        Self {
            state: self.state.push_order(value.into().asc()),
            projection: self.projection,
        }
    }

    pub fn desc<V: Into<QueryValue>>(self, value: V) -> Self {
        Self {
            state: self.state.push_order(value.into().desc()),
            projection: self.projection,
        }
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.state.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: usize) -> Self {
        self.state.offset = Some(offset);
        self
    }

    fn build_query(self) -> Query {
        let SelectBuilder {
            state:
                BuilderState {
                    source: _,
                    filter,
                    group_bys,
                    having,
                    order_bys,
                    limit,
                    offset,
                    ..
                },
            projection,
        } = self;

        select_query(
            M::table_name(),
            projection.into_select_items(),
            filter,
            group_bys,
            having,
            order_bys,
            limit,
            offset,
        )
    }

    fn into_statement(self) -> (Q, Statement) {
        let SelectBuilder {
            state:
                BuilderState {
                    source,
                    filter,
                    group_bys,
                    having,
                    order_bys,
                    limit,
                    offset,
                    ..
                },
            projection,
        } = self;

        let statement = Statement::Query(Box::new(select_query(
            M::table_name(),
            projection.into_select_items(),
            filter,
            group_bys,
            having,
            order_bys,
            limit,
            offset,
        )));

        (source, statement)
    }

    impl_select_builder_compare_methods!(eq, ne, gt, gte, lt, lte);

    impl_select_builder_null_methods!(is_null, is_not_null);

    impl_select_builder_like_methods!(like, not_like);

    pub fn in_list<L, I, V>(self, left: L, values: I) -> Self
    where
        L: Into<QueryValue>,
        I: IntoIterator<Item = V>,
        V: Into<QueryValue>,
    {
        Self {
            state: self
                .state
                .push_filter(left.into().in_list(values), FilterMode::Replace),
            projection: self.projection,
        }
    }

    pub fn not_in_list<L, I, V>(self, left: L, values: I) -> Self
    where
        L: Into<QueryValue>,
        I: IntoIterator<Item = V>,
        V: Into<QueryValue>,
    {
        Self {
            state: self
                .state
                .push_filter(left.into().not_in_list(values), FilterMode::Replace),
            projection: self.projection,
        }
    }

    pub fn between<L, Low, High>(self, expr: L, low: Low, high: High) -> Self
    where
        L: Into<QueryValue>,
        Low: Into<QueryValue>,
        High: Into<QueryValue>,
    {
        Self {
            state: self
                .state
                .push_filter(expr.into().between(low, high), FilterMode::Replace),
            projection: self.projection,
        }
    }

    pub fn not_between<L, Low, High>(self, expr: L, low: Low, high: High) -> Self
    where
        L: Into<QueryValue>,
        Low: Into<QueryValue>,
        High: Into<QueryValue>,
    {
        Self {
            state: self
                .state
                .push_filter(expr.into().not_between(low, high), FilterMode::Replace),
            projection: self.projection,
        }
    }

    pub fn in_subquery<L: Into<QueryValue>, S: SubquerySource>(self, left: L, subquery: S) -> Self {
        Self {
            state: self
                .state
                .push_filter(left.into().in_subquery(subquery), FilterMode::Replace),
            projection: self.projection,
        }
    }

    pub fn not_in_subquery<L: Into<QueryValue>, S: SubquerySource>(
        self,
        left: L,
        subquery: S,
    ) -> Self {
        Self {
            state: self
                .state
                .push_filter(left.into().not_in_subquery(subquery), FilterMode::Replace),
            projection: self.projection,
        }
    }

    pub fn raw(self) -> Result<Q::Iter, DatabaseError> {
        let (source, statement) = self.into_statement();
        source.execute_statement(&statement, &[])
    }

    pub fn exists(self) -> Result<bool, DatabaseError> {
        let mut iter = self.limit(1).raw()?;
        Ok(iter.next().transpose()?.is_some())
    }

    pub fn count(self) -> Result<usize, DatabaseError> {
        let has_grouping = !self.state.group_bys.is_empty() || self.state.having.is_some();
        if has_grouping {
            let mut iter = self.raw()?;
            let mut count = 0usize;
            while iter.next().transpose()?.is_some() {
                count += 1;
            }
            iter.done()?;
            return Ok(count);
        }

        let BuilderState { source, filter, .. } = self.state;
        let statement = orm_count_statement(M::table_name(), filter);
        let mut iter = source.execute_statement(&statement, &[])?;
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

impl<Q: StatementSource, M: Model, P: ProjectionSpec<M>> private::Sealed
    for SelectBuilder<Q, M, P>
{
}

impl<Q: StatementSource, M: Model, P: ProjectionSpec<M>> SubquerySource for SelectBuilder<Q, M, P> {
    fn into_subquery(self) -> Query {
        self.build_query()
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
    filter: Option<QueryExpr>,
    group_bys: Vec<QueryValue>,
    having: Option<QueryExpr>,
    order_bys: Vec<SortExpr>,
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
            selection: filter.map(QueryExpr::into_ast),
            connect_by: vec![],
            group_by: GroupByExpr::Expressions(
                group_bys.into_iter().map(QueryValue::into_ast).collect(),
                vec![],
            ),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: having.map(QueryExpr::into_ast),
            named_window: vec![],
            qualify: None,
            window_before_qualify: false,
            value_table_mode: None,
            flavor: SelectFlavor::Standard,
        }))),
        order_by: (!order_bys.is_empty()).then(|| OrderBy {
            kind: OrderByKind::Expressions(order_bys.into_iter().map(SortExpr::into_ast).collect()),
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
        vec![],
        None,
        vec![],
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

fn orm_count_statement(table_name: &str, filter: Option<QueryExpr>) -> Statement {
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
        vec![],
        None,
        vec![],
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

/// Typed adapter over a [`ResultIter`] that yields projected values instead of raw tuples.
pub struct ProjectValueIter<I, T> {
    inner: I,
    _marker: PhantomData<T>,
}

impl<I, T> ProjectValueIter<I, T>
where
    I: ResultIter,
    T: FromDataValue,
{
    fn new(inner: I) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }

    /// Finishes the underlying raw iterator.
    pub fn done(self) -> Result<(), DatabaseError> {
        self.inner.done()
    }
}

impl<I, T> Iterator for ProjectValueIter<I, T>
where
    I: ResultIter,
    T: FromDataValue,
{
    type Item = Result<T, DatabaseError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|result| result.and_then(extract_value_from_tuple::<T>))
    }
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

fn extract_value_from_tuple<T: FromDataValue>(mut tuple: Tuple) -> Result<T, DatabaseError> {
    let value = if tuple.values.len() == 1 {
        tuple.values.swap_remove(0)
    } else {
        return Err(DatabaseError::MisMatch(
            "one projected expression",
            "the query result",
        ));
    };

    let value = match T::logical_type() {
        Some(ty) => value.cast(&ty)?,
        None => value,
    };

    T::from_data_value(value).ok_or_else(|| {
        DatabaseError::InvalidValue(format!(
            "failed to convert projected value into {}",
            std::any::type_name::<T>()
        ))
    })
}

fn extract_optional_value<I, T>(mut iter: I) -> Result<Option<T>, DatabaseError>
where
    I: ResultIter,
    T: FromDataValue,
{
    Ok(match iter.next() {
        Some(tuple) => Some(extract_value_from_tuple(tuple?)?),
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
