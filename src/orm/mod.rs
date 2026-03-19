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
#[doc(hidden)]
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
#[doc(hidden)]
pub struct OrmColumn {
    pub name: &'static str,
    pub ddl_type: String,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub default_expr: Option<&'static str>,
}

impl OrmColumn {
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
///
/// Most users obtain this through generated model accessors such as `User::id()`
/// rather than constructing it directly.
pub struct Field<M, T> {
    table: &'static str,
    column: &'static str,
    _marker: PhantomData<(M, T)>,
}

trait ValueExpressionOps: Sized {
    fn into_query_value(self) -> QueryValue;

    fn eq_expr<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        QueryExpr::from_expr(Expr::BinaryOp {
            left: Box::new(self.into_query_value().into_expr()),
            op: CompareOp::Eq.as_ast(),
            right: Box::new(value.into().into_expr()),
        })
    }

    fn ne_expr<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        QueryExpr::from_expr(Expr::BinaryOp {
            left: Box::new(self.into_query_value().into_expr()),
            op: CompareOp::Ne.as_ast(),
            right: Box::new(value.into().into_expr()),
        })
    }

    fn gt_expr<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        QueryExpr::from_expr(Expr::BinaryOp {
            left: Box::new(self.into_query_value().into_expr()),
            op: CompareOp::Gt.as_ast(),
            right: Box::new(value.into().into_expr()),
        })
    }

    fn gte_expr<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        QueryExpr::from_expr(Expr::BinaryOp {
            left: Box::new(self.into_query_value().into_expr()),
            op: CompareOp::Gte.as_ast(),
            right: Box::new(value.into().into_expr()),
        })
    }

    fn lt_expr<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        QueryExpr::from_expr(Expr::BinaryOp {
            left: Box::new(self.into_query_value().into_expr()),
            op: CompareOp::Lt.as_ast(),
            right: Box::new(value.into().into_expr()),
        })
    }

    fn lte_expr<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        QueryExpr::from_expr(Expr::BinaryOp {
            left: Box::new(self.into_query_value().into_expr()),
            op: CompareOp::Lte.as_ast(),
            right: Box::new(value.into().into_expr()),
        })
    }

    fn is_null_expr(self) -> QueryExpr {
        QueryExpr::from_expr(Expr::IsNull(Box::new(self.into_query_value().into_expr())))
    }

    fn is_not_null_expr(self) -> QueryExpr {
        QueryExpr::from_expr(Expr::IsNotNull(Box::new(
            self.into_query_value().into_expr(),
        )))
    }

    fn like_expr<V: Into<QueryValue>>(self, pattern: V) -> QueryExpr {
        QueryExpr::from_expr(Expr::Like {
            negated: false,
            expr: Box::new(self.into_query_value().into_expr()),
            pattern: Box::new(pattern.into().into_expr()),
            escape_char: None,
            any: false,
        })
    }

    fn not_like_expr<V: Into<QueryValue>>(self, pattern: V) -> QueryExpr {
        QueryExpr::from_expr(Expr::Like {
            negated: true,
            expr: Box::new(self.into_query_value().into_expr()),
            pattern: Box::new(pattern.into().into_expr()),
            escape_char: None,
            any: false,
        })
    }

    fn in_list_expr<I, V>(self, values: I) -> QueryExpr
    where
        I: IntoIterator<Item = V>,
        V: Into<QueryValue>,
    {
        QueryExpr::from_expr(Expr::InList {
            expr: Box::new(self.into_query_value().into_expr()),
            list: values
                .into_iter()
                .map(Into::into)
                .map(QueryValue::into_expr)
                .collect(),
            negated: false,
        })
    }

    fn not_in_list_expr<I, V>(self, values: I) -> QueryExpr
    where
        I: IntoIterator<Item = V>,
        V: Into<QueryValue>,
    {
        QueryExpr::from_expr(Expr::InList {
            expr: Box::new(self.into_query_value().into_expr()),
            list: values
                .into_iter()
                .map(Into::into)
                .map(QueryValue::into_expr)
                .collect(),
            negated: true,
        })
    }

    fn between_expr<L: Into<QueryValue>, H: Into<QueryValue>>(self, low: L, high: H) -> QueryExpr {
        QueryExpr::from_expr(Expr::Between {
            expr: Box::new(self.into_query_value().into_expr()),
            negated: false,
            low: Box::new(low.into().into_expr()),
            high: Box::new(high.into().into_expr()),
        })
    }

    fn not_between_expr<L: Into<QueryValue>, H: Into<QueryValue>>(
        self,
        low: L,
        high: H,
    ) -> QueryExpr {
        QueryExpr::from_expr(Expr::Between {
            expr: Box::new(self.into_query_value().into_expr()),
            negated: true,
            low: Box::new(low.into().into_expr()),
            high: Box::new(high.into().into_expr()),
        })
    }

    fn cast_value(self, data_type: &str) -> Result<QueryValue, DatabaseError> {
        Ok(self.cast_to_value(parse_data_type_fragment(data_type)?))
    }

    fn cast_to_value(self, data_type: DataType) -> QueryValue {
        QueryValue::from_expr(Expr::Cast {
            kind: CastKind::Cast,
            expr: Box::new(self.into_query_value().into_expr()),
            data_type,
            array: false,
            format: None,
        })
    }

    fn alias_value(self, alias: &str) -> ProjectedValue {
        ProjectedValue {
            item: SelectItem::ExprWithAlias {
                expr: self.into_query_value().into_expr(),
                alias: ident(alias),
            },
        }
    }

    fn in_subquery_expr<S: SubquerySource>(self, subquery: S) -> QueryExpr {
        QueryExpr::from_expr(Expr::InSubquery {
            expr: Box::new(self.into_query_value().into_expr()),
            subquery: Box::new(subquery.into_subquery()),
            negated: false,
        })
    }

    fn not_in_subquery_expr<S: SubquerySource>(self, subquery: S) -> QueryExpr {
        QueryExpr::from_expr(Expr::InSubquery {
            expr: Box::new(self.into_query_value().into_expr()),
            subquery: Box::new(subquery.into_subquery()),
            negated: true,
        })
    }
}

impl<M, T> Field<M, T> {
    #[doc(hidden)]
    pub const fn new(table: &'static str, column: &'static str) -> Self {
        Self {
            table,
            column,
            _marker: PhantomData,
        }
    }

    fn value(self) -> QueryValue {
        QueryValue::from_expr(Expr::CompoundIdentifier(vec![
            ident(self.table),
            ident(self.column),
        ]))
    }

    /// Builds `field = value`.
    pub fn eq<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        ValueExpressionOps::eq_expr(self, value)
    }

    /// Builds `field <> value`.
    pub fn ne<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        ValueExpressionOps::ne_expr(self, value)
    }

    /// Builds `field > value`.
    pub fn gt<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        ValueExpressionOps::gt_expr(self, value)
    }

    /// Builds `field >= value`.
    pub fn gte<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        ValueExpressionOps::gte_expr(self, value)
    }

    /// Builds `field < value`.
    pub fn lt<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        ValueExpressionOps::lt_expr(self, value)
    }

    /// Builds `field <= value`.
    pub fn lte<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        ValueExpressionOps::lte_expr(self, value)
    }

    /// Builds `field IS NULL`.
    pub fn is_null(self) -> QueryExpr {
        ValueExpressionOps::is_null_expr(self)
    }

    /// Builds `field IS NOT NULL`.
    pub fn is_not_null(self) -> QueryExpr {
        ValueExpressionOps::is_not_null_expr(self)
    }

    /// Builds `field LIKE pattern`.
    pub fn like<V: Into<QueryValue>>(self, pattern: V) -> QueryExpr {
        ValueExpressionOps::like_expr(self, pattern)
    }

    /// Builds `field NOT LIKE pattern`.
    pub fn not_like<V: Into<QueryValue>>(self, pattern: V) -> QueryExpr {
        ValueExpressionOps::not_like_expr(self, pattern)
    }

    /// Builds `field IN (...)`.
    pub fn in_list<I, V>(self, values: I) -> QueryExpr
    where
        I: IntoIterator<Item = V>,
        V: Into<QueryValue>,
    {
        ValueExpressionOps::in_list_expr(self, values)
    }

    /// Builds `field NOT IN (...)`.
    pub fn not_in_list<I, V>(self, values: I) -> QueryExpr
    where
        I: IntoIterator<Item = V>,
        V: Into<QueryValue>,
    {
        ValueExpressionOps::not_in_list_expr(self, values)
    }

    /// Builds `field BETWEEN low AND high`.
    pub fn between<L: Into<QueryValue>, H: Into<QueryValue>>(self, low: L, high: H) -> QueryExpr {
        ValueExpressionOps::between_expr(self, low, high)
    }

    /// Builds `field NOT BETWEEN low AND high`.
    pub fn not_between<L: Into<QueryValue>, H: Into<QueryValue>>(
        self,
        low: L,
        high: H,
    ) -> QueryExpr {
        ValueExpressionOps::not_between_expr(self, low, high)
    }

    /// Casts this field using a SQL type string such as `"BIGINT"`.
    pub fn cast(self, data_type: &str) -> Result<QueryValue, DatabaseError> {
        ValueExpressionOps::cast_value(self, data_type)
    }

    /// Casts this field using an explicit SQL AST data type.
    pub fn cast_to(self, data_type: DataType) -> QueryValue {
        ValueExpressionOps::cast_to_value(self, data_type)
    }

    /// Aliases this field in the select list.
    pub fn alias(self, alias: &str) -> ProjectedValue {
        ValueExpressionOps::alias_value(self, alias)
    }

    /// Builds `field IN (subquery)`.
    pub fn in_subquery<S: SubquerySource>(self, subquery: S) -> QueryExpr {
        ValueExpressionOps::in_subquery_expr(self, subquery)
    }

    /// Builds `field NOT IN (subquery)`.
    pub fn not_in_subquery<S: SubquerySource>(self, subquery: S) -> QueryExpr {
        ValueExpressionOps::not_in_subquery_expr(self, subquery)
    }
}

#[derive(Debug, Clone, PartialEq)]
/// A lightweight ORM expression wrapper for value-producing SQL AST nodes.
///
/// `QueryValue` is the common currency for computed ORM expressions such as
/// functions, aggregates, `CASE` expressions, casts, and subqueries.
///
/// It also supports the same comparison and predicate-style composition used by
/// [`Field`], including helpers such as `eq`, `gt`, `like`, `in_list`, and
/// `between`.
pub struct QueryValue {
    expr: Expr,
}

#[derive(Debug, Clone, PartialEq)]
/// A projected ORM expression, optionally carrying a select-list alias.
///
/// This is typically produced by calling `.alias(...)` on a [`Field`] or
/// [`QueryValue`], and then passed into `project_value(...)` or
/// `project_tuple(...)`.
#[doc(hidden)]
pub struct ProjectedValue {
    item: SelectItem,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompareOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug, Clone, PartialEq)]
/// A lightweight ORM expression wrapper for predicate-oriented SQL AST nodes.
///
/// `QueryExpr` is used for `WHERE` and `HAVING` clauses, as well as boolean
/// composition such as `and`, `or`, and `not`.
pub struct QueryExpr {
    expr: Expr,
}

/// Builds a scalar function call expression.
///
/// This is commonly used for UDFs registered on the database.
///
/// ```rust,ignore
/// let expr = kite_sql::orm::func("add_one", [User::id()]);
/// let row = database.from::<User>().eq(expr, 2).get()?;
/// # Ok::<(), kite_sql::errors::DatabaseError>(())
/// ```
pub fn func<N, I, V>(name: N, args: I) -> QueryValue
where
    N: Into<String>,
    I: IntoIterator<Item = V>,
    V: Into<QueryValue>,
{
    QueryValue::function(name, args)
}

/// Builds `count(expr)`.
pub fn count<V: Into<QueryValue>>(value: V) -> QueryValue {
    QueryValue::aggregate("count", [value.into()])
}

/// Builds `count(*)`.
///
/// ```rust,ignore
/// let total = database
///     .from::<User>()
///     .project_value(kite_sql::orm::count_all().alias("total_users"))
///     .get::<i32>()?;
/// # Ok::<(), kite_sql::errors::DatabaseError>(())
/// ```
pub fn count_all() -> QueryValue {
    QueryValue::aggregate_all("count")
}

/// Builds `sum(expr)`.
pub fn sum<V: Into<QueryValue>>(value: V) -> QueryValue {
    QueryValue::aggregate("sum", [value.into()])
}

/// Builds `avg(expr)`.
pub fn avg<V: Into<QueryValue>>(value: V) -> QueryValue {
    QueryValue::aggregate("avg", [value.into()])
}

/// Builds `min(expr)`.
pub fn min<V: Into<QueryValue>>(value: V) -> QueryValue {
    QueryValue::aggregate("min", [value.into()])
}

/// Builds `max(expr)`.
pub fn max<V: Into<QueryValue>>(value: V) -> QueryValue {
    QueryValue::aggregate("max", [value.into()])
}

/// Builds a searched `CASE WHEN ... THEN ... ELSE ... END` expression.
///
/// ```rust,ignore
/// let bucket = kite_sql::orm::case_when(
///     [(User::age().is_null(), "unknown"), (User::age().lt(20), "minor")],
///     "adult",
/// );
/// # let _ = bucket;
/// ```
pub fn case_when<I, C, R, E>(conditions: I, else_result: E) -> QueryValue
where
    I: IntoIterator<Item = (C, R)>,
    C: Into<QueryExpr>,
    R: Into<QueryValue>,
    E: Into<QueryValue>,
{
    QueryValue::searched_case(conditions, else_result)
}

/// Builds a simple `CASE value WHEN ... THEN ... ELSE ... END` expression.
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
    fn from_expr(expr: Expr) -> QueryExpr {
        Self { expr }
    }

    fn into_expr(self) -> Expr {
        self.expr
    }

    /// Combines two predicates with `AND`.
    pub fn and(self, rhs: QueryExpr) -> QueryExpr {
        QueryExpr::from_expr(Expr::BinaryOp {
            left: Box::new(nested_expr(self.into_expr())),
            op: SqlBinaryOperator::And,
            right: Box::new(nested_expr(rhs.into_expr())),
        })
    }

    /// Combines two predicates with `OR`.
    pub fn or(self, rhs: QueryExpr) -> QueryExpr {
        QueryExpr::from_expr(Expr::BinaryOp {
            left: Box::new(nested_expr(self.into_expr())),
            op: SqlBinaryOperator::Or,
            right: Box::new(nested_expr(rhs.into_expr())),
        })
    }

    /// Negates a predicate with `NOT`.
    pub fn not(self) -> QueryExpr {
        QueryExpr::from_expr(Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Not,
            expr: Box::new(nested_expr(self.into_expr())),
        })
    }

    /// Builds an `EXISTS (subquery)` predicate.
    ///
    /// ```rust,ignore
    /// let expr = kite_sql::orm::QueryExpr::exists(
    ///     database.from::<User>().project_value(User::id()).eq(User::id(), 1),
    /// );
    /// let found = database.from::<User>().filter(expr).exists()?;
    /// # Ok::<(), kite_sql::errors::DatabaseError>(())
    /// ```
    pub fn exists<S: SubquerySource>(subquery: S) -> QueryExpr {
        QueryExpr::from_expr(Expr::Exists {
            subquery: Box::new(subquery.into_subquery()),
            negated: false,
        })
    }

    /// Builds a `NOT EXISTS (subquery)` predicate.
    pub fn not_exists<S: SubquerySource>(subquery: S) -> QueryExpr {
        QueryExpr::from_expr(Expr::Exists {
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
            expr: self.value.into_expr(),
            options: OrderByOptions {
                asc: Some(!self.desc),
                nulls_first: None,
            },
            with_fill: None,
        }
    }
}

impl QueryValue {
    fn from_expr(expr: Expr) -> Self {
        Self { expr }
    }

    fn into_expr(self) -> Expr {
        self.expr
    }

    /// Builds a scalar function call.
    ///
    /// ```rust,ignore
    /// let expr = kite_sql::orm::QueryValue::function("add_one", [User::id()]);
    /// let row = database.from::<User>().eq(expr, 2).get()?;
    /// # Ok::<(), kite_sql::errors::DatabaseError>(())
    /// ```
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
                .map(QueryValue::into_expr)
                .map(FunctionArgExpr::Expr),
        )
    }

    /// Builds an aggregate function call such as `sum(expr)` or `count(expr)`.
    pub fn aggregate<N, I, V>(name: N, args: I) -> Self
    where
        N: Into<String>,
        I: IntoIterator<Item = V>,
        V: Into<QueryValue>,
    {
        Self::function(name, args)
    }

    /// Builds an aggregate function call that uses `*`, such as `count(*)`.
    pub fn aggregate_all(name: impl Into<String>) -> Self {
        Self::function_with_args(name, [FunctionArgExpr::Wildcard])
    }

    /// Assigns a select-list alias to this value expression.
    pub fn alias(self, alias: &str) -> ProjectedValue {
        ProjectedValue {
            item: SelectItem::ExprWithAlias {
                expr: self.into_expr(),
                alias: ident(alias),
            },
        }
    }

    /// Builds a searched `CASE WHEN ... THEN ... ELSE ... END` expression.
    pub fn searched_case<I, C, R, E>(conditions: I, else_result: E) -> Self
    where
        I: IntoIterator<Item = (C, R)>,
        C: Into<QueryExpr>,
        R: Into<QueryValue>,
        E: Into<QueryValue>,
    {
        Self::from_expr(Expr::Case {
            case_token: AttachedToken::empty(),
            end_token: AttachedToken::empty(),
            operand: None,
            conditions: conditions
                .into_iter()
                .map(|(condition, result)| CaseWhen {
                    condition: condition.into().into_expr(),
                    result: result.into().into_expr(),
                })
                .collect(),
            else_result: Some(Box::new(else_result.into().into_expr())),
        })
    }

    /// Builds a simple `CASE value WHEN ... THEN ... ELSE ... END` expression.
    pub fn simple_case<O, I, W, R, E>(operand: O, conditions: I, else_result: E) -> Self
    where
        O: Into<QueryValue>,
        I: IntoIterator<Item = (W, R)>,
        W: Into<QueryValue>,
        R: Into<QueryValue>,
        E: Into<QueryValue>,
    {
        Self::from_expr(Expr::Case {
            case_token: AttachedToken::empty(),
            end_token: AttachedToken::empty(),
            operand: Some(Box::new(operand.into().into_expr())),
            conditions: conditions
                .into_iter()
                .map(|(condition, result)| CaseWhen {
                    condition: condition.into().into_expr(),
                    result: result.into().into_expr(),
                })
                .collect(),
            else_result: Some(Box::new(else_result.into().into_expr())),
        })
    }

    /// Builds `expr = value`.
    pub fn eq<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        ValueExpressionOps::eq_expr(self, value)
    }

    /// Builds `expr <> value`.
    pub fn ne<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        ValueExpressionOps::ne_expr(self, value)
    }

    /// Builds `expr > value`.
    pub fn gt<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        ValueExpressionOps::gt_expr(self, value)
    }

    /// Builds `expr >= value`.
    pub fn gte<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        ValueExpressionOps::gte_expr(self, value)
    }

    /// Builds `expr < value`.
    pub fn lt<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        ValueExpressionOps::lt_expr(self, value)
    }

    /// Builds `expr <= value`.
    pub fn lte<V: Into<QueryValue>>(self, value: V) -> QueryExpr {
        ValueExpressionOps::lte_expr(self, value)
    }

    /// Builds `expr IS NULL`.
    pub fn is_null(self) -> QueryExpr {
        ValueExpressionOps::is_null_expr(self)
    }

    /// Builds `expr IS NOT NULL`.
    pub fn is_not_null(self) -> QueryExpr {
        ValueExpressionOps::is_not_null_expr(self)
    }

    /// Builds `expr LIKE pattern`.
    pub fn like<V: Into<QueryValue>>(self, pattern: V) -> QueryExpr {
        ValueExpressionOps::like_expr(self, pattern)
    }

    /// Builds `expr NOT LIKE pattern`.
    pub fn not_like<V: Into<QueryValue>>(self, pattern: V) -> QueryExpr {
        ValueExpressionOps::not_like_expr(self, pattern)
    }

    /// Builds `expr IN (...)`.
    pub fn in_list<I, V>(self, values: I) -> QueryExpr
    where
        I: IntoIterator<Item = V>,
        V: Into<QueryValue>,
    {
        ValueExpressionOps::in_list_expr(self, values)
    }

    /// Builds `expr NOT IN (...)`.
    pub fn not_in_list<I, V>(self, values: I) -> QueryExpr
    where
        I: IntoIterator<Item = V>,
        V: Into<QueryValue>,
    {
        ValueExpressionOps::not_in_list_expr(self, values)
    }

    /// Builds `expr BETWEEN low AND high`.
    pub fn between<L: Into<QueryValue>, H: Into<QueryValue>>(self, low: L, high: H) -> QueryExpr {
        ValueExpressionOps::between_expr(self, low, high)
    }

    /// Builds `expr NOT BETWEEN low AND high`.
    pub fn not_between<L: Into<QueryValue>, H: Into<QueryValue>>(
        self,
        low: L,
        high: H,
    ) -> QueryExpr {
        ValueExpressionOps::not_between_expr(self, low, high)
    }

    /// Casts this expression using a SQL type string such as `"BIGINT"`.
    pub fn cast(self, data_type: &str) -> Result<QueryValue, DatabaseError> {
        ValueExpressionOps::cast_value(self, data_type)
    }

    /// Casts this expression using an explicit SQL AST data type.
    ///
    /// ```rust,ignore
    /// use sqlparser::ast::DataType;
    ///
    /// let expr = User::id().cast_to(DataType::BigInt(None));
    /// let row = database.from::<User>().eq(expr, 1_i64).get()?;
    /// # Ok::<(), kite_sql::errors::DatabaseError>(())
    /// ```
    pub fn cast_to(self, data_type: DataType) -> QueryValue {
        ValueExpressionOps::cast_to_value(self, data_type)
    }

    /// Wraps a query builder as a scalar subquery expression.
    pub fn subquery<S: SubquerySource>(query: S) -> QueryValue {
        QueryValue::from_expr(Expr::Subquery(Box::new(query.into_subquery())))
    }

    /// Builds `expr IN (subquery)`.
    pub fn in_subquery<S: SubquerySource>(self, subquery: S) -> QueryExpr {
        ValueExpressionOps::in_subquery_expr(self, subquery)
    }

    /// Builds `expr NOT IN (subquery)`.
    pub fn not_in_subquery<S: SubquerySource>(self, subquery: S) -> QueryExpr {
        ValueExpressionOps::not_in_subquery_expr(self, subquery)
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
        Self::from_expr(Expr::Function(Function {
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

impl<M, T> ValueExpressionOps for Field<M, T> {
    fn into_query_value(self) -> QueryValue {
        self.value()
    }
}

impl ValueExpressionOps for QueryValue {
    fn into_query_value(self) -> QueryValue {
        self
    }
}

impl ProjectedValue {
    fn into_select_item(self) -> SelectItem {
        self.item
    }
}

#[doc(hidden)]
pub fn projection_value<M: Model>(column: &'static str, alias: &'static str) -> ProjectedValue {
    Field::<M, ()>::new(M::table_name(), column).alias(alias)
}

#[doc(hidden)]
pub fn projection_column<M: Model>(column: &'static str) -> ProjectedValue {
    ProjectedValue::from(Field::<M, ()>::new(M::table_name(), column))
}

impl<V: Into<QueryValue>> From<V> for ProjectedValue {
    fn from(value: V) -> Self {
        Self {
            item: SelectItem::UnnamedExpr(value.into().into_expr()),
        }
    }
}

macro_rules! impl_into_projected_tuple {
    ($(($($name:ident),+)),+ $(,)?) => {
        $(
            impl<$($name),+> IntoProjectedTuple for ($($name,)+)
            where
                $($name: Into<ProjectedValue>,)+
            {
                #[allow(non_snake_case)]
                fn into_projected_values(self) -> Vec<ProjectedValue> {
                    let ($($name,)+) = self;
                    vec![$($name.into(),)+]
                }
            }
        )+
    };
}

impl_into_projected_tuple!(
    (A, B),
    (A, B, C),
    (A, B, C, D),
    (A, B, C, D, E),
    (A, B, C, D, E, F),
    (A, B, C, D, E, F, G),
    (A, B, C, D, E, F, G, H),
);

impl<T: ToDataValue> From<T> for QueryValue {
    fn from(value: T) -> Self {
        QueryValue::from_expr(data_value_to_ast_expr(&value.to_data_value()))
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

#[doc(hidden)]
pub trait StatementSource {
    type Iter: ResultIter;

    /// Executes a prepared ORM statement with named parameters.
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

struct QueryBuilder<Q: StatementSource, M: Model, P = ModelProjection> {
    state: BuilderState<Q, M>,
    projection: P,
}

/// Lightweight single-table query builder for ORM models.
///
/// This is the main entry point returned by `Database::from::<M>()` and
/// `DBTransaction::from::<M>()`.
pub struct FromBuilder<Q: StatementSource, M: Model, P = ModelProjection> {
    inner: QueryBuilder<Q, M, P>,
}

#[doc(hidden)]
pub struct ModelProjection;

#[doc(hidden)]
pub struct ValueProjection {
    value: ProjectedValue,
}

#[doc(hidden)]
pub struct TupleProjection {
    values: Vec<ProjectedValue>,
}

#[doc(hidden)]
pub struct StructProjection<T> {
    _marker: PhantomData<T>,
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

/// Declares a struct-backed ORM projection used by [`FromBuilder::project`].
///
/// This trait is typically derived with `#[derive(Projection)]`.
pub trait Projection: for<'a> From<(&'a SchemaRef, Tuple)> {
    /// Returns the projected select-list items for model `M`.
    fn projected_values<M: Model>() -> Vec<ProjectedValue>;
}

#[doc(hidden)]
pub trait IntoProjectedTuple {
    fn into_projected_values(self) -> Vec<ProjectedValue>;
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

impl<M: Model> ProjectionSpec<M> for TupleProjection {
    fn into_select_items(self) -> Vec<SelectItem> {
        self.values
            .into_iter()
            .map(ProjectedValue::into_select_item)
            .collect()
    }
}

impl<M: Model, T: Projection> ProjectionSpec<M> for StructProjection<T> {
    fn into_select_items(self) -> Vec<SelectItem> {
        T::projected_values::<M>()
            .into_iter()
            .map(ProjectedValue::into_select_item)
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterMode {
    Replace,
    And,
    Or,
}

impl<Q: StatementSource, M: Model> QueryBuilder<Q, M, ModelProjection> {
    fn new(source: Q) -> Self {
        Self {
            state: BuilderState::new(source),
            projection: ModelProjection,
        }
    }
}

impl<Q: StatementSource, M: Model, P> QueryBuilder<Q, M, P> {
    fn with_projection<P2>(self, projection: P2) -> QueryBuilder<Q, M, P2> {
        QueryBuilder {
            state: self.state,
            projection,
        }
    }
}

impl<Q: StatementSource, M: Model, P> FromBuilder<Q, M, P> {
    fn from_inner(inner: QueryBuilder<Q, M, P>) -> Self {
        Self { inner }
    }

    fn with_projection<P2>(self, projection: P2) -> FromBuilder<Q, M, P2> {
        FromBuilder::from_inner(self.inner.with_projection(projection))
    }
}

impl<Q: StatementSource, M: Model> FromBuilder<Q, M, ModelProjection> {
    /// Switches the query into a struct projection.
    ///
    /// ```rust,ignore
    /// #[derive(Default, kite_sql::Projection)]
    /// struct UserSummary {
    ///     id: i32,
    ///     #[projection(rename = "user_name")]
    ///     display_name: String,
    /// }
    ///
    /// let users = database.from::<User>().project::<UserSummary>().fetch()?;
    /// # Ok::<(), kite_sql::errors::DatabaseError>(())
    /// ```
    pub fn project<T: Projection>(self) -> FromBuilder<Q, M, StructProjection<T>> {
        self.with_projection(StructProjection {
            _marker: PhantomData,
        })
    }

    /// Switches the query into a single-value projection.
    ///
    /// ```rust,ignore
    /// let ids = database
    ///     .from::<User>()
    ///     .project_value(User::id())
    ///     .fetch::<i32>()?;
    /// # Ok::<(), kite_sql::errors::DatabaseError>(())
    /// ```
    pub fn project_value<V: Into<ProjectedValue>>(
        self,
        value: V,
    ) -> FromBuilder<Q, M, ValueProjection> {
        self.with_projection(ValueProjection {
            value: value.into(),
        })
    }

    /// Switches the query into a tuple projection.
    ///
    /// ```rust,ignore
    /// let rows = database
    ///     .from::<User>()
    ///     .project_tuple((User::id(), User::name()))
    ///     .fetch::<(i32, String)>()?;
    /// # Ok::<(), kite_sql::errors::DatabaseError>(())
    /// ```
    pub fn project_tuple<V: IntoProjectedTuple>(
        self,
        values: V,
    ) -> FromBuilder<Q, M, TupleProjection> {
        self.with_projection(TupleProjection {
            values: values.into_projected_values(),
        })
    }

    /// Executes the query and decodes rows into the model type.
    pub fn fetch(self) -> Result<OrmIter<Q::Iter, M>, DatabaseError> {
        Ok(self.raw()?.orm::<M>())
    }

    /// Executes the query with `LIMIT 1` semantics and decodes one model row.
    pub fn get(self) -> Result<Option<M>, DatabaseError> {
        extract_optional_model(self.limit(1).raw()?)
    }
}

impl<Q: StatementSource, M: Model, P: ProjectionSpec<M>> FromBuilder<Q, M, P> {
    /// Replaces the current `WHERE` predicate.
    pub fn filter(self, expr: QueryExpr) -> Self {
        Self::from_inner(self.inner.filter(expr))
    }

    /// Appends `left AND right` to the current filter state.
    pub fn and(self, left: QueryExpr, right: QueryExpr) -> Self {
        Self::from_inner(self.inner.and(left, right))
    }

    /// Appends `left OR right` to the current filter state.
    pub fn or(self, left: QueryExpr, right: QueryExpr) -> Self {
        Self::from_inner(self.inner.or(left, right))
    }

    /// Replaces the current filter with `NOT expr`.
    pub fn not(self, expr: QueryExpr) -> Self {
        Self::from_inner(self.inner.not(expr))
    }

    /// Replaces the current filter with `EXISTS (subquery)`.
    pub fn where_exists<S: SubquerySource>(self, subquery: S) -> Self {
        Self::from_inner(self.inner.where_exists(subquery))
    }

    /// Replaces the current filter with `NOT EXISTS (subquery)`.
    pub fn where_not_exists<S: SubquerySource>(self, subquery: S) -> Self {
        Self::from_inner(self.inner.where_not_exists(subquery))
    }

    /// Appends a `GROUP BY` expression.
    pub fn group_by<V: Into<QueryValue>>(self, value: V) -> Self {
        Self::from_inner(self.inner.group_by(value))
    }

    /// Sets the `HAVING` predicate.
    pub fn having(self, expr: QueryExpr) -> Self {
        Self::from_inner(self.inner.having(expr))
    }

    /// Appends an ascending sort key.
    pub fn asc<V: Into<QueryValue>>(self, value: V) -> Self {
        Self::from_inner(self.inner.asc(value))
    }

    /// Appends a descending sort key.
    pub fn desc<V: Into<QueryValue>>(self, value: V) -> Self {
        Self::from_inner(self.inner.desc(value))
    }

    /// Sets the query `LIMIT`.
    pub fn limit(self, limit: usize) -> Self {
        Self::from_inner(self.inner.limit(limit))
    }

    /// Sets the query `OFFSET`.
    pub fn offset(self, offset: usize) -> Self {
        Self::from_inner(self.inner.offset(offset))
    }

    /// Appends `left = right` to the filter state.
    pub fn eq<L: Into<QueryValue>, R: Into<QueryValue>>(self, left: L, right: R) -> Self {
        Self::from_inner(self.inner.eq(left, right))
    }

    /// Appends `left <> right` to the filter state.
    pub fn ne<L: Into<QueryValue>, R: Into<QueryValue>>(self, left: L, right: R) -> Self {
        Self::from_inner(self.inner.ne(left, right))
    }

    /// Appends `left > right` to the filter state.
    pub fn gt<L: Into<QueryValue>, R: Into<QueryValue>>(self, left: L, right: R) -> Self {
        Self::from_inner(self.inner.gt(left, right))
    }

    /// Appends `left >= right` to the filter state.
    pub fn gte<L: Into<QueryValue>, R: Into<QueryValue>>(self, left: L, right: R) -> Self {
        Self::from_inner(self.inner.gte(left, right))
    }

    /// Appends `left < right` to the filter state.
    pub fn lt<L: Into<QueryValue>, R: Into<QueryValue>>(self, left: L, right: R) -> Self {
        Self::from_inner(self.inner.lt(left, right))
    }

    /// Appends `left <= right` to the filter state.
    pub fn lte<L: Into<QueryValue>, R: Into<QueryValue>>(self, left: L, right: R) -> Self {
        Self::from_inner(self.inner.lte(left, right))
    }

    /// Appends `value IS NULL` to the filter state.
    pub fn is_null<V: Into<QueryValue>>(self, value: V) -> Self {
        Self::from_inner(self.inner.is_null(value))
    }

    /// Appends `value IS NOT NULL` to the filter state.
    pub fn is_not_null<V: Into<QueryValue>>(self, value: V) -> Self {
        Self::from_inner(self.inner.is_not_null(value))
    }

    /// Appends `value LIKE pattern` to the filter state.
    pub fn like<L: Into<QueryValue>, R: Into<QueryValue>>(self, value: L, pattern: R) -> Self {
        Self::from_inner(self.inner.like(value, pattern))
    }

    /// Appends `value NOT LIKE pattern` to the filter state.
    pub fn not_like<L: Into<QueryValue>, R: Into<QueryValue>>(self, value: L, pattern: R) -> Self {
        Self::from_inner(self.inner.not_like(value, pattern))
    }

    /// Appends `left IN (...)` to the filter state.
    pub fn in_list<L, I, V>(self, left: L, values: I) -> Self
    where
        L: Into<QueryValue>,
        I: IntoIterator<Item = V>,
        V: Into<QueryValue>,
    {
        Self::from_inner(self.inner.in_list(left, values))
    }

    /// Appends `left NOT IN (...)` to the filter state.
    pub fn not_in_list<L, I, V>(self, left: L, values: I) -> Self
    where
        L: Into<QueryValue>,
        I: IntoIterator<Item = V>,
        V: Into<QueryValue>,
    {
        Self::from_inner(self.inner.not_in_list(left, values))
    }

    /// Appends `expr BETWEEN low AND high` to the filter state.
    pub fn between<L, Low, High>(self, expr: L, low: Low, high: High) -> Self
    where
        L: Into<QueryValue>,
        Low: Into<QueryValue>,
        High: Into<QueryValue>,
    {
        Self::from_inner(self.inner.between(expr, low, high))
    }

    /// Appends `expr NOT BETWEEN low AND high` to the filter state.
    pub fn not_between<L, Low, High>(self, expr: L, low: Low, high: High) -> Self
    where
        L: Into<QueryValue>,
        Low: Into<QueryValue>,
        High: Into<QueryValue>,
    {
        Self::from_inner(self.inner.not_between(expr, low, high))
    }

    /// Appends `left IN (subquery)` to the filter state.
    pub fn in_subquery<L: Into<QueryValue>, S: SubquerySource>(self, left: L, subquery: S) -> Self {
        Self::from_inner(self.inner.in_subquery(left, subquery))
    }

    /// Appends `left NOT IN (subquery)` to the filter state.
    pub fn not_in_subquery<L: Into<QueryValue>, S: SubquerySource>(
        self,
        left: L,
        subquery: S,
    ) -> Self {
        Self::from_inner(self.inner.not_in_subquery(left, subquery))
    }

    /// Executes the query and returns the raw result iterator.
    pub fn raw(self) -> Result<Q::Iter, DatabaseError> {
        self.inner.raw()
    }

    /// Returns whether the query produces at least one row.
    pub fn exists(self) -> Result<bool, DatabaseError> {
        self.inner.exists()
    }

    /// Returns the row count for the current query shape.
    pub fn count(self) -> Result<usize, DatabaseError> {
        self.inner.count()
    }
}

impl<Q: StatementSource, M: Model> FromBuilder<Q, M, ValueProjection> {
    /// Executes a single-value projection and decodes each row into `T`.
    pub fn fetch<T: FromDataValue>(self) -> Result<ProjectValueIter<Q::Iter, T>, DatabaseError> {
        Ok(ProjectValueIter::new(self.raw()?))
    }

    /// Executes a single-value projection and decodes one value.
    pub fn get<T: FromDataValue>(self) -> Result<Option<T>, DatabaseError> {
        extract_optional_value(self.limit(1).raw()?)
    }
}

impl<Q: StatementSource, M: Model> FromBuilder<Q, M, TupleProjection> {
    /// Executes a tuple projection and decodes each row into `T`.
    pub fn fetch<T: FromQueryTuple>(self) -> Result<ProjectTupleIter<Q::Iter, T>, DatabaseError> {
        Ok(ProjectTupleIter::new(self.raw()?))
    }

    /// Executes a tuple projection and decodes one row into `T`.
    pub fn get<T: FromQueryTuple>(self) -> Result<Option<T>, DatabaseError> {
        extract_optional_tuple(self.limit(1).raw()?)
    }
}

impl<Q: StatementSource, M: Model, T: Projection> FromBuilder<Q, M, StructProjection<T>> {
    /// Executes a struct projection and decodes each row into `T`.
    pub fn fetch(self) -> Result<OrmIter<Q::Iter, T>, DatabaseError> {
        Ok(self.raw()?.orm::<T>())
    }

    /// Executes a struct projection and decodes one row into `T`.
    pub fn get(self) -> Result<Option<T>, DatabaseError> {
        extract_optional_row(self.limit(1).raw()?)
    }
}

impl<Q: StatementSource, M: Model, P: ProjectionSpec<M>> QueryBuilder<Q, M, P> {
    fn push_filter(self, expr: QueryExpr, mode: FilterMode) -> Self {
        Self {
            state: self.state.push_filter(expr, mode),
            projection: self.projection,
        }
    }

    fn filter(mut self, expr: QueryExpr) -> Self {
        self.state.filter = Some(expr);
        self
    }

    fn and(self, left: QueryExpr, right: QueryExpr) -> Self {
        Self {
            state: self.state.push_filter(left.and(right), FilterMode::And),
            projection: self.projection,
        }
    }

    fn or(self, left: QueryExpr, right: QueryExpr) -> Self {
        Self {
            state: self.state.push_filter(left.or(right), FilterMode::Or),
            projection: self.projection,
        }
    }

    fn not(self, expr: QueryExpr) -> Self {
        Self {
            state: self.state.push_filter(expr.not(), FilterMode::Replace),
            projection: self.projection,
        }
    }

    fn where_exists<S: SubquerySource>(self, subquery: S) -> Self {
        Self {
            state: self
                .state
                .push_filter(QueryExpr::exists(subquery), FilterMode::Replace),
            projection: self.projection,
        }
    }

    fn where_not_exists<S: SubquerySource>(self, subquery: S) -> Self {
        Self {
            state: self
                .state
                .push_filter(QueryExpr::not_exists(subquery), FilterMode::Replace),
            projection: self.projection,
        }
    }

    fn group_by<V: Into<QueryValue>>(self, value: V) -> Self {
        Self {
            state: self.state.push_group_by(value.into()),
            projection: self.projection,
        }
    }

    fn having(mut self, expr: QueryExpr) -> Self {
        self.state.having = Some(expr);
        self
    }

    fn asc<V: Into<QueryValue>>(self, value: V) -> Self {
        Self {
            state: self.state.push_order(value.into().asc()),
            projection: self.projection,
        }
    }

    fn desc<V: Into<QueryValue>>(self, value: V) -> Self {
        Self {
            state: self.state.push_order(value.into().desc()),
            projection: self.projection,
        }
    }

    fn limit(mut self, limit: usize) -> Self {
        self.state.limit = Some(limit);
        self
    }

    fn offset(mut self, offset: usize) -> Self {
        self.state.offset = Some(offset);
        self
    }

    fn build_query(self) -> Query {
        let QueryBuilder {
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
        let QueryBuilder {
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

    fn eq<L: Into<QueryValue>, R: Into<QueryValue>>(self, left: L, right: R) -> Self {
        self.push_filter(left.into().eq(right), FilterMode::Replace)
    }

    fn ne<L: Into<QueryValue>, R: Into<QueryValue>>(self, left: L, right: R) -> Self {
        self.push_filter(left.into().ne(right), FilterMode::Replace)
    }

    fn gt<L: Into<QueryValue>, R: Into<QueryValue>>(self, left: L, right: R) -> Self {
        self.push_filter(left.into().gt(right), FilterMode::Replace)
    }

    fn gte<L: Into<QueryValue>, R: Into<QueryValue>>(self, left: L, right: R) -> Self {
        self.push_filter(left.into().gte(right), FilterMode::Replace)
    }

    fn lt<L: Into<QueryValue>, R: Into<QueryValue>>(self, left: L, right: R) -> Self {
        self.push_filter(left.into().lt(right), FilterMode::Replace)
    }

    fn lte<L: Into<QueryValue>, R: Into<QueryValue>>(self, left: L, right: R) -> Self {
        self.push_filter(left.into().lte(right), FilterMode::Replace)
    }

    fn is_null<V: Into<QueryValue>>(self, value: V) -> Self {
        self.push_filter(value.into().is_null(), FilterMode::Replace)
    }

    fn is_not_null<V: Into<QueryValue>>(self, value: V) -> Self {
        self.push_filter(value.into().is_not_null(), FilterMode::Replace)
    }

    fn like<L: Into<QueryValue>, R: Into<QueryValue>>(self, value: L, pattern: R) -> Self {
        self.push_filter(value.into().like(pattern), FilterMode::Replace)
    }

    fn not_like<L: Into<QueryValue>, R: Into<QueryValue>>(self, value: L, pattern: R) -> Self {
        self.push_filter(value.into().not_like(pattern), FilterMode::Replace)
    }

    fn in_list<L, I, V>(self, left: L, values: I) -> Self
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

    fn not_in_list<L, I, V>(self, left: L, values: I) -> Self
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

    fn between<L, Low, High>(self, expr: L, low: Low, high: High) -> Self
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

    fn not_between<L, Low, High>(self, expr: L, low: Low, high: High) -> Self
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

    fn in_subquery<L: Into<QueryValue>, S: SubquerySource>(self, left: L, subquery: S) -> Self {
        Self {
            state: self
                .state
                .push_filter(left.into().in_subquery(subquery), FilterMode::Replace),
            projection: self.projection,
        }
    }

    fn not_in_subquery<L: Into<QueryValue>, S: SubquerySource>(self, left: L, subquery: S) -> Self {
        Self {
            state: self
                .state
                .push_filter(left.into().not_in_subquery(subquery), FilterMode::Replace),
            projection: self.projection,
        }
    }

    fn raw(self) -> Result<Q::Iter, DatabaseError> {
        let (source, statement) = self.into_statement();
        source.execute_statement(&statement, &[])
    }

    fn exists(self) -> Result<bool, DatabaseError> {
        let mut iter = self.limit(1).raw()?;
        Ok(iter.next().transpose()?.is_some())
    }

    fn count(self) -> Result<usize, DatabaseError> {
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

impl<Q: StatementSource, M: Model, P> private::Sealed for FromBuilder<Q, M, P> {}

impl<Q: StatementSource, M: Model, P: ProjectionSpec<M>> SubquerySource for FromBuilder<Q, M, P> {
    fn into_subquery(self) -> Query {
        self.inner.build_query()
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
            selection: filter.map(QueryExpr::into_expr),
            connect_by: vec![],
            group_by: GroupByExpr::Expressions(
                group_bys.into_iter().map(QueryValue::into_expr).collect(),
                vec![],
            ),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: having.map(QueryExpr::into_expr),
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

    /// Returns metadata for the primary-key field.
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
    /// Returns the logical SQL type used for conversion, when one is required.
    fn logical_type() -> Option<LogicalType>;

    /// Attempts to convert a raw [`DataValue`] into `Self`.
    fn from_data_value(value: DataValue) -> Option<Self>;
}

/// Conversion trait from a projected result tuple into a Rust value.
///
/// This is implemented for tuples such as `(i32, String)` by the ORM itself.
pub trait FromQueryTuple: Sized {
    /// Decodes one projected tuple into `Self`.
    fn from_query_tuple(tuple: Tuple) -> Result<Self, DatabaseError>;
}

/// Typed adapter over a [`ResultIter`] that yields projected values instead of raw tuples.
///
/// This is returned by `project_value(...).fetch::<T>()`.
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

/// Typed adapter over a [`ResultIter`] that yields projected tuples.
///
/// This is returned by `project_tuple(...).fetch::<T>()`.
pub struct ProjectTupleIter<I, T> {
    inner: I,
    _marker: PhantomData<T>,
}

impl<I, T> ProjectTupleIter<I, T>
where
    I: ResultIter,
    T: FromQueryTuple,
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
    /// Each item is one projected scalar value decoded into `T`.
    type Item = Result<T, DatabaseError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|result| result.and_then(extract_value_from_tuple::<T>))
    }
}

impl<I, T> Iterator for ProjectTupleIter<I, T>
where
    I: ResultIter,
    T: FromQueryTuple,
{
    /// Each item is one projected row decoded into `T`.
    type Item = Result<T, DatabaseError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|result| result.and_then(extract_projected_tuple::<T>))
    }
}

/// Conversion trait from Rust values into [`DataValue`] for ORM parameters.
///
/// This trait is mainly intended for framework internals and derive-generated
/// code.
pub trait ToDataValue {
    /// Converts the value into a [`DataValue`].
    fn to_data_value(&self) -> DataValue;
}

/// Maps a Rust field type to the SQL column type used by ORM DDL helpers.
///
/// `#[derive(Model)]` relies on this trait to build `CREATE TABLE` statements.
/// Most built-in scalar types already implement it, and custom types can opt in
/// by implementing this trait together with [`FromDataValue`] and [`ToDataValue`].
pub trait ModelColumnType {
    /// Returns the SQL type name used in ORM-generated DDL.
    fn ddl_type() -> String;

    /// Whether this field type maps to a nullable SQL column.
    fn nullable() -> bool {
        false
    }
}

/// Marker trait for string-like model fields that support `#[model(varchar = N)]`
/// and `#[model(char = N)]`.
///
/// This is mainly used by the `Model` derive macro.
pub trait StringType {}

/// Marker trait for decimal-like model fields that support precision/scale DDL attributes.
///
/// This is mainly used by the `Model` derive macro.
pub trait DecimalType {}

#[doc(hidden)]
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

macro_rules! impl_from_query_tuple {
    ($(($($name:ident),+)),+ $(,)?) => {
        $(
            impl<$($name),+> FromQueryTuple for ($($name,)+)
            where
                $($name: FromDataValue,)+
            {
                #[allow(non_snake_case)]
                fn from_query_tuple(tuple: Tuple) -> Result<Self, DatabaseError> {
                    let expected_len = [$(stringify!($name)),+].len();
                    let mut values = tuple.values.into_iter();

                    $(
                        let $name = extract_projected_data_value::<$name>(
                            values.next(),
                            expected_len,
                        )?;
                    )+

                    if values.next().is_some() {
                        return Err(DatabaseError::MisMatch(
                            "the expected tuple projection width",
                            "the query result",
                        ));
                    }

                    Ok(($($name,)+))
                }
            }
        )+
    };
}

impl_from_query_tuple!(
    (A, B),
    (A, B, C),
    (A, B, C, D),
    (A, B, C, D, E),
    (A, B, C, D, E, F),
    (A, B, C, D, E, F, G),
    (A, B, C, D, E, F, G, H),
);

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

fn extract_optional_model<I, M>(iter: I) -> Result<Option<M>, DatabaseError>
where
    I: ResultIter,
    M: Model,
{
    extract_optional_row(iter)
}

fn extract_optional_row<I, T>(mut iter: I) -> Result<Option<T>, DatabaseError>
where
    I: ResultIter,
    T: for<'a> From<(&'a SchemaRef, Tuple)>,
{
    let schema = iter.schema().clone();

    Ok(match iter.next() {
        Some(tuple) => Some(T::from((&schema, tuple?))),
        None => None,
    })
}

fn convert_projected_value<T: FromDataValue>(value: DataValue) -> Result<T, DatabaseError> {
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

fn extract_projected_data_value<T: FromDataValue>(
    value: Option<DataValue>,
    _expected_len: usize,
) -> Result<T, DatabaseError> {
    let value = value.ok_or(DatabaseError::MisMatch(
        "the expected tuple projection width",
        "the query result",
    ))?;
    convert_projected_value::<T>(value)
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

    convert_projected_value::<T>(value)
}

fn extract_projected_tuple<T: FromQueryTuple>(tuple: Tuple) -> Result<T, DatabaseError> {
    T::from_query_tuple(tuple)
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

fn extract_optional_tuple<I, T>(mut iter: I) -> Result<Option<T>, DatabaseError>
where
    I: ResultIter,
    T: FromQueryTuple,
{
    Ok(match iter.next() {
        Some(tuple) => Some(extract_projected_tuple(tuple?)?),
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
