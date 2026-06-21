#![doc = include_str!("README.md")]

use crate::binder::{
    with_query_bind_step, BindPlanFrom, BindPlanSelectList, Binder, JoinConstraintInput,
    QueryBindStep, SetOperatorKind, TableAliasInput,
};
use crate::catalog::{ColumnCatalog, ColumnRef, TableCatalog, TableName};
use crate::db::{
    BindSource, BorrowResultIter, DBTransaction, Database, DatabaseIter, OrmIter, ResultIter,
    TransactionIter,
};
use crate::errors::DatabaseError;
use crate::expression::{self, AliasType, ScalarExpression};
use crate::planner::operator::alter_table::change_column::{DefaultChange, NotNullChange};
use crate::planner::operator::join::JoinType;
use crate::planner::operator::mark_apply::MarkApplyQuantifier;
use crate::planner::operator::sort::SortField;
use crate::planner::{LogicalPlan, PlanArena};
use crate::storage::{Storage, Transaction};
use crate::types::tuple::{SchemaView, Tuple};
use crate::types::value::DataValue;
use crate::types::CharLengthUnits;
use crate::types::LogicalType;
#[cfg(feature = "decimal")]
use rust_decimal::Decimal;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::rc::Rc;
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
    pub column_index: usize,
    pub placeholder: &'static str,
    pub primary_key: bool,
    pub unique: bool,
}

/// One row returned by [`Database::describe`] or [`DBTransaction::describe`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeColumn {
    pub field: String,
    pub data_type: String,
    pub len: String,
    pub nullable: bool,
    pub key: String,
    pub default: String,
}

impl From<(&SchemaView<'_, '_>, Tuple)> for DescribeColumn {
    fn from((_, tuple): (&SchemaView<'_, '_>, Tuple)) -> Self {
        let mut values = tuple.values.into_iter();

        let field = describe_text_value(values.next());
        let data_type = describe_text_value(values.next());
        let len = describe_text_value(values.next());
        let nullable = matches!(
            values.next(),
            Some(DataValue::Utf8 { value, .. }) if value == "true"
        );
        let key = describe_text_value(values.next());
        let default = describe_text_value(values.next());

        Self {
            field,
            data_type,
            len,
            nullable,
            key,
            default,
        }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct FieldSort<M, T> {
    field: Field<M, T>,
    asc: bool,
    nulls_first: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QuerySource {
    table_name: String,
    alias: Option<String>,
}

impl QuerySource {
    fn model<M: Model>() -> Self {
        Self {
            table_name: M::table_name().to_string(),
            alias: None,
        }
    }

    fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
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

    pub fn table_name(&self) -> &'static str {
        self.table
    }

    pub fn column_name(&self) -> &'static str {
        self.column
    }

    pub fn asc(self) -> FieldSort<M, T> {
        FieldSort::new(self).asc()
    }

    pub fn desc(self) -> FieldSort<M, T> {
        FieldSort::new(self).desc()
    }

    pub fn nulls_first(self) -> FieldSort<M, T> {
        FieldSort::new(self).nulls_first()
    }

    pub fn nulls_last(self) -> FieldSort<M, T> {
        FieldSort::new(self).nulls_last()
    }
}

impl<M, T> FieldSort<M, T> {
    fn new(field: Field<M, T>) -> Self {
        Self {
            field,
            asc: true,
            nulls_first: false,
        }
    }

    pub fn asc(mut self) -> Self {
        self.asc = true;
        self
    }

    pub fn desc(mut self) -> Self {
        self.asc = false;
        self
    }

    pub fn nulls_first(mut self) -> Self {
        self.nulls_first = true;
        self
    }

    pub fn nulls_last(mut self) -> Self {
        self.nulls_first = false;
        self
    }
}

#[doc(hidden)]
pub trait BindOrmScalar<'bind, 'parent, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    fn bind_scalar(
        self,
        scope: &mut ExprBindScope<'_, 'bind, 'parent, 'arena, T, A>,
    ) -> Result<ScalarExpression, DatabaseError>;
}

impl<'bind, 'parent, 'arena, T, A, M, V> BindOrmScalar<'bind, 'parent, 'arena, T, A> for Field<M, V>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    fn bind_scalar(
        self,
        scope: &mut ExprBindScope<'_, 'bind, 'parent, 'arena, T, A>,
    ) -> Result<ScalarExpression, DatabaseError> {
        scope.column(self).map(CtxExpression::into_scalar)
    }
}

impl<'bind, 'parent, 'arena, T, A> BindOrmScalar<'bind, 'parent, 'arena, T, A> for ScalarExpression
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    fn bind_scalar(
        self,
        _scope: &mut ExprBindScope<'_, 'bind, 'parent, 'arena, T, A>,
    ) -> Result<ScalarExpression, DatabaseError> {
        Ok(self)
    }
}

impl<'bind, 'parent, 'arena, T, A> BindOrmScalar<'bind, 'parent, 'arena, T, A>
    for CtxExpression<'bind, 'parent, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    fn bind_scalar(
        self,
        _scope: &mut ExprBindScope<'_, 'bind, 'parent, 'arena, T, A>,
    ) -> Result<ScalarExpression, DatabaseError> {
        Ok(self.into_scalar())
    }
}

#[doc(hidden)]
pub trait BindOrmSort<'bind, 'parent, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    fn bind_sort<'scope>(
        self,
        scope: &'scope mut ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A>,
    ) -> Result<SortField, DatabaseError>;
}

impl<'bind, 'parent, 'arena, T, A, M, V> BindOrmSort<'bind, 'parent, 'arena, T, A> for Field<M, V>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    fn bind_sort<'scope>(
        self,
        scope: &'scope mut ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A>,
    ) -> Result<SortField, DatabaseError> {
        self.bind_scalar(scope).map(SortField::from)
    }
}

impl<'bind, 'parent, 'arena, T, A, M, V> BindOrmSort<'bind, 'parent, 'arena, T, A>
    for FieldSort<M, V>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    fn bind_sort<'scope>(
        self,
        scope: &'scope mut ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A>,
    ) -> Result<SortField, DatabaseError> {
        let mut sort = self.field.bind_scalar(scope).map(SortField::from)?;
        sort.asc = self.asc;
        sort.nulls_first = self.nulls_first;
        Ok(sort)
    }
}

impl<'bind, 'parent, 'arena, T, A> BindOrmSort<'bind, 'parent, 'arena, T, A> for ScalarExpression
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    fn bind_sort<'scope>(
        self,
        _scope: &'scope mut ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A>,
    ) -> Result<SortField, DatabaseError> {
        Ok(self.into())
    }
}

impl<'bind, 'parent, 'arena, T, A> BindOrmSort<'bind, 'parent, 'arena, T, A> for SortField
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    fn bind_sort<'scope>(
        self,
        _scope: &'scope mut ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A>,
    ) -> Result<SortField, DatabaseError> {
        Ok(self)
    }
}

impl<'bind, 'parent, 'arena, T, A> BindOrmSort<'bind, 'parent, 'arena, T, A>
    for CtxExpression<'bind, 'parent, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    fn bind_sort<'scope>(
        self,
        _scope: &'scope mut ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A>,
    ) -> Result<SortField, DatabaseError> {
        Ok(self.into_scalar().into())
    }
}

#[doc(hidden)]
pub trait IntoOrmScalarExpression {
    fn into_orm_scalar(self) -> ScalarExpression;
}

impl<E> IntoOrmScalarExpression for E
where
    E: Into<ScalarExpression>,
{
    fn into_orm_scalar(self) -> ScalarExpression {
        self.into()
    }
}

#[doc(hidden)]
pub trait BindOrmScalarList<'bind, 'parent, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    fn bind_scalar_list(
        self,
        scope: &mut ExprBindScope<'_, 'bind, 'parent, 'arena, T, A>,
    ) -> Result<Vec<ScalarExpression>, DatabaseError>;
}

macro_rules! impl_bind_orm_scalar_list {
    ($(($($name:ident),+)),+ $(,)?) => {
        $(
            impl<'bind, 'parent, 'arena, Tx, Args, $($name),+> BindOrmScalarList<'bind, 'parent, 'arena, Tx, Args>
                for ($($name,)+)
            where
                Tx: Transaction,
                Args: AsRef<[(&'static str, DataValue)]>,
                $($name: BindOrmScalar<'bind, 'parent, 'arena, Tx, Args>,)+
            {
                #[allow(non_snake_case)]
                fn bind_scalar_list(
                    self,
                    scope: &mut ExprBindScope<'_, 'bind, 'parent, 'arena, Tx, Args>,
                ) -> Result<Vec<ScalarExpression>, DatabaseError> {
                    let ($($name,)+) = self;
                    Ok(vec![
                        $($name.bind_scalar(scope)?,)+
                    ])
                }
            }
        )+
    };
}

impl_bind_orm_scalar_list!(
    (A, B),
    (A, B, C),
    (A, B, C, D),
    (A, B, C, D, E),
    (A, B, C, D, E, F),
    (A, B, C, D, E, F, G),
    (A, B, C, D, E, F, G, H),
);

macro_rules! impl_quantified_subquery_methods {
    ($($method:ident, $quantifier:ident, $negated:expr, $op:ident;)+) => {
        $(
            pub fn $method<F>(self, build: F) -> Result<Self, DatabaseError>
            where
                F: for<'scope, 'sub_bind, 'sub_parent> FnOnce(
                    &'scope mut OrmContext<'scope, 'sub_bind, 'sub_parent, 'arena, T, A>,
                ) -> Result<LogicalPlan, DatabaseError>,
            {
                self.quantified_subquery(
                    MarkApplyQuantifier::$quantifier,
                    $negated,
                    expression::BinaryOperator::$op,
                    build,
                )
            }
        )+
    };
}

#[allow(clippy::type_complexity)]
struct ExprBindScopeHandle<'bind, 'parent, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    binder: NonNull<Binder<'bind, 'parent, T, A>>,
    arena: NonNull<PlanArena<'arena>>,
    _marker: PhantomData<(&'bind (), &'parent (), &'arena (), T, A, Rc<()>)>,
}

impl<'bind, 'parent, 'arena, T, A> Clone for ExprBindScopeHandle<'bind, 'parent, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<'bind, 'parent, 'arena, T, A> Copy for ExprBindScopeHandle<'bind, 'parent, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
}

impl<'bind, 'parent, 'arena, T, A> ExprBindScopeHandle<'bind, 'parent, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    fn new<'ctx>(scope: &ExprBindScope<'ctx, 'bind, 'parent, 'arena, T, A>) -> Self {
        Self {
            binder: NonNull::new((&*scope.binder) as *const _ as *mut _).unwrap(),
            arena: NonNull::new((&*scope.arena) as *const _ as *mut _).unwrap(),
            _marker: PhantomData,
        }
    }

    fn wrap(self, expr: ScalarExpression) -> CtxExpression<'bind, 'parent, 'arena, T, A> {
        CtxExpression { expr, scope: self }
    }

    #[allow(clippy::mut_from_ref)]
    fn binder(&self) -> &mut Binder<'bind, 'parent, T, A> {
        // SAFETY: ExprBindScopeHandle is created only from an active ExprBindScope
        // during synchronous ORM binding. CtxExpression is !Send and !Sync, and
        // all public ORM entry points immediately normalize expressions before
        // leaving the bind/filter/project closure, so this pointer is never used
        // after its owning binder scope has ended.
        unsafe { &mut *self.binder.as_ptr() }
    }

    #[allow(clippy::mut_from_ref)]
    fn arena(&self) -> &mut PlanArena<'arena> {
        // SAFETY: See binder(); the arena pointer has the same scope-bound
        // lifetime and is accessed only through ORM expression binding methods.
        unsafe { &mut *self.arena.as_ptr() }
    }

    fn binary(
        self,
        left: ScalarExpression,
        op: expression::BinaryOperator,
        right: ScalarExpression,
    ) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError> {
        self.binder()
            .bind_binary_op_expr(left, right, op, self.arena())
            .map(|expr| self.wrap(expr))
    }

    fn unary(
        self,
        op: expression::UnaryOperator,
        expr: ScalarExpression,
    ) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError> {
        self.binder()
            .bind_unary_op_expr(expr, op, self.arena())
            .map(|expr| self.wrap(expr))
    }

    fn function(
        self,
        name: impl Into<String>,
        args: Vec<ScalarExpression>,
    ) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError> {
        self.binder()
            .bind_function_call(name.into(), args, false, self.arena())
            .map(|expr| self.wrap(expr))
    }

    fn scalar_subquery<F>(
        self,
        build: F,
    ) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError>
    where
        F: for<'scope, 'sub_bind, 'sub_parent> FnOnce(
            &'scope mut OrmContext<'scope, 'sub_bind, 'sub_parent, 'arena, T, A>,
        )
            -> Result<LogicalPlan, DatabaseError>,
    {
        self.binder()
            .bind_scalar_subquery_plan(self.arena(), |binder, arena| {
                let mut context = OrmContext { binder, arena };
                build(&mut context)
            })
            .map(|expr| self.wrap(expr))
    }

    fn exists_subquery<F>(
        self,
        negated: bool,
        build: F,
    ) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError>
    where
        F: for<'scope, 'sub_bind, 'sub_parent> FnOnce(
            &'scope mut OrmContext<'scope, 'sub_bind, 'sub_parent, 'arena, T, A>,
        )
            -> Result<LogicalPlan, DatabaseError>,
    {
        self.binder()
            .bind_exists_subquery_plan(negated, self.arena(), |binder, arena| {
                let mut context = OrmContext { binder, arena };
                build(&mut context)
            })
            .map(|expr| self.wrap(expr))
    }

    fn quantified_subquery<F>(
        self,
        quantifier: MarkApplyQuantifier,
        negated: bool,
        left_expr: ScalarExpression,
        compare_op: expression::BinaryOperator,
        build: F,
    ) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError>
    where
        F: for<'scope, 'sub_bind, 'sub_parent> FnOnce(
            &'scope mut OrmContext<'scope, 'sub_bind, 'sub_parent, 'arena, T, A>,
        )
            -> Result<LogicalPlan, DatabaseError>,
    {
        self.binder()
            .bind_quantified_subquery_plan(
                quantifier,
                negated,
                left_expr,
                compare_op,
                self.arena(),
                |binder, arena| {
                    let mut context = OrmContext { binder, arena };
                    build(&mut context)
                },
            )
            .map(|expr| self.wrap(expr))
    }
}

/// ORM expression bound to the current query scope.
///
/// `CtxExpression` is a scope-bound ORM expression handle, not a reusable core
/// expression value. It exists so ORM code can use natural chained binding such
/// as `e.column(User::age())?.gte(18)?`. Convert it to a core
/// [`ScalarExpression`] only at ORM binder boundaries with [`Self::into_scalar`].
///
/// This type intentionally cannot be sent or shared across threads, and its
/// internal scope handle is private.
pub struct CtxExpression<'bind, 'parent, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    expr: ScalarExpression,
    scope: ExprBindScopeHandle<'bind, 'parent, 'arena, T, A>,
}

impl<'bind, 'parent, 'arena, T, A> CtxExpression<'bind, 'parent, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    pub fn into_scalar(self) -> ScalarExpression {
        self.expr
    }

    pub fn into_sort(self) -> SortField {
        self.into_scalar().into()
    }

    pub fn asc(self) -> SortField {
        self.into_sort().asc()
    }

    pub fn desc(self) -> SortField {
        self.into_sort().desc()
    }

    pub fn nulls_first(self) -> SortField {
        self.into_sort().nulls_first()
    }

    pub fn nulls_last(self) -> SortField {
        self.into_sort().nulls_last()
    }

    pub fn eq<R: IntoOrmScalarExpression>(self, right: R) -> Result<Self, DatabaseError> {
        self.scope.binary(
            self.expr,
            expression::BinaryOperator::Eq,
            right.into_orm_scalar(),
        )
    }

    pub fn ne<R: IntoOrmScalarExpression>(self, right: R) -> Result<Self, DatabaseError> {
        self.scope.binary(
            self.expr,
            expression::BinaryOperator::NotEq,
            right.into_orm_scalar(),
        )
    }

    pub fn gt<R: IntoOrmScalarExpression>(self, right: R) -> Result<Self, DatabaseError> {
        self.scope.binary(
            self.expr,
            expression::BinaryOperator::Gt,
            right.into_orm_scalar(),
        )
    }

    pub fn gte<R: IntoOrmScalarExpression>(self, right: R) -> Result<Self, DatabaseError> {
        self.scope.binary(
            self.expr,
            expression::BinaryOperator::GtEq,
            right.into_orm_scalar(),
        )
    }

    pub fn lt<R: IntoOrmScalarExpression>(self, right: R) -> Result<Self, DatabaseError> {
        self.scope.binary(
            self.expr,
            expression::BinaryOperator::Lt,
            right.into_orm_scalar(),
        )
    }

    pub fn lte<R: IntoOrmScalarExpression>(self, right: R) -> Result<Self, DatabaseError> {
        self.scope.binary(
            self.expr,
            expression::BinaryOperator::LtEq,
            right.into_orm_scalar(),
        )
    }

    pub fn like<R: IntoOrmScalarExpression>(self, right: R) -> Result<Self, DatabaseError> {
        self.scope.binary(
            self.expr,
            expression::BinaryOperator::Like(None),
            right.into_orm_scalar(),
        )
    }

    pub fn not_like<R: IntoOrmScalarExpression>(self, right: R) -> Result<Self, DatabaseError> {
        self.scope.binary(
            self.expr,
            expression::BinaryOperator::NotLike(None),
            right.into_orm_scalar(),
        )
    }

    pub fn and<R: IntoOrmScalarExpression>(self, right: R) -> Result<Self, DatabaseError> {
        self.scope.binary(
            self.expr,
            expression::BinaryOperator::And,
            right.into_orm_scalar(),
        )
    }

    pub fn or<R: IntoOrmScalarExpression>(self, right: R) -> Result<Self, DatabaseError> {
        self.scope.binary(
            self.expr,
            expression::BinaryOperator::Or,
            right.into_orm_scalar(),
        )
    }

    #[allow(clippy::should_implement_trait)]
    pub fn not(self) -> Result<Self, DatabaseError> {
        self.scope.unary(expression::UnaryOperator::Not, self.expr)
    }

    pub fn is_null(self) -> Self {
        let scope = self.scope;
        let expr = ScalarExpression::IsNull {
            negated: false,
            expr: Box::new(self.expr),
        };
        scope.wrap(expr)
    }

    pub fn is_not_null(self) -> Self {
        let scope = self.scope;
        let expr = ScalarExpression::IsNull {
            negated: true,
            expr: Box::new(self.expr),
        };
        scope.wrap(expr)
    }

    pub fn in_list<I, E>(self, values: I) -> Result<Self, DatabaseError>
    where
        I: IntoIterator<Item = E>,
        E: IntoOrmScalarExpression,
    {
        let scope = self.scope;
        let expr = ScalarExpression::In {
            negated: false,
            expr: Box::new(self.expr),
            args: values
                .into_iter()
                .map(IntoOrmScalarExpression::into_orm_scalar)
                .collect(),
        };
        Ok(scope.wrap(expr))
    }

    pub fn not_in_list<I, E>(self, values: I) -> Result<Self, DatabaseError>
    where
        I: IntoIterator<Item = E>,
        E: IntoOrmScalarExpression,
    {
        let scope = self.scope;
        let expr = ScalarExpression::In {
            negated: true,
            expr: Box::new(self.expr),
            args: values
                .into_iter()
                .map(IntoOrmScalarExpression::into_orm_scalar)
                .collect(),
        };
        Ok(scope.wrap(expr))
    }

    pub fn between<L, H>(self, low: L, high: H) -> Result<Self, DatabaseError>
    where
        L: IntoOrmScalarExpression,
        H: IntoOrmScalarExpression,
    {
        let scope = self.scope;
        let expr = ScalarExpression::Between {
            negated: false,
            expr: Box::new(self.expr),
            left_expr: Box::new(low.into_orm_scalar()),
            right_expr: Box::new(high.into_orm_scalar()),
        };
        Ok(scope.wrap(expr))
    }

    pub fn not_between<L, H>(self, low: L, high: H) -> Result<Self, DatabaseError>
    where
        L: IntoOrmScalarExpression,
        H: IntoOrmScalarExpression,
    {
        let scope = self.scope;
        let expr = ScalarExpression::Between {
            negated: true,
            expr: Box::new(self.expr),
            left_expr: Box::new(low.into_orm_scalar()),
            right_expr: Box::new(high.into_orm_scalar()),
        };
        Ok(scope.wrap(expr))
    }

    pub fn alias(self, alias: impl Into<String>) -> Self {
        let scope = self.scope;
        let alias = alias.into();
        scope
            .binder()
            .context
            .add_alias(None, alias.clone(), self.expr.clone());
        let expr = ScalarExpression::Alias {
            expr: Box::new(self.expr),
            alias: AliasType::Name(alias),
        };
        scope.wrap(expr)
    }

    pub fn cast(self, ty: LogicalType) -> Result<Self, DatabaseError> {
        let scope = self.scope;
        ScalarExpression::type_cast(self.expr, Cow::Owned(ty), scope.arena())
            .map(|expr| scope.wrap(expr))
    }

    pub fn function<E>(
        self,
        name: impl Into<String>,
        args: impl IntoIterator<Item = E>,
    ) -> Result<Self, DatabaseError>
    where
        E: IntoOrmScalarExpression,
    {
        let mut args = args
            .into_iter()
            .map(IntoOrmScalarExpression::into_orm_scalar)
            .collect::<Vec<_>>();
        args.insert(0, self.expr);
        self.scope.function(name, args)
    }

    fn quantified_subquery<F>(
        self,
        quantifier: MarkApplyQuantifier,
        negated: bool,
        compare_op: expression::BinaryOperator,
        build: F,
    ) -> Result<Self, DatabaseError>
    where
        F: for<'scope, 'sub_bind, 'sub_parent> FnOnce(
            &'scope mut OrmContext<'scope, 'sub_bind, 'sub_parent, 'arena, T, A>,
        )
            -> Result<LogicalPlan, DatabaseError>,
    {
        self.scope
            .quantified_subquery(quantifier, negated, self.expr, compare_op, build)
    }

    impl_quantified_subquery_methods! {
        eq_any, Any, false, Eq;
        eq_all, All, false, Eq;
        gt_any, Any, false, Gt;
        gt_all, All, false, Gt;
        gte_any, Any, false, GtEq;
        gte_all, All, false, GtEq;
        lt_any, Any, false, Lt;
        lt_all, All, false, Lt;
        lte_any, Any, false, LtEq;
        lte_all, All, false, LtEq;
        in_subquery, Any, false, Eq;
        not_in_subquery, Any, true, Eq;
    }
}

impl<'bind, 'parent, 'arena, T, A> fmt::Debug for CtxExpression<'bind, 'parent, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.expr.fmt(f)
    }
}

impl<'bind, 'parent, 'arena, T, A> Clone for CtxExpression<'bind, 'parent, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    fn clone(&self) -> Self {
        Self {
            expr: self.expr.clone(),
            scope: self.scope,
        }
    }
}

impl<'bind, 'parent, 'arena, T, A> PartialEq for CtxExpression<'bind, 'parent, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    fn eq(&self, other: &Self) -> bool {
        self.expr == other.expr
    }
}

impl<'bind, 'parent, 'arena, T, A> Eq for CtxExpression<'bind, 'parent, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
}

impl<'bind, 'parent, 'arena, T, A> Hash for CtxExpression<'bind, 'parent, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
    }
}

impl<'bind, 'parent, 'arena, T, A> IntoOrmScalarExpression
    for CtxExpression<'bind, 'parent, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    fn into_orm_scalar(self) -> ScalarExpression {
        self.into_scalar()
    }
}

fn bind_orm_context<E, F>(executor: E, build: F) -> Result<E::Iter, DatabaseError>
where
    E: BindSource,
    F: for<'ctx, 'bind, 'parent, 'arena> FnOnce(
        &'ctx mut OrmContext<
            'ctx,
            'bind,
            'parent,
            'arena,
            E::Transaction,
            &'static [(&'static str, DataValue)],
        >,
    ) -> Result<LogicalPlan, DatabaseError>,
{
    static EMPTY_BIND_PARAMS: &[(&str, DataValue)] = &[];
    executor.execute(EMPTY_BIND_PARAMS, |binder, arena| {
        let mut context = OrmContext { binder, arena };
        build(&mut context)
    })
}

fn explain_orm_context<E, F>(executor: E, build: F) -> Result<String, DatabaseError>
where
    E: BindSource,
    F: for<'ctx, 'bind, 'parent, 'arena> FnOnce(
        &'ctx mut OrmContext<
            'ctx,
            'bind,
            'parent,
            'arena,
            E::Transaction,
            &'static [(&'static str, DataValue)],
        >,
    ) -> Result<LogicalPlan, DatabaseError>,
{
    static EMPTY_BIND_PARAMS: &[(&str, DataValue)] = &[];
    executor.explain(EMPTY_BIND_PARAMS, |binder, arena| {
        let mut context = OrmContext { binder, arena };
        build(&mut context)
    })
}

/// Binder-backed ORM query context.
///
/// This context is created by [`Database::bind`] or [`DBTransaction::bind`]. Query construction inside
/// the closure binds directly into [`ScalarExpression`] and [`LogicalPlan`]
/// values; it does not build an ORM expression tree first.
pub struct OrmContext<'ctx, 'bind, 'parent, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    binder: &'ctx mut Binder<'bind, 'parent, T, A>,
    arena: &'ctx mut PlanArena<'arena>,
}

/// Narrow expression binding scope borrowed from an [`OrmContext`].
pub struct ExprBindScope<'ctx, 'bind, 'parent, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    binder: &'ctx mut Binder<'bind, 'parent, T, A>,
    arena: &'ctx mut PlanArena<'arena>,
}

pub struct UpdateBindScope<'ctx, 'bind, 'parent, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    binder: &'ctx mut Binder<'bind, 'parent, T, A>,
    arena: &'ctx mut PlanArena<'arena>,
    source_name: String,
    value_exprs: Vec<(ColumnRef, ScalarExpression)>,
}

impl<'ctx, 'bind, 'parent, 'arena, T, A> OrmContext<'ctx, 'bind, 'parent, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    pub fn from<'scope, M: Model>(
        &'scope mut self,
    ) -> Result<BindPlanFrom<'scope, 'bind, 'parent, 'arena, T, A, M>, DatabaseError> {
        self.from_source(QuerySource::model::<M>(), false)
    }

    pub fn from_as<'scope, M: Model>(
        &'scope mut self,
        alias: impl Into<String>,
    ) -> Result<BindPlanFrom<'scope, 'bind, 'parent, 'arena, T, A, M>, DatabaseError> {
        self.from_source(QuerySource::model::<M>().with_alias(alias), false)
    }

    pub fn mutate<'scope, M: Model>(
        &'scope mut self,
    ) -> Result<BindPlanFrom<'scope, 'bind, 'parent, 'arena, T, A, M>, DatabaseError> {
        self.from_source(QuerySource::model::<M>(), true)
    }

    pub fn mutate_as<'scope, M: Model>(
        &'scope mut self,
        alias: impl Into<String>,
    ) -> Result<BindPlanFrom<'scope, 'bind, 'parent, 'arena, T, A, M>, DatabaseError> {
        self.from_source(QuerySource::model::<M>().with_alias(alias), true)
    }

    #[allow(clippy::wrong_self_convention)]
    fn from_source<'scope, M: Model>(
        &'scope mut self,
        source: QuerySource,
        mutation_source: bool,
    ) -> Result<BindPlanFrom<'scope, 'bind, 'parent, 'arena, T, A, M>, DatabaseError> {
        if mutation_source {
            self.binder.with_pk(source.table_name.as_str().into());
        }
        let plan = bind_orm_source(self.binder, source.clone(), None, self.arena);
        if mutation_source {
            self.binder.clear_with_pk();
        }
        let plan = plan?;
        self.binder
            .build_plan(self.arena)
            .from_plan(plan)
            .map(|from| from.typed())
    }

    fn set_operation<L, R>(
        &mut self,
        op: SetOperatorKind,
        all: bool,
        left: L,
        right: R,
    ) -> Result<LogicalPlan, DatabaseError>
    where
        L: for<'scope, 'child_bind, 'child_parent> FnOnce(
            &'scope mut OrmContext<'scope, 'child_bind, 'child_parent, 'arena, T, A>,
        )
            -> Result<LogicalPlan, DatabaseError>,
        R: for<'scope, 'child_bind, 'child_parent> FnOnce(
            &'scope mut OrmContext<'scope, 'child_bind, 'child_parent, 'arena, T, A>,
        )
            -> Result<LogicalPlan, DatabaseError>,
    {
        let left_plan = self.child_plan(left)?;
        let right_plan = self.child_plan(right)?;
        self.binder
            .bind_set_operation_plans(op, all, left_plan, right_plan, self.arena)
    }

    fn child_plan<F>(&mut self, build: F) -> Result<LogicalPlan, DatabaseError>
    where
        F: for<'scope, 'child_bind, 'child_parent> FnOnce(
            &'scope mut OrmContext<'scope, 'child_bind, 'child_parent, 'arena, T, A>,
        )
            -> Result<LogicalPlan, DatabaseError>,
    {
        let mut child_binder = Binder::new(
            self.binder.context.fork(),
            self.binder.args,
            self.binder.parent,
        );
        let plan = {
            let mut context = OrmContext {
                binder: &mut child_binder,
                arena: self.arena,
            };
            build(&mut context)?
        };
        if child_binder.context.has_outer_refs() {
            self.binder.context.mark_outer_ref();
        }
        Ok(plan)
    }

    pub fn union<L, R>(
        &mut self,
        all: bool,
        left: L,
        right: R,
    ) -> Result<LogicalPlan, DatabaseError>
    where
        L: for<'scope, 'child_bind, 'child_parent> FnOnce(
            &'scope mut OrmContext<'scope, 'child_bind, 'child_parent, 'arena, T, A>,
        )
            -> Result<LogicalPlan, DatabaseError>,
        R: for<'scope, 'child_bind, 'child_parent> FnOnce(
            &'scope mut OrmContext<'scope, 'child_bind, 'child_parent, 'arena, T, A>,
        )
            -> Result<LogicalPlan, DatabaseError>,
    {
        self.set_operation(SetOperatorKind::Union, all, left, right)
    }

    pub fn except<L, R>(
        &mut self,
        all: bool,
        left: L,
        right: R,
    ) -> Result<LogicalPlan, DatabaseError>
    where
        L: for<'scope, 'child_bind, 'child_parent> FnOnce(
            &'scope mut OrmContext<'scope, 'child_bind, 'child_parent, 'arena, T, A>,
        )
            -> Result<LogicalPlan, DatabaseError>,
        R: for<'scope, 'child_bind, 'child_parent> FnOnce(
            &'scope mut OrmContext<'scope, 'child_bind, 'child_parent, 'arena, T, A>,
        )
            -> Result<LogicalPlan, DatabaseError>,
    {
        self.set_operation(SetOperatorKind::Except, all, left, right)
    }

    pub fn intersect<L, R>(
        &mut self,
        all: bool,
        left: L,
        right: R,
    ) -> Result<LogicalPlan, DatabaseError>
    where
        L: for<'scope, 'child_bind, 'child_parent> FnOnce(
            &'scope mut OrmContext<'scope, 'child_bind, 'child_parent, 'arena, T, A>,
        )
            -> Result<LogicalPlan, DatabaseError>,
        R: for<'scope, 'child_bind, 'child_parent> FnOnce(
            &'scope mut OrmContext<'scope, 'child_bind, 'child_parent, 'arena, T, A>,
        )
            -> Result<LogicalPlan, DatabaseError>,
    {
        self.set_operation(SetOperatorKind::Intersect, all, left, right)
    }

    pub fn insert_select<M, C, F>(
        &mut self,
        columns: C,
        build: F,
    ) -> Result<LogicalPlan, DatabaseError>
    where
        M: Model,
        C: IntoIterator,
        C::Item: Into<String>,
        F: for<'scope, 'child_bind, 'child_parent> FnOnce(
            &'scope mut OrmContext<'scope, 'child_bind, 'child_parent, 'arena, T, A>,
        )
            -> Result<LogicalPlan, DatabaseError>,
    {
        self.insert_select_inner::<M, C, F>(columns, false, build)
    }

    pub fn overwrite_select<M, C, F>(
        &mut self,
        columns: C,
        build: F,
    ) -> Result<LogicalPlan, DatabaseError>
    where
        M: Model,
        C: IntoIterator,
        C::Item: Into<String>,
        F: for<'scope, 'child_bind, 'child_parent> FnOnce(
            &'scope mut OrmContext<'scope, 'child_bind, 'child_parent, 'arena, T, A>,
        )
            -> Result<LogicalPlan, DatabaseError>,
    {
        self.insert_select_inner::<M, C, F>(columns, true, build)
    }

    fn insert_select_inner<M, C, F>(
        &mut self,
        columns: C,
        overwrite: bool,
        build: F,
    ) -> Result<LogicalPlan, DatabaseError>
    where
        M: Model,
        C: IntoIterator,
        C::Item: Into<String>,
        F: for<'scope, 'child_bind, 'child_parent> FnOnce(
            &'scope mut OrmContext<'scope, 'child_bind, 'child_parent, 'arena, T, A>,
        )
            -> Result<LogicalPlan, DatabaseError>,
    {
        let input_plan = self.child_plan(build)?;
        bind_orm_insert_plan(
            self.binder,
            M::table_name(),
            columns.into_iter().map(Into::into).collect(),
            input_plan,
            overwrite,
            self.arena,
        )
    }

    pub fn truncate<M: Model>(&mut self) -> Result<LogicalPlan, DatabaseError> {
        self.binder.bind_truncate(M::table_name().into())
    }
}

impl<'ctx, 'bind, 'parent, 'arena, T, A> ExprBindScope<'ctx, 'bind, 'parent, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    fn handle(&self) -> ExprBindScopeHandle<'bind, 'parent, 'arena, T, A> {
        ExprBindScopeHandle::new(self)
    }

    fn wrap(&self, expr: ScalarExpression) -> CtxExpression<'bind, 'parent, 'arena, T, A> {
        self.handle().wrap(expr)
    }

    pub fn column<M, V>(
        &self,
        field: Field<M, V>,
    ) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError> {
        let scope = self.handle();
        let expr = scope.binder().bind_column_ref_by_name(
            Some(field.table),
            field.column,
            None,
            scope.arena(),
        )?;
        Ok(scope.wrap(expr))
    }

    pub fn qualified_column<M, V>(
        &self,
        relation: &str,
        field: Field<M, V>,
    ) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError> {
        let scope = self.handle();
        let expr = scope.binder().bind_column_ref_by_name(
            Some(relation),
            field.column,
            None,
            scope.arena(),
        )?;
        Ok(scope.wrap(expr))
    }

    #[doc(hidden)]
    pub fn column_ref(
        &self,
        relation: &str,
        column: &str,
    ) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError> {
        let scope = self.handle();
        let expr =
            scope
                .binder()
                .bind_column_ref_by_name(Some(relation), column, None, scope.arena())?;
        Ok(scope.wrap(expr))
    }

    pub fn value<V: ToDataValue>(&self, value: V) -> CtxExpression<'bind, 'parent, 'arena, T, A> {
        self.wrap(ScalarExpression::Constant(value.to_data_value()))
    }

    pub fn data_value(&self, value: DataValue) -> CtxExpression<'bind, 'parent, 'arena, T, A> {
        self.wrap(ScalarExpression::Constant(value))
    }

    pub fn alias(
        &self,
        expr: impl IntoOrmScalarExpression,
        alias: impl Into<String>,
    ) -> CtxExpression<'bind, 'parent, 'arena, T, A> {
        self.wrap(expr.into_orm_scalar()).alias(alias)
    }

    pub fn cast(
        &self,
        expr: impl IntoOrmScalarExpression,
        ty: LogicalType,
    ) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError> {
        let scope = self.handle();
        let expr =
            ScalarExpression::type_cast(expr.into_orm_scalar(), Cow::Owned(ty), scope.arena())?;
        Ok(scope.wrap(expr))
    }

    pub fn unary(
        &self,
        op: expression::UnaryOperator,
        expr: impl IntoOrmScalarExpression,
    ) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError> {
        self.handle().unary(op, expr.into_orm_scalar())
    }

    pub fn binary(
        &self,
        left: impl IntoOrmScalarExpression,
        op: expression::BinaryOperator,
        right: impl IntoOrmScalarExpression,
    ) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError> {
        self.handle()
            .binary(left.into_orm_scalar(), op, right.into_orm_scalar())
    }

    pub fn eq(
        &self,
        left: impl IntoOrmScalarExpression,
        right: impl IntoOrmScalarExpression,
    ) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError> {
        self.binary(left, expression::BinaryOperator::Eq, right)
    }

    pub fn ne(
        &self,
        left: impl IntoOrmScalarExpression,
        right: impl IntoOrmScalarExpression,
    ) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError> {
        self.binary(left, expression::BinaryOperator::NotEq, right)
    }

    pub fn gt(
        &self,
        left: impl IntoOrmScalarExpression,
        right: impl IntoOrmScalarExpression,
    ) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError> {
        self.binary(left, expression::BinaryOperator::Gt, right)
    }

    pub fn gte(
        &self,
        left: impl IntoOrmScalarExpression,
        right: impl IntoOrmScalarExpression,
    ) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError> {
        self.binary(left, expression::BinaryOperator::GtEq, right)
    }

    pub fn lt(
        &self,
        left: impl IntoOrmScalarExpression,
        right: impl IntoOrmScalarExpression,
    ) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError> {
        self.binary(left, expression::BinaryOperator::Lt, right)
    }

    pub fn lte(
        &self,
        left: impl IntoOrmScalarExpression,
        right: impl IntoOrmScalarExpression,
    ) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError> {
        self.binary(left, expression::BinaryOperator::LtEq, right)
    }

    pub fn and(
        &self,
        left: impl IntoOrmScalarExpression,
        right: impl IntoOrmScalarExpression,
    ) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError> {
        self.binary(left, expression::BinaryOperator::And, right)
    }

    pub fn or(
        &self,
        left: impl IntoOrmScalarExpression,
        right: impl IntoOrmScalarExpression,
    ) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError> {
        self.binary(left, expression::BinaryOperator::Or, right)
    }

    pub fn is_null(
        &self,
        expr: impl IntoOrmScalarExpression,
    ) -> CtxExpression<'bind, 'parent, 'arena, T, A> {
        self.wrap(expr.into_orm_scalar()).is_null()
    }

    pub fn is_not_null(
        &self,
        expr: impl IntoOrmScalarExpression,
    ) -> CtxExpression<'bind, 'parent, 'arena, T, A> {
        self.wrap(expr.into_orm_scalar()).is_not_null()
    }

    pub fn in_list<I, E>(
        &self,
        expr: impl IntoOrmScalarExpression,
        args: I,
    ) -> CtxExpression<'bind, 'parent, 'arena, T, A>
    where
        I: IntoIterator<Item = E>,
        E: IntoOrmScalarExpression,
    {
        let expr = ScalarExpression::In {
            negated: false,
            expr: Box::new(expr.into_orm_scalar()),
            args: args
                .into_iter()
                .map(IntoOrmScalarExpression::into_orm_scalar)
                .collect(),
        };
        self.wrap(expr)
    }

    pub fn not_in_list<I, E>(
        &self,
        expr: impl IntoOrmScalarExpression,
        args: I,
    ) -> CtxExpression<'bind, 'parent, 'arena, T, A>
    where
        I: IntoIterator<Item = E>,
        E: IntoOrmScalarExpression,
    {
        let expr = ScalarExpression::In {
            negated: true,
            expr: Box::new(expr.into_orm_scalar()),
            args: args
                .into_iter()
                .map(IntoOrmScalarExpression::into_orm_scalar)
                .collect(),
        };
        self.wrap(expr)
    }

    pub fn between(
        &self,
        expr: impl IntoOrmScalarExpression,
        low: impl IntoOrmScalarExpression,
        high: impl IntoOrmScalarExpression,
    ) -> CtxExpression<'bind, 'parent, 'arena, T, A> {
        let expr = ScalarExpression::Between {
            negated: false,
            expr: Box::new(expr.into_orm_scalar()),
            left_expr: Box::new(low.into_orm_scalar()),
            right_expr: Box::new(high.into_orm_scalar()),
        };
        self.wrap(expr)
    }

    pub fn not_between(
        &self,
        expr: impl IntoOrmScalarExpression,
        low: impl IntoOrmScalarExpression,
        high: impl IntoOrmScalarExpression,
    ) -> CtxExpression<'bind, 'parent, 'arena, T, A> {
        let expr = ScalarExpression::Between {
            negated: true,
            expr: Box::new(expr.into_orm_scalar()),
            left_expr: Box::new(low.into_orm_scalar()),
            right_expr: Box::new(high.into_orm_scalar()),
        };
        self.wrap(expr)
    }

    pub fn not(
        &self,
        expr: impl IntoOrmScalarExpression,
    ) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError> {
        self.unary(expression::UnaryOperator::Not, expr)
    }

    pub fn function<E>(
        &self,
        name: impl Into<String>,
        args: impl IntoIterator<Item = E>,
    ) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError>
    where
        E: IntoOrmScalarExpression,
    {
        let args = args
            .into_iter()
            .map(IntoOrmScalarExpression::into_orm_scalar)
            .collect();
        self.handle().function(name, args)
    }

    pub fn aggregate<E>(
        &self,
        name: impl Into<String>,
        args: impl IntoIterator<Item = E>,
    ) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError>
    where
        E: IntoOrmScalarExpression,
    {
        self.function(name, args)
    }

    pub fn count_all(&self) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError> {
        self.function(
            "count",
            vec![Binder::<'bind, 'parent, T, A>::wildcard_expr()],
        )
    }

    pub fn case_when<C, V, E>(
        &self,
        expr_pairs: impl IntoIterator<Item = (C, V)>,
        else_expr: Option<E>,
    ) -> CtxExpression<'bind, 'parent, 'arena, T, A>
    where
        C: IntoOrmScalarExpression,
        V: IntoOrmScalarExpression,
        E: IntoOrmScalarExpression,
    {
        let expr_pairs = expr_pairs
            .into_iter()
            .map(|(condition, value)| (condition.into_orm_scalar(), value.into_orm_scalar()))
            .collect::<Vec<_>>();
        let else_expr = else_expr.map(IntoOrmScalarExpression::into_orm_scalar);
        let ty = expr_pairs
            .first()
            .map(|(_, value)| value.return_type(self.arena).into_owned())
            .or_else(|| {
                else_expr
                    .as_ref()
                    .map(|value| value.return_type(self.arena).into_owned())
            })
            .unwrap_or(LogicalType::SqlNull);
        self.wrap(ScalarExpression::CaseWhen {
            operand_expr: None,
            expr_pairs,
            else_expr: else_expr.map(Box::new),
            ty,
        })
    }

    pub fn case_value<K, V, E>(
        &self,
        operand_expr: impl IntoOrmScalarExpression,
        expr_pairs: impl IntoIterator<Item = (K, V)>,
        else_expr: Option<E>,
    ) -> CtxExpression<'bind, 'parent, 'arena, T, A>
    where
        K: IntoOrmScalarExpression,
        V: IntoOrmScalarExpression,
        E: IntoOrmScalarExpression,
    {
        let expr_pairs = expr_pairs
            .into_iter()
            .map(|(key, value)| (key.into_orm_scalar(), value.into_orm_scalar()))
            .collect::<Vec<_>>();
        let else_expr = else_expr.map(IntoOrmScalarExpression::into_orm_scalar);
        let ty = expr_pairs
            .first()
            .map(|(_, value)| value.return_type(self.arena).into_owned())
            .or_else(|| {
                else_expr
                    .as_ref()
                    .map(|value| value.return_type(self.arena).into_owned())
            })
            .unwrap_or(LogicalType::SqlNull);
        self.wrap(ScalarExpression::CaseWhen {
            operand_expr: Some(Box::new(operand_expr.into_orm_scalar())),
            expr_pairs,
            else_expr: else_expr.map(Box::new),
            ty,
        })
    }

    pub fn scalar_subquery<F>(
        &self,
        build: F,
    ) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError>
    where
        F: for<'scope, 'sub_bind, 'sub_parent> FnOnce(
            &'scope mut OrmContext<'scope, 'sub_bind, 'sub_parent, 'arena, T, A>,
        )
            -> Result<LogicalPlan, DatabaseError>,
    {
        self.handle().scalar_subquery(build)
    }

    pub fn exists_subquery<F>(
        &self,
        negated: bool,
        build: F,
    ) -> Result<CtxExpression<'bind, 'parent, 'arena, T, A>, DatabaseError>
    where
        F: for<'scope, 'sub_bind, 'sub_parent> FnOnce(
            &'scope mut OrmContext<'scope, 'sub_bind, 'sub_parent, 'arena, T, A>,
        )
            -> Result<LogicalPlan, DatabaseError>,
    {
        self.handle().exists_subquery(negated, build)
    }
}

impl<'ctx, 'bind, 'parent, 'arena, T, A> UpdateBindScope<'ctx, 'bind, 'parent, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    pub fn set_value<M, V, D>(&mut self, field: Field<M, V>, value: D) -> Result<(), DatabaseError>
    where
        D: ToDataValue,
    {
        let expr = ScalarExpression::Constant(value.to_data_value());
        self.push_assignment(field.column, expr)
    }

    pub fn set<M, V, D>(&mut self, field: Field<M, V>, value: D) -> Result<(), DatabaseError>
    where
        D: ToDataValue,
    {
        self.set_value(field, value)
    }

    pub fn set_bound_expr<M, V>(
        &mut self,
        field: Field<M, V>,
        build: impl BindOrmScalar<'bind, 'parent, 'arena, T, A>,
    ) -> Result<(), DatabaseError> {
        let expr = with_query_bind_step!(self.binder, QueryBindStep::Project, {
            let mut scope = ExprBindScope {
                binder: self.binder,
                arena: self.arena,
            };
            build.bind_scalar(&mut scope)?
        });
        self.push_assignment(field.column, expr?)
    }

    pub fn set_expr<M, V, E>(
        &mut self,
        field: Field<M, V>,
        build: impl for<'scope> FnOnce(
            &'scope mut ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A>,
        ) -> Result<E, DatabaseError>,
    ) -> Result<(), DatabaseError>
    where
        E: IntoOrmScalarExpression,
    {
        let expr = with_query_bind_step!(self.binder, QueryBindStep::Project, {
            let mut scope = ExprBindScope {
                binder: self.binder,
                arena: self.arena,
            };
            build(&mut scope)?.into_orm_scalar()
        });
        self.push_assignment(field.column, expr?)
    }

    fn push_assignment(
        &mut self,
        column_name: &str,
        mut expr: ScalarExpression,
    ) -> Result<(), DatabaseError> {
        let column =
            bind_orm_target_column(self.binder, &self.source_name, column_name, self.arena)?;
        if matches!(expr, ScalarExpression::Empty) {
            let column_catalog = self.arena.column(column);
            let default_value = column_catalog
                .default_value()?
                .ok_or(DatabaseError::DefaultNotExist)?;
            expr = ScalarExpression::Constant(default_value);
        }
        let column_catalog = self.arena.column(column);
        expr = ScalarExpression::type_cast(
            expr,
            Cow::Borrowed(column_catalog.datatype()),
            self.arena,
        )?;
        self.value_exprs.push((column, expr));
        Ok(())
    }

    fn finish(
        self,
        table_name: TableName,
        plan: LogicalPlan,
    ) -> Result<LogicalPlan, DatabaseError> {
        self.binder.context.allow_default = false;
        if self.value_exprs.is_empty() {
            return Err(DatabaseError::ColumnsEmpty);
        }
        self.binder.bind_update(table_name, self.value_exprs, plan)
    }
}

impl<'scope_ctx, 'bind, 'parent, 'arena, T, A, M>
    BindPlanFrom<'scope_ctx, 'bind, 'parent, 'arena, T, A, M>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
    M: Model,
{
    fn model_table_name(&self) -> Result<TableName, DatabaseError> {
        Ok(M::table_name().into())
    }

    fn model_relation_name(&self) -> Result<String, DatabaseError> {
        let table_name = M::table_name();
        self.binder
            .context
            .bind_table
            .iter()
            .rev()
            .find(|source| source.table_name.as_ref() == table_name)
            .map(|source| source.visible_name().to_string())
            .ok_or_else(|| DatabaseError::invalid_table(table_name))
    }

    fn expr_scope<'scope>(&'scope mut self) -> ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A> {
        ExprBindScope {
            binder: self.binder,
            arena: self.arena,
        }
    }

    pub fn filter<E>(
        mut self,
        build: impl for<'scope> FnOnce(
            &'scope mut ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A>,
        ) -> Result<E, DatabaseError>,
    ) -> Result<Self, DatabaseError>
    where
        E: IntoOrmScalarExpression,
    {
        let predicate = with_query_bind_step!(self.binder, QueryBindStep::Where, {
            let mut scope = self.expr_scope();
            build(&mut scope)?.into_orm_scalar()
        });
        let predicate = predicate?;
        self.filter_expr(predicate)
    }

    fn join_with<N: Model>(
        self,
        join_type: JoinType,
        alias: Option<String>,
        constraint: JoinConstraintInput,
    ) -> Result<Self, DatabaseError> {
        let source = match alias {
            Some(alias) => QuerySource::model::<N>().with_alias(alias),
            None => QuerySource::model::<N>(),
        };
        let (right_plan, right_context) = {
            let mut right_binder = Binder::new(
                self.binder.context.fork_empty(),
                self.binder.args,
                Some(&self.binder.context),
            );
            let right_plan =
                bind_orm_source(&mut right_binder, source, Some(join_type), self.arena)?;
            (right_plan, right_binder.context)
        };
        self.join_plan(right_plan, right_context, join_type, constraint)
    }

    fn join_on<N: Model, E>(
        mut self,
        join_type: JoinType,
        alias: Option<String>,
        build: impl for<'scope> FnOnce(
            &'scope mut ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A>,
        ) -> Result<E, DatabaseError>,
    ) -> Result<Self, DatabaseError>
    where
        E: IntoOrmScalarExpression,
    {
        let source = match alias {
            Some(alias) => QuerySource::model::<N>().with_alias(alias),
            None => QuerySource::model::<N>(),
        };
        let (right_plan, right_context) = {
            let mut right_binder = Binder::new(
                self.binder.context.fork_empty(),
                self.binder.args,
                Some(&self.binder.context),
            );
            let right_plan =
                bind_orm_source(&mut right_binder, source, Some(join_type), self.arena)?;
            (right_plan, right_binder.context)
        };
        self.binder.extend(right_context);
        let on = with_query_bind_step!(self.binder, QueryBindStep::From, {
            let mut scope = self.expr_scope();
            build(&mut scope)?.into_orm_scalar()
        });
        self.plan = self.binder.bind_join_plans(
            self.plan,
            right_plan,
            join_type,
            JoinConstraintInput::On(on?),
            self.arena,
        )?;
        Ok(self)
    }

    pub fn inner_join<N: Model, E>(
        self,
        build: impl for<'scope> FnOnce(
            &'scope mut ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A>,
        ) -> Result<E, DatabaseError>,
    ) -> Result<Self, DatabaseError>
    where
        E: IntoOrmScalarExpression,
    {
        self.join_on::<N, E>(JoinType::Inner, None, build)
    }

    pub fn inner_join_as<N: Model, E>(
        self,
        alias: impl Into<String>,
        build: impl for<'scope> FnOnce(
            &'scope mut ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A>,
        ) -> Result<E, DatabaseError>,
    ) -> Result<Self, DatabaseError>
    where
        E: IntoOrmScalarExpression,
    {
        self.join_on::<N, E>(JoinType::Inner, Some(alias.into()), build)
    }

    pub fn left_join<N: Model, E>(
        self,
        build: impl for<'scope> FnOnce(
            &'scope mut ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A>,
        ) -> Result<E, DatabaseError>,
    ) -> Result<Self, DatabaseError>
    where
        E: IntoOrmScalarExpression,
    {
        self.join_on::<N, E>(JoinType::LeftOuter, None, build)
    }

    pub fn left_join_as<N: Model, E>(
        self,
        alias: impl Into<String>,
        build: impl for<'scope> FnOnce(
            &'scope mut ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A>,
        ) -> Result<E, DatabaseError>,
    ) -> Result<Self, DatabaseError>
    where
        E: IntoOrmScalarExpression,
    {
        self.join_on::<N, E>(JoinType::LeftOuter, Some(alias.into()), build)
    }

    pub fn right_join<N: Model, E>(
        self,
        build: impl for<'scope> FnOnce(
            &'scope mut ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A>,
        ) -> Result<E, DatabaseError>,
    ) -> Result<Self, DatabaseError>
    where
        E: IntoOrmScalarExpression,
    {
        self.join_on::<N, E>(JoinType::RightOuter, None, build)
    }

    pub fn full_join<N: Model, E>(
        self,
        build: impl for<'scope> FnOnce(
            &'scope mut ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A>,
        ) -> Result<E, DatabaseError>,
    ) -> Result<Self, DatabaseError>
    where
        E: IntoOrmScalarExpression,
    {
        self.join_on::<N, E>(JoinType::Full, None, build)
    }

    pub fn cross_join<N: Model>(self) -> Result<Self, DatabaseError> {
        self.join_with::<N>(JoinType::Cross, None, JoinConstraintInput::None)
    }

    fn join_using<N: Model>(
        self,
        join_type: JoinType,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, DatabaseError> {
        self.join_with::<N>(
            join_type,
            None,
            JoinConstraintInput::Using(columns.into_iter().map(Into::into).collect()),
        )
    }

    pub fn inner_join_using<N: Model>(
        self,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, DatabaseError> {
        self.join_using::<N>(JoinType::Inner, columns)
    }

    pub fn left_join_using<N: Model>(
        self,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, DatabaseError> {
        self.join_using::<N>(JoinType::LeftOuter, columns)
    }

    pub fn right_join_using<N: Model>(
        self,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, DatabaseError> {
        self.join_using::<N>(JoinType::RightOuter, columns)
    }

    pub fn full_join_using<N: Model>(
        self,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, DatabaseError> {
        self.join_using::<N>(JoinType::Full, columns)
    }

    pub fn project_model(
        mut self,
    ) -> Result<BindPlanSelectList<'scope_ctx, 'bind, 'parent, 'arena, T, A, M>, DatabaseError>
    {
        let relation = self.model_relation_name()?;
        let mut select_list = Vec::with_capacity(M::fields().len());
        with_query_bind_step!(self.binder, QueryBindStep::Project, {
            let scope = self.expr_scope();
            for field in M::fields() {
                select_list.push(
                    scope
                        .qualified_column(
                            &relation,
                            Field::<M, ()>::new(M::table_name(), field.column),
                        )?
                        .into_orm_scalar(),
                );
            }
        })?;
        Ok(self.select_list(select_list))
    }

    pub fn project<P: Projection>(
        mut self,
    ) -> Result<BindPlanSelectList<'scope_ctx, 'bind, 'parent, 'arena, T, A, M>, DatabaseError>
    {
        let relation = self.model_relation_name()?;
        let projection = with_query_bind_step!(self.binder, QueryBindStep::Project, {
            let mut scope = self.expr_scope();
            P::bind_projection(&mut scope, &relation)?
        });
        Ok(self.select_list(projection?))
    }

    pub fn project_value<E>(
        mut self,
        build: impl for<'scope> FnOnce(
            &'scope mut ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A>,
        ) -> Result<E, DatabaseError>,
    ) -> Result<BindPlanSelectList<'scope_ctx, 'bind, 'parent, 'arena, T, A, M>, DatabaseError>
    where
        E: IntoOrmScalarExpression,
    {
        let expr = with_query_bind_step!(self.binder, QueryBindStep::Project, {
            let mut scope = self.expr_scope();
            build(&mut scope)?.into_orm_scalar()
        });
        Ok(self.select_list(vec![expr?]))
    }

    pub fn project_tuple<E>(
        mut self,
        build: impl for<'scope> FnOnce(
            &'scope mut ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A>,
        ) -> Result<Vec<E>, DatabaseError>,
    ) -> Result<BindPlanSelectList<'scope_ctx, 'bind, 'parent, 'arena, T, A, M>, DatabaseError>
    where
        E: IntoOrmScalarExpression,
    {
        let exprs = with_query_bind_step!(self.binder, QueryBindStep::Project, {
            let mut scope = self.expr_scope();
            build(&mut scope)?
        });
        Ok(self.select_list(
            exprs?
                .into_iter()
                .map(IntoOrmScalarExpression::into_orm_scalar)
                .collect(),
        ))
    }

    pub fn project_scalar(
        mut self,
        build: impl BindOrmScalar<'bind, 'parent, 'arena, T, A>,
    ) -> Result<BindPlanSelectList<'scope_ctx, 'bind, 'parent, 'arena, T, A, M>, DatabaseError>
    {
        let expr = with_query_bind_step!(self.binder, QueryBindStep::Project, {
            let mut scope = self.expr_scope();
            build.bind_scalar(&mut scope)?
        });
        Ok(self.select_list(vec![expr?]))
    }

    pub fn project_scalars(
        mut self,
        build: impl BindOrmScalarList<'bind, 'parent, 'arena, T, A>,
    ) -> Result<BindPlanSelectList<'scope_ctx, 'bind, 'parent, 'arena, T, A, M>, DatabaseError>
    {
        let exprs = with_query_bind_step!(self.binder, QueryBindStep::Project, {
            let mut scope = self.expr_scope();
            build.bind_scalar_list(&mut scope)?
        });
        Ok(self.select_list(exprs?))
    }

    pub fn group_by<E>(
        self,
        build: impl for<'scope> FnOnce(
            &'scope mut ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A>,
        ) -> Result<E, DatabaseError>,
    ) -> Result<BindPlanSelectList<'scope_ctx, 'bind, 'parent, 'arena, T, A, M>, DatabaseError>
    where
        E: IntoOrmScalarExpression,
    {
        self.project_model()?.group_by(build)
    }

    pub fn having<E>(
        self,
        build: impl for<'scope> FnOnce(
            &'scope mut ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A>,
        ) -> Result<E, DatabaseError>,
    ) -> Result<BindPlanSelectList<'scope_ctx, 'bind, 'parent, 'arena, T, A, M>, DatabaseError>
    where
        E: IntoOrmScalarExpression,
    {
        self.project_model()?.having(build)
    }

    pub fn group_by_scalar(
        self,
        build: impl BindOrmScalar<'bind, 'parent, 'arena, T, A>,
    ) -> Result<BindPlanSelectList<'scope_ctx, 'bind, 'parent, 'arena, T, A, M>, DatabaseError>
    {
        self.project_model()?.group_by_scalar(build)
    }

    pub fn having_scalar(
        self,
        build: impl BindOrmScalar<'bind, 'parent, 'arena, T, A>,
    ) -> Result<BindPlanSelectList<'scope_ctx, 'bind, 'parent, 'arena, T, A, M>, DatabaseError>
    {
        self.project_model()?.having_scalar(build)
    }

    pub fn order_by(
        self,
        build: impl BindOrmSort<'bind, 'parent, 'arena, T, A>,
    ) -> Result<BindPlanSelectList<'scope_ctx, 'bind, 'parent, 'arena, T, A, M>, DatabaseError>
    {
        self.project_model()?.order_by(build)
    }

    pub fn order_by_expr(
        self,
        build: impl for<'scope> FnOnce(
            &'scope mut ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A>,
        ) -> Result<SortField, DatabaseError>,
    ) -> Result<BindPlanSelectList<'scope_ctx, 'bind, 'parent, 'arena, T, A, M>, DatabaseError>
    {
        self.project_model()?.order_by_expr(build)
    }

    pub fn count(mut self) -> Result<LogicalPlan, DatabaseError> {
        let count = with_query_bind_step!(self.binder, QueryBindStep::Project, {
            let scope = self.expr_scope();
            let count = scope.count_all()?;
            scope.alias(count, "count").into_orm_scalar()
        });
        self.select_list(vec![count?]).count()
    }

    pub fn exists(self) -> Result<LogicalPlan, DatabaseError> {
        self.binder.bind_limit_values(self.plan, None, Some(1))
    }

    pub fn delete(self) -> Result<LogicalPlan, DatabaseError> {
        let table_name = self.model_table_name()?;
        let primary_keys = self
            .binder
            .context
            .table(table_name.clone())?
            .ok_or(DatabaseError::TableNotFound)?
            .primary_keys()
            .iter()
            .map(|(_, column)| *column)
            .collect();
        self.binder.with_pk(table_name.clone());
        self.binder.bind_delete(table_name, primary_keys, self.plan)
    }

    pub fn update(
        self,
        build: impl FnOnce(
            &mut UpdateBindScope<'scope_ctx, 'bind, 'parent, 'arena, T, A>,
        ) -> Result<(), DatabaseError>,
    ) -> Result<LogicalPlan, DatabaseError> {
        let table_name = self.model_table_name()?;
        let source_name = self.model_relation_name()?;
        self.binder.context.allow_default = true;
        self.binder.with_pk(table_name.clone());
        let mut scope = UpdateBindScope {
            binder: self.binder,
            arena: self.arena,
            source_name,
            value_exprs: Vec::new(),
        };
        build(&mut scope)?;
        scope.finish(table_name, self.plan)
    }

    pub fn finish(self) -> Result<LogicalPlan, DatabaseError> {
        self.project_model()?.finish()
    }
}

impl<'scope_ctx, 'bind, 'parent, 'arena, T, A, M>
    BindPlanSelectList<'scope_ctx, 'bind, 'parent, 'arena, T, A, M>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
    M: Model,
{
    fn expr_scope<'scope>(&'scope mut self) -> ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A> {
        ExprBindScope {
            binder: self.binder,
            arena: self.arena,
        }
    }

    pub fn project_value<E>(
        mut self,
        build: impl for<'scope> FnOnce(
            &'scope mut ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A>,
        ) -> Result<E, DatabaseError>,
    ) -> Result<Self, DatabaseError>
    where
        E: IntoOrmScalarExpression,
    {
        let expr = with_query_bind_step!(self.binder, QueryBindStep::Project, {
            let mut scope = self.expr_scope();
            build(&mut scope)?.into_orm_scalar()
        });
        Ok(self.set_select_list(vec![expr?]))
    }

    pub fn project_tuple<E>(
        mut self,
        build: impl for<'scope> FnOnce(
            &'scope mut ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A>,
        ) -> Result<Vec<E>, DatabaseError>,
    ) -> Result<Self, DatabaseError>
    where
        E: IntoOrmScalarExpression,
    {
        let exprs = with_query_bind_step!(self.binder, QueryBindStep::Project, {
            let mut scope = self.expr_scope();
            build(&mut scope)?
        });
        Ok(self.set_select_list(
            exprs?
                .into_iter()
                .map(IntoOrmScalarExpression::into_orm_scalar)
                .collect(),
        ))
    }

    pub fn project_scalar(
        mut self,
        build: impl BindOrmScalar<'bind, 'parent, 'arena, T, A>,
    ) -> Result<Self, DatabaseError> {
        let expr = with_query_bind_step!(self.binder, QueryBindStep::Project, {
            let mut scope = self.expr_scope();
            build.bind_scalar(&mut scope)?
        });
        Ok(self.set_select_list(vec![expr?]))
    }

    pub fn project_scalars(
        mut self,
        build: impl BindOrmScalarList<'bind, 'parent, 'arena, T, A>,
    ) -> Result<Self, DatabaseError> {
        let exprs = with_query_bind_step!(self.binder, QueryBindStep::Project, {
            let mut scope = self.expr_scope();
            build.bind_scalar_list(&mut scope)?
        });
        Ok(self.set_select_list(exprs?))
    }

    pub fn group_by<E>(
        mut self,
        build: impl for<'scope> FnOnce(
            &'scope mut ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A>,
        ) -> Result<E, DatabaseError>,
    ) -> Result<Self, DatabaseError>
    where
        E: IntoOrmScalarExpression,
    {
        let expr = with_query_bind_step!(self.binder, QueryBindStep::Agg, {
            let mut scope = self.expr_scope();
            build(&mut scope)?.into_orm_scalar()
        });
        self.group_by_expr(expr?)
    }

    pub fn having<E>(
        mut self,
        build: impl for<'scope> FnOnce(
            &'scope mut ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A>,
        ) -> Result<E, DatabaseError>,
    ) -> Result<Self, DatabaseError>
    where
        E: IntoOrmScalarExpression,
    {
        let expr = with_query_bind_step!(self.binder, QueryBindStep::Having, {
            let mut scope = self.expr_scope();
            build(&mut scope)?.into_orm_scalar()
        });
        self.having_expr(expr?)
    }

    pub fn group_by_scalar(
        mut self,
        build: impl BindOrmScalar<'bind, 'parent, 'arena, T, A>,
    ) -> Result<Self, DatabaseError> {
        let expr = with_query_bind_step!(self.binder, QueryBindStep::Agg, {
            let mut scope = self.expr_scope();
            build.bind_scalar(&mut scope)?
        });
        self.group_by_expr(expr?)
    }

    pub fn having_scalar(
        mut self,
        build: impl BindOrmScalar<'bind, 'parent, 'arena, T, A>,
    ) -> Result<Self, DatabaseError> {
        let expr = with_query_bind_step!(self.binder, QueryBindStep::Having, {
            let mut scope = self.expr_scope();
            build.bind_scalar(&mut scope)?
        });
        self.having_expr(expr?)
    }

    pub fn order_by(
        mut self,
        build: impl BindOrmSort<'bind, 'parent, 'arena, T, A>,
    ) -> Result<Self, DatabaseError> {
        let sort = with_query_bind_step!(self.binder, QueryBindStep::Sort, {
            let mut scope = self.expr_scope();
            build.bind_sort(&mut scope)?
        });
        self.sort_field(sort?)
    }

    pub fn order_by_expr(
        mut self,
        build: impl for<'scope> FnOnce(
            &'scope mut ExprBindScope<'scope, 'bind, 'parent, 'arena, T, A>,
        ) -> Result<SortField, DatabaseError>,
    ) -> Result<Self, DatabaseError> {
        let sort = with_query_bind_step!(self.binder, QueryBindStep::Sort, {
            let mut scope = self.expr_scope();
            build(&mut scope)?
        });
        self.sort_field(sort?)
    }

    pub fn count(mut self) -> Result<LogicalPlan, DatabaseError> {
        let count = with_query_bind_step!(self.binder, QueryBindStep::Project, {
            let scope = self.expr_scope();
            let count = scope.count_all()?;
            scope.alias(count, "count").into_orm_scalar()
        });
        self.set_select_list(vec![count?])
            .aggregate_without_group()?
            .finish()
    }
}

#[doc(hidden)]
pub trait Projection: FromQueryRow {
    fn bind_projection<'ctx, 'bind, 'parent, 'arena, T, A>(
        scope: &mut ExprBindScope<'ctx, 'bind, 'parent, 'arena, T, A>,
        relation: &str,
    ) -> Result<Vec<ScalarExpression>, DatabaseError>
    where
        T: Transaction,
        A: AsRef<[(&'static str, DataValue)]>;
}

fn orm_table_alias(source: &QuerySource) -> Option<TableAliasInput> {
    source.alias.as_ref().map(|alias| TableAliasInput {
        name: alias.as_str().into(),
        columns: Vec::new(),
    })
}

fn bind_orm_source<'bind, 'parent, 'arena, T, A>(
    binder: &mut Binder<'bind, 'parent, T, A>,
    source: QuerySource,
    join_type: Option<JoinType>,
    arena: &mut PlanArena<'arena>,
) -> Result<LogicalPlan, DatabaseError>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    let alias = orm_table_alias(&source);
    binder.bind_base_table_ref(join_type, source.table_name.as_str().into(), alias, arena)
}

fn bind_orm_target_column<'bind, 'parent, 'arena, T, A>(
    binder: &mut Binder<'bind, 'parent, T, A>,
    source_name: &str,
    column_name: &str,
    arena: &mut PlanArena<'arena>,
) -> Result<ColumnRef, DatabaseError>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    match binder.bind_column_ref_by_name(None, column_name, Some(source_name), arena)? {
        ScalarExpression::ColumnRef { column, .. } => Ok(column),
        _ => Err(DatabaseError::invalid_column(column_name.to_string())),
    }
}

fn bind_orm_insert_plan<'bind, 'parent, 'arena, T, A>(
    binder: &mut Binder<'bind, 'parent, T, A>,
    table_name: &str,
    columns: Vec<String>,
    mut input_plan: LogicalPlan,
    overwrite: bool,
    arena: &mut PlanArena<'arena>,
) -> Result<LogicalPlan, DatabaseError>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    let table_name: TableName = table_name.into();
    let input_schema = input_plan.output_schema(arena).clone();
    let input_len = input_schema.len();

    let projection = {
        let source = binder
            .context
            .source(&table_name)?
            .ok_or(DatabaseError::TableNotFound)?;

        if columns.is_empty() {
            let table_schema = source.schema();
            if input_len > table_schema.len() {
                return Err(DatabaseError::ValuesLenMismatch(
                    table_schema.len(),
                    input_len,
                ));
            }
            table_schema[..input_len]
                .iter()
                .copied()
                .enumerate()
                .map(|(position, target_column)| ScalarExpression::Alias {
                    expr: Box::new(ScalarExpression::column_expr(
                        input_schema[position],
                        position,
                    )),
                    alias: AliasType::Name(arena.column(target_column).name().to_string()),
                })
                .collect::<Vec<_>>()
        } else {
            if input_len != columns.len() {
                return Err(DatabaseError::ValuesLenMismatch(columns.len(), input_len));
            }
            let mut projection = Vec::with_capacity(columns.len());
            for (position, column_name) in columns.into_iter().enumerate() {
                let column = source
                    .column(&column_name, arena)
                    .ok_or_else(|| DatabaseError::column_not_found(column_name.clone()))?;
                projection.push(ScalarExpression::Alias {
                    expr: Box::new(ScalarExpression::column_expr(
                        input_schema[position],
                        position,
                    )),
                    alias: AliasType::Name(arena.column(column).name().to_string()),
                });
            }
            projection
        }
    };
    input_plan = binder.bind_project(input_plan, projection, arena)?;

    binder.bind_insert_query(table_name, input_plan, overwrite)
}

fn bind_orm_insert_model<'bind, 'parent, 'arena, T, A, M>(
    binder: &mut Binder<'bind, 'parent, T, A>,
    params: Vec<(&'static str, DataValue)>,
    arena: &mut PlanArena<'arena>,
) -> Result<LogicalPlan, DatabaseError>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
    M: Model,
{
    let table_name: TableName = M::table_name().into();
    let source = binder
        .context
        .source_and_bind(table_name.clone(), None, None, false)?
        .ok_or(DatabaseError::TableNotFound)?;
    let params = params.into_iter().collect::<BTreeMap<_, _>>();
    let mut schema_ref = Vec::with_capacity(M::fields().len());
    let mut row = Vec::with_capacity(M::fields().len());

    for field in M::fields() {
        let column = source
            .column(field.column, arena)
            .ok_or_else(|| DatabaseError::column_not_found(field.column.to_string()))?;
        let column_catalog = arena.column(column);
        let value = params
            .get(field.placeholder)
            .ok_or_else(|| DatabaseError::parameter_not_found(field.placeholder))?
            .clone()
            .cast(column_catalog.datatype())?;
        value.check_len(column_catalog.datatype())?;
        if matches!(value, DataValue::Null) && !column_catalog.nullable() {
            return Err(DatabaseError::not_null_column(
                column_catalog.name().to_string(),
            ));
        }
        schema_ref.push(column);
        row.push(value);
    }

    binder.bind_insert_values(table_name, schema_ref, vec![row], false, true)
}

fn describe_text_value(value: Option<DataValue>) -> String {
    match value {
        Some(DataValue::Utf8 { value, .. }) => value,
        Some(other) => other.to_string(),
        None => String::new(),
    }
}

/// Trait implemented by ORM models.
///
/// In normal usage you should derive this trait with `#[derive(Model)]` rather
/// than implementing it by hand. The derive macro generates tuple mapping and
/// model metadata.
pub trait Model: Sized + FromQueryRow {
    /// Rust type used as the model primary key.
    ///
    /// This associated type lets APIs such as
    /// [`Database::get`](crate::orm::Database::get)
    /// infer the key type directly from the model, so callers only need to
    /// write `database.get::<User>(&id)`.
    type PrimaryKey: ToDataValue;

    /// Returns the backing table name for the model.
    fn table_name() -> &'static str;

    /// Returns metadata for every persisted field on the model.
    fn fields() -> &'static [OrmField];

    /// Returns persisted column catalogs for the model.
    ///
    /// `#[derive(Model)]` generates this automatically. Manual implementations
    /// can override it to opt into [`Database::migrate`](crate::orm::Database::migrate).
    fn columns() -> &'static [ColumnCatalog] {
        &[]
    }

    /// Returns secondary indexes declared by the model.
    fn indexes() -> &'static [(&'static str, &'static [&'static str], bool)] {
        &[]
    }

    /// Converts the model into named query parameters.
    fn params(&self) -> Vec<(&'static str, DataValue)>;

    /// Returns a reference to the current primary-key value.
    fn primary_key(&self) -> &Self::PrimaryKey;

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
/// code, but it also powers scalar projections decoded from binder-backed ORM plans.
///
/// Built-in scalar types already implement this trait, so most users only need
/// to pick the target type when decoding:
///
/// ```rust,ignore
/// let ids = database
///     .bind(|ctx| ctx.from::<User>()?.project_scalar(User::id()))?
///     .project_value::<i32>();
/// # Ok::<(), kite_sql::errors::DatabaseError>(())
/// ```
pub trait FromDataValue: Sized {
    /// Returns the logical SQL type used for conversion, when one is required.
    fn logical_type() -> Option<LogicalType>;

    /// Converts a raw [`DataValue`] into `Self`.
    fn from_data_value(value: DataValue) -> Result<Self, DatabaseError>;
}

/// Conversion trait from a projected result tuple into a Rust value.
///
/// This is implemented for tuples such as `(i32, String)` by the ORM itself.
///
/// ```rust,ignore
/// let rows = database
///     .bind(|ctx| ctx.from::<User>()?.project_scalars((User::id(), User::name())))?
///     .project_tuple::<(i32, String)>();
/// # Ok::<(), kite_sql::errors::DatabaseError>(())
/// ```
pub trait FromQueryTuple: Sized {
    /// Decodes one projected tuple into `Self`.
    fn from_query_tuple(tuple: Tuple) -> Result<Self, DatabaseError>;
}

/// Conversion trait from a query result row into a Rust value.
///
/// `#[derive(Model)]` and `#[derive(Projection)]` generate this automatically.
/// Types that still implement the older `From<(&SchemaView, Tuple)>` mapping
/// are also accepted through a compatibility implementation.
pub trait FromQueryRow: Sized {
    /// Decodes one result row into `Self`.
    fn from_query_row(schema: &SchemaView<'_, '_>, tuple: Tuple) -> Result<Self, DatabaseError>;
}

impl<T> FromQueryRow for T
where
    T: for<'view, 'schema, 'arena> From<(&'view SchemaView<'schema, 'arena>, Tuple)>,
{
    fn from_query_row(schema: &SchemaView<'_, '_>, tuple: Tuple) -> Result<Self, DatabaseError> {
        Ok(T::from((schema, tuple)))
    }
}

/// Typed adapter over a [`ResultIter`] that yields projected values instead of raw tuples.
///
/// This adapts a raw ORM result iterator into scalar projected values.
///
/// ```rust,ignore
/// let mut ids = database
///     .bind(|ctx| ctx.from::<User>()?.project_scalar(User::id()))?
///     .project_value::<i32>();
///
/// let first = ids.next().transpose()?;
/// ids.done()?;
/// # let _ = first;
/// # Ok::<(), kite_sql::errors::DatabaseError>(())
/// ```
pub struct ProjectValueIter<I, T> {
    inner: I,
    _marker: PhantomData<T>,
}

/// Convenience adapters for raw result iterators produced by binder-backed ORM plans.
pub trait OrmQueryResultExt: ResultIter + Sized {
    fn project_value<T: FromDataValue>(self) -> ProjectValueIter<Self, T> {
        ProjectValueIter::new(self)
    }

    fn project_tuple<T: FromQueryTuple>(self) -> ProjectTupleIter<Self, T> {
        ProjectTupleIter::new(self)
    }
}

impl<I: ResultIter> OrmQueryResultExt for I {}

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

    /// Finishes the underlying raw iterator explicitly.
    ///
    /// This is useful when you stop iterating early and want to release the
    /// underlying result stream.
    pub fn done(self) -> Result<(), DatabaseError> {
        self.inner.done()
    }
}

/// Typed adapter over a [`ResultIter`] that yields projected tuples.
///
/// This adapts a raw ORM result iterator into tuple projected rows.
///
/// ```rust,ignore
/// let mut rows = database
///     .bind(|ctx| ctx.from::<User>()?.project_scalars((User::id(), User::name())))?
///     .project_tuple::<(i32, String)>();
///
/// let first = rows.next().transpose()?;
/// rows.done()?;
/// # let _ = first;
/// # Ok::<(), kite_sql::errors::DatabaseError>(())
/// ```
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

    /// Finishes the underlying raw iterator explicitly.
    ///
    /// This is useful when you stop iterating early and want to release the
    /// underlying result stream.
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
/// code. It is what allows model fields, filter values, and primary keys to be
/// passed into prepared ORM statements.
pub trait ToDataValue {
    /// Converts the value into a [`DataValue`].
    fn to_data_value(&self) -> DataValue;
}

/// Maps a Rust field type to the SQL column type used by ORM DDL helpers.
///
/// `#[derive(Model)]` relies on this trait to build `CREATE TABLE` statements.
/// Most built-in scalar types already implement it, and custom types can opt in
/// by implementing this trait together with [`FromDataValue`] and [`ToDataValue`].
///
/// This trait only affects ORM-generated DDL. Query decoding still goes through
/// [`FromDataValue`], and bound parameters still go through [`ToDataValue`].
pub trait ModelColumnType {
    /// Returns the core logical type used in ORM-generated DDL.
    fn logical_type() -> LogicalType;

    /// Whether this field type maps to a nullable SQL column.
    fn nullable() -> bool {
        false
    }
}

/// Marker trait for string-like model fields that support `#[model(varchar = N)]`
/// and `#[model(char = N)]`.
///
/// This is mainly used by the `Model` derive macro and usually does not need to
/// be implemented manually unless you are introducing a custom string wrapper
/// type.
pub trait StringType {}

/// Marker trait for decimal-like model fields that support precision/scale DDL attributes.
///
/// This is mainly used by the `Model` derive macro and usually does not need to
/// be implemented manually unless you are introducing a custom decimal wrapper
/// type.
pub trait DecimalType {}

#[doc(hidden)]
pub fn take_value_at<T: FromDataValue>(
    tuple: &mut Tuple,
    index: Option<usize>,
    field_name: &str,
) -> Result<T, DatabaseError> {
    let idx = index.ok_or_else(|| DatabaseError::ColumnNotFound {
        name: field_name.to_string(),
        span: None,
    })?;
    let value = tuple.values.get_mut(idx).ok_or(DatabaseError::MisMatch(
        "the query result schema",
        "the query result tuple",
    ))?;
    let value = std::mem::replace(value, DataValue::Null);
    let value = match T::logical_type() {
        Some(ty) => value.cast(&ty)?,
        None => value,
    };

    T::from_data_value(value)
}

macro_rules! impl_from_data_value_by_method {
    ($ty:ty, $method:ident) => {
        impl FromDataValue for $ty {
            fn logical_type() -> Option<LogicalType> {
                LogicalType::type_trans::<Self>()
            }

            fn from_data_value(value: DataValue) -> Result<Self, crate::errors::DatabaseError> {
                value
                    .$method()
                    .ok_or_else(|| crate::orm::invalid_from_data_value::<Self>(&value))
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
#[cfg(feature = "decimal")]
impl_from_data_value_by_method!(Decimal, decimal);

impl_to_data_value_by_clone!(bool, i8, i16, i32, i64, u8, u16, u32, u64, f32, f64, String);
#[cfg(feature = "decimal")]
impl_to_data_value_by_clone!(Decimal);

macro_rules! impl_model_column_type {
    ($logical_type:expr; $($ty:ty),+ $(,)?) => {
        $(
            impl ModelColumnType for $ty {
                fn logical_type() -> LogicalType {
                    $logical_type
                }
            }
        )+
    };
}

impl_model_column_type!(LogicalType::Boolean; bool);
impl_model_column_type!(LogicalType::Tinyint; i8);
impl_model_column_type!(LogicalType::Smallint; i16);
impl_model_column_type!(LogicalType::Integer; i32);
impl_model_column_type!(LogicalType::Bigint; i64);
impl_model_column_type!(LogicalType::UTinyint; u8);
impl_model_column_type!(LogicalType::USmallint; u16);
impl_model_column_type!(LogicalType::UInteger; u32);
impl_model_column_type!(LogicalType::UBigint; u64);
impl_model_column_type!(LogicalType::Float; f32);
impl_model_column_type!(LogicalType::Double; f64);
#[cfg(feature = "decimal")]
impl_model_column_type!(LogicalType::Decimal(None, None); Decimal);
impl_model_column_type!(LogicalType::Varchar(None, CharLengthUnits::Characters); String, Arc<str>);

impl StringType for String {}
impl StringType for Arc<str> {}
#[cfg(feature = "decimal")]
impl DecimalType for Decimal {}

#[cfg(feature = "time")]
mod chrono_orm {
    use super::{FromDataValue, ModelColumnType, ToDataValue};
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

    impl_from_data_value_by_method!(NaiveDate, date);
    impl_from_data_value_by_method!(NaiveDateTime, datetime);
    impl_from_data_value_by_method!(NaiveTime, time);

    impl_model_column_type!(LogicalType::Date; NaiveDate);
    impl_model_column_type!(LogicalType::DateTime; NaiveDateTime);
    impl_model_column_type!(LogicalType::Time(Some(0)); NaiveTime);

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
}

impl FromDataValue for String {
    fn logical_type() -> Option<LogicalType> {
        LogicalType::type_trans::<Self>()
    }

    fn from_data_value(value: DataValue) -> Result<Self, DatabaseError> {
        if let DataValue::Utf8 { value, .. } = value {
            Ok(value)
        } else {
            Err(invalid_from_data_value::<Self>(&value))
        }
    }
}

impl FromDataValue for Arc<str> {
    fn logical_type() -> Option<LogicalType> {
        Some(LogicalType::Varchar(None, CharLengthUnits::Characters))
    }

    fn from_data_value(value: DataValue) -> Result<Self, DatabaseError> {
        if let DataValue::Utf8 { value, .. } = value {
            Ok(value.into())
        } else {
            Err(invalid_from_data_value::<Self>(&value))
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

impl<T: FromDataValue> FromDataValue for Option<T> {
    fn logical_type() -> Option<LogicalType> {
        T::logical_type()
    }

    fn from_data_value(value: DataValue) -> Result<Self, DatabaseError> {
        if matches!(value, DataValue::Null) {
            Ok(None)
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
    fn logical_type() -> LogicalType {
        T::logical_type()
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

fn model_column_default(model: &ColumnCatalog) -> Result<Option<DataValue>, DatabaseError> {
    model.default_value()
}

fn catalog_column_default(column: &ColumnCatalog) -> Result<Option<DataValue>, DatabaseError> {
    column.default_value()
}

fn model_column_type_matches_catalog(model: &ColumnCatalog, column: &ColumnCatalog) -> bool {
    model.datatype() == column.datatype()
}

fn model_column_matches_catalog(
    model: &ColumnCatalog,
    column: &ColumnCatalog,
) -> Result<bool, DatabaseError> {
    Ok(model.desc().is_primary() == column.desc().is_primary()
        && model.desc().is_unique() == column.desc().is_unique()
        && model.nullable() == column.nullable()
        && model_column_type_matches_catalog(model, column)
        && model_column_default(model)? == catalog_column_default(column)?)
}

fn model_column_rename_compatible(
    model: &ColumnCatalog,
    column: &ColumnCatalog,
) -> Result<bool, DatabaseError> {
    Ok(model.desc().is_primary() == column.desc().is_primary()
        && model.desc().is_unique() == column.desc().is_unique()
        && model.nullable() == column.nullable()
        && model_column_type_matches_catalog(model, column)
        && model_column_default(model)? == catalog_column_default(column)?)
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
    T: FromQueryRow,
{
    Ok(match iter.next() {
        Some(tuple) => {
            let tuple = tuple?;
            Some(iter.schema(|schema| T::from_query_row(schema, tuple))?)
        }
        None => None,
    })
}

fn convert_projected_value<T: FromDataValue>(value: DataValue) -> Result<T, DatabaseError> {
    let value = match T::logical_type() {
        Some(ty) => value.cast(&ty)?,
        None => value,
    };

    T::from_data_value(value)
}

fn invalid_from_data_value<T>(value: &DataValue) -> DatabaseError {
    DatabaseError::InvalidValue(format!(
        "failed to convert {} value `{value}` into {}",
        value.logical_type(),
        std::any::type_name::<T>()
    ))
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

fn orm_analyze<E: BindSource, M: Model>(executor: E) -> Result<(), DatabaseError> {
    executor
        .execute(&[], |binder, arena| {
            binder.bind_analyze(M::table_name().into(), arena)
        })?
        .done()
}

fn orm_insert<E: BindSource, M: Model>(executor: E, model: &M) -> Result<(), DatabaseError> {
    let params = model.params();
    executor
        .execute(&[], |binder, arena| {
            bind_orm_insert_model::<_, _, M>(binder, params, arena)
        })?
        .done()
}

fn orm_get<E: BindSource, M: Model>(
    executor: E,
    key: &M::PrimaryKey,
) -> Result<Option<M>, DatabaseError> {
    let primary_key = M::primary_key_field();
    let key = key.to_data_value();
    extract_optional_model(bind_orm_context(executor, |ctx| {
        let plan: LogicalPlan = ctx
            .from::<M>()?
            .filter(|expr| {
                let column = expr.qualified_column(
                    M::table_name(),
                    Field::<M, ()>::new(M::table_name(), primary_key.column),
                )?;
                column.eq(expr.data_value(key))
            })?
            .finish()?;
        Ok(plan)
    })?)
}

fn orm_list<E: BindSource, M: Model>(executor: E) -> Result<OrmIter<E::Iter, M>, DatabaseError> {
    Ok(bind_orm_context(executor, |ctx| {
        let plan: LogicalPlan = ctx.from::<M>()?.finish()?;
        Ok(plan)
    })?
    .orm::<M>())
}
