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

macro_rules! with_query_bind_step {
    ($binder:expr, $step:expr, $body:block) => {{
        let current_step = $binder.context.step_now();
        $binder.context.step($step);
        let result = (|| -> Result<_, DatabaseError> { Ok($body) })();
        $binder.context.step(current_step);
        result
    }};
}

#[cfg(feature = "orm")]
pub(crate) use with_query_bind_step;

pub mod aggregate;
mod alter_table;
mod analyze;
#[cfg(feature = "copy")]
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
#[cfg(feature = "parser")]
mod parser;
mod select;
mod show_table;
mod show_view;
mod truncate;
mod update;
mod window;

#[cfg(feature = "parser")]
pub use parser::{command_type, CommandType, Statement};
#[cfg(feature = "orm")]
pub use select::{BindPlanFrom, BindPlanSelectList};
#[cfg(feature = "orm")]
pub(crate) use select::{JoinConstraintInput, TableAliasInput};
use std::collections::{BTreeMap, HashMap};

use crate::catalog::view::View;
use crate::catalog::{ColumnRef, TableCatalog, TableName};
use crate::db::{ScalaFunctions, TableFunctions};
use crate::errors::DatabaseError;
use crate::expression::{ScalarExpression, TypeCast};
use crate::planner::operator::join::JoinType;
use crate::planner::operator::mark_apply::MarkApplyQuantifier;
use crate::planner::{ExprRef, LogicalPlan, PlanArena, PlanRef, ScalarQueryRef};
use crate::storage::{TableCache, Transaction, ViewCache};
use crate::types::tuple::Schema;
use crate::types::LogicalType;

pub enum InputRefType {
    AggCall,
    GroupBy,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum SetOperatorKind {
    Union,
    Except,
    Intersect,
}

// Tips: only query now!
#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
pub enum QueryBindStep {
    From,
    Join,
    Where,
    Agg,
    Having,
    Window,
    Distinct,
    Sort,
    Project,
    Limit,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum SubQueryType {
    ExistsSubQuery {
        plan: LogicalPlan,
        correlated: bool,
        output_column: ColumnRef,
    },
    QuantifiedSubQuery {
        quantifier: MarkApplyQuantifier,
        negated: bool,
        plan: LogicalPlan,
        correlated: bool,
        output_column: ColumnRef,
        predicate: ScalarExpression,
    },
}

#[derive(Debug, Clone)]
pub enum Source<'a> {
    Table(&'a TableCatalog),
    View(&'a View),
    Schema(Schema),
}

#[derive(Debug, Clone)]
pub struct BoundSource<'a> {
    pub(crate) table_name: TableName,
    pub(crate) alias: Option<TableName>,
    pub(crate) join_type: Option<JoinType>,
    pub(crate) source: Source<'a>,
    // The first source with this visible name owns qualified name resolution.
    qualified_visible: bool,
}

// Sorted by (name, visible source name, source index, position). A source entry
// has no column; names are borrowed from existing metadata instead of copied.
#[derive(Debug, Clone, Copy)]
struct NameBinding {
    source: usize,
    column: Option<ColumnRef>,
    position: usize,
}

impl NameBinding {
    fn key<'n>(
        &self,
        sources: &'n [BoundSource<'_>],
        arena: &'n PlanArena<'_>,
    ) -> (&'n str, Option<&'n str>) {
        let source_name = sources[self.source].visible_name().as_ref();
        match self.column {
            Some(column) => (arena.column(column).name(), Some(source_name)),
            None => (source_name, None),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CteBinding {
    pub(crate) table_name: TableName,
    pub(crate) depth: usize,
    pub(crate) plan_ref: PlanRef,
}

#[derive(Clone, Copy)]
pub(crate) struct CteCheckpoint {
    len: usize,
    depth: usize,
}

impl BoundSource<'_> {
    pub(crate) fn same_binding(
        &self,
        table_name: &TableName,
        alias: Option<&TableName>,
        join_type: Option<JoinType>,
    ) -> bool {
        self.table_name == *table_name
            && self.alias.as_ref() == alias
            && self.join_type == join_type
    }

    pub(crate) fn visible_name(&self) -> &TableName {
        self.alias.as_ref().unwrap_or(&self.table_name)
    }
}

#[derive(Clone)]
pub(crate) struct UsingColumn {
    join_type: JoinType,
    left_column: ColumnRef,
    left_position: usize,
    right_column: ColumnRef,
    right_position: usize,
}

impl UsingColumn {
    fn new(
        join_type: JoinType,
        left_column: ColumnRef,
        left_position: usize,
        right_column: ColumnRef,
        right_position: usize,
    ) -> Self {
        Self {
            join_type,
            left_column,
            left_position,
            right_column,
            right_position,
        }
    }

    fn left_expr(&self) -> ScalarExpression {
        ScalarExpression::column_expr(self.left_column, self.left_position)
    }

    fn right_expr(&self) -> ScalarExpression {
        ScalarExpression::column_expr(self.right_column, self.right_position)
    }

    pub(crate) fn visible_expr(
        &self,
        arena: &mut PlanArena,
    ) -> Result<ScalarExpression, DatabaseError> {
        match self.join_type {
            JoinType::RightOuter => Ok(self.right_expr()),
            JoinType::Full => {
                let left_expr = arena.alloc_expression(self.left_expr());
                let right_expr = arena.alloc_expression(self.right_expr());
                let left_ty = left_expr.return_type(arena);
                let right_ty = right_expr.return_type(arena);
                let ty = LogicalType::max_logical_type(&left_ty, &right_ty)?.into_owned();

                Ok(ScalarExpression::Coalesce {
                    exprs: vec![left_expr, right_expr],
                    ty,
                })
            }
            JoinType::Inner | JoinType::LeftOuter | JoinType::Cross => Ok(self.left_expr()),
        }
    }

    pub(crate) fn hides_column(&self, column: &ColumnRef, arena: &PlanArena) -> bool {
        let hidden_column = if self.join_type.is_right() {
            &self.left_column
        } else {
            &self.right_column
        };
        arena.same_column(*hidden_column, *column)
    }
}

pub(crate) struct BoundScalarQuery {
    value: ExprRef,
    step: QueryBindStep,
    plan: LogicalPlan,
    param_bindings: Vec<(ScalarQueryRef, ExprRef)>,
}

pub struct BinderContext<'a, T: Transaction> {
    pub(crate) scala_functions: &'a ScalaFunctions,
    pub(crate) table_functions: &'a TableFunctions,
    pub(crate) table_cache: &'a TableCache,
    pub(crate) view_cache: &'a ViewCache,
    pub(crate) transaction: &'a T,
    // Tips: retain binding order so wildcard expansion and position derivation
    // follow FROM/JOIN order directly.
    pub(crate) bind_table: Vec<BoundSource<'a>>,
    name_bindings: Vec<NameBinding>,
    // Sources before this index were inherited from an enclosing query by `fork`
    // and do not share the current FROM namespace.
    source_scope_start: usize,
    ctes: Vec<CteBinding>,
    pub(crate) cte_depth: usize,
    // alias
    expr_aliases: BTreeMap<(Option<String>, String), ExprRef>,
    table_aliases: HashMap<TableName, TableName>,
    // agg
    group_by_exprs: Vec<ExprRef>,
    pub(crate) agg_calls: Vec<ExprRef>,
    // join
    using: HashMap<String, UsingColumn>,
    has_join_outer_ref: bool,
    // subquery
    sub_queries: Vec<SubQueryType>,
    scalar_queries: Vec<BoundScalarQuery>,
    capture_scalar_outer: bool,
    scalar_outer_bindings: Vec<(ScalarQueryRef, ExprRef)>,

    bind_step: QueryBindStep,
    pub(crate) allow_default: bool,
}

impl Source<'_> {
    pub(crate) fn column(&self, name: &str, arena: &PlanArena) -> Option<ColumnRef> {
        match self {
            Source::Table(table) => table.get_column_by_name(name),
            Source::View(view) => view
                .schema
                .iter()
                .find(|column| arena.column(**column).name() == name)
                .copied(),
            Source::Schema(schema_ref) => schema_ref
                .iter()
                .find(|column| arena.column(**column).name() == name)
                .copied(),
        }
    }

    pub(crate) fn schema(&self) -> &[ColumnRef] {
        match self {
            Source::Table(table) => table.columns().as_slice(),
            Source::View(view) => &view.schema,
            Source::Schema(schema_ref) => schema_ref,
        }
    }

    pub(crate) fn schema_len(&self) -> usize {
        match self {
            Source::Table(table) => table.columns_len(),
            Source::View(view) => view.schema.len(),
            Source::Schema(schema_ref) => schema_ref.len(),
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
    ) -> Self {
        BinderContext {
            scala_functions,
            table_functions,
            table_cache,
            view_cache,
            transaction,
            bind_table: Default::default(),
            name_bindings: Vec::new(),
            source_scope_start: 0,
            ctes: Default::default(),
            cte_depth: 0,
            expr_aliases: Default::default(),
            table_aliases: Default::default(),
            group_by_exprs: vec![],
            agg_calls: Default::default(),
            using: Default::default(),
            bind_step: QueryBindStep::From,
            sub_queries: Default::default(),
            scalar_queries: Vec::new(),
            capture_scalar_outer: false,
            scalar_outer_bindings: Vec::new(),
            has_join_outer_ref: false,
            allow_default: false,
        }
    }

    /// Creates a child context that starts with the current binding scope.
    ///
    /// This is used for nested query bodies that should be able to resolve the
    /// same local sources and aliases while keeping their mutations isolated.
    pub(crate) fn fork(&self) -> Self {
        BinderContext {
            scala_functions: self.scala_functions,
            table_functions: self.table_functions,
            table_cache: self.table_cache,
            view_cache: self.view_cache,
            transaction: self.transaction,
            bind_table: self.bind_table.clone(),
            name_bindings: self.name_bindings.clone(),
            source_scope_start: self.bind_table.len(),
            ctes: self.ctes.clone(),
            cte_depth: self.cte_depth,
            expr_aliases: self.expr_aliases.clone(),
            table_aliases: self.table_aliases.clone(),
            group_by_exprs: self.group_by_exprs.clone(),
            agg_calls: self.agg_calls.clone(),
            using: self.using.clone(),
            bind_step: self.bind_step,
            sub_queries: Default::default(),
            scalar_queries: Vec::new(),
            capture_scalar_outer: false,
            scalar_outer_bindings: Vec::new(),
            has_join_outer_ref: false,
            allow_default: self.allow_default,
        }
    }

    /// Creates a child context with shared catalogs but without local bindings.
    ///
    /// This is used while binding an independent input, such as the right side
    /// of a join, before merging its newly bound sources into the parent scope.
    pub(crate) fn fork_empty(&self) -> Self {
        let mut context = BinderContext::new(
            self.table_cache,
            self.view_cache,
            self.transaction,
            self.scala_functions,
            self.table_functions,
        );
        context.ctes = self.ctes.clone();
        context.cte_depth = self.cte_depth;
        context
    }

    pub(crate) fn cte(&self, table_name: &TableName) -> Option<&CteBinding> {
        self.ctes
            .iter()
            .rev()
            .find(|cte| cte.table_name == *table_name)
    }

    pub(crate) fn add_cte(&mut self, cte: CteBinding) -> Result<(), DatabaseError> {
        if self
            .ctes
            .iter()
            .rev()
            .take_while(|binding| binding.depth == cte.depth)
            .any(|binding| binding.table_name == cte.table_name)
        {
            return Err(DatabaseError::UnsupportedStmt(format!(
                "duplicate CTE name: {}",
                cte.table_name
            )));
        }
        self.ctes.push(cte);
        Ok(())
    }

    pub(crate) fn cte_checkpoint(&self) -> CteCheckpoint {
        CteCheckpoint {
            len: self.ctes.len(),
            depth: self.cte_depth,
        }
    }

    pub(crate) fn restore_ctes(&mut self, checkpoint: CteCheckpoint) {
        self.ctes.truncate(checkpoint.len);
        self.cte_depth = checkpoint.depth;
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
        self.sub_queries.push(sub_query);
    }

    pub fn mark_join_outer_ref(&mut self) {
        self.has_join_outer_ref = true;
    }

    pub fn has_join_outer_ref(&self) -> bool {
        self.has_join_outer_ref
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
            self.transaction.view(
                self.table_cache,
                self.view_cache,
                self.scala_functions,
                self.table_functions,
                real_name.clone(),
            )
        } else {
            self.transaction.view(
                self.table_cache,
                self.view_cache,
                self.scala_functions,
                self.table_functions,
                view_name.clone(),
            )
        }
    }

    pub fn source_and_bind(
        &mut self,
        table_name: TableName,
        alias: Option<&TableName>,
        join_type: Option<JoinType>,
        only_table: bool,
        arena: &PlanArena,
    ) -> Result<Option<Source<'_>>, DatabaseError> {
        if let Some(source) = self.bound_source(&table_name, alias, join_type) {
            if !only_table || matches!(source, Source::Table(_)) {
                return Ok(Some(source.clone()));
            }
        }
        let source = if only_table {
            let name = self.table_aliases.get(&table_name).unwrap_or(&table_name);
            self.transaction
                .table(self.table_cache, name.clone())?
                .map(Source::Table)
        } else {
            self.source(&table_name)?
        };
        if let Some(source) = &source {
            self.add_bound_source(
                table_name.clone(),
                alias.cloned(),
                join_type,
                source.clone(),
                arena,
            );
        }
        Ok(source)
    }

    pub fn source(&self, table_name: &TableName) -> Result<Option<Source<'a>>, DatabaseError> {
        let mut source = if let Some(real_name) = self.table_aliases.get(table_name.as_ref()) {
            self.transaction.table(self.table_cache, real_name.clone())
        } else {
            self.transaction.table(self.table_cache, table_name.clone())
        }?
        .map(Source::Table);

        if source.is_none() {
            source = if let Some(real_name) = self.table_aliases.get(table_name.as_ref()) {
                self.transaction.view(
                    self.table_cache,
                    self.view_cache,
                    self.scala_functions,
                    self.table_functions,
                    real_name.clone(),
                )
            } else {
                self.transaction.view(
                    self.table_cache,
                    self.view_cache,
                    self.scala_functions,
                    self.table_functions,
                    table_name.clone(),
                )
            }?
            .map(Source::View);
        }

        Ok(source)
    }

    /// Rejects a FROM source whose visible name is already bound in the current
    /// FROM namespace. Two such sources would also expose identical column
    /// identities, which later stages cannot tell apart.
    pub(crate) fn ensure_unique_source_name(&self, name: &TableName) -> Result<(), DatabaseError> {
        if self.bind_table[self.source_scope_start..]
            .iter()
            .any(|bound_source| bound_source.visible_name() == name)
        {
            return Err(DatabaseError::DuplicateTableName(name.to_string()));
        }
        Ok(())
    }

    fn bound_source(
        &self,
        table_name: &TableName,
        alias: Option<&TableName>,
        join_type: Option<JoinType>,
    ) -> Option<&Source<'a>> {
        self.bind_table
            .iter()
            .find(|bound| bound.same_binding(table_name, alias, join_type))
            .map(|bound| &bound.source)
    }

    pub fn add_bound_source(
        &mut self,
        table_name: TableName,
        alias: Option<TableName>,
        join_type: Option<JoinType>,
        source: Source<'a>,
        arena: &PlanArena,
    ) {
        if self
            .bound_source(&table_name, alias.as_ref(), join_type)
            .is_some()
        {
            return;
        }
        let offset = self
            .bind_table
            .iter()
            .map(|bound| bound.source.schema_len())
            .sum::<usize>();
        let index = self.bind_table.len();
        let qualified_visible = self
            .name_binding(alias.as_ref().unwrap_or(&table_name), None, arena)
            .is_none();
        self.bind_table.push(BoundSource {
            table_name,
            alias,
            join_type,
            source,
            qualified_visible,
        });
        self.name_bindings
            .reserve(self.bind_table[index].source.schema_len() + 1);
        self.insert_name_binding(
            NameBinding {
                source: index,
                column: None,
                position: offset,
            },
            arena,
        );
        for position in 0..self.bind_table[index].source.schema_len() {
            let column = self.bind_table[index].source.schema()[position];
            self.insert_name_binding(
                NameBinding {
                    source: index,
                    column: Some(column),
                    position: offset + position,
                },
                arena,
            );
        }
    }

    fn insert_name_binding(&mut self, binding: NameBinding, arena: &PlanArena) {
        let key = binding.key(&self.bind_table, arena);
        let index = self.name_bindings.partition_point(|existing| {
            existing
                .key(&self.bind_table, arena)
                .cmp(&key)
                .then(existing.source.cmp(&binding.source))
                .then(existing.position.cmp(&binding.position))
                .is_lt()
        });
        self.name_bindings.insert(index, binding);
    }

    fn name_binding(
        &self,
        name: &str,
        source: Option<&str>,
        arena: &PlanArena,
    ) -> Option<NameBinding> {
        let key = (name, source);
        let index = self
            .name_bindings
            .partition_point(|binding| binding.key(&self.bind_table, arena) < key);
        self.name_bindings
            .get(index)
            .filter(|binding| {
                binding.key(&self.bind_table, arena) == key
                    && (source.is_none() || self.bind_table[binding.source].qualified_visible)
            })
            .copied()
    }

    fn resolve_column(
        &self,
        name: &str,
        source: Option<&str>,
        arena: &PlanArena,
    ) -> Result<Option<ScalarExpression>, DatabaseError> {
        // Fast path for single source
        if let [bound] = self.bind_table.as_slice() {
            if let Some(source) = source {
                if bound.visible_name().as_ref() != source {
                    return Err(DatabaseError::invalid_table(source));
                }
            }
            return Ok(bound
                .source
                .schema()
                .iter()
                .enumerate()
                .find(|(_, column)| arena.column(**column).name() == name)
                .map(|(position, column)| ScalarExpression::column_expr(*column, position)));
        }
        let binding = if let Some(source) = source {
            let binding = self.name_binding(name, Some(source), arena);
            if binding.is_none() && self.name_binding(source, None, arena).is_none() {
                return Err(DatabaseError::invalid_table(source));
            }
            binding
        } else {
            let start = self
                .name_bindings
                .partition_point(|binding| binding.key(&self.bind_table, arena).0 < name);
            let mut columns = self.name_bindings[start..]
                .iter()
                .take_while(|binding| binding.key(&self.bind_table, arena).0 == name)
                .filter(|binding| binding.column.is_some());
            let Some(first) = columns.next() else {
                return Ok(None);
            };
            if columns.any(|binding| binding.source != first.source) {
                return Err(DatabaseError::AmbiguousColumn(name.to_string()));
            }
            Some(*first)
        };
        Ok(binding.and_then(|binding| {
            binding
                .column
                .map(|column| ScalarExpression::column_expr(column, binding.position))
        }))
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
        name: String,
        join_type: JoinType,
        left_column: &ColumnRef,
        left_position: usize,
        right_column: &ColumnRef,
        right_position: usize,
    ) -> Result<(), DatabaseError> {
        if self.using.contains_key(&name) {
            return Err(DatabaseError::UnsupportedStmt(format!(
                "duplicate `USING({name})` across joins is not supported"
            )));
        }
        let using_column = UsingColumn::new(
            join_type,
            *left_column,
            left_position,
            *right_column,
            right_position,
        );
        self.using.insert(name, using_column);
        Ok(())
    }

    pub fn add_alias(&mut self, alias_table: Option<String>, alias_column: String, expr: ExprRef) {
        self.expr_aliases.insert((alias_table, alias_column), expr);
    }

    pub fn add_table_alias(&mut self, alias: TableName, table: TableName) {
        self.table_aliases.insert(alias.clone(), table.clone());
    }

    pub fn has_agg_call(&self, expr: &ExprRef) -> bool {
        self.group_by_exprs.contains(expr)
    }
}

pub struct Binder<'a, 'parent, T: Transaction, A: AsRef<[(usize, LogicalType)]>> {
    pub(crate) context: BinderContext<'a, T>,
    pub(crate) args: &'a A,
    pub(crate) force_spill: bool,
    pub(crate) force_nested_loop: bool,
    with_pk: Option<TableName>,
    pub(crate) parent: Option<&'parent BinderContext<'a, T>>,
}

impl<'a, 'parent, T: Transaction, A: AsRef<[(usize, LogicalType)]>> Binder<'a, 'parent, T, A> {
    pub fn new(
        context: BinderContext<'a, T>,
        args: &'a A,
        parent: Option<&'parent BinderContext<'a, T>>,
    ) -> Self {
        Binder {
            context,
            args,
            force_spill: false,
            force_nested_loop: false,
            with_pk: None,
            parent,
        }
    }

    pub fn with_pk(&mut self, table_name: TableName) {
        self.with_pk = Some(table_name);
    }

    pub fn clear_with_pk(&mut self) {
        self.with_pk = None;
    }

    pub fn is_scan_with_pk(&self, table_name: &TableName) -> bool {
        if let Some(with_pk_table) = self.with_pk.as_ref() {
            return with_pk_table == table_name;
        }
        false
    }

    pub(crate) fn extend(
        &mut self,
        context: BinderContext<'a, T>,
        arena: &PlanArena,
    ) -> Result<(), DatabaseError> {
        for bound_source in context.bind_table {
            self.context
                .ensure_unique_source_name(bound_source.visible_name())?;
            self.context.add_bound_source(
                bound_source.table_name,
                bound_source.alias,
                bound_source.join_type,
                bound_source.source,
                arena,
            );
        }
        for (key, expr) in context.expr_aliases {
            self.context.expr_aliases.insert(key, expr);
        }
        for (key, table_name) in context.table_aliases {
            self.context.table_aliases.insert(key, table_name);
        }
        Ok(())
    }
}

pub(crate) fn is_valid_identifier(s: &str) -> bool {
    s.chars().all(|c| c.is_alphanumeric() || c == '_')
        && !s.chars().next().unwrap_or_default().is_numeric()
        && !s.chars().all(|c| c == '_')
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod test {
    use crate::binder::{is_valid_identifier, Binder, BinderContext};
    use crate::catalog::{ColumnCatalog, ColumnDesc, TableCatalog};
    use crate::errors::DatabaseError;
    use crate::planner::{LogicalPlan, PlanArena, TableArenaCell};
    use crate::storage::rocksdb::RocksStorage;
    use crate::storage::{table_codec::TableCodec, Storage, TableCache, Transaction, ViewCache};
    use crate::types::ColumnId;
    use crate::types::LogicalType::Integer;
    use std::path::PathBuf;
    use tempfile::TempDir;

    pub(crate) struct TableState<S: Storage> {
        pub(crate) table: TableCatalog,
        pub(crate) table_cache: TableCache,
        pub(crate) view_cache: ViewCache,
        pub(crate) table_arena: TableArenaCell,
        pub(crate) storage: S,
    }

    impl<S: Storage> TableState<S> {
        pub(crate) fn plan<T: AsRef<str>>(&self, sql: T) -> Result<LogicalPlan, DatabaseError> {
            let mut plan_arena = PlanArena::new(&self.table_arena);
            self.plan_with_arena(sql, &mut plan_arena)
        }

        pub(crate) fn plan_with_arena<T: AsRef<str>>(
            &self,
            sql: T,
            plan_arena: &mut PlanArena,
        ) -> Result<LogicalPlan, DatabaseError> {
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
                ),
                &[],
                None,
            );
            let stmt = crate::parser::parse_sql(sql)?;
            let stmt = stmt.into_iter().next().unwrap();

            binder.bind(&stmt, plan_arena)
        }

        pub(crate) fn column_id_by_name(&self, name: &str) -> &ColumnId {
            self.table.get_column_id_by_name(name).unwrap()
        }
    }

    pub(crate) fn build_t1_table() -> Result<TableState<RocksStorage>, DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut table_cache = crate::storage::TableCache::default();
        let view_cache = crate::storage::ViewCache::default();
        let table_arena = TableArenaCell::default();
        let storage = build_test_catalog(&mut table_cache, temp_dir.path(), &table_arena)?;
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
            table_arena,
            storage,
        })
    }

    pub(crate) fn build_test_catalog(
        table_cache: &mut TableCache,
        path: impl Into<PathBuf> + Send,
        table_arena: &TableArenaCell,
    ) -> Result<RocksStorage, DatabaseError> {
        let storage = RocksStorage::new(path)?;
        let mut transaction = storage.transaction()?;
        let mut table_codec = TableCodec::default();

        let mut plan_arena = PlanArena::new(table_arena);
        if let Some(table) = transaction.create_table(
            &mut table_codec,
            &mut plan_arena,
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
        )? {
            let table = table.transplant_to_table_arena(&plan_arena)?;
            table_cache.insert(table.name().clone(), table);
        }

        let mut plan_arena = PlanArena::new(table_arena);
        if let Some(table) = transaction.create_table(
            &mut table_codec,
            &mut plan_arena,
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
        )? {
            let table = table.transplant_to_table_arena(&plan_arena)?;
            table_cache.insert(table.name().clone(), table);
        }
        transaction.commit()?;

        Ok(storage)
    }

    #[test]
    fn sorted_bindings_preserve_names_positions_and_scope_isolation() -> Result<(), DatabaseError> {
        use crate::binder::Source;
        use crate::expression::ScalarExpression;

        let state = build_t1_table()?;
        let arena = PlanArena::new(&state.table_arena);
        let transaction = state.storage.transaction()?;
        let scala_functions = Default::default();
        let table_functions = Default::default();
        let mut context = BinderContext::new(
            &state.table_cache,
            &state.view_cache,
            &transaction,
            &scala_functions,
            &table_functions,
        );
        let c1 = state.table.get_column_by_name("c1").unwrap();
        let c2 = state.table.get_column_by_name("c2").unwrap();
        // A source name may equal a column name; duplicates within that source
        // retain their first position and are not ambiguous.
        context.add_bound_source(
            "a".into(),
            Some("c1".into()),
            None,
            Source::Schema(vec![c1, c1]),
            &arena,
        );
        assert!(matches!(
            context.resolve_column("c1", None, &arena)?,
            Some(ScalarExpression::ColumnRef { column, position: 0 }) if column == c1
        ));
        context.add_bound_source("b".into(), None, None, Source::Schema(vec![c2]), &arena);
        assert!(matches!(
            context.resolve_column("c2", Some("b"), &arena)?,
            Some(ScalarExpression::ColumnRef { column, position: 2 }) if column == c2
        ));
        // Re-registering a binding preserves its original columns and positions.
        context.add_bound_source("b".into(), None, None, Source::Schema(vec![c1]), &arena);
        assert!(matches!(
            context.resolve_column("c2", Some("b"), &arena)?,
            Some(ScalarExpression::ColumnRef { column, position: 2 }) if column == c2
        ));
        assert!(matches!(
            context.resolve_column("c1", Some("c1"), &arena)?,
            Some(ScalarExpression::ColumnRef { position: 0, .. })
        ));
        context.add_bound_source(
            "a".into(),
            Some("c1".into()),
            None,
            Source::Schema(vec![c2]),
            &arena,
        );
        assert_eq!(context.name_binding("b", None, &arena).unwrap().position, 2);
        assert!(matches!(
            context.resolve_column("c1", None, &arena)?,
            Some(ScalarExpression::ColumnRef { column, position: 0 }) if column == c1
        ));
        assert!(matches!(
            context.resolve_column("c1", Some("a"), &arena),
            Err(DatabaseError::InvalidTable { .. })
        ));
        let mut fork = context.fork();
        fork.add_bound_source("c".into(), None, None, Source::Schema(vec![c1]), &arena);
        assert!(matches!(
            fork.resolve_column("c1", None, &arena),
            Err(DatabaseError::AmbiguousColumn(_))
        ));
        assert!(context.resolve_column("c1", None, &arena)?.is_some());
        assert!(context
            .fork_empty()
            .resolve_column("c1", None, &arena)?
            .is_none());
        Ok(())
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
// GRCOV_EXCL_STOP
