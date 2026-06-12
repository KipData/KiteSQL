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

use crate::catalog::{ColumnCatalog, ColumnRef, TableName};
use crate::types::index::{IndexMeta, IndexMetaRef};
use crate::types::tuple::Schema;
use std::cell::UnsafeCell;
use std::collections::HashSet;
use std::fmt;

pub struct TableArena {
    dummy_columns: [ColumnCatalog; DUMMY_COLUMN_COUNT],
    columns: Vec<TableArenaColumn>,
    indexes: Vec<TableArenaIndex>,
    version: usize,
}

struct TableArenaColumn {
    catalog: ColumnCatalog,
    live: bool,
}

struct TableArenaIndex {
    meta: IndexMeta,
    live: bool,
}

pub struct TableArenaCell {
    value: UnsafeCell<TableArena>,
}

// SAFETY: table arena mutation is only exposed through database APIs that require
// `&mut Database`; read execution only borrows already-loaded metadata.
unsafe impl Send for TableArenaCell {}
unsafe impl Sync for TableArenaCell {}

#[derive(Debug)]
pub struct PlanArena<'a> {
    table_arena: &'a TableArenaCell,
    #[cfg(debug_assertions)]
    table_arena_version: usize,
    columns: Vec<ColumnCatalog>,
    indexes: Vec<IndexMeta>,
}

pub trait MetaArena {
    fn alloc_column(&mut self, column: ColumnCatalog) -> ColumnRef;

    fn alloc_index(&mut self, index: IndexMeta) -> IndexMetaRef;

    fn alloc_columns<I>(&mut self, columns: I) -> Schema
    where
        Self: Sized,
        I: IntoIterator<Item = ColumnCatalog>,
    {
        columns
            .into_iter()
            .map(|column| self.alloc_column(column))
            .collect()
    }

    fn column(&self, column: ColumnRef) -> &ColumnCatalog;

    fn index(&self, index: IndexMetaRef) -> &IndexMeta;

    fn find_column(&self, column: &ColumnCatalog) -> Option<ColumnRef>;

    fn find_index(&self, index: &IndexMeta) -> Option<IndexMetaRef>;
}

const DUMMY_COLUMN_NAMES: [&str; DUMMY_COLUMN_COUNT] = [
    "TABLE",
    "VIEW",
    "PLAN",
    "FIELD",
    "TYPE",
    "LEN",
    "NULL",
    "Key",
    "DEFAULT",
    "INSERTED",
    "UPDATED",
    "DELETED",
    "STATISTICS_META_PATH",
    "ADD COLUMN SUCCESS",
    "CHANGE COLUMN SUCCESS",
    "DROP COLUMN SUCCESS",
    "CREATE TABLE SUCCESS",
    "CREATE INDEX SUCCESS",
    "CREATE VIEW SUCCESS",
    "DROP TABLE SUCCESS",
    "DROP VIEW SUCCESS",
    "DROP INDEX SUCCESS",
    "TRUNCATE TABLE SUCCESS",
    "COPY FROM SOURCE",
    "COPY TO TARGET",
];
const DUMMY_COLUMN_COUNT: usize = 25;
const DUMMY_COLUMN_BASE: usize = usize::MAX - DUMMY_COLUMN_COUNT + 1;

impl TableArenaCell {
    pub(crate) fn new(value: TableArena) -> Self {
        Self {
            value: UnsafeCell::new(value),
        }
    }

    pub(crate) fn borrow(&self) -> &TableArena {
        unsafe { &*self.value.get() }
    }

    pub(crate) fn borrow_mut(&self) -> &mut TableArena {
        unsafe { &mut *self.value.get() }
    }
}

impl Default for TableArenaCell {
    fn default() -> Self {
        Self::new(TableArena::default())
    }
}

impl Default for TableArena {
    fn default() -> Self {
        Self {
            dummy_columns: std::array::from_fn(|i| {
                ColumnCatalog::new_dummy(DUMMY_COLUMN_NAMES[i].to_string())
            }),
            columns: Vec::new(),
            indexes: Vec::new(),
            version: 0,
        }
    }
}

impl fmt::Debug for TableArenaCell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.borrow().fmt(f)
    }
}

impl TableArena {
    pub(crate) fn alloc_dummy(&self, name: &str) -> ColumnRef {
        DUMMY_COLUMN_NAMES
            .iter()
            .position(|dummy_name| *dummy_name == name)
            .map(|index| ColumnRef::new(DUMMY_COLUMN_BASE + index))
            .unwrap_or_else(|| panic!("unknown dummy column: {name}"))
    }

    pub fn alloc_table_column(
        &mut self,
        table_name: TableName,
        mut column: ColumnCatalog,
    ) -> ColumnRef {
        column.set_ref_table(table_name, ulid::Ulid::new(), false);
        self.alloc_column(column)
    }

    pub(crate) fn alloc_column(&mut self, column: ColumnCatalog) -> ColumnRef {
        <Self as MetaArena>::alloc_column(self, column)
    }

    pub fn alloc_index(&mut self, index: IndexMeta) -> IndexMetaRef {
        <Self as MetaArena>::alloc_index(self, index)
    }

    pub(crate) fn column(&self, column: ColumnRef) -> &ColumnCatalog {
        <Self as MetaArena>::column(self, column)
    }

    pub(crate) fn index(&self, index: IndexMetaRef) -> &IndexMeta {
        <Self as MetaArena>::index(self, index)
    }

    fn dummy_column(&self, column: ColumnRef) -> Option<&ColumnCatalog> {
        column
            .pos()
            .checked_sub(DUMMY_COLUMN_BASE)
            .and_then(|index| self.dummy_columns.get(index))
    }

    pub(crate) fn columns_len(&self) -> usize {
        self.columns.len()
    }

    pub(crate) fn indexes_len(&self) -> usize {
        self.indexes.len()
    }

    pub(crate) fn live_columns_len(&self) -> usize {
        self.columns.iter().filter(|column| column.live).count()
    }

    pub(crate) fn version(&self) -> usize {
        self.version
    }

    pub(crate) fn recycle_unreferenced_positions(&mut self, live_columns: HashSet<usize>) {
        let mut changed = false;

        for (pos, column) in self.columns.iter_mut().enumerate() {
            let live = live_columns.contains(&pos);
            if column.live != live {
                column.live = live;
                changed = true;
            }
        }

        if changed {
            self.increment_version();
        }
    }

    fn increment_version(&mut self) {
        self.version = self
            .version
            .checked_add(1)
            .expect("TableArena version overflow");
    }
}

impl fmt::Debug for TableArena {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableArena")
            .field("columns_len", &self.live_columns_len())
            .field("slots_len", &self.columns_len())
            .field("indexes_len", &self.indexes_len())
            .field("version", &self.version())
            .finish()
    }
}

impl MetaArena for TableArena {
    fn alloc_column(&mut self, column: ColumnCatalog) -> ColumnRef {
        if let Some(column_ref) = self.find_column(&column) {
            return column_ref;
        }

        if let Some((pos, slot)) = self
            .columns
            .iter_mut()
            .enumerate()
            .find(|(_, column)| !column.live)
        {
            *slot = TableArenaColumn {
                catalog: column,
                live: true,
            };
            self.increment_version();
            return ColumnRef::new(pos);
        }

        let pos = self.columns.len();
        self.columns.push(TableArenaColumn {
            catalog: column,
            live: true,
        });
        self.increment_version();
        ColumnRef::new(pos)
    }

    fn alloc_index(&mut self, index: IndexMeta) -> IndexMetaRef {
        if let Some(index_ref) = self.find_index(&index) {
            return index_ref;
        }

        if let Some((pos, slot)) = self
            .indexes
            .iter_mut()
            .enumerate()
            .find(|(_, index)| !index.live)
        {
            *slot = TableArenaIndex {
                meta: index,
                live: true,
            };
            self.increment_version();
            return IndexMetaRef::new(pos);
        }

        let pos = self.indexes.len();
        self.indexes.push(TableArenaIndex {
            meta: index,
            live: true,
        });
        self.increment_version();
        IndexMetaRef::new(pos)
    }

    fn column(&self, column: ColumnRef) -> &ColumnCatalog {
        if let Some(column) = self.dummy_column(column) {
            return column;
        }
        let column = &self.columns[column.pos()];
        if !column.live {
            panic!("accessing recycled TableArena column");
        }
        &column.catalog
    }

    fn index(&self, index: IndexMetaRef) -> &IndexMeta {
        let index = &self.indexes[index.pos()];
        if !index.live {
            panic!("accessing recycled TableArena index");
        }
        &index.meta
    }

    fn find_column(&self, column: &ColumnCatalog) -> Option<ColumnRef> {
        self.columns
            .iter()
            .position(|candidate| candidate.live && candidate.catalog == *column)
            .map(ColumnRef::new)
    }

    fn find_index(&self, index: &IndexMeta) -> Option<IndexMetaRef> {
        self.indexes
            .iter()
            .position(|candidate| candidate.live && candidate.meta == *index)
            .map(IndexMetaRef::new)
    }
}

impl<'a> PlanArena<'a> {
    pub fn new(table_arena: &'a TableArenaCell) -> Self {
        #[cfg(debug_assertions)]
        let table_arena_version = table_arena.borrow().version();
        Self {
            table_arena,
            #[cfg(debug_assertions)]
            table_arena_version,
            columns: Vec::new(),
            indexes: Vec::new(),
        }
    }

    pub(crate) fn table_arena_cell(&self) -> &'a TableArenaCell {
        self.table_arena
    }

    pub(crate) fn materialize_into_table_arena(&self) {
        self.assert_table_arena_unchanged();

        let table_arena = self.table_arena.borrow_mut();
        // PlanArena column refs are encoded as a contiguous suffix after the
        // TableArena slots that existed when the plan was built. For cached
        // view plans we preserve those refs verbatim by appending this suffix
        // into TableArena at the same positions. This deliberately skips the
        // dead-slot reuse path used by ordinary table metadata allocation:
        // a little short-term slack is cheaper than remapping every ColumnRef
        // embedded in a view plan, and recycle_unreferenced can reclaim the
        // older dead slots for future non-view allocations.
        for column in &self.columns {
            table_arena.columns.push(TableArenaColumn {
                catalog: column.clone(),
                live: true,
            });
        }
        for index in &self.indexes {
            table_arena.indexes.push(TableArenaIndex {
                meta: index.clone(),
                live: true,
            });
        }
        if !self.columns.is_empty() || !self.indexes.is_empty() {
            table_arena.increment_version();
        }
    }

    #[cfg(debug_assertions)]
    fn assert_table_arena_unchanged(&self) {
        let current_version = self.table_arena.borrow().version();
        if current_version != self.table_arena_version {
            panic!("TableArena was modified while PlanArena is still active");
        }
    }

    #[cfg(not(debug_assertions))]
    fn assert_table_arena_unchanged(&self) {}

    pub(crate) fn clone_column(&self, column: ColumnRef) -> ColumnCatalog {
        self.column(column).clone()
    }

    pub(crate) fn same_column(&self, left: ColumnRef, right: ColumnRef) -> bool {
        self.column(left).summary() == self.column(right).summary()
    }

    pub(crate) fn nullable_for_join(
        &mut self,
        column: ColumnRef,
        nullable: bool,
    ) -> Option<ColumnRef> {
        let source = self.column(column);
        if source.nullable() == nullable {
            return None;
        }
        // FIXME
        let mut joined = source.clone();
        joined.set_nullable(nullable);
        joined.set_in_join(true);
        Some(self.alloc_column(joined))
    }

    pub(crate) fn alloc_column(&mut self, column: ColumnCatalog) -> ColumnRef {
        <Self as MetaArena>::alloc_column(self, column)
    }

    pub fn alloc_index(&mut self, index: IndexMeta) -> IndexMetaRef {
        <Self as MetaArena>::alloc_index(self, index)
    }

    pub(crate) fn alloc_dummy(&mut self, name: &str) -> ColumnRef {
        self.assert_table_arena_unchanged();
        self.table_arena.borrow().alloc_dummy(name)
    }

    pub fn column(&self, column: ColumnRef) -> &ColumnCatalog {
        <Self as MetaArena>::column(self, column)
    }

    pub fn index(&self, index: IndexMetaRef) -> &IndexMeta {
        <Self as MetaArena>::index(self, index)
    }
}

impl MetaArena for PlanArena<'_> {
    fn alloc_column(&mut self, column: ColumnCatalog) -> ColumnRef {
        self.assert_table_arena_unchanged();

        if let Some(column_ref) = self.find_column(&column) {
            return column_ref;
        }

        let pos = self.table_arena.borrow().columns_len() + self.columns.len();
        self.columns.push(column);
        ColumnRef::new(pos)
    }

    fn alloc_index(&mut self, index: IndexMeta) -> IndexMetaRef {
        self.assert_table_arena_unchanged();

        if let Some(index_ref) = self.find_index(&index) {
            return index_ref;
        }

        let pos = self.table_arena.borrow().indexes_len() + self.indexes.len();
        self.indexes.push(index);
        IndexMetaRef::new(pos)
    }

    fn column(&self, column: ColumnRef) -> &ColumnCatalog {
        self.assert_table_arena_unchanged();
        let table_arena = self.table_arena.borrow();
        if let Some(column) = table_arena.dummy_column(column) {
            return column;
        }
        let table_columns_len = table_arena.columns_len();
        if column.pos() < table_columns_len {
            table_arena.column(column)
        } else {
            &self.columns[column.pos() - table_columns_len]
        }
    }

    fn index(&self, index: IndexMetaRef) -> &IndexMeta {
        self.assert_table_arena_unchanged();
        let table_arena = self.table_arena.borrow();
        let table_indexes_len = table_arena.indexes_len();
        if index.pos() < table_indexes_len {
            table_arena.index(index)
        } else {
            &self.indexes[index.pos() - table_indexes_len]
        }
    }

    fn find_column(&self, column: &ColumnCatalog) -> Option<ColumnRef> {
        self.assert_table_arena_unchanged();
        let table_arena = self.table_arena.borrow();
        if column.is_persistent_table_column() {
            if let Some(column_ref) = table_arena.find_column(column) {
                return Some(column_ref);
            }
        }
        let table_columns_len = table_arena.columns_len();
        self.columns
            .iter()
            .position(|candidate| candidate == column)
            .map(|offset| ColumnRef::new(table_columns_len + offset))
    }

    fn find_index(&self, index: &IndexMeta) -> Option<IndexMetaRef> {
        self.assert_table_arena_unchanged();
        let table_arena = self.table_arena.borrow();
        if let Some(index_ref) = table_arena.find_index(index) {
            return Some(index_ref);
        }
        let table_indexes_len = table_arena.indexes_len();
        self.indexes
            .iter()
            .position(|candidate| candidate == index)
            .map(|offset| IndexMetaRef::new(table_indexes_len + offset))
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::types::LogicalType;

    fn column(name: &str) -> ColumnCatalog {
        ColumnCatalog::new(
            name.to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
        )
    }

    fn table_column(name: &str, is_temp: bool) -> ColumnCatalog {
        let mut column = column(name);
        column.set_ref_table("t".to_string().into(), ulid::Ulid::new(), is_temp);
        column
    }

    #[test]
    fn table_arena_reuses_recycled_slot() {
        let arena = crate::planner::TableArenaCell::default();
        let first = arena.borrow_mut().alloc_column(column("a"));
        let second = arena.borrow_mut().alloc_column(column("b"));

        arena
            .borrow_mut()
            .recycle_unreferenced_positions([first.pos()].into_iter().collect());
        let reused = arena.borrow_mut().alloc_column(column("c"));

        assert_eq!(reused, second);
        assert_eq!(arena.borrow().column(reused).name(), "c");
        assert_eq!(arena.borrow().columns_len(), 2);
        assert_eq!(arena.borrow().live_columns_len(), 2);
    }

    #[test]
    fn plan_arena_reuses_only_persistent_table_columns_from_table_arena() {
        let table_arena = crate::planner::TableArenaCell::default();
        let persistent = table_column("a", false);
        let temp = table_column("b", true);

        let persistent_ref = table_arena.borrow_mut().alloc_column(persistent.clone());
        let temp_table_ref = table_arena.borrow_mut().alloc_column(temp.clone());

        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        assert_eq!(plan_arena.alloc_column(persistent), persistent_ref);

        let temp_plan_ref = plan_arena.alloc_column(temp);
        assert_ne!(temp_plan_ref, temp_table_ref);
        assert!(temp_plan_ref.pos() >= table_arena.borrow().columns_len());
    }

    #[test]
    fn materializing_plan_arena_preserves_local_column_positions() {
        let table_arena = crate::planner::TableArenaCell::default();
        let first = table_arena.borrow_mut().alloc_column(column("a"));
        let second = table_arena.borrow_mut().alloc_column(column("b"));
        table_arena
            .borrow_mut()
            .recycle_unreferenced_positions([first.pos()].into_iter().collect());

        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let local = plan_arena.alloc_column(column("c"));
        assert_eq!(local.pos(), 2);

        plan_arena.materialize_into_table_arena();

        assert_eq!(table_arena.borrow().columns_len(), 3);
        assert_eq!(table_arena.borrow().column(local).name(), "c");
        assert_eq!(table_arena.borrow().column(first).name(), "a");
        assert_eq!(second.pos(), 1);
    }
}
