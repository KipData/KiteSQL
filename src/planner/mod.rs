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

mod arena;
pub mod operator;

use crate::catalog::{ColumnCatalog, TableName};
use crate::planner::operator::set_membership::SetMembershipOperator;
use crate::planner::operator::union::UnionOperator;
use crate::planner::operator::values::ValuesOperator;
use crate::planner::operator::{Operator, PhysicalOption};
use crate::types::tuple::Schema;
use itertools::Itertools;
use kite_sql_serde_macros::ReferenceSerialization;

pub use arena::{MetaArena, PlanArena, SchemaSlot, TableArena, TableArenaCell};

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub enum Childrens {
    None,
    Only(Box<LogicalPlan>),
    Twins {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    },
}

impl Childrens {
    pub fn iter(&self) -> ChildrensIter<'_> {
        ChildrensIter {
            inner: self,
            pos: 0,
        }
    }

    pub fn pop_only(self) -> LogicalPlan {
        match self {
            Childrens::Only(plan) => *plan,
            _ => {
                unreachable!()
            }
        }
    }

    pub fn pop_twins(self) -> (LogicalPlan, LogicalPlan) {
        match self {
            Childrens::Twins { left, right } => (*left, *right),
            _ => unreachable!(),
        }
    }
}

pub struct ChildrensIter<'a> {
    inner: &'a Childrens,
    pos: usize,
}

impl<'a> Iterator for ChildrensIter<'a> {
    type Item = &'a LogicalPlan;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner {
            Childrens::Only(plan) => {
                if self.pos > 0 {
                    return None;
                }
                self.pos += 1;
                Some(plan.as_ref())
            }
            Childrens::Twins { left, right } => {
                let option = match self.pos {
                    0 => Some(left.as_ref()),
                    1 => Some(right.as_ref()),
                    _ => None,
                };
                self.pos += 1;
                option
            }
            Childrens::None => None,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct LogicalPlan {
    pub(crate) operator: Operator,
    pub(crate) childrens: Box<Childrens>,
    pub(crate) physical_option: Option<PhysicalOption>,
}

impl LogicalPlan {
    pub fn new(operator: Operator, childrens: Childrens) -> Self {
        Self {
            operator,
            childrens: Box::new(childrens),
            physical_option: None,
        }
    }

    pub(crate) fn take(&mut self) -> Self {
        std::mem::replace(self, Self::new(Operator::Dummy, Childrens::None))
    }

    pub fn referenced_table(&self) -> Vec<TableName> {
        fn collect_table(plan: &LogicalPlan, results: &mut Vec<TableName>) {
            if let Operator::TableScan(op) = &plan.operator {
                results.push(op.table_name.clone());
            }
            for child in plan.childrens.iter() {
                collect_table(child, results);
            }
        }

        let mut tables = Vec::new();
        collect_table(self, &mut tables);
        tables
    }

    pub(crate) fn visit_column_refs<A, F>(&self, arena: &mut A, f: &mut F)
    where
        A: MetaArena,
        F: FnMut(&crate::catalog::ColumnRef) + ?Sized,
    {
        self.operator
            .visit_referenced_columns(arena, &mut |_, column| {
                f(column);
                true
            });
        for child in self.childrens.iter() {
            child.visit_column_refs(arena, f);
        }
    }

    pub fn output_schema_to<'arena>(
        &self,
        arena: &'arena mut PlanArena,
        slot: SchemaSlot,
    ) -> &'arena Schema {
        match &self.operator {
            Operator::Filter(_)
            | Operator::Sort(_)
            | Operator::Limit(_)
            | Operator::TopK(_)
            | Operator::ScalarSubquery(_) => self
                .childrens
                .iter()
                .next()
                .unwrap()
                .output_schema_to(arena, slot),
            Operator::ScalarApply(_) | Operator::Join(_) => {
                let mut childrens = self.childrens.iter();
                let left = childrens.next().unwrap();
                let right = childrens.next().unwrap();
                left.output_schema_to(arena, SchemaSlot::S0);
                right.output_schema_to(arena, SchemaSlot::S1);

                match slot {
                    SchemaSlot::S0 => {
                        let right = arena.schema(SchemaSlot::S1).clone();
                        arena.append_schema(SchemaSlot::S0, right);
                    }
                    SchemaSlot::S1 => {
                        let mut columns = arena.schema(SchemaSlot::S0).clone();
                        columns.extend(arena.schema(SchemaSlot::S1).iter().cloned());
                        arena.write_schema(SchemaSlot::S1, columns);
                    }
                }
                arena.schema(slot)
            }
            Operator::MarkApply(op) => {
                if let Some(left) = self.childrens.iter().next() {
                    left.output_schema_to(arena, slot);
                } else {
                    arena.write_schema(slot, std::iter::empty());
                }
                arena.append_schema(slot, std::iter::once(op.output_column().clone()));
                arena.schema(slot)
            }
            Operator::Aggregate(op) => {
                let columns = op
                    .agg_calls
                    .iter()
                    .chain(op.groupby_exprs.iter())
                    .map(|expr| expr.output_column_ref(arena))
                    .collect_vec();
                arena.write_schema(slot, columns);
                arena.schema(slot)
            }
            Operator::Project(op) => {
                let columns = op
                    .exprs
                    .iter()
                    .map(|expr| expr.output_column_ref(arena))
                    .collect_vec();
                arena.write_schema(slot, columns);
                arena.schema(slot)
            }
            Operator::TableScan(op) => {
                arena.write_schema(slot, op.columns.iter().cloned());
                arena.schema(slot)
            }
            Operator::FunctionScan(op) => {
                op.table_function.output_schema_into(arena.schema_mut(slot));
                arena.schema(slot)
            }
            Operator::Values(ValuesOperator { schema_ref, .. })
            | Operator::Union(UnionOperator {
                left_schema_ref: schema_ref,
                ..
            })
            | Operator::SetMembership(SetMembershipOperator {
                left_schema_ref: schema_ref,
                ..
            }) => {
                arena.write_schema(slot, schema_ref.iter().cloned());
                arena.schema(slot)
            }
            Operator::Dummy => {
                arena.write_schema(slot, std::iter::empty());
                arena.schema(slot)
            }
            Operator::ShowTable => self.write_dummy_schema(arena, slot, ["TABLE"]),
            Operator::ShowView => self.write_dummy_schema(arena, slot, ["VIEW"]),
            Operator::Explain => self.write_dummy_schema(arena, slot, ["PLAN"]),
            Operator::Describe(_) => self.write_dummy_schema(
                arena,
                slot,
                ["FIELD", "TYPE", "LEN", "NULL", "Key", "DEFAULT"],
            ),
            Operator::Insert(_) => self.write_dummy_schema(arena, slot, ["INSERTED"]),
            Operator::Update(_) => self.write_dummy_schema(arena, slot, ["UPDATED"]),
            Operator::Delete(_) => self.write_dummy_schema(arena, slot, ["DELETED"]),
            Operator::Analyze(_) => self.write_dummy_schema(arena, slot, ["STATISTICS_META_PATH"]),
            Operator::AddColumn(_) => self.write_dummy_schema(arena, slot, ["ADD COLUMN SUCCESS"]),
            Operator::ChangeColumn(_) => {
                self.write_dummy_schema(arena, slot, ["CHANGE COLUMN SUCCESS"])
            }
            Operator::DropColumn(_) => {
                self.write_dummy_schema(arena, slot, ["DROP COLUMN SUCCESS"])
            }
            Operator::CreateTable(_) => {
                self.write_dummy_schema(arena, slot, ["CREATE TABLE SUCCESS"])
            }
            Operator::CreateIndex(_) => {
                self.write_dummy_schema(arena, slot, ["CREATE INDEX SUCCESS"])
            }
            Operator::CreateView(_) => {
                self.write_dummy_schema(arena, slot, ["CREATE VIEW SUCCESS"])
            }
            Operator::DropTable(_) => self.write_dummy_schema(arena, slot, ["DROP TABLE SUCCESS"]),
            Operator::DropView(_) => self.write_dummy_schema(arena, slot, ["DROP VIEW SUCCESS"]),
            Operator::DropIndex(_) => self.write_dummy_schema(arena, slot, ["DROP INDEX SUCCESS"]),
            Operator::Truncate(_) => {
                self.write_dummy_schema(arena, slot, ["TRUNCATE TABLE SUCCESS"])
            }
            Operator::CopyFromFile(_) => self.write_dummy_schema(arena, slot, ["COPY FROM SOURCE"]),
            Operator::CopyToFile(_) => self.write_dummy_schema(arena, slot, ["COPY TO TARGET"]),
        }
    }

    fn write_dummy_schema<'arena, const N: usize>(
        &self,
        arena: &'arena mut PlanArena,
        slot: SchemaSlot,
        names: [&str; N],
    ) -> &'arena Schema {
        arena.write_schema(slot, std::iter::empty());
        for name in names {
            let column = arena.alloc_column(ColumnCatalog::new_dummy(name.to_string()));
            arena.append_schema(slot, std::iter::once(column));
        }
        arena.schema(slot)
    }

    pub fn with_output_schema_to<R>(
        &self,
        arena: &mut PlanArena,
        slot: SchemaSlot,
        f: impl FnOnce(&PlanArena, &Schema) -> R,
    ) -> R {
        self.output_schema_to(arena, slot);
        f(arena, arena.schema(slot))
    }

    pub fn explain(&self, arena: &mut PlanArena, indentation: usize) -> String {
        let mut result = format!("{:indent$}{}", "", self.operator, indent = indentation);

        if let Some(physical_option) = &self.physical_option {
            result.push_str(&format!(" [{physical_option}]"));
        }

        for child in self.childrens.iter() {
            result.push('\n');
            result.push_str(&child.explain(arena, indentation + 2));
        }

        result
    }
}
