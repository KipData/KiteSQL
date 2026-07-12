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

use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::planner::operator::set_membership::SetMembershipOperator;
use crate::planner::operator::union::UnionOperator;
use crate::planner::operator::values::ValuesOperator;
use crate::planner::operator::{Operator, PhysicalOption};
use kite_sql_serde_macros::ReferenceSerialization;
use std::hash::{Hash, Hasher};

pub use arena::{MetaArena, PlanArena, TableArena, TableArenaCell};

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

#[derive(Debug)]
pub struct LogicalPlan {
    pub(crate) operator: Operator,
    pub(crate) childrens: Box<Childrens>,
    pub(crate) physical_option: Option<PhysicalOption>,
    output_schema: Option<crate::types::tuple::Schema>,
}

impl LogicalPlan {
    pub fn new(operator: Operator, childrens: Childrens) -> Self {
        Self {
            operator,
            childrens: Box::new(childrens),
            physical_option: None,
            output_schema: None,
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

    pub(crate) fn visit_column_refs<A, F>(
        &self,
        arena: &mut A,
        f: &mut F,
    ) -> Result<(), DatabaseError>
    where
        A: MetaArena,
        F: FnMut(&crate::catalog::ColumnRef) + ?Sized,
    {
        self.operator
            .visit_referenced_columns(arena, &mut |_, column| {
                f(column);
                true
            })?;
        for child in self.childrens.iter() {
            child.visit_column_refs(arena, f)?;
        }
        Ok(())
    }

    pub fn output_schema<'plan>(
        &'plan mut self,
        arena: &mut PlanArena,
    ) -> &'plan crate::types::tuple::Schema {
        let LogicalPlan {
            operator,
            childrens,
            output_schema,
            ..
        } = self;
        output_schema.get_or_insert_with(|| Self::compute_output_schema(operator, childrens, arena))
    }

    pub fn take_schema(&mut self, arena: &mut PlanArena) -> crate::types::tuple::Schema {
        let LogicalPlan {
            operator,
            childrens,
            output_schema,
            ..
        } = self;
        output_schema
            .take()
            .unwrap_or_else(|| Self::compute_output_schema(operator, childrens, arena))
    }

    fn compute_output_schema(
        operator: &mut Operator,
        childrens: &mut Childrens,
        arena: &mut PlanArena,
    ) -> crate::types::tuple::Schema {
        match operator {
            Operator::Filter(_)
            | Operator::Sort(_)
            | Operator::Limit(_)
            | Operator::TopK(_)
            | Operator::ScalarSubquery(_) => match childrens {
                Childrens::Only(child) => child.output_schema(arena).clone(),
                _ => unreachable!(),
            },
            Operator::Window(op) => match childrens {
                Childrens::Only(child) => {
                    let mut schema = child.output_schema(arena).clone();
                    schema.extend_from_slice(&op.output_columns);
                    schema
                }
                _ => unreachable!(),
            },
            Operator::ScalarApply(_) | Operator::Join(_) => match childrens {
                Childrens::Twins { left, right } => {
                    let mut schema = left.output_schema(arena).clone();
                    schema.extend_from_slice(right.output_schema(arena));
                    schema
                }
                _ => unreachable!(),
            },
            Operator::MarkApply(op) => {
                let mut schema = match childrens {
                    Childrens::Only(left) => left.output_schema(arena).clone(),
                    Childrens::Twins { left, .. } => left.output_schema(arena).clone(),
                    Childrens::None => Vec::new(),
                };
                schema.push(*op.output_column());
                schema
            }
            Operator::Aggregate(op) => op
                .agg_calls
                .iter()
                .chain(op.groupby_exprs.iter())
                .map(|expr| expr.output_column_ref(arena))
                .collect(),
            Operator::Project(op) => op
                .exprs
                .iter()
                .map(|expr| expr.output_column_ref(arena))
                .collect(),
            Operator::TableScan(op) => op.columns.clone(),
            Operator::FunctionScan(op) => {
                let mut schema = Vec::new();
                op.table_function.output_schema_into(&mut schema);
                schema
            }
            Operator::Values(ValuesOperator { schema_ref, .. })
            | Operator::Union(UnionOperator {
                left_schema_ref: schema_ref,
                ..
            })
            | Operator::SetMembership(SetMembershipOperator {
                left_schema_ref: schema_ref,
                ..
            }) => schema_ref.clone(),
            Operator::Dummy => Vec::new(),
            Operator::ShowTable => Self::dummy_schema(arena, ["TABLE"]),
            Operator::ShowView => Self::dummy_schema(arena, ["VIEW"]),
            Operator::Explain => Self::dummy_schema(arena, ["PLAN"]),
            Operator::Describe(_) => Self::dummy_schema(
                arena,
                [
                    "FIELD",
                    "TYPE",
                    "LEN",
                    "NULL",
                    "Key",
                    "DEFAULT",
                    "COLUMN_REF",
                ],
            ),
            Operator::Insert(_) => Self::dummy_schema(arena, ["INSERTED"]),
            Operator::Update(_) => Self::dummy_schema(arena, ["UPDATED"]),
            Operator::Delete(_) => Self::dummy_schema(arena, ["DELETED"]),
            Operator::Analyze(_) => Self::dummy_schema(arena, ["STATISTICS_META_PATH"]),
            Operator::AddColumn(_) => Self::dummy_schema(arena, ["ADD COLUMN SUCCESS"]),
            Operator::ChangeColumn(_) => Self::dummy_schema(arena, ["CHANGE COLUMN SUCCESS"]),
            Operator::DropColumn(_) => Self::dummy_schema(arena, ["DROP COLUMN SUCCESS"]),
            Operator::CreateTable(_) => Self::dummy_schema(arena, ["CREATE TABLE SUCCESS"]),
            Operator::CreateIndex(_) => Self::dummy_schema(arena, ["CREATE INDEX SUCCESS"]),
            Operator::CreateView(_) => Self::dummy_schema(arena, ["CREATE VIEW SUCCESS"]),
            Operator::DropTable(_) => Self::dummy_schema(arena, ["DROP TABLE SUCCESS"]),
            Operator::DropView(_) => Self::dummy_schema(arena, ["DROP VIEW SUCCESS"]),
            Operator::DropIndex(_) => Self::dummy_schema(arena, ["DROP INDEX SUCCESS"]),
            Operator::Truncate(_) => Self::dummy_schema(arena, ["TRUNCATE TABLE SUCCESS"]),
            #[cfg(feature = "copy")]
            Operator::CopyFromFile(_) => Self::dummy_schema(arena, ["COPY FROM SOURCE"]),
            #[cfg(feature = "copy")]
            Operator::CopyToFile(_) => Self::dummy_schema(arena, ["COPY TO TARGET"]),
        }
    }

    fn dummy_schema<const N: usize>(
        arena: &mut PlanArena,
        names: [&str; N],
    ) -> crate::types::tuple::Schema {
        names
            .into_iter()
            .map(|name| arena.alloc_dummy(name))
            .collect()
    }

    pub fn reset_output_schema_cache(&mut self) {
        self.output_schema = None;
    }

    pub fn reset_output_schema_cache_recursive(&mut self) {
        self.reset_output_schema_cache();
        match self.childrens.as_mut() {
            Childrens::Only(child) => child.reset_output_schema_cache_recursive(),
            Childrens::Twins { left, right } => {
                left.reset_output_schema_cache_recursive();
                right.reset_output_schema_cache_recursive();
            }
            Childrens::None => (),
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    pub fn explain(&self, arena: &mut PlanArena, indentation: usize) -> String {
        let mut result = format!("{:indent$}{}", "", self.operator, indent = indentation);

        if let Some(physical_option) = &self.physical_option {
            result.push_str(&format!(" [{physical_option}]"));
        }

        for child in self.childrens.iter() {
            let child = child.explain(arena, indentation + 2);
            result.push(' ');
            result.push_str(child.trim_start());
        }

        result
    }
}

impl Clone for LogicalPlan {
    fn clone(&self) -> Self {
        Self {
            operator: self.operator.clone(),
            childrens: self.childrens.clone(),
            physical_option: self.physical_option.clone(),
            output_schema: None,
        }
    }
}

impl PartialEq for LogicalPlan {
    fn eq(&self, other: &Self) -> bool {
        self.operator == other.operator
            && self.childrens == other.childrens
            && self.physical_option == other.physical_option
    }
}

impl Eq for LogicalPlan {}

impl Hash for LogicalPlan {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.operator.hash(state);
        self.childrens.hash(state);
        self.physical_option.hash(state);
    }
}

impl crate::serdes::ReferenceSerialization for LogicalPlan {
    fn encode<W: std::io::Write, A: crate::planner::MetaArena>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut crate::serdes::ReferenceTables,
        arena: &A,
    ) -> Result<(), crate::errors::DatabaseError> {
        crate::serdes::ReferenceSerialization::encode(
            &self.operator,
            writer,
            is_direct,
            reference_tables,
            arena,
        )?;
        crate::serdes::ReferenceSerialization::encode(
            &self.childrens,
            writer,
            is_direct,
            reference_tables,
            arena,
        )?;
        crate::serdes::ReferenceSerialization::encode(
            &self.physical_option,
            writer,
            is_direct,
            reference_tables,
            arena,
        )
    }

    fn decode<T: crate::storage::Transaction, R: std::io::Read, A: crate::planner::MetaArena>(
        reader: &mut R,
        context: Option<&crate::serdes::ReferenceDecodeContext<'_, T>>,
        reference_tables: &crate::serdes::ReferenceTables,
        arena: &mut A,
    ) -> Result<Self, crate::errors::DatabaseError> {
        let operator = <Operator as crate::serdes::ReferenceSerialization>::decode(
            reader,
            context,
            reference_tables,
            arena,
        )?;
        let childrens = <Box<Childrens> as crate::serdes::ReferenceSerialization>::decode(
            reader,
            context,
            reference_tables,
            arena,
        )?;
        let physical_option =
            <Option<PhysicalOption> as crate::serdes::ReferenceSerialization>::decode(
                reader,
                context,
                reference_tables,
                arena,
            )?;

        Ok(Self {
            operator,
            childrens,
            physical_option,
            output_schema: None,
        })
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::planner::operator::describe::DescribeOperator;
    use crate::planner::operator::limit::LimitOperator;
    use crate::planner::operator::table_scan::TableScanOperator;
    use crate::planner::operator::{PlanImpl, SortOption};
    use crate::types::LogicalType;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    fn column(name: &str, ty: LogicalType, arena: &mut PlanArena) -> ColumnRef {
        arena.alloc_column(ColumnCatalog::new(
            name.to_string(),
            true,
            ColumnDesc::new(ty, None, false, None).unwrap(),
        ))
    }

    fn table_scan(table: &str, columns: Vec<ColumnRef>) -> LogicalPlan {
        LogicalPlan::new(
            Operator::TableScan(TableScanOperator {
                table_name: table.to_string().into(),
                columns,
                limit: (None, None),
                index_infos: Vec::new(),
                with_pk: false,
            }),
            Childrens::None,
        )
    }

    fn schema_names(schema: &[ColumnRef], arena: &PlanArena) -> Vec<String> {
        schema
            .iter()
            .map(|column| arena.column(*column).name().to_string())
            .collect()
    }

    #[test]
    fn childrens_iterates_and_pops_expected_plans() {
        let left = LogicalPlan::new(Operator::ShowTable, Childrens::None);
        let right = LogicalPlan::new(Operator::ShowView, Childrens::None);

        assert_eq!(Childrens::None.iter().count(), 0);

        let only = Childrens::Only(Box::new(left.clone()));
        assert_eq!(only.iter().count(), 1);
        assert!(matches!(only.pop_only().operator, Operator::ShowTable));

        let twins = Childrens::Twins {
            left: Box::new(left),
            right: Box::new(right),
        };
        let operators = twins
            .iter()
            .map(|plan| format!("{}", plan.operator))
            .collect::<Vec<_>>();
        assert_eq!(operators, vec!["Show Tables", "Show Views"]);

        let (left, right) = twins.pop_twins();
        assert!(matches!(left.operator, Operator::ShowTable));
        assert!(matches!(right.operator, Operator::ShowView));
    }

    #[test]
    fn logical_plan_collects_tables_explains_and_hashes_without_schema_cache() {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let id = column("id", LogicalType::Integer, &mut arena);
        let scan = table_scan("users", vec![id]);
        let mut plan = LimitOperator::build(Some(2), Some(5), scan);
        plan.physical_option = Some(PhysicalOption::new(PlanImpl::Limit, SortOption::Follow));

        let tables = plan
            .referenced_table()
            .into_iter()
            .map(|table| table.to_string())
            .collect::<Vec<_>>();
        assert_eq!(tables, vec!["users"]);
        assert_eq!(
            plan.explain(&mut arena, 0),
            "Limit 5, Offset 2 [Limit => (Sort Option: Follow)] TableScan users -> [#0]"
        );

        let _ = plan.output_schema(&mut arena);
        let cloned = plan.clone();
        assert_eq!(plan, cloned);

        let mut left = DefaultHasher::new();
        let mut right = DefaultHasher::new();
        plan.hash(&mut left);
        cloned.hash(&mut right);
        assert_eq!(left.finish(), right.finish());
    }

    #[test]
    fn output_schema_inherits_from_child_and_can_be_recursively_reset() {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let id = column("id", LogicalType::Integer, &mut arena);
        let name = column(
            "name",
            LogicalType::Varchar(None, crate::types::CharLengthUnits::Characters),
            &mut arena,
        );
        let scan = table_scan("users", vec![id]);
        let mut plan = LimitOperator::build(None, Some(10), scan);

        let schema = plan.output_schema(&mut arena).clone();
        assert_eq!(schema_names(&schema, &arena), vec!["id"]);

        if let Childrens::Only(child) = plan.childrens.as_mut() {
            if let Operator::TableScan(scan) = &mut child.operator {
                scan.columns = vec![name];
            }
        }

        let cached = plan.output_schema(&mut arena).clone();
        assert_eq!(schema_names(&cached, &arena), vec!["id"]);

        plan.reset_output_schema_cache_recursive();
        let recomputed = plan.output_schema(&mut arena).clone();
        assert_eq!(schema_names(&recomputed, &arena), vec!["name"]);
    }

    #[test]
    fn dummy_operators_produce_expected_output_schema() {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let cases = [
            (Operator::ShowTable, vec!["TABLE"]),
            (Operator::ShowView, vec!["VIEW"]),
            (Operator::Explain, vec!["PLAN"]),
            (
                Operator::Describe(DescribeOperator {
                    table_name: "users".into(),
                }),
                vec![
                    "FIELD",
                    "TYPE",
                    "LEN",
                    "NULL",
                    "Key",
                    "DEFAULT",
                    "COLUMN_REF",
                ],
            ),
        ];

        for (operator, expected) in cases {
            let mut plan = LogicalPlan::new(operator, Childrens::None);
            let schema = plan.output_schema(&mut arena).clone();
            assert_eq!(schema_names(&schema, &arena), expected);
        }
    }
}
// GRCOV_EXCL_STOP
