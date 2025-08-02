pub mod operator;

use crate::catalog::{ColumnCatalog, ColumnRef, TableName};
use crate::planner::operator::except::ExceptOperator;
use crate::planner::operator::join::JoinType;
use crate::planner::operator::union::UnionOperator;
use crate::planner::operator::values::ValuesOperator;
use crate::planner::operator::{Operator, PhysicalOption};
use crate::types::tuple::{Schema, SchemaRef};
use itertools::Itertools;
use kite_sql_serde_macros::ReferenceSerialization;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) enum SchemaOutput {
    Schema(Schema),
    SchemaRef(SchemaRef),
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub enum Childrens {
    None,
    Only(LogicalPlan),
    Twins {
        left: LogicalPlan,
        right: LogicalPlan,
    },
}

impl Childrens {
    pub fn iter(&self) -> ChildrensIter {
        ChildrensIter {
            inner: self,
            pos: 0,
        }
    }

    pub fn pop_only(self) -> LogicalPlan {
        match self {
            Childrens::Only(plan) => plan,
            _ => {
                unreachable!()
            }
        }
    }

    pub fn pop_twins(self) -> (LogicalPlan, LogicalPlan) {
        match self {
            Childrens::Twins { left, right } => (left, right),
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
                Some(plan)
            }
            Childrens::Twins { left, right } => {
                let option = match self.pos {
                    0 => Some(left),
                    1 => Some(right),
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

    pub(crate) _output_schema_ref: Option<SchemaRef>,
}

impl SchemaOutput {
    pub(crate) fn columns(&self) -> impl Iterator<Item = &ColumnRef> {
        match self {
            SchemaOutput::Schema(schema) => schema.iter(),
            SchemaOutput::SchemaRef(schema_ref) => schema_ref.iter(),
        }
    }
}

impl LogicalPlan {
    pub fn new(operator: Operator, childrens: Childrens) -> Self {
        Self {
            operator,
            childrens: Box::new(childrens),
            physical_option: None,
            _output_schema_ref: None,
        }
    }

    pub fn referenced_table(&self) -> Vec<TableName> {
        fn collect_table(plan: &LogicalPlan, results: &mut Vec<TableName>) {
            if let Operator::TableScan(op) = &plan.operator {
                results.push(op.table_name.clone());
            } else if let Operator::Values(op) = &plan.operator {
                results.push(op.schema_ref[0].table_name().unwrap().clone());
            }
            for child in plan.childrens.iter() {
                collect_table(child, results);
            }
        }

        let mut tables = Vec::new();
        collect_table(self, &mut tables);
        tables
    }

    pub(crate) fn _output_schema_direct(
        operator: &Operator,
        mut childrens_iter: ChildrensIter,
    ) -> SchemaOutput {
        match operator {
            Operator::Filter(_) | Operator::Sort(_) | Operator::Limit(_) => {
                childrens_iter.next().unwrap().output_schema_direct()
            }
            Operator::Aggregate(op) => SchemaOutput::Schema(
                op.agg_calls
                    .iter()
                    .chain(op.groupby_exprs.iter())
                    .map(|expr| expr.output_column())
                    .collect_vec(),
            ),
            Operator::Join(op) => {
                if matches!(op.join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
                    return childrens_iter.next().unwrap().output_schema_direct();
                }
                let mut columns = Vec::new();

                for plan in childrens_iter {
                    for column in plan.output_schema_direct().columns() {
                        columns.push(column.clone());
                    }
                }
                SchemaOutput::Schema(columns)
            }
            Operator::Project(op) => SchemaOutput::Schema(
                op.exprs
                    .iter()
                    .map(|expr| expr.output_column())
                    .collect_vec(),
            ),
            Operator::TableScan(op) => {
                SchemaOutput::Schema(op.columns.values().cloned().collect_vec())
            }
            Operator::FunctionScan(op) => {
                SchemaOutput::SchemaRef(op.table_function.output_schema().clone())
            }
            Operator::Values(ValuesOperator { schema_ref, .. })
            | Operator::Union(UnionOperator {
                left_schema_ref: schema_ref,
                ..
            })
            | Operator::Except(ExceptOperator {
                left_schema_ref: schema_ref,
                ..
            }) => SchemaOutput::SchemaRef(schema_ref.clone()),
            Operator::Dummy => SchemaOutput::Schema(vec![]),
            Operator::ShowTable => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("TABLE".to_string()),
            )]),
            Operator::ShowView => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("VIEW".to_string()),
            )]),
            Operator::Explain => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("PLAN".to_string()),
            )]),
            Operator::Describe(_) => SchemaOutput::Schema(vec![
                ColumnRef::from(ColumnCatalog::new_dummy("FIELD".to_string())),
                ColumnRef::from(ColumnCatalog::new_dummy("TYPE".to_string())),
                ColumnRef::from(ColumnCatalog::new_dummy("LEN".to_string())),
                ColumnRef::from(ColumnCatalog::new_dummy("NULL".to_string())),
                ColumnRef::from(ColumnCatalog::new_dummy("Key".to_string())),
                ColumnRef::from(ColumnCatalog::new_dummy("DEFAULT".to_string())),
            ]),
            Operator::Insert(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("INSERTED".to_string()),
            )]),
            Operator::Update(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("UPDATED".to_string()),
            )]),
            Operator::Delete(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("DELETED".to_string()),
            )]),
            Operator::Analyze(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("STATISTICS_META_PATH".to_string()),
            )]),
            Operator::AddColumn(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("ADD COLUMN SUCCESS".to_string()),
            )]),
            Operator::DropColumn(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("DROP COLUMN SUCCESS".to_string()),
            )]),
            Operator::CreateTable(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("CREATE TABLE SUCCESS".to_string()),
            )]),
            Operator::CreateIndex(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("CREATE INDEX SUCCESS".to_string()),
            )]),
            Operator::CreateView(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("CREATE VIEW SUCCESS".to_string()),
            )]),
            Operator::DropTable(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("DROP TABLE SUCCESS".to_string()),
            )]),
            Operator::DropView(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("DROP VIEW SUCCESS".to_string()),
            )]),
            Operator::DropIndex(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("DROP INDEX SUCCESS".to_string()),
            )]),
            Operator::Truncate(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("TRUNCATE TABLE SUCCESS".to_string()),
            )]),
            Operator::CopyFromFile(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("COPY FROM SOURCE".to_string()),
            )]),
            Operator::CopyToFile(_) => SchemaOutput::Schema(vec![ColumnRef::from(
                ColumnCatalog::new_dummy("COPY TO TARGET".to_string()),
            )]),
        }
    }

    pub(crate) fn output_schema_direct(&self) -> SchemaOutput {
        Self::_output_schema_direct(&self.operator, self.childrens.iter())
    }

    pub fn output_schema(&mut self) -> &SchemaRef {
        self._output_schema_ref.get_or_insert_with(|| {
            match Self::_output_schema_direct(&self.operator, self.childrens.iter()) {
                SchemaOutput::Schema(schema) => Arc::new(schema),
                SchemaOutput::SchemaRef(schema_ref) => schema_ref.clone(),
            }
        })
    }

    pub fn explain(&self, indentation: usize) -> String {
        let mut result = format!("{:indent$}{}", "", self.operator, indent = indentation);

        if let Some(physical_option) = &self.physical_option {
            result.push_str(&format!(" [{}]", physical_option));
        }

        for child in self.childrens.iter() {
            result.push('\n');
            result.push_str(&child.explain(indentation + 2));
        }

        result
    }
}
