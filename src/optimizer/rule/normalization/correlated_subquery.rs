use crate::catalog::{ColumnRef, TableName};
use crate::errors::DatabaseError;
use crate::expression::visitor::Visitor;
use crate::expression::HasCountStar;
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{MatchPattern, NormalizationRule};
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::Operator;
use crate::planner::operator::Operator::{Join, TableScan};
use crate::types::index::IndexInfo;
use crate::types::ColumnId;
use itertools::Itertools;
use std::collections::BTreeMap;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

static CORRELATED_SUBQUERY_RULE: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |op| matches!(op, Join(_)),
    children: PatternChildrenPredicate::None,
});

#[derive(Clone)]
pub struct CorrelatedSubquery;

macro_rules! trans_references {
    ($columns:expr) => {{
        let mut column_references = HashSet::with_capacity($columns.len());
        for column in $columns {
            column_references.insert(column);
        }
        column_references
    }};
}

impl CorrelatedSubquery {
    fn _apply(
        column_references: HashSet<&ColumnRef>,
        mut used_scan: HashMap<TableName, TableScanOperator>,
        node_id: HepNodeId,
        graph: &mut HepGraph,
    ) -> Result<HashMap<TableName, TableScanOperator>, DatabaseError> {
        let operator = &graph.operator(node_id).clone();

        match operator {
            Operator::Aggregate(op) => {
                let is_distinct = op.is_distinct;
                let referenced_columns = operator.referenced_columns(true);
                let mut new_column_references = trans_references!(&referenced_columns);
                // on distinct
                if is_distinct {
                    for summary in column_references {
                        new_column_references.insert(summary);
                    }
                }

                Self::recollect_apply(new_column_references, used_scan, node_id, graph)
            }
            Operator::Project(op) => {
                let referenced_columns = operator.referenced_columns(true);
                let new_column_references = trans_references!(&referenced_columns);

                Self::recollect_apply(new_column_references, used_scan, node_id, graph)
            }
            TableScan(op) => {
                let table_columns: HashSet<&ColumnRef> = op.columns.values().collect();
                let mut parent_scan_to_added = HashMap::new();
                for col in column_references {
                    if table_columns.contains(col) {
                        continue;
                    }
                    if let Some(table_name) = col.table_name() {
                        if !used_scan.contains_key(table_name) {
                            continue;
                        }
                        parent_scan_to_added
                            .entry(table_name)
                            .or_insert(HashSet::new())
                            .insert(col);
                    }
                }
                for (table_name, table_columns) in parent_scan_to_added {
                    let op = used_scan.get(table_name).unwrap();
                    let left_operator = graph.operator(node_id).clone();
                    let right_operator = TableScan(TableScanOperator {
                        table_name: table_name.clone(),
                        primary_keys: op.primary_keys.clone(),
                        columns: op
                            .columns
                            .iter()
                            .filter(|(_, column)| table_columns.contains(column))
                            .map(|(i, col)| (*i, col.clone()))
                            .collect(),
                        limit: (None, None),
                        index_infos: op.index_infos.clone(),
                        with_pk: false,
                    });
                    let join_operator = Join(JoinOperator {
                        on: JoinCondition::None,
                        join_type: JoinType::Cross,
                    });

                    match &left_operator {
                        TableScan(_) => {
                            graph.replace_node(node_id, join_operator);
                            graph.add_node(node_id, None, left_operator);
                            graph.add_node(node_id, None, right_operator);
                        }
                        Join(_) => {
                            let left_id = graph.eldest_child_at(node_id).unwrap();
                            let left_id = graph.add_node(node_id, Some(left_id), join_operator);
                            graph.add_node(left_id, None, right_operator);
                        }
                        _ => unreachable!(),
                    }
                }
                used_scan.insert(op.table_name.clone(), op.clone());
                Ok(used_scan)
            }
            Operator::Sort(_) | Operator::Limit(_) | Operator::Filter(_) | Operator::Union(_) => {
                let temp_columns = operator.referenced_columns(true);
                let mut column_references = column_references;
                for column in temp_columns.iter() {
                    column_references.insert(column);
                }
                Self::recollect_apply(column_references, used_scan, node_id, graph)
            }
            Join(_) => {
                let used_scan =
                    Self::recollect_apply(column_references.clone(), used_scan, node_id, graph)?;
                let temp_columns = operator.referenced_columns(true);
                let mut column_references = column_references;
                for column in temp_columns.iter() {
                    column_references.insert(column);
                }
                Ok(used_scan)
                //todo Supplemental testing is required
            }
            // Last Operator
            Operator::Dummy | Operator::Values(_) | Operator::FunctionScan(_) => Ok(used_scan),
            Operator::Explain => {
                if let Some(child_id) = graph.eldest_child_at(node_id) {
                    Self::_apply(column_references, used_scan, child_id, graph)
                } else {
                    unreachable!()
                }
            }
            // DDL Based on Other Plan
            Operator::Insert(_)
            | Operator::Update(_)
            | Operator::Delete(_)
            | Operator::Analyze(_) => {
                let referenced_columns = operator.referenced_columns(true);
                let new_column_references = trans_references!(&referenced_columns);

                if let Some(child_id) = graph.eldest_child_at(node_id) {
                    Self::recollect_apply(new_column_references, used_scan, child_id, graph)
                } else {
                    unreachable!();
                }
            }
            // DDL Single Plan
            Operator::CreateTable(_)
            | Operator::CreateIndex(_)
            | Operator::CreateView(_)
            | Operator::DropTable(_)
            | Operator::DropView(_)
            | Operator::DropIndex(_)
            | Operator::Truncate(_)
            | Operator::ShowTable
            | Operator::ShowView
            | Operator::CopyFromFile(_)
            | Operator::CopyToFile(_)
            | Operator::AddColumn(_)
            | Operator::DropColumn(_)
            | Operator::Describe(_) => Ok(used_scan),
        }
    }

    fn recollect_apply(
        referenced_columns: HashSet<&ColumnRef>,
        mut used_scan: HashMap<TableName, TableScanOperator>,
        node_id: HepNodeId,
        graph: &mut HepGraph,
    ) -> Result<HashMap<TableName, TableScanOperator>, DatabaseError> {
        for child_id in graph.children_at(node_id).collect_vec() {
            let copy_references = referenced_columns.clone();
            let copy_scan = used_scan.clone();
            let scan = Self::_apply(copy_references, copy_scan, child_id, graph)?;
            used_scan.extend(scan);
        }
        Ok(used_scan)
    }
}

impl MatchPattern for CorrelatedSubquery {
    fn pattern(&self) -> &Pattern {
        &CORRELATED_SUBQUERY_RULE
    }
}

impl NormalizationRule for CorrelatedSubquery {
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), DatabaseError> {
        Self::_apply(HashSet::new(), HashMap::new(), node_id, graph)?;
        // mark changed to skip this rule batch
        graph.version += 1;

        Ok(())
    }
}
