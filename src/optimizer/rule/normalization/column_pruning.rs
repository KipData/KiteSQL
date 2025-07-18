use crate::catalog::ColumnSummary;
use crate::errors::DatabaseError;
use crate::expression::agg::AggKind;
use crate::expression::visitor::Visitor;
use crate::expression::{HasCountStar, ScalarExpression};
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{MatchPattern, NormalizationRule};
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::planner::operator::Operator;
use crate::types::value::{DataValue, Utf8Type};
use crate::types::LogicalType;
use itertools::Itertools;
use sqlparser::ast::CharLengthUnits;
use std::collections::HashSet;
use std::sync::LazyLock;

static COLUMN_PRUNING_RULE: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |_| true,
    children: PatternChildrenPredicate::None,
});

#[derive(Clone)]
pub struct ColumnPruning;

macro_rules! trans_references {
    ($columns:expr) => {{
        let mut column_references = HashSet::with_capacity($columns.len());
        for column in $columns {
            column_references.insert(column.summary());
        }
        column_references
    }};
}

impl ColumnPruning {
    fn clear_exprs(column_references: &HashSet<&ColumnSummary>, exprs: &mut Vec<ScalarExpression>) {
        exprs.retain(|expr| {
            if column_references.contains(expr.output_column().summary()) {
                return true;
            }
            expr.referenced_columns(false)
                .iter()
                .any(|column| column_references.contains(column.summary()))
        })
    }

    fn _apply(
        column_references: HashSet<&ColumnSummary>,
        all_referenced: bool,
        node_id: HepNodeId,
        graph: &mut HepGraph,
    ) -> Result<(), DatabaseError> {
        let operator = graph.operator_mut(node_id);

        match operator {
            Operator::Aggregate(op) => {
                if !all_referenced {
                    Self::clear_exprs(&column_references, &mut op.agg_calls);

                    if op.agg_calls.is_empty() && op.groupby_exprs.is_empty() {
                        let value = DataValue::Utf8 {
                            value: "*".to_string(),
                            ty: Utf8Type::Variable(None),
                            unit: CharLengthUnits::Characters,
                        };
                        // only single COUNT(*) is not depend on any column
                        // removed all expressions from the aggregate: push a COUNT(*)
                        op.agg_calls.push(ScalarExpression::AggCall {
                            distinct: false,
                            kind: AggKind::Count,
                            args: vec![ScalarExpression::Constant(value)],
                            ty: LogicalType::Integer,
                        })
                    }
                }
                let is_distinct = op.is_distinct;
                let referenced_columns = operator.referenced_columns(false);
                let mut new_column_references = trans_references!(&referenced_columns);
                // on distinct
                if is_distinct {
                    for summary in column_references {
                        new_column_references.insert(summary);
                    }
                }

                Self::recollect_apply(new_column_references, false, node_id, graph)?;
            }
            Operator::Project(op) => {
                let mut has_count_star = HasCountStar::default();
                for expr in &op.exprs {
                    has_count_star.visit(expr)?;
                }
                if !has_count_star.value {
                    if !all_referenced {
                        Self::clear_exprs(&column_references, &mut op.exprs);
                    }
                    let referenced_columns = operator.referenced_columns(false);
                    let new_column_references = trans_references!(&referenced_columns);

                    Self::recollect_apply(new_column_references, false, node_id, graph)?;
                }
            }
            Operator::TableScan(op) => {
                if !all_referenced {
                    op.columns
                        .retain(|_, column| column_references.contains(column.summary()));
                }
            }
            Operator::Sort(_)
            | Operator::Limit(_)
            | Operator::Join(_)
            | Operator::Filter(_)
            | Operator::Union(_) => {
                let temp_columns = operator.referenced_columns(false);
                // why?
                let mut column_references = column_references;
                for column in temp_columns.iter() {
                    column_references.insert(column.summary());
                }
                for child_id in graph.children_at(node_id).collect_vec() {
                    let copy_references = column_references.clone();

                    Self::_apply(copy_references, all_referenced, child_id, graph)?;
                }
            }
            // Last Operator
            Operator::Dummy | Operator::Values(_) | Operator::FunctionScan(_) => (),
            Operator::Explain => {
                if let Some(child_id) = graph.eldest_child_at(node_id) {
                    Self::_apply(column_references, true, child_id, graph)?;
                } else {
                    unreachable!()
                }
            }
            // DDL Based on Other Plan
            Operator::Insert(_)
            | Operator::Update(_)
            | Operator::Delete(_)
            | Operator::Analyze(_) => {
                let referenced_columns = operator.referenced_columns(false);
                let new_column_references = trans_references!(&referenced_columns);

                if let Some(child_id) = graph.eldest_child_at(node_id) {
                    Self::recollect_apply(new_column_references, true, child_id, graph)?;
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
            | Operator::Truncate(_)
            | Operator::ShowTable
            | Operator::ShowView
            | Operator::CopyFromFile(_)
            | Operator::CopyToFile(_)
            | Operator::AddColumn(_)
            | Operator::DropColumn(_)
            | Operator::Describe(_) => (),
        }

        Ok(())
    }

    fn recollect_apply(
        referenced_columns: HashSet<&ColumnSummary>,
        all_referenced: bool,
        node_id: HepNodeId,
        graph: &mut HepGraph,
    ) -> Result<(), DatabaseError> {
        for child_id in graph.children_at(node_id).collect_vec() {
            let copy_references: HashSet<&ColumnSummary> = referenced_columns.clone();

            Self::_apply(copy_references, all_referenced, child_id, graph)?;
        }
        Ok(())
    }
}

impl MatchPattern for ColumnPruning {
    fn pattern(&self) -> &Pattern {
        &COLUMN_PRUNING_RULE
    }
}

impl NormalizationRule for ColumnPruning {
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), DatabaseError> {
        Self::_apply(HashSet::new(), true, node_id, graph)?;
        // mark changed to skip this rule batch
        graph.version += 1;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::binder::test::build_t1_table;
    use crate::errors::DatabaseError;
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizer;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::join::JoinCondition;
    use crate::planner::operator::Operator;
    use crate::planner::Childrens;
    use crate::storage::rocksdb::RocksTransaction;

    #[test]
    fn test_column_pruning() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan = table_state.plan("select c1, c3 from t1 left join t2 on c1 = c3")?;

        let best_plan = HepOptimizer::new(plan.clone())
            .batch(
                "test_column_pruning".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::ColumnPruning],
            )
            .find_best::<RocksTransaction>(None)?;

        assert!(matches!(best_plan.childrens.as_ref(), Childrens::Only(_)));
        match best_plan.operator {
            Operator::Project(op) => {
                assert_eq!(op.exprs.len(), 2);
            }
            _ => unreachable!("Should be a project operator"),
        }
        let join_op = best_plan.childrens.pop_only();
        match &join_op.operator {
            Operator::Join(op) => match &op.on {
                JoinCondition::On { on, filter } => {
                    assert_eq!(on.len(), 1);
                    assert!(filter.is_none());
                }
                _ => unreachable!("Should be a on condition"),
            },
            _ => unreachable!("Should be a join operator"),
        }
        assert!(matches!(
            join_op.childrens.as_ref(),
            Childrens::Twins { .. }
        ));

        for grandson_plan in join_op.childrens.iter() {
            match &grandson_plan.operator {
                Operator::TableScan(op) => {
                    assert_eq!(op.columns.len(), 1);
                }
                _ => unreachable!("Should be a scan operator"),
            }
        }

        Ok(())
    }
}
