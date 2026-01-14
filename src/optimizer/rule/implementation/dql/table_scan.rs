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

use crate::errors::DatabaseError;
use crate::optimizer::core::memo::{Expression, GroupExpression};
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{ImplementationRule, MatchPattern};
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::planner::operator::{Operator, PhysicalOption};
use crate::storage::Transaction;
use crate::types::index::IndexType;
use std::sync::LazyLock;

static TABLE_SCAN_PATTERN: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |op| matches!(op, Operator::TableScan(_)),
    children: PatternChildrenPredicate::None,
});

#[derive(Clone)]
pub struct SeqScanImplementation;

impl MatchPattern for SeqScanImplementation {
    fn pattern(&self) -> &Pattern {
        &TABLE_SCAN_PATTERN
    }
}

impl<T: Transaction> ImplementationRule<T> for SeqScanImplementation {
    fn to_expression(
        &self,
        op: &Operator,
        loader: &StatisticMetaLoader<T>,
        group_expr: &mut GroupExpression,
    ) -> Result<(), DatabaseError> {
        if let Operator::TableScan(scan_op) = op {
            let cost = scan_op
                .index_infos
                .iter()
                .find(|index_info| index_info.meta.column_ids == scan_op.primary_keys)
                .map(|index_info| loader.load(&scan_op.table_name, index_info.meta.id))
                .transpose()?
                .flatten()
                .map(|statistics_meta| statistics_meta.histogram().values_len());

            group_expr.append_expr(Expression {
                op: PhysicalOption::SeqScan,
                cost,
            });

            Ok(())
        } else {
            unreachable!("invalid operator!")
        }
    }
}

pub struct IndexScanImplementation;

impl MatchPattern for IndexScanImplementation {
    fn pattern(&self) -> &Pattern {
        &TABLE_SCAN_PATTERN
    }
}

impl<T: Transaction> ImplementationRule<T> for IndexScanImplementation {
    fn to_expression(
        &self,
        op: &Operator,
        loader: &StatisticMetaLoader<'_, T>,
        group_expr: &mut GroupExpression,
    ) -> Result<(), DatabaseError> {
        if let Operator::TableScan(scan_op) = op {
            for index_info in scan_op.index_infos.iter() {
                if index_info.range.is_none() {
                    continue;
                }
                let mut cost = None;

                if let Some(range) = &index_info.range {
                    if let Some(statistics_meta) =
                        loader.load(&scan_op.table_name, index_info.meta.id)?
                    {
                        let mut row_count = statistics_meta.collect_count(range)?;

                        if index_info.covered_deserializers.is_none()
                            && !matches!(index_info.meta.ty, IndexType::PrimaryKey { .. })
                        {
                            // need to return table query(non-covering index)
                            row_count *= 2;
                        }
                        cost = Some(row_count);
                    }
                }

                group_expr.append_expr(Expression {
                    op: PhysicalOption::IndexScan(index_info.clone()),
                    cost,
                })
            }

            Ok(())
        } else {
            unreachable!("invalid operator!")
        }
    }
}
