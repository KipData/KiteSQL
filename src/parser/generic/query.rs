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

use super::{unsupported, Lower};
use crate::ast;
use crate::errors::DatabaseError;
use sqlparser::ast::{
    CteAsMaterialized, Distinct, GroupByExpr, Join, JoinConstraint, JoinOperator, LimitClause,
    OrderByKind, Query, Select, SelectItem, SelectItemQualifiedWildcardKind, SetExpr, SetOperator,
    SetQuantifier, TableAlias, TableFactor, TableWithJoins,
};

impl Lower<'_> {
    pub(super) fn query(&self, query: Query) -> Result<ast::Query, DatabaseError> {
        let with = query
            .with
            .map(|with| {
                let cte_tables = with
                    .cte_tables
                    .into_iter()
                    .map(|cte| {
                        if cte.from.is_some() {
                            return Err(unsupported("CTE FROM clauses are not supported"));
                        }
                        if matches!(cte.materialized, Some(CteAsMaterialized::Materialized)) {
                            return Err(unsupported("materialized CTEs are not supported"));
                        }
                        Ok(ast::Cte {
                            alias: self.table_alias(cte.alias),
                            query: self.query(*cte.query).map(Box::new)?,
                        })
                    })
                    .collect::<Result<_, _>>()?;
                Ok::<_, DatabaseError>(ast::With {
                    recursive: with.recursive,
                    cte_tables,
                })
            })
            .transpose()?;
        let order_by = query
            .order_by
            .map(|order_by| match order_by.kind {
                OrderByKind::Expressions(exprs) => exprs
                    .into_iter()
                    .map(|expr| self.order_by(expr))
                    .collect::<Result<_, _>>(),
                OrderByKind::All(_) => Err(unsupported("ORDER BY ALL is not supported")),
            })
            .transpose()?;
        let limit = query
            .limit_clause
            .map(|limit| match limit {
                LimitClause::LimitOffset {
                    limit,
                    offset,
                    limit_by,
                } => {
                    if !limit_by.is_empty() {
                        return Err(unsupported("LIMIT BY is not supported"));
                    }
                    Ok(ast::Limit {
                        limit: limit.map(|expr| self.expr(expr)).transpose()?,
                        offset: offset.map(|offset| self.expr(offset.value)).transpose()?,
                    })
                }
                LimitClause::OffsetCommaLimit { offset, limit } => Ok(ast::Limit {
                    limit: Some(self.expr(limit)?),
                    offset: Some(self.expr(offset)?),
                }),
            })
            .transpose()?;

        Ok(ast::Query {
            with,
            body: self.set_expr(*query.body).map(Box::new)?,
            order_by,
            limit,
            for_update: !query.locks.is_empty(),
        })
    }

    fn set_expr(&self, set_expr: SetExpr) -> Result<ast::SetExpr, DatabaseError> {
        Ok(match set_expr {
            SetExpr::Select(select) => ast::SetExpr::Select(self.select(*select).map(Box::new)?),
            SetExpr::Query(query) => ast::SetExpr::Query(self.query(*query).map(Box::new)?),
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => {
                let all = match set_quantifier {
                    SetQuantifier::All => true,
                    SetQuantifier::Distinct | SetQuantifier::None => false,
                    SetQuantifier::ByName
                    | SetQuantifier::AllByName
                    | SetQuantifier::DistinctByName => {
                        return Err(unsupported("set quantifier BY NAME is not supported"))
                    }
                };
                let op = match op {
                    SetOperator::Union => ast::SetOperator::Union,
                    SetOperator::Except => ast::SetOperator::Except,
                    SetOperator::Intersect => ast::SetOperator::Intersect,
                    op => return Err(unsupported(format!("set operator: {op:?}"))),
                };
                ast::SetExpr::SetOperation {
                    op,
                    all,
                    left: self.set_expr(*left).map(Box::new)?,
                    right: self.set_expr(*right).map(Box::new)?,
                }
            }
            SetExpr::Values(values) => ast::SetExpr::Values(
                values
                    .rows
                    .into_iter()
                    .map(|row| row.into_iter().map(|expr| self.expr(expr)).collect())
                    .collect::<Result<_, _>>()?,
            ),
            set_expr => return Err(unsupported(format!("query body: {set_expr:?}"))),
        })
    }

    fn select(&self, select: Select) -> Result<ast::Select, DatabaseError> {
        let Select {
            optimizer_hint,
            distinct,
            projection,
            into,
            from,
            selection,
            group_by,
            having,
            named_window,
            qualify,
            ..
        } = select;
        if !named_window.is_empty() {
            return Err(unsupported("named windows are not supported"));
        }
        if qualify.is_some() {
            return Err(unsupported("QUALIFY is not supported"));
        }
        let group_by = match group_by {
            GroupByExpr::Expressions(exprs, modifiers) => {
                if !modifiers.is_empty() {
                    return Err(unsupported("GROUP BY modifiers are not supported"));
                }
                exprs
                    .into_iter()
                    .map(|expr| self.expr(expr))
                    .collect::<Result<_, _>>()?
            }
            GroupByExpr::All(_) => return Err(unsupported("GROUP BY ALL is not supported")),
        };

        Ok(ast::Select {
            optimizer_hint: optimizer_hint.map(|hint| hint.text),
            distinct: matches!(distinct, Some(Distinct::Distinct)),
            projection: projection
                .into_iter()
                .map(|item| self.select_item(item))
                .collect::<Result<_, _>>()?,
            into: into.map(|into| self.object_name(into.name)).transpose()?,
            from: self.from(from)?,
            selection: selection.map(|expr| self.expr(expr)).transpose()?,
            group_by,
            having: having.map(|expr| self.expr(expr)).transpose()?,
        })
    }

    fn select_item(&self, item: SelectItem) -> Result<ast::SelectItem, DatabaseError> {
        Ok(match item {
            SelectItem::UnnamedExpr(expr) => ast::SelectItem::UnnamedExpr(self.expr(expr)?),
            SelectItem::ExprWithAlias { expr, alias } => ast::SelectItem::ExprWithAlias {
                expr: self.expr(expr)?,
                alias: self.ident(alias),
            },
            SelectItem::Wildcard(_) => ast::SelectItem::Wildcard,
            SelectItem::QualifiedWildcard(kind, _) => match kind {
                SelectItemQualifiedWildcardKind::ObjectName(name) => {
                    ast::SelectItem::QualifiedWildcard(self.object_name(name)?)
                }
                SelectItemQualifiedWildcardKind::Expr(expr) => {
                    return Err(unsupported(format!("qualified wildcard expr: {expr}")))
                }
            },
        })
    }

    pub(super) fn from(
        &self,
        from: Vec<TableWithJoins>,
    ) -> Result<Vec<ast::TableWithJoins>, DatabaseError> {
        from.into_iter()
            .map(|table| self.table_with_joins(table))
            .collect()
    }

    pub(super) fn table_with_joins(
        &self,
        table: TableWithJoins,
    ) -> Result<ast::TableWithJoins, DatabaseError> {
        Ok(ast::TableWithJoins {
            relation: self.table_factor(table.relation)?,
            joins: table
                .joins
                .into_iter()
                .map(|join| self.join(join))
                .collect::<Result<_, _>>()?,
        })
    }

    pub(super) fn table_alias(&self, alias: TableAlias) -> ast::TableAlias {
        ast::TableAlias {
            name: self.ident(alias.name),
            columns: alias
                .columns
                .into_iter()
                .map(|column| self.ident(column.name))
                .collect(),
        }
    }

    fn table_factor(&self, table: TableFactor) -> Result<ast::TableFactor, DatabaseError> {
        Ok(match table {
            TableFactor::Table { name, alias, .. } => ast::TableFactor::Table {
                name: self.object_name(name)?,
                alias: alias.map(|alias| self.table_alias(alias)),
            },
            TableFactor::Derived { lateral: true, .. } => {
                return Err(unsupported("LATERAL derived tables are not supported"))
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => ast::TableFactor::Derived {
                subquery: self.query(*subquery).map(Box::new)?,
                alias: alias.map(|alias| self.table_alias(alias)),
            },
            TableFactor::TableFunction { expr, alias } => ast::TableFactor::TableFunction {
                expr: self.expr(expr)?,
                alias: alias.map(|alias| self.table_alias(alias)),
            },
            table => return Err(unsupported(format!("{table:#?}"))),
        })
    }

    fn join(&self, join: Join) -> Result<ast::Join, DatabaseError> {
        let (join_operator, constraint) = match join.join_operator {
            JoinOperator::Join(constraint)
            | JoinOperator::Inner(constraint)
            | JoinOperator::StraightJoin(constraint) => (ast::JoinOperator::Inner, constraint),
            JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => {
                (ast::JoinOperator::LeftOuter, constraint)
            }
            JoinOperator::Right(constraint) | JoinOperator::RightOuter(constraint) => {
                (ast::JoinOperator::RightOuter, constraint)
            }
            JoinOperator::FullOuter(constraint) => (ast::JoinOperator::FullOuter, constraint),
            JoinOperator::CrossJoin(constraint) => (ast::JoinOperator::Cross, constraint),
            join_operator => return Err(unsupported(format!("{join_operator:?}"))),
        };
        let constraint = match constraint {
            JoinConstraint::On(expr) => ast::JoinConstraint::On(self.expr(expr)?),
            JoinConstraint::Using(names) => ast::JoinConstraint::Using(
                names
                    .into_iter()
                    .map(|name| self.object_name(name))
                    .collect::<Result<_, _>>()?,
            ),
            JoinConstraint::Natural => ast::JoinConstraint::Natural,
            JoinConstraint::None => ast::JoinConstraint::None,
        };
        Ok(ast::Join {
            relation: self.table_factor(join.relation)?,
            join_operator,
            constraint,
        })
    }
}
