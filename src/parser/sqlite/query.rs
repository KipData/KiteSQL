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

use super::expr::{expr, exprs, function, order_by};
use super::{name, object_name, qualified_name, unsupported};
use crate::ast;
use crate::errors::DatabaseError;
use sqlite3_parser::ast::{
    As, CompoundOperator, Distinctness, FromClause, JoinConstraint, JoinOperator, JoinType,
    Materialized, Name, OneSelect, ResultColumn, Select, SelectTable, With,
};

pub(super) fn select(select: &Select<'_>) -> Result<ast::Query, DatabaseError> {
    let mut body = one_select(&select.body.select)?;
    for compound in select.body.compounds.iter().flatten() {
        let (op, all) = match compound.operator {
            CompoundOperator::Union => (ast::SetOperator::Union, false),
            CompoundOperator::UnionAll => (ast::SetOperator::Union, true),
            CompoundOperator::Except => (ast::SetOperator::Except, false),
            CompoundOperator::Intersect => (ast::SetOperator::Intersect, false),
        };
        body = ast::SetExpr::SetOperation {
            op,
            all,
            left: Box::new(body),
            right: Box::new(one_select(&compound.select)?),
        };
    }

    Ok(ast::Query {
        with: select.with.as_ref().map(with).transpose()?,
        body: Box::new(body),
        order_by: select
            .order_by
            .map(|columns| columns.iter().map(order_by).collect())
            .transpose()?,
        limit: select
            .limit
            .map(|limit| {
                Ok::<_, DatabaseError>(ast::Limit {
                    limit: Some(expr(&limit.expr)?),
                    offset: limit.offset.as_ref().map(expr).transpose()?,
                })
            })
            .transpose()?,
        for_update: false,
    })
}

pub(super) fn with(with: &With<'_>) -> Result<ast::With, DatabaseError> {
    let cte_tables = with
        .ctes
        .iter()
        .map(|cte| {
            if matches!(cte.materialized, Materialized::Yes) {
                return Err(unsupported("materialized CTEs are not supported"));
            }
            Ok(ast::Cte {
                alias: ast::TableAlias {
                    name: name(&cte.tbl_name),
                    columns: cte
                        .columns
                        .unwrap_or_default()
                        .iter()
                        .map(|column| name(&column.col_name))
                        .collect(),
                },
                query: select(cte.select).map(Box::new)?,
            })
        })
        .collect::<Result<_, _>>()?;
    Ok(ast::With {
        recursive: with.recursive,
        cte_tables,
    })
}

fn one_select(select: &OneSelect<'_>) -> Result<ast::SetExpr, DatabaseError> {
    match select {
        OneSelect::Values(rows) => rows
            .iter()
            .map(|row| exprs(row))
            .collect::<Result<_, _>>()
            .map(ast::SetExpr::Values),
        OneSelect::Select {
            distinctness,
            columns,
            from,
            where_clause,
            group_by,
            having,
            window_clause,
        } => {
            if window_clause.is_some() {
                return Err(unsupported("named windows are not supported"));
            }
            let projection = columns
                .iter()
                .map(result_column)
                .collect::<Result<_, DatabaseError>>()?;

            Ok(ast::SetExpr::Select(Box::new(ast::Select {
                optimizer_hint: None,
                distinct: matches!(distinctness, Some(Distinctness::Distinct)),
                projection,
                into: None,
                from: from
                    .as_ref()
                    .map(self::from)
                    .transpose()?
                    .unwrap_or_default(),
                selection: where_clause.map(expr).transpose()?,
                group_by: exprs(group_by.unwrap_or_default())?,
                having: having.map(expr).transpose()?,
            })))
        }
    }
}

fn result_column(column: &ResultColumn<'_>) -> Result<ast::SelectItem, DatabaseError> {
    Ok(match column {
        ResultColumn::Expr(column, None) => ast::SelectItem::UnnamedExpr(expr(column)?),
        ResultColumn::Expr(column, Some(alias)) => ast::SelectItem::ExprWithAlias {
            expr: expr(column)?,
            alias: name(alias_name(alias)),
        },
        ResultColumn::Star => ast::SelectItem::Wildcard,
        ResultColumn::TableStar(table) => ast::SelectItem::QualifiedWildcard(object_name(table)),
    })
}

fn alias_name<'a>(alias: &'a As<'a>) -> &'a Name<'a> {
    match alias {
        As::As(name) | As::Elided(name) => name,
    }
}

fn table_alias(alias: Option<&As<'_>>) -> Option<ast::TableAlias> {
    alias.map(|alias| ast::TableAlias {
        name: name(alias_name(alias)),
        columns: Vec::new(),
    })
}

pub(super) fn from(from: &FromClause<'_>) -> Result<Vec<ast::TableWithJoins>, DatabaseError> {
    let Some(first) = from.select else {
        return Ok(Vec::new());
    };
    let mut tables = vec![ast::TableWithJoins {
        relation: table(first)?,
        joins: Vec::new(),
    }];
    for join in from.joins.iter().flatten() {
        let relation = table(&join.table)?;
        let join_type = match join.operator {
            // `FROM a, b` lists another table, as in standard SQL.
            JoinOperator::Comma => {
                tables.push(ast::TableWithJoins {
                    relation,
                    joins: Vec::new(),
                });
                continue;
            }
            JoinOperator::TypedJoin(join_type) => join_type.unwrap_or(JoinType::INNER),
        };
        let join_operator = if join_type.contains(JoinType::LEFT | JoinType::RIGHT) {
            ast::JoinOperator::FullOuter
        } else if join_type.contains(JoinType::LEFT) {
            ast::JoinOperator::LeftOuter
        } else if join_type.contains(JoinType::RIGHT) {
            ast::JoinOperator::RightOuter
        } else if join_type.contains(JoinType::CROSS) {
            ast::JoinOperator::Cross
        } else {
            ast::JoinOperator::Inner
        };
        let constraint = match &join.constraint {
            _ if join_type.contains(JoinType::NATURAL) => ast::JoinConstraint::Natural,
            Some(JoinConstraint::On(on)) => ast::JoinConstraint::On(expr(on)?),
            Some(JoinConstraint::Using(names)) => {
                ast::JoinConstraint::Using(names.iter().map(object_name).collect())
            }
            None => ast::JoinConstraint::None,
        };
        tables.last_mut().unwrap().joins.push(ast::Join {
            relation,
            join_operator,
            constraint,
        });
    }
    Ok(tables)
}

fn table(table: &SelectTable<'_>) -> Result<ast::TableFactor, DatabaseError> {
    Ok(match table {
        SelectTable::Table(name, alias, _) => ast::TableFactor::Table {
            name: qualified_name(name),
            alias: table_alias(alias.as_ref()).or_else(|| {
                name.alias.as_ref().map(|alias| ast::TableAlias {
                    name: self::name(alias),
                    columns: Vec::new(),
                })
            }),
        },
        SelectTable::TableCall(name, args, alias) => ast::TableFactor::TableFunction {
            expr: function(name.name.0, args.unwrap_or_default(), false, None)?,
            alias: table_alias(alias.as_ref()),
        },
        SelectTable::Select(subquery, alias) => ast::TableFactor::Derived {
            subquery: select(subquery).map(Box::new)?,
            alias: table_alias(alias.as_ref()),
        },
        SelectTable::Sub(..) => return Err(unsupported("parenthesized joins are not supported")),
    })
}
