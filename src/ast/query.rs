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

use super::{Expr, Ident, ObjectName, OrderByExpr};

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub with: Option<With>,
    pub body: Box<SetExpr>,
    pub order_by: Option<Vec<OrderByExpr>>,
    pub limit: Option<Limit>,
    /// A locking read, such as `FOR UPDATE`, which needs a write transaction.
    pub for_update: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct With {
    pub recursive: bool,
    pub cte_tables: Vec<Cte>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Cte {
    pub alias: TableAlias,
    pub query: Box<Query>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Limit {
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetExpr {
    Select(Box<Select>),
    Query(Box<Query>),
    SetOperation {
        op: SetOperator,
        /// `ALL`; without it the result has no duplicate rows.
        all: bool,
        left: Box<SetExpr>,
        right: Box<SetExpr>,
    },
    /// `VALUES (..), (..)`
    Values(Vec<Vec<Expr>>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOperator {
    Union,
    Except,
    Intersect,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Select {
    /// The text of an optimizer hint comment, `/*+ .. */`.
    pub optimizer_hint: Option<String>,
    pub distinct: bool,
    pub projection: Vec<SelectItem>,
    /// `SELECT .. INTO table`
    pub into: Option<ObjectName>,
    pub from: Vec<TableWithJoins>,
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    UnnamedExpr(Expr),
    ExprWithAlias {
        expr: Expr,
        alias: Ident,
    },
    /// `*`
    Wildcard,
    /// `table.*`
    QualifiedWildcard(ObjectName),
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableWithJoins {
    pub relation: TableFactor,
    pub joins: Vec<Join>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableFactor {
    Table {
        name: ObjectName,
        alias: Option<TableAlias>,
    },
    Derived {
        subquery: Box<Query>,
        alias: Option<TableAlias>,
    },
    /// A table function call, such as `TABLE(numbers(3))`.
    TableFunction {
        expr: Expr,
        alias: Option<TableAlias>,
    },
}

/// `AS name (column, ..)`
#[derive(Debug, Clone, PartialEq)]
pub struct TableAlias {
    pub name: Ident,
    pub columns: Vec<Ident>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    pub relation: TableFactor,
    pub join_operator: JoinOperator,
    pub constraint: JoinConstraint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinOperator {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Cross,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinConstraint {
    On(Expr),
    Using(Vec<ObjectName>),
    Natural,
    None,
}
