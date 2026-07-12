// Copyright 2024 KipData/KiteSQL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::expression::agg::AggKind;
use crate::expression::ScalarExpression;
use crate::planner::operator::sort::SortField;
use crate::types::LogicalType;
use kite_sql_serde_macros::ReferenceSerialization;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ReferenceSerialization)]
pub enum WindowFunctionKind {
    RowNumber,
    Rank,
    DenseRank,
    Aggregate(AggKind),
}

impl WindowFunctionKind {
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        match name {
            "row_number" => Some(Self::RowNumber),
            "rank" => Some(Self::Rank),
            "dense_rank" => Some(Self::DenseRank),
            name => AggKind::from_name(name).map(Self::Aggregate),
        }
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::RowNumber => "row_number",
            Self::Rank => "rank",
            Self::DenseRank => "dense_rank",
            Self::Aggregate(kind) => kind.name(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, ReferenceSerialization)]
pub struct WindowFunction {
    pub kind: WindowFunctionKind,
    pub args: Vec<ScalarExpression>,
    pub ty: LogicalType,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, ReferenceSerialization)]
pub struct WindowSpec {
    pub partition_by: Vec<ScalarExpression>,
    pub order_by: Vec<SortField>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, ReferenceSerialization)]
pub struct WindowCall {
    pub function: WindowFunction,
    pub spec: WindowSpec,
}
