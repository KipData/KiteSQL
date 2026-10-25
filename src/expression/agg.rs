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

use kite_sql_serde_macros::ReferenceSerialization;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ReferenceSerialization)]
pub enum AggKind {
    Avg,
    Max,
    Min,
    Sum,
    Count,
    /// SQLite `total()`: a float sum that is 0.0, not NULL, over no rows.
    Total,
    /// SQLite `group_concat()`: non-NULL values joined with `,`.
    GroupConcat,
}

impl AggKind {
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        match name {
            "avg" => Some(Self::Avg),
            "max" => Some(Self::Max),
            "min" => Some(Self::Min),
            "sum" => Some(Self::Sum),
            "count" => Some(Self::Count),
            "total" => Some(Self::Total),
            "group_concat" => Some(Self::GroupConcat),
            _ => None,
        }
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Avg => "avg",
            Self::Max => "max",
            Self::Min => "min",
            Self::Sum => "sum",
            Self::Count => "count",
            Self::Total => "total",
            Self::GroupConcat => "group_concat",
        }
    }

    pub fn allow_distinct(&self) -> bool {
        match self {
            AggKind::Avg => true,
            AggKind::Max => false,
            AggKind::Min => false,
            AggKind::Sum => true,
            AggKind::Count => true,
            AggKind::Total => true,
            AggKind::GroupConcat => true,
        }
    }
}
