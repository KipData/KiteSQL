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

use crate::catalog::view::View;
use crate::catalog::{TableCatalog, TableName};
use crate::optimizer::core::statistics_meta::StatisticsMeta;
use crate::types::index::IndexId;

pub(crate) enum DDLApply {
    UpsertTable {
        table: TableCatalog,
        clear_statistics: bool,
    },
    DropTable {
        name: TableName,
    },
    UpsertView {
        view: View,
    },
    DropView {
        name: TableName,
    },
    UpsertStatisticsMeta {
        table_name: TableName,
        index_id: IndexId,
        meta: StatisticsMeta,
    },
    RemoveStatisticsMeta {
        table_name: TableName,
        index_id: IndexId,
    },
}

impl DDLApply {
    pub(crate) fn upsert_table(table: TableCatalog, clear_statistics: bool) -> Self {
        Self::UpsertTable {
            table,
            clear_statistics,
        }
    }

    pub(crate) fn upsert_view(view: View) -> Self {
        Self::UpsertView { view }
    }
}
