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

use crate::catalog::TableMeta;
use crate::errors::DatabaseError;
use crate::execution::ExecArena;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, Utf8Type};
use sqlparser::ast::CharLengthUnits;

pub struct ShowTables {
    pub(crate) metas: Option<std::vec::IntoIter<TableMeta>>,
}

impl ShowTables {
    pub(crate) fn next_tuple<'a, T: Transaction>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
    ) -> Result<Option<Tuple>, DatabaseError> {
        if self.metas.is_none() {
            self.metas = Some(arena.transaction_mut().table_metas()?.into_iter());
        }

        let Some(TableMeta { table_name }) = self.metas.as_mut().and_then(|metas| metas.next())
        else {
            return Ok(None);
        };

        let values = vec![DataValue::Utf8 {
            value: table_name.to_string(),
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        }];

        Ok(Some(Tuple::new(None, values)))
    }
}
