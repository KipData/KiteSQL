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
use crate::errors::DatabaseError;
use crate::execution::{ExecArena, ExecId};
use crate::storage::Transaction;
use crate::types::value::{DataValue, Utf8Type};
use sqlparser::ast::CharLengthUnits;

pub struct ShowViews {
    pub(crate) metas: Option<std::vec::IntoIter<View>>,
}

impl ShowViews {
    pub(crate) fn next_tuple<'a, T: Transaction>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        id: ExecId,
    ) -> Result<(), DatabaseError> {
        let _ = id;
        if self.metas.is_none() {
            self.metas = Some(
                arena
                    .transaction_mut()
                    .views(arena.table_cache())?
                    .into_iter(),
            );
        }

        let Some(View { name, .. }) = self.metas.as_mut().and_then(|metas| metas.next()) else {
            arena.finish();
            return Ok(());
        };

        let output = arena.result_tuple_mut();
        output.pk = None;
        output.values.clear();
        output.values.push(DataValue::Utf8 {
            value: name.to_string(),
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        });

        arena.resume();
        Ok(())
    }
}
