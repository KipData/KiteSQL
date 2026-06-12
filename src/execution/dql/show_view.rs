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
use crate::execution::ExecArena;
use crate::storage::{Transaction, ViewIter};
use crate::types::value::{DataValue, Utf8Type};
use crate::types::CharLengthUnits;

pub struct ShowViews<'a, T: Transaction + 'a> {
    pub(crate) metas: Option<ViewIter<'a, T>>,
}

impl<'a, T: Transaction + 'a> ShowViews<'a, T> {
    pub(crate) fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        if self.metas.is_none() {
            let context = arena.read_context();
            let mut state = arena.local_state(plan_arena);
            let (transaction, table_codec) = state.transaction_codec();
            self.metas = Some(transaction.views(
                table_codec,
                context.table_cache(),
                plan_arena.table_arena_cell(),
                context.scala_functions(),
                context.table_functions(),
            )?);
        }

        let Some(view) = self
            .metas
            .as_mut()
            .expect("show views iterator initialized")
            .try_next()?
        else {
            arena.finish();
            return Ok(());
        };

        let output = arena.result_tuple_mut();
        output.pk = None;
        output.values.clear();
        output.values.push(DataValue::Utf8 {
            value: view.name.to_string(),
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        });

        arena.resume();
        Ok(())
    }
}
