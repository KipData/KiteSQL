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
use crate::execution::{ExecArena, ExecNode, ExecutorNode, IterSlot, ReadExecutor, StorageIter};
use crate::planner::{ExecMetaArena, MetaArena};
use crate::storage::StorageTransaction;
use crate::types::value::{DataValue, Utf8Type};
use crate::types::CharLengthUnits;

pub struct ShowViews {
    metas: IterSlot,
}

impl<'a, T: StorageTransaction + 'a> ReadExecutor<'a, T> for ShowViews {
    type Input = ();

    fn into_executor(
        _: Self::Input,
        arena: &mut ExecArena<'a, '_, T>,
        _: &mut (dyn MetaArena + 'a),
    ) -> ExecNode<'a> {
        let metas = arena.alloc_iter();
        ExecNode::ShowViews(Box::new(ShowViews { metas }))
    }
}

impl<'a, T: StorageTransaction + 'a> ExecutorNode<'a, T> for ShowViews {
    fn reset<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, '_, T>,
        _plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        *arena.iter_mut(self.metas) = StorageIter::Empty;
        Ok(())
    }

    fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        if arena.iter_mut(self.metas).is_empty() {
            let mut state = arena.local_state(plan_arena);
            let (transaction, table_codec) = state.transaction_codec();
            let metas = transaction.views(table_codec)?;
            state.iters[self.metas] = StorageIter::View(metas);
        }

        let metas = arena.iter_mut(self.metas).as_view();
        let Some(name) = metas.try_next(plan_arena)? else {
            arena.finish();
            return Ok(());
        };

        let output = arena.output_tuple_mut();
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
