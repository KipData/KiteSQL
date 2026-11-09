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

use crate::dual::dual;
use crate::errors::DatabaseError;
use crate::execution::hash::FxBuildHasher;
use crate::execution::{
    build_read, ExecBuilder, ExecContext, ExecNode, ExecutorNode, ReadExecutor,
};
use crate::planner::operator::set_membership::SetMembershipKind;
use crate::planner::LogicalPlan;
use crate::planner::{ExecMetaArena, MetaArena};
use crate::storage::StorageTransaction;
use crate::types::tuple::TupleImpl;
use crate::types::value::DataValue;
use hashbrown::HashTable;
use std::hash::{BuildHasher, Hash, Hasher};

struct RightRow {
    offset: usize,
    count: usize,
}

pub struct SetMembership<'a> {
    kind: SetMembershipKind,
    left_input: ExecNode<'a>,
    right_input: ExecNode<'a>,
    hasher: FxBuildHasher,
    right_rows: Vec<DataValue>,
    right_counts: HashTable<RightRow>,
    built: bool,
}

impl<'a> ReadExecutor<'a> for SetMembership<'a> {
    type Input = (SetMembershipKind, &'a LogicalPlan, &'a LogicalPlan);

    fn into_executor<B: ExecBuilder>(
        (kind, left_plan, right_plan): Self::Input,
        arena: &mut B,
        plan_arena: &(dyn MetaArena + 'a),
    ) -> ExecNode<'a> {
        let left_input = build_read(arena, plan_arena, left_plan);
        let right_input = build_read(arena, plan_arena, right_plan);
        ExecNode::SetMembership(Box::new(SetMembership {
            kind,
            left_input,
            right_input,
            hasher: FxBuildHasher::default(),
            right_rows: Vec::new(),
            right_counts: HashTable::new(),
            built: false,
        }))
    }
}

#[dual]
impl<'a, T: StorageTransaction + 'a> ExecutorNode<'a, T> for SetMembership<'a> {
    async fn reset<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecContext<'_, 'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        self.built = false;
        self.right_rows.clear();
        self.right_counts.clear();
        self.left_input.reset(arena, plan_arena).await?;
        self.right_input.reset(arena, plan_arena).await
    }

    async fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecContext<'_, 'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        if !self.built {
            while self.right_input.next_tuple(arena, plan_arena).await? {
                let tuple = arena.result_tuple();
                let width = tuple.len();
                let hash = hash_row(&self.hasher, tuple.iter());
                if let Some(row) = self.find_mut(hash, tuple) {
                    row.count += 1;
                    continue;
                }
                let offset = self.right_rows.len();
                // SAFETY: the rows live until `reset` clears them, before the next execution.
                self.right_rows
                    .extend(tuple.iter().map(|value| unsafe { value.clone_in_txn() }));
                let (hasher, rows) = (&self.hasher, &self.right_rows);
                self.right_counts
                    .insert_unique(hash, RightRow { offset, count: 1 }, |row| {
                        hash_row(hasher, &rows[row.offset..][..width])
                    });
            }
            self.built = true;
        }

        loop {
            if !self.left_input.next_tuple(arena, plan_arena).await? {
                arena.finish();
                return Ok(());
            }

            let matched = self.consume_right_match(arena.result_tuple());
            let should_emit = match self.kind {
                SetMembershipKind::Except => !matched,
                SetMembershipKind::Intersect => matched,
            };

            if should_emit {
                arena.resume();
                return Ok(());
            }
        }
    }
}

fn hash_row<'v>(hasher: &FxBuildHasher, values: impl IntoIterator<Item = &'v DataValue>) -> u64 {
    let mut state = hasher.build_hasher();
    for value in values {
        value.hash(&mut state);
    }
    state.finish()
}

impl SetMembership<'_> {
    fn find_mut(&mut self, hash: u64, tuple: &TupleImpl) -> Option<&mut RightRow> {
        let rows = &self.right_rows;
        self.right_counts.find_mut(hash, |row| {
            rows[row.offset..][..tuple.len()].iter().eq(tuple.iter())
        })
    }

    fn consume_right_match(&mut self, tuple: &TupleImpl) -> bool {
        let hash = hash_row(&self.hasher, tuple.iter());
        match self.find_mut(hash, tuple) {
            Some(row) if row.count > 0 => {
                row.count -= 1;
                true
            }
            _ => false,
        }
    }
}
