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
use crate::execution::dql::join::hash::{
    filter, join_rows, JoinProbeState, LeftDropState, LeftDropTuples, ProbeState,
};
use crate::execution::dql::join::hash_join::BuildState;
use crate::planner::ExprRef;
use crate::planner::MetaArena;
use crate::types::bitset::BitSet;
use crate::types::tuple::{SplitTupleRef, TupleImpl};
use crate::types::value::DataValue;

pub(crate) struct FullJoinState {
    pub(crate) left_schema_len: usize,
    pub(crate) right_schema_len: usize,
    pub(crate) bits: BitSet,
}

impl JoinProbeState for FullJoinState {
    fn probe_next(
        &mut self,
        probe_state: &mut ProbeState,
        build_state: Option<&mut BuildState>,
        filter_expr: Option<&ExprRef>,
        plan_arena: &(dyn MetaArena + '_),
    ) -> Result<Option<TupleImpl>, DatabaseError> {
        if probe_state.is_keys_has_null {
            if probe_state.emitted_unmatched {
                probe_state.finished = true;
                return Ok(None);
            }
            probe_state.emitted_unmatched = true;
            probe_state.finished = true;
            probe_state
                .probe_tuple
                .prepend(std::iter::repeat_n(DataValue::Null, self.left_schema_len));
            return Ok(Some(std::mem::take(&mut probe_state.probe_tuple)));
        }

        let Some(build_state) = build_state else {
            if probe_state.emitted_unmatched {
                probe_state.finished = true;
                return Ok(None);
            }
            probe_state.emitted_unmatched = true;
            probe_state.finished = true;
            probe_state
                .probe_tuple
                .prepend(std::iter::repeat_n(DataValue::Null, self.left_schema_len));
            return Ok(Some(std::mem::take(&mut probe_state.probe_tuple)));
        };

        while probe_state.index < build_state.tuples.len() {
            let (i, build_tuple) = &build_state.tuples[probe_state.index];
            probe_state.index += 1;

            if let Some(filter_expr) = filter_expr {
                let full_values = SplitTupleRef::new(build_tuple, &probe_state.probe_tuple);
                if !filter(&full_values, filter_expr, plan_arena)? {
                    continue;
                }
            }
            self.bits.insert(*i);
            probe_state.produced = true;
            return Ok(Some(join_rows(
                build_tuple,
                &mut probe_state.probe_tuple,
                false,
            )));
        }

        probe_state.finished = true;
        if !probe_state.produced && !probe_state.emitted_unmatched {
            probe_state.emitted_unmatched = true;
            probe_state
                .probe_tuple
                .prepend(std::iter::repeat_n(DataValue::Null, self.left_schema_len));
            return Ok(Some(std::mem::take(&mut probe_state.probe_tuple)));
        }
        Ok(None)
    }

    fn left_drop_next(
        &mut self,
        left_drop_state: &mut LeftDropState,
        _filter_expr: Option<&ExprRef>,
        _plan_arena: &(dyn MetaArena + '_),
    ) -> Result<Option<TupleImpl>, DatabaseError> {
        let full_schema_len = self.right_schema_len + self.left_schema_len;

        loop {
            if let Some(LeftDropTuples { tuples }) = left_drop_state.current.as_mut() {
                for (i, mut left_tuple) in tuples.by_ref() {
                    if self.bits.contains(i) {
                        continue;
                    }
                    left_tuple.resize(full_schema_len, DataValue::Null);
                    return Ok(Some(left_tuple));
                }
                left_drop_state.current = None;
            }

            let Some((_, state)) = left_drop_state.states.next() else {
                return Ok(None);
            };

            left_drop_state.current = Some(LeftDropTuples {
                tuples: state.tuples.into_iter(),
            });
        }
    }
}
