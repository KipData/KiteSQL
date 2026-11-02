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
    filter, join_rows, BuildRow, JoinProbeState, LeftDropState, ProbeState,
};
use crate::planner::ExprRef;
use crate::planner::MetaArena;
use crate::types::bitset::BitSet;
use crate::types::tuple::{SplitTupleRef, TupleImpl};
use crate::types::value::DataValue;

pub(crate) struct LeftJoinState {
    pub(crate) left_schema_len: usize,
    pub(crate) right_schema_len: usize,
    pub(crate) bits: BitSet,
}

impl JoinProbeState for LeftJoinState {
    fn probe_next(
        &mut self,
        probe_state: &mut ProbeState,
        build: &[BuildRow],
        filter_expr: Option<&ExprRef>,
        plan_arena: &(dyn MetaArena + '_),
    ) -> Result<Option<TupleImpl>, DatabaseError> {
        while let Some(i) = probe_state.next_row {
            probe_state.next_row = build[i].next;
            let build_tuple = &build[i].tuple;

            if let Some(filter_expr) = filter_expr {
                let full_values = SplitTupleRef::new(build_tuple, &probe_state.probe_tuple);
                if !filter(&full_values, filter_expr, plan_arena)? {
                    continue;
                }
            }
            self.bits.insert(i);
            return Ok(Some(join_rows(
                build_tuple,
                &mut probe_state.probe_tuple,
                false,
            )));
        }

        probe_state.finished = true;
        Ok(None)
    }

    fn left_drop_next(
        &mut self,
        left_drop_state: &mut LeftDropState,
        _filter_expr: Option<&ExprRef>,
        _plan_arena: &(dyn MetaArena + '_),
    ) -> Result<Option<TupleImpl>, DatabaseError> {
        let full_schema_len = self.right_schema_len + self.left_schema_len;

        for (i, row) in left_drop_state.rows.by_ref() {
            if self.bits.contains(i) {
                continue;
            }
            let mut left_tuple = row.tuple;
            left_tuple.resize(full_schema_len, DataValue::Null);
            return Ok(Some(left_tuple));
        }
        Ok(None)
    }
}
