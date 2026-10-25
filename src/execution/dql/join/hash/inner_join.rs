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
use crate::execution::dql::join::hash::{filter, JoinProbeState, ProbeState};
use crate::execution::dql::join::hash_join::BuildState;
use crate::planner::ExprRef;
use crate::planner::MetaArena;
use crate::types::tuple::{SplitTupleRef, Tuple};

pub(crate) struct InnerJoinState;

impl JoinProbeState for InnerJoinState {
    fn probe_next(
        &mut self,
        probe_state: &mut ProbeState,
        build_state: Option<&mut BuildState>,
        filter_expr: Option<&ExprRef>,
        plan_arena: &(dyn MetaArena + '_),
    ) -> Result<Option<Tuple>, DatabaseError> {
        if probe_state.is_keys_has_null {
            probe_state.finished = true;
            return Ok(None);
        }

        let Some(build_state) = build_state else {
            probe_state.finished = true;
            return Ok(None);
        };

        while probe_state.index < build_state.tuples.len() {
            let (_, build_tuple) = &build_state.tuples[probe_state.index];
            probe_state.index += 1;
            if let Some(filter_expr) = filter_expr {
                let full_values = SplitTupleRef::new(build_tuple, &probe_state.probe_tuple);
                if !filter(&full_values, filter_expr, plan_arena)? {
                    continue;
                }
            }
            let probe = &mut probe_state.probe_tuple;
            let mut values = Vec::with_capacity(build_tuple.values.len() + probe.values.len());
            values.extend_from_slice(&build_tuple.values);
            let mut pk = build_tuple.pk.clone();
            if probe_state.index == build_state.tuples.len() {
                // No future match can read this probe row, so move its values and pk.
                probe_state.finished = true;
                values.append(&mut probe.values);
                pk = pk.or(probe.pk.take());
            } else {
                values.extend_from_slice(&probe.values);
                pk = pk.or_else(|| probe.pk.clone());
            }
            return Ok(Some(Tuple::new(pk, values)));
        }

        probe_state.finished = true;
        Ok(None)
    }
}
