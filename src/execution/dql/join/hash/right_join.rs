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
use crate::execution::dql::join::hash::full_join::FullJoinState;
use crate::execution::dql::join::hash::{filter, JoinProbeState, ProbeState};
use crate::execution::dql::join::hash_join::BuildState;
use crate::expression::ScalarExpression;
use crate::types::tuple::{SplitTupleRef, Tuple};

pub(crate) struct RightJoinState {
    pub(crate) left_schema_len: usize,
}

impl JoinProbeState for RightJoinState {
    fn probe_next(
        &mut self,
        probe_state: &mut ProbeState,
        build_state: Option<&mut BuildState>,
        filter_expr: Option<&ScalarExpression>,
    ) -> Result<Option<Tuple>, DatabaseError> {
        if probe_state.is_keys_has_null {
            if probe_state.emitted_unmatched {
                probe_state.finished = true;
                return Ok(None);
            }
            probe_state.emitted_unmatched = true;
            probe_state.finished = true;
            return Ok(Some(FullJoinState::full_right_row(
                self.left_schema_len,
                &probe_state.probe_tuple,
            )));
        }

        let Some(build_state) = build_state else {
            if probe_state.emitted_unmatched {
                probe_state.finished = true;
                return Ok(None);
            }
            probe_state.emitted_unmatched = true;
            probe_state.finished = true;
            return Ok(Some(FullJoinState::full_right_row(
                self.left_schema_len,
                &probe_state.probe_tuple,
            )));
        };

        while probe_state.index < build_state.tuples.len() {
            let (_, Tuple { values, pk }) = &build_state.tuples[probe_state.index];
            probe_state.index += 1;

            if let Some(filter_expr) = filter_expr {
                let full_values =
                    SplitTupleRef::from_slices(values, &probe_state.probe_tuple.values);
                if !filter(&full_values, filter_expr)? {
                    probe_state.has_filtered = true;
                    continue;
                }
            }
            let full_values = Vec::from_iter(
                values
                    .iter()
                    .chain(probe_state.probe_tuple.values.iter())
                    .cloned(),
            );
            probe_state.produced = true;
            build_state.is_used = true;
            build_state.has_filted = probe_state.has_filtered;
            return Ok(Some(Tuple::new(pk.clone(), full_values)));
        }

        build_state.is_used = probe_state.produced;
        build_state.has_filted = probe_state.has_filtered;

        if !probe_state.produced && !probe_state.emitted_unmatched {
            probe_state.emitted_unmatched = true;
            probe_state.finished = true;
            return Ok(Some(FullJoinState::full_right_row(
                self.left_schema_len,
                &probe_state.probe_tuple,
            )));
        }

        probe_state.finished = true;
        Ok(None)
    }
}
