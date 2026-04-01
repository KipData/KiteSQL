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
use crate::execution::dql::join::hash::{filter, FilterArgs, JoinProbeState, ProbeState};
use crate::execution::dql::join::hash_join::BuildState;
use crate::types::tuple::{SplitTupleRef, Tuple};

pub(crate) struct InnerJoinState;

impl JoinProbeState for InnerJoinState {
    fn probe_next(
        &mut self,
        probe_state: &mut ProbeState,
        build_state: Option<&mut BuildState>,
        filter_args: Option<&FilterArgs>,
    ) -> Result<Option<Tuple>, DatabaseError> {
        if probe_state.is_keys_has_null {
            probe_state.finished = true;
            return Ok(None);
        }

        let Some(build_state) = build_state else {
            probe_state.finished = true;
            return Ok(None);
        };

        build_state.is_used = true;
        while probe_state.index < build_state.tuples.len() {
            let (_, Tuple { values, pk }) = &build_state.tuples[probe_state.index];
            probe_state.index += 1;

            if let Some(filter_args) = filter_args {
                let full_values =
                    SplitTupleRef::from_slices(values, &probe_state.probe_tuple.values);
                if !filter(&full_values, filter_args)? {
                    continue;
                }
            }
            let full_values = Vec::from_iter(
                values
                    .iter()
                    .chain(probe_state.probe_tuple.values.iter())
                    .cloned(),
            );
            return Ok(Some(Tuple::new(pk.clone(), full_values)));
        }

        probe_state.finished = true;
        Ok(None)
    }
}
