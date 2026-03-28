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
    filter, FilterArgs, JoinProbeState, LeftDropState, LeftDropTuples, ProbeState,
};
use crate::execution::dql::join::hash_join::BuildState;
use crate::types::tuple::Tuple;
use fixedbitset::FixedBitSet;

pub(crate) struct LeftSemiJoinState {
    pub(crate) bits: FixedBitSet,
}

impl JoinProbeState for LeftSemiJoinState {
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

        while probe_state.index < build_state.tuples.len() {
            let (i, Tuple { values, .. }) = &build_state.tuples[probe_state.index];
            probe_state.index += 1;
            let full_values = Vec::from_iter(
                values
                    .iter()
                    .chain(probe_state.probe_tuple.values.iter())
                    .cloned(),
            );

            if let Some(filter_args) = filter_args {
                if !filter(&full_values, filter_args)? {
                    probe_state.has_filtered = true;
                    self.bits.set(*i, true);
                }
            }
        }
        build_state.is_used = true;
        build_state.has_filted = probe_state.has_filtered;
        probe_state.finished = true;

        Ok(None)
    }

    fn left_drop_next(
        &mut self,
        left_drop_state: &mut LeftDropState,
        _filter_args: Option<&FilterArgs>,
    ) -> Result<Option<Tuple>, DatabaseError> {
        loop {
            if let Some(LeftDropTuples {
                tuples, has_filted, ..
            }) = left_drop_state.current.as_mut()
            {
                for (i, tuple) in tuples.by_ref() {
                    if self.bits.contains(i) && *has_filted {
                        continue;
                    }
                    return Ok(Some(tuple));
                }
                left_drop_state.current = None;
            }

            let Some((_, state)) = left_drop_state.states.next() else {
                return Ok(None);
            };

            if !state.is_used {
                continue;
            }
            left_drop_state.current = Some(LeftDropTuples {
                tuples: state.tuples.into_iter(),
                has_filted: state.has_filted,
            });
        }
    }
}
