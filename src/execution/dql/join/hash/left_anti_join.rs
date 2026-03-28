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
use crate::execution::dql::join::hash::left_semi_join::LeftSemiJoinState;
use crate::execution::dql::join::hash::{
    filter, FilterArgs, JoinProbeState, LeftDropState, LeftDropTuples, ProbeState,
};
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use fixedbitset::FixedBitSet;

pub(crate) struct LeftAntiJoinState {
    pub(crate) right_schema_len: usize,
    pub(crate) inner: LeftSemiJoinState,
}

impl JoinProbeState for LeftAntiJoinState {
    fn probe_next(
        &mut self,
        probe_state: &mut ProbeState,
        build_state: Option<&mut crate::execution::dql::join::hash_join::BuildState>,
        filter_args: Option<&FilterArgs>,
    ) -> Result<Option<Tuple>, DatabaseError> {
        self.inner.probe_next(probe_state, build_state, filter_args)
    }

    fn left_drop_next(
        &mut self,
        left_drop_state: &mut LeftDropState,
        filter_args: Option<&FilterArgs>,
    ) -> Result<Option<Tuple>, DatabaseError> {
        let bits: &FixedBitSet = &self.inner.bits;
        let right_schema_len = self.right_schema_len;

        loop {
            if let Some(LeftDropTuples {
                tuples, has_filted, ..
            }) = left_drop_state.current.as_mut()
            {
                for (i, tuple) in tuples.by_ref() {
                    if bits.contains(i) && *has_filted {
                        continue;
                    }
                    if let Some(filter_args) = filter_args {
                        let full_values = Vec::from_iter(
                            tuple
                                .values
                                .iter()
                                .cloned()
                                .chain((0..right_schema_len).map(|_| DataValue::Null)),
                        );
                        if !filter(&full_values, filter_args)? {
                            continue;
                        }
                    }
                    return Ok(Some(tuple));
                }
                left_drop_state.current = None;
            }

            let Some((_, state)) = left_drop_state.states.next() else {
                return Ok(None);
            };

            if state.is_used {
                continue;
            }
            left_drop_state.current = Some(LeftDropTuples {
                tuples: state.tuples.into_iter(),
                has_filted: state.has_filted,
            });
        }
    }
}
