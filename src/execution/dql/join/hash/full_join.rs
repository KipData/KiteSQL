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
use crate::types::value::DataValue;
use fixedbitset::FixedBitSet;

pub(crate) struct FullJoinState {
    pub(crate) left_schema_len: usize,
    pub(crate) right_schema_len: usize,
    pub(crate) bits: FixedBitSet,
}

impl JoinProbeState for FullJoinState {
    fn probe_next(
        &mut self,
        probe_state: &mut ProbeState,
        build_state: Option<&mut BuildState>,
        filter_args: Option<&FilterArgs>,
    ) -> Result<Option<Tuple>, DatabaseError> {
        if probe_state.is_keys_has_null {
            if probe_state.emitted_unmatched {
                probe_state.finished = true;
                return Ok(None);
            }
            probe_state.emitted_unmatched = true;
            probe_state.finished = true;
            return Ok(Some(Self::full_right_row(
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
            return Ok(Some(Self::full_right_row(
                self.left_schema_len,
                &probe_state.probe_tuple,
            )));
        };

        while probe_state.index < build_state.tuples.len() {
            let (i, Tuple { values, pk }) = &build_state.tuples[probe_state.index];
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
                    return Ok(Some(Self::full_right_row(
                        self.left_schema_len,
                        &probe_state.probe_tuple,
                    )));
                }
            }
            build_state.is_used = true;
            build_state.has_filted = probe_state.has_filtered;
            return Ok(Some(Tuple::new(pk.clone(), full_values)));
        }

        build_state.is_used = !probe_state.has_filtered;
        build_state.has_filted = probe_state.has_filtered;
        probe_state.finished = true;
        Ok(None)
    }

    fn left_drop_next(
        &mut self,
        left_drop_state: &mut LeftDropState,
        _filter_args: Option<&FilterArgs>,
    ) -> Result<Option<Tuple>, DatabaseError> {
        let full_schema_len = self.right_schema_len + self.left_schema_len;

        loop {
            if let Some(LeftDropTuples {
                tuples, has_filted, ..
            }) = left_drop_state.current.as_mut()
            {
                while let Some((i, mut left_tuple)) = tuples.next() {
                    if !self.bits.contains(i) && *has_filted {
                        continue;
                    }
                    left_tuple.values.resize(full_schema_len, DataValue::Null);
                    return Ok(Some(left_tuple));
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

impl FullJoinState {
    pub(crate) fn full_right_row(left_schema_len: usize, probe_tuple: &Tuple) -> Tuple {
        let full_values = Vec::from_iter(
            (0..left_schema_len)
                .map(|_| DataValue::Null)
                .chain(probe_tuple.values.iter().cloned()),
        );

        Tuple::new(probe_tuple.pk.clone(), full_values)
    }
}
