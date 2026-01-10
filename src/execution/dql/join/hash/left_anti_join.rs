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

use crate::execution::dql::join::hash::left_semi_join::LeftSemiJoinState;
use crate::execution::dql::join::hash::{filter, FilterArgs, JoinProbeState, ProbeArgs};
use crate::execution::dql::join::hash_join::BuildState;
use crate::execution::dql::sort::BumpVec;
use crate::execution::{spawn_executor, Executor};
use crate::throw;
use crate::types::value::DataValue;
use ahash::HashMap;
use fixedbitset::FixedBitSet;

pub(crate) struct LeftAntiJoinState {
    pub(crate) right_schema_len: usize,
    pub(crate) inner: LeftSemiJoinState,
}

impl<'a> JoinProbeState<'a> for LeftAntiJoinState {
    fn probe(
        &mut self,
        probe_args: ProbeArgs<'a>,
        filter_args: Option<&'a FilterArgs>,
    ) -> Executor<'a> {
        self.inner.probe(probe_args, filter_args)
    }

    fn left_drop(
        &mut self,
        _build_map: HashMap<BumpVec<'a, DataValue>, BuildState>,
        filter_args: Option<&'a FilterArgs>,
    ) -> Option<Executor<'a>> {
        let bits_ptr: *mut FixedBitSet = &mut self.inner.bits;
        let right_schema_len = self.right_schema_len;
        Some(spawn_executor(move |co| async move {
            for (
                _,
                BuildState {
                    tuples,
                    is_used,
                    has_filted,
                },
            ) in _build_map
            {
                if is_used {
                    continue;
                }
                for (i, tuple) in tuples {
                    unsafe {
                        if (*bits_ptr).contains(i) && has_filted {
                            continue;
                        }
                    }
                    if let Some(filter_args) = filter_args {
                        let full_values = Vec::from_iter(
                            tuple
                                .values
                                .iter()
                                .cloned()
                                .chain((0..right_schema_len).map(|_| DataValue::Null)),
                        );
                        if !throw!(co, filter(&full_values, filter_args)) {
                            continue;
                        }
                    }
                    co.yield_(Ok(tuple)).await;
                }
            }
        }))
    }
}
