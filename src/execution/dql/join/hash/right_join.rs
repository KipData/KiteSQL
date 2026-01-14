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

use crate::execution::dql::join::hash::full_join::FullJoinState;
use crate::execution::dql::join::hash::{filter, FilterArgs, JoinProbeState, ProbeArgs};
use crate::execution::{spawn_executor, Executor};
use crate::throw;
use crate::types::tuple::Tuple;

pub(crate) struct RightJoinState {
    pub(crate) left_schema_len: usize,
}

impl<'a> JoinProbeState<'a> for RightJoinState {
    fn probe(
        &mut self,
        probe_args: ProbeArgs<'a>,
        filter_args: Option<&'a FilterArgs>,
    ) -> Executor<'a> {
        let left_schema_len = self.left_schema_len;

        spawn_executor(move |co| async move {
            let ProbeArgs { probe_tuple, .. } = probe_args;

            if let ProbeArgs {
                is_keys_has_null: false,
                build_state: Some(build_state),
                ..
            } = probe_args
            {
                let mut has_filtered = false;
                let mut produced = false;
                for (_, Tuple { values, pk }) in build_state.tuples.iter() {
                    let full_values =
                        Vec::from_iter(values.iter().chain(probe_tuple.values.iter()).cloned());

                    match &filter_args {
                        None => (),
                        Some(filter_args) => {
                            if !throw!(co, filter(&full_values, filter_args)) {
                                has_filtered = true;
                                continue;
                            }
                        }
                    }
                    produced = true;
                    co.yield_(Ok(Tuple::new(pk.clone(), full_values))).await;
                }
                if !produced {
                    co.yield_(Ok(FullJoinState::full_right_row(
                        left_schema_len,
                        &probe_tuple,
                    )))
                    .await;
                }
                build_state.is_used = produced;
                build_state.has_filted = has_filtered;
                return;
            }

            co.yield_(Ok(FullJoinState::full_right_row(
                left_schema_len,
                &probe_tuple,
            )))
            .await;
        })
    }
}
