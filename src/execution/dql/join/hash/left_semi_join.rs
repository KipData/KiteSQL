use crate::execution::dql::join::hash::{filter, FilterArgs, JoinProbeState, ProbeArgs};
use crate::execution::dql::join::hash_join::BuildState;
use crate::execution::dql::sort::BumpVec;
use crate::execution::{spawn_executor, Executor};
use crate::throw;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use ahash::HashMap;
use fixedbitset::FixedBitSet;

pub(crate) struct LeftSemiJoinState {
    pub(crate) bits: FixedBitSet,
}

impl<'a> JoinProbeState<'a> for LeftSemiJoinState {
    fn probe(
        &mut self,
        probe_args: ProbeArgs<'a>,
        filter_args: Option<&'a FilterArgs>,
    ) -> Executor<'a> {
        let bits_ptr: *mut FixedBitSet = &mut self.bits;
        spawn_executor(move |co| async move {
            let ProbeArgs {
                is_keys_has_null: false,
                probe_tuple,
                build_state: Some(build_state),
                ..
            } = probe_args
            else {
                return;
            };

            let mut has_filted = false;
            for (i, Tuple { values, .. }) in build_state.tuples.iter() {
                let full_values =
                    Vec::from_iter(values.iter().chain(probe_tuple.values.iter()).cloned());

                match &filter_args {
                    None => (),
                    Some(filter_args) => {
                        if !throw!(co, filter(&full_values, filter_args)) {
                            has_filted = true;
                            unsafe {
                                (*bits_ptr).set(*i, true);
                            }
                            continue;
                        }
                    }
                }
            }
            build_state.is_used = true;
            build_state.has_filted = has_filted;
        })
    }

    fn left_drop(
        &mut self,
        _build_map: HashMap<BumpVec<'a, DataValue>, BuildState>,
        _filter_args: Option<&'a FilterArgs>,
    ) -> Option<Executor<'a>> {
        let bits_ptr: *mut FixedBitSet = &mut self.bits;
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
                if !is_used {
                    continue;
                }
                for (i, tuple) in tuples {
                    unsafe {
                        if (*bits_ptr).contains(i) && has_filted {
                            continue;
                        }
                    }
                    co.yield_(Ok(tuple)).await;
                }
            }
        }))
    }
}
