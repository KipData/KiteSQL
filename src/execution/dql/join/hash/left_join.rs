use crate::execution::dql::join::hash::{filter, FilterArgs, JoinProbeState, ProbeArgs};
use crate::execution::dql::join::hash_join::BuildState;
use crate::execution::dql::sort::BumpVec;
use crate::execution::{spawn_executor, Executor};
use crate::throw;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use ahash::HashMap;
use fixedbitset::FixedBitSet;

pub(crate) struct LeftJoinState {
    pub(crate) left_schema_len: usize,
    pub(crate) right_schema_len: usize,
    pub(crate) bits: FixedBitSet,
}

impl<'a> JoinProbeState<'a> for LeftJoinState {
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
            for (i, Tuple { values, pk }) in build_state.tuples.iter() {
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
                co.yield_(Ok(Tuple::new(pk.clone(), full_values))).await;
            }
            build_state.is_used = !has_filted;
            build_state.has_filted = has_filted;
        })
    }

    fn left_drop(
        &mut self,
        _build_map: HashMap<BumpVec<'a, DataValue>, BuildState>,
        _filter_args: Option<&'a FilterArgs>,
    ) -> Option<Executor<'a>> {
        let full_schema_len = self.right_schema_len + self.left_schema_len;
        let bits_ptr: *mut FixedBitSet = &mut self.bits;

        Some(spawn_executor(move |co| async move {
            for (_, state) in _build_map {
                if state.is_used {
                    continue;
                }
                for (i, mut left_tuple) in state.tuples {
                    unsafe {
                        if !(*bits_ptr).contains(i) && state.has_filted {
                            continue;
                        }
                    }
                    left_tuple.values.resize(full_schema_len, DataValue::Null);
                    co.yield_(Ok(left_tuple)).await;
                }
            }
        }))
    }
}
