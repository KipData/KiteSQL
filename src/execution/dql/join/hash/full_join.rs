use crate::execution::dql::join::hash::{filter, FilterArgs, JoinProbeState, ProbeArgs};
use crate::execution::dql::join::hash_join::BuildState;
use crate::execution::dql::sort::BumpVec;
use crate::execution::Executor;
use crate::throw;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use ahash::HashMap;
use fixedbitset::FixedBitSet;

pub(crate) struct FullJoinState {
    pub(crate) left_schema_len: usize,
    pub(crate) right_schema_len: usize,
    pub(crate) bits: FixedBitSet,
}

impl<'a> JoinProbeState<'a> for FullJoinState {
    fn probe(
        &mut self,
        probe_args: ProbeArgs<'a>,
        filter_args: Option<&'a FilterArgs>,
    ) -> Executor<'a> {
        let left_schema_len = self.left_schema_len;
        let bits_ptr: *mut FixedBitSet = &mut self.bits;

        Box::new(
            #[coroutine]
            move || {
                let ProbeArgs { probe_tuple, .. } = probe_args;

                if let ProbeArgs {
                    is_keys_has_null: false,
                    build_state: Some(build_state),
                    ..
                } = probe_args
                {
                    let mut has_filtered = false;
                    for (i, Tuple { values, pk }) in build_state.tuples.iter() {
                        let full_values =
                            Vec::from_iter(values.iter().chain(probe_tuple.values.iter()).cloned());

                        match &filter_args {
                            None => (),
                            Some(filter_args) => {
                                if !throw!(filter(&full_values, filter_args)) {
                                    has_filtered = true;
                                    unsafe {
                                        (*bits_ptr).set(*i, true);
                                    }
                                    yield Ok(Self::full_right_row(left_schema_len, &probe_tuple));
                                    continue;
                                }
                            }
                        }
                        yield Ok(Tuple::new(pk.clone(), full_values));
                    }
                    build_state.is_used = !has_filtered;
                    build_state.has_filted = has_filtered;
                    return;
                }

                yield Ok(Self::full_right_row(left_schema_len, &probe_tuple));
            },
        )
    }

    fn left_drop(
        &mut self,
        _build_map: HashMap<BumpVec<'a, DataValue>, BuildState>,
        _filter_args: Option<&'a FilterArgs>,
    ) -> Option<Executor<'a>> {
        let full_schema_len = self.right_schema_len + self.left_schema_len;
        let bits_ptr: *mut FixedBitSet = &mut self.bits;

        Some(Box::new(
            #[coroutine]
            move || {
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
                        yield Ok(left_tuple);
                    }
                }
            },
        ))
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
