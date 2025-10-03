use crate::execution::dql::join::hash::full_join::FullJoinState;
use crate::execution::dql::join::hash::{filter, FilterArgs, JoinProbeState, ProbeArgs};
use crate::execution::Executor;
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
                    for (_, Tuple { values, pk }) in build_state.tuples.iter() {
                        let full_values =
                            Vec::from_iter(values.iter().chain(probe_tuple.values.iter()).cloned());

                        match &filter_args {
                            None => (),
                            Some(filter_args) => {
                                if !throw!(filter(&full_values, filter_args)) {
                                    has_filtered = true;
                                    yield Ok(FullJoinState::full_right_row(
                                        left_schema_len,
                                        &probe_tuple,
                                    ));
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

                yield Ok(FullJoinState::full_right_row(left_schema_len, &probe_tuple));
            },
        )
    }
}
