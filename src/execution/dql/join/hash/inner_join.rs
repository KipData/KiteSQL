use crate::execution::dql::join::hash::{filter, FilterArgs, JoinProbeState, ProbeArgs};
use crate::execution::Executor;
use crate::throw;
use crate::types::tuple::Tuple;

pub(crate) struct InnerJoinState;

impl<'a> JoinProbeState<'a> for InnerJoinState {
    fn probe(
        &mut self,
        probe_args: ProbeArgs<'a>,
        filter_args: Option<&'a FilterArgs>,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let ProbeArgs {
                    is_keys_has_null: false,
                    probe_tuple,
                    build_state: Some(build_state),
                    ..
                } = probe_args
                else {
                    return;
                };

                build_state.is_used = true;
                for (_, Tuple { values, pk }) in build_state.tuples.iter() {
                    let full_values =
                        Vec::from_iter(values.iter().chain(probe_tuple.values.iter()).cloned());

                    match &filter_args {
                        None => (),
                        Some(filter_args) => {
                            if !throw!(filter(&full_values, filter_args)) {
                                continue;
                            }
                        }
                    }
                    yield Ok(Tuple::new(pk.clone(), full_values));
                }
            },
        )
    }
}
