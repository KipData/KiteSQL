pub(crate) mod full_join;
pub(crate) mod inner_join;
pub(crate) mod left_anti_join;
pub(crate) mod left_join;
pub(crate) mod left_semi_join;
pub(crate) mod right_join;

use crate::errors::DatabaseError;
use crate::execution::dql::join::hash::full_join::FullJoinState;
use crate::execution::dql::join::hash::inner_join::InnerJoinState;
use crate::execution::dql::join::hash::left_anti_join::LeftAntiJoinState;
use crate::execution::dql::join::hash::left_join::LeftJoinState;
use crate::execution::dql::join::hash::left_semi_join::LeftSemiJoinState;
use crate::execution::dql::join::hash::right_join::RightJoinState;
use crate::execution::dql::join::hash_join::BuildState;
use crate::execution::dql::sort::BumpVec;
use crate::execution::Executor;
use crate::expression::ScalarExpression;
use crate::types::tuple::{SchemaRef, Tuple};
use crate::types::value::DataValue;
use ahash::HashMap;

#[derive(Debug)]
pub(crate) struct ProbeArgs<'a> {
    pub(crate) is_keys_has_null: bool,
    pub(crate) probe_tuple: Tuple,
    pub(crate) build_state: Option<&'a mut BuildState>,
}

pub(crate) struct FilterArgs {
    pub(crate) full_schema: SchemaRef,
    pub(crate) filter_expr: ScalarExpression,
}

pub(crate) trait JoinProbeState<'a> {
    fn probe(
        &mut self,
        probe_args: ProbeArgs<'a>,
        filter_args: Option<&'a FilterArgs>,
    ) -> Executor<'a>;

    #[allow(clippy::mutable_key_type)]
    fn left_drop(
        &mut self,
        _build_map: HashMap<BumpVec<'a, DataValue>, BuildState>,
        _filter_args: Option<&'a FilterArgs>,
    ) -> Option<Executor<'a>> {
        None
    }
}

pub(crate) enum JoinProbeStateImpl {
    Inner(InnerJoinState),
    Left(LeftJoinState),
    Right(RightJoinState),
    Full(FullJoinState),
    LeftSemi(LeftSemiJoinState),
    LeftAnti(LeftAntiJoinState),
}

impl<'a> JoinProbeState<'a> for JoinProbeStateImpl {
    fn probe(
        &mut self,
        probe_args: ProbeArgs<'a>,
        filter_args: Option<&'a FilterArgs>,
    ) -> Executor<'a> {
        match self {
            JoinProbeStateImpl::Inner(state) => state.probe(probe_args, filter_args),
            JoinProbeStateImpl::Left(state) => state.probe(probe_args, filter_args),
            JoinProbeStateImpl::Right(state) => state.probe(probe_args, filter_args),
            JoinProbeStateImpl::Full(state) => state.probe(probe_args, filter_args),
            JoinProbeStateImpl::LeftSemi(state) => state.probe(probe_args, filter_args),
            JoinProbeStateImpl::LeftAnti(state) => state.probe(probe_args, filter_args),
        }
    }

    fn left_drop(
        &mut self,
        _build_map: HashMap<BumpVec<'a, DataValue>, BuildState>,
        filter_args: Option<&'a FilterArgs>,
    ) -> Option<Executor<'a>> {
        match self {
            JoinProbeStateImpl::Inner(state) => state.left_drop(_build_map, filter_args),
            JoinProbeStateImpl::Left(state) => state.left_drop(_build_map, filter_args),
            JoinProbeStateImpl::Right(state) => state.left_drop(_build_map, filter_args),
            JoinProbeStateImpl::Full(state) => state.left_drop(_build_map, filter_args),
            JoinProbeStateImpl::LeftSemi(state) => state.left_drop(_build_map, filter_args),
            JoinProbeStateImpl::LeftAnti(state) => state.left_drop(_build_map, filter_args),
        }
    }
}

pub(crate) fn filter(values: &[DataValue], filter_arg: &FilterArgs) -> Result<bool, DatabaseError> {
    let FilterArgs {
        full_schema,
        filter_expr,
        ..
    } = filter_arg;

    match &filter_expr.eval(Some((values, full_schema)))? {
        DataValue::Boolean(false) | DataValue::Null => Ok(false),
        DataValue::Boolean(true) => Ok(true),
        _ => Err(DatabaseError::InvalidType),
    }
}
