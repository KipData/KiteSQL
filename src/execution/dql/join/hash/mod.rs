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
use crate::expression::ScalarExpression;
use crate::types::tuple::{SchemaRef, Tuple};
use crate::types::value::DataValue;
use std::collections::hash_map::IntoIter as HashMapIntoIter;

pub(crate) struct FilterArgs {
    pub(crate) full_schema: SchemaRef,
    pub(crate) filter_expr: ScalarExpression,
}

pub(crate) struct ProbeState {
    pub(crate) is_keys_has_null: bool,
    pub(crate) probe_tuple: Tuple,
    pub(crate) index: usize,
    pub(crate) has_filtered: bool,
    pub(crate) produced: bool,
    pub(crate) finished: bool,
    pub(crate) emitted_unmatched: bool,
}

pub(crate) struct LeftDropState {
    pub(crate) states: HashMapIntoIter<BumpVec<'static, DataValue>, BuildState>,
    pub(crate) current: Option<LeftDropTuples>,
}

pub(crate) struct LeftDropTuples {
    pub(crate) tuples: std::vec::IntoIter<(usize, Tuple)>,
    pub(crate) has_filted: bool,
}

pub(crate) trait JoinProbeState {
    fn probe_next(
        &mut self,
        probe_state: &mut ProbeState,
        build_state: Option<&mut BuildState>,
        filter_args: Option<&FilterArgs>,
    ) -> Result<Option<Tuple>, DatabaseError>;

    fn left_drop_next(
        &mut self,
        _left_drop_state: &mut LeftDropState,
        _filter_args: Option<&FilterArgs>,
    ) -> Result<Option<Tuple>, DatabaseError> {
        Ok(None)
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

impl JoinProbeState for JoinProbeStateImpl {
    fn probe_next(
        &mut self,
        probe_state: &mut ProbeState,
        build_state: Option<&mut BuildState>,
        filter_args: Option<&FilterArgs>,
    ) -> Result<Option<Tuple>, DatabaseError> {
        match self {
            JoinProbeStateImpl::Inner(state) => {
                state.probe_next(probe_state, build_state, filter_args)
            }
            JoinProbeStateImpl::Left(state) => {
                state.probe_next(probe_state, build_state, filter_args)
            }
            JoinProbeStateImpl::Right(state) => {
                state.probe_next(probe_state, build_state, filter_args)
            }
            JoinProbeStateImpl::Full(state) => {
                state.probe_next(probe_state, build_state, filter_args)
            }
            JoinProbeStateImpl::LeftSemi(state) => {
                state.probe_next(probe_state, build_state, filter_args)
            }
            JoinProbeStateImpl::LeftAnti(state) => {
                state.probe_next(probe_state, build_state, filter_args)
            }
        }
    }

    fn left_drop_next(
        &mut self,
        left_drop_state: &mut LeftDropState,
        filter_args: Option<&FilterArgs>,
    ) -> Result<Option<Tuple>, DatabaseError> {
        match self {
            JoinProbeStateImpl::Inner(state) => state.left_drop_next(left_drop_state, filter_args),
            JoinProbeStateImpl::Left(state) => state.left_drop_next(left_drop_state, filter_args),
            JoinProbeStateImpl::Right(state) => state.left_drop_next(left_drop_state, filter_args),
            JoinProbeStateImpl::Full(state) => state.left_drop_next(left_drop_state, filter_args),
            JoinProbeStateImpl::LeftSemi(state) => {
                state.left_drop_next(left_drop_state, filter_args)
            }
            JoinProbeStateImpl::LeftAnti(state) => {
                state.left_drop_next(left_drop_state, filter_args)
            }
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
