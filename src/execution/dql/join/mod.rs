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

use crate::planner::operator::join::JoinType;

mod hash;
pub(crate) mod hash_join;
pub(crate) mod nested_loop_join;

pub fn joins_nullable(join_type: &JoinType) -> (bool, bool) {
    match join_type {
        JoinType::Inner => (false, false),
        JoinType::LeftOuter | JoinType::LeftSemi | JoinType::LeftAnti => (false, true),
        JoinType::RightOuter => (true, false),
        JoinType::Full => (true, true),
        JoinType::Cross => (true, true),
    }
}
