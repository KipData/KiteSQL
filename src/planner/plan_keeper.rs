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

use crate::planner::LogicalPlan;
use std::marker::PhantomData;

pub(crate) struct PlanKeeper<'p> {
    owned: *mut LogicalPlan,
    _marker: PhantomData<&'p LogicalPlan>,
}

impl<'p> PlanKeeper<'p> {
    pub(crate) fn new(plan: LogicalPlan) -> Self {
        Self {
            owned: Box::into_raw(Box::new(plan)),
            _marker: PhantomData,
        }
    }

    pub(crate) fn plan(&self) -> &'p LogicalPlan {
        // SAFETY: `owned` is a heap allocation freed only in `Drop`, and is
        // never moved or mutated afterwards.
        unsafe { &*self.owned }
    }
}

// SAFETY: `PlanKeeper` owns the plan like a `Box` and only lends `&LogicalPlan`; moving it to
// another thread moves the plan and every borrower with it, which needs `LogicalPlan: Send + Sync`.
unsafe impl Send for PlanKeeper<'_> where LogicalPlan: Send + Sync {}
// SAFETY: shared access only ever reads the plan.
unsafe impl Sync for PlanKeeper<'_> where LogicalPlan: Sync {}

impl Drop for PlanKeeper<'_> {
    fn drop(&mut self) {
        // SAFETY: `owned` comes from `Box::into_raw` and is only freed here.
        drop(unsafe { Box::from_raw(self.owned) });
    }
}
