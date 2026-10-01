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

#[allow(clippy::large_enum_variant)]
pub(crate) enum PlanInput<'p> {
    Owned(LogicalPlan),
    Borrowed(&'p LogicalPlan),
}

impl From<LogicalPlan> for PlanInput<'_> {
    fn from(plan: LogicalPlan) -> Self {
        PlanInput::Owned(plan)
    }
}

pub(crate) struct PlanKeeper<'p> {
    owned: *mut LogicalPlan,
    borrowed: Option<&'p LogicalPlan>,
}

impl<'p> PlanKeeper<'p> {
    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub(crate) fn empty() -> Self {
        Self {
            owned: std::ptr::null_mut(),
            borrowed: None,
        }
    }

    pub(crate) fn new(input: PlanInput<'p>) -> Self {
        match input {
            PlanInput::Owned(plan) => Self {
                owned: Box::into_raw(Box::new(plan)),
                borrowed: None,
            },
            PlanInput::Borrowed(plan) => Self {
                owned: std::ptr::null_mut(),
                borrowed: Some(plan),
            },
        }
    }

    pub(crate) fn plan(&self) -> &'p LogicalPlan {
        match self.borrowed {
            Some(plan) => plan,
            // SAFETY: `owned` is a heap allocation freed only in `Drop`, and is
            // never moved or mutated afterwards.
            None => unsafe { &*self.owned },
        }
    }
}

impl Drop for PlanKeeper<'_> {
    fn drop(&mut self) {
        if !self.owned.is_null() {
            // SAFETY: `owned` comes from `Box::into_raw` and is only freed here.
            drop(unsafe { Box::from_raw(self.owned) });
        }
    }
}
