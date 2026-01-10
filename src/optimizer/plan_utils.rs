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

use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use std::mem;

#[allow(dead_code)]
pub fn child_count(plan: &LogicalPlan) -> usize {
    match plan.childrens.as_ref() {
        Childrens::None => 0,
        Childrens::Only(_) => 1,
        Childrens::Twins { .. } => 2,
    }
}

#[allow(dead_code)]
pub fn only_child(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan.childrens.as_ref() {
        Childrens::Only(child) => Some(child.as_ref()),
        _ => None,
    }
}

pub fn only_child_mut(plan: &mut LogicalPlan) -> Option<&mut LogicalPlan> {
    match plan.childrens.as_mut() {
        Childrens::Only(child) => Some(child.as_mut()),
        _ => None,
    }
}

pub fn left_child(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan.childrens.as_ref() {
        Childrens::Only(child) => Some(child.as_ref()),
        Childrens::Twins { left, .. } => Some(left.as_ref()),
        Childrens::None => None,
    }
}

#[allow(dead_code)]
pub fn left_child_mut(plan: &mut LogicalPlan) -> Option<&mut LogicalPlan> {
    match plan.childrens.as_mut() {
        Childrens::Only(child) => Some(child.as_mut()),
        Childrens::Twins { left, .. } => Some(left.as_mut()),
        Childrens::None => None,
    }
}

pub fn right_child(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan.childrens.as_ref() {
        Childrens::Twins { right, .. } => Some(right.as_ref()),
        _ => None,
    }
}

#[allow(dead_code)]
pub fn right_child_mut(plan: &mut LogicalPlan) -> Option<&mut LogicalPlan> {
    match plan.childrens.as_mut() {
        Childrens::Twins { right, .. } => Some(right.as_mut()),
        _ => None,
    }
}

#[allow(dead_code)]
pub fn child(plan: &LogicalPlan, idx: usize) -> Option<&LogicalPlan> {
    match (plan.childrens.as_ref(), idx) {
        (Childrens::Only(child), 0) => Some(child.as_ref()),
        (Childrens::Twins { left, .. }, 0) => Some(left.as_ref()),
        (Childrens::Twins { right, .. }, 1) => Some(right.as_ref()),
        _ => None,
    }
}

pub fn child_mut(plan: &mut LogicalPlan, idx: usize) -> Option<&mut LogicalPlan> {
    match (plan.childrens.as_mut(), idx) {
        (Childrens::Only(child), 0) => Some(child.as_mut()),
        (Childrens::Twins { left, .. }, 0) => Some(left.as_mut()),
        (Childrens::Twins { right, .. }, 1) => Some(right.as_mut()),
        _ => None,
    }
}

#[allow(dead_code)]
pub fn children(plan: &LogicalPlan) -> Vec<&LogicalPlan> {
    match plan.childrens.as_ref() {
        Childrens::None => vec![],
        Childrens::Only(child) => vec![child.as_ref()],
        Childrens::Twins { left, right } => vec![left.as_ref(), right.as_ref()],
    }
}

pub fn replace_with_only_child(plan: &mut LogicalPlan) -> bool {
    if let Childrens::Only(child) = take_childrens(plan) {
        *plan = *child;
        true
    } else {
        false
    }
}

#[allow(dead_code)]
pub fn replace_child_with_only_child(plan: &mut LogicalPlan, child_idx: usize) -> bool {
    if let Some(child_plan) = child_mut(plan, child_idx) {
        return replace_with_only_child(child_plan);
    }
    false
}

pub fn wrap_child_with(plan: &mut LogicalPlan, child_idx: usize, operator: Operator) -> bool {
    if let Some(slot) = child_mut(plan, child_idx) {
        let previous = mem::replace(slot, LogicalPlan::new(operator, Childrens::None));
        slot.childrens = Box::new(Childrens::Only(Box::new(previous)));
        true
    } else {
        false
    }
}

fn take_childrens(plan: &mut LogicalPlan) -> Childrens {
    mem::replace(&mut *plan.childrens, Childrens::None)
}
