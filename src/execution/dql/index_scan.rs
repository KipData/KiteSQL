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

use crate::errors::DatabaseError;
use crate::execution::{
    ExecArena, ExecId, ExecNode, ExecRuntime, ExecutorNode, ReadExecutionContext, ReadExecutor,
    RuntimeCursorId,
};
use crate::expression::range_detacher::Range;
use crate::planner::operator::table_scan::TableScanOperator;
use crate::storage::{IndexRanges, Transaction};
use crate::types::index::{IndexLookup, IndexMetaRef, RuntimeIndexProbe};
use crate::types::serialize::TupleValueSerializableImpl;

pub(crate) struct IndexScan {
    op: Option<TableScanOperator>,
    index_by: IndexMetaRef,
    lookup: Option<IndexLookup>,
    covered_deserializers: Option<Vec<TupleValueSerializableImpl>>,
    cover_mapping: Option<Vec<usize>>,
    cursor: Option<RuntimeCursorId>,
}

impl
    From<(
        TableScanOperator,
        IndexMetaRef,
        IndexLookup,
        Option<Vec<TupleValueSerializableImpl>>,
        Option<Vec<usize>>,
    )> for IndexScan
{
    fn from(
        (op, index_by, lookup, covered_deserializers, cover_mapping): (
            TableScanOperator,
            IndexMetaRef,
            IndexLookup,
            Option<Vec<TupleValueSerializableImpl>>,
            Option<Vec<usize>>,
        ),
    ) -> Self {
        Self {
            op: Some(op),
            index_by,
            lookup: Some(lookup),
            covered_deserializers,
            cover_mapping,
            cursor: None,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for IndexScan {
    type Input = (
        TableScanOperator,
        IndexMetaRef,
        IndexLookup,
        Option<Vec<TupleValueSerializableImpl>>,
        Option<Vec<usize>>,
    );

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena,
        _plan_arena: &mut crate::planner::PlanArena<'a>,
        _: ReadExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        arena.push(ExecNode::IndexScan(IndexScan::from(input)))
    }
}

impl<'a> ExecutorNode<'a> for IndexScan {
    fn next_tuple(
        &mut self,
        runtime: &mut dyn ExecRuntime<'a>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        if self.cursor.is_none() {
            let Some(op) = self.op.take() else {
                runtime.finish();
                return Ok(());
            };
            let ranges = ranges_from_lookup(
                self.lookup.take().expect("index scan lookup initialized"),
                runtime,
            );
            self.cursor = Some(runtime.open_index_scan(
                plan_arena,
                op,
                self.index_by.clone(),
                ranges,
                self.covered_deserializers.take(),
                self.cover_mapping.take(),
            )?);
        }

        if runtime.next_scan_tuple(self.cursor.expect("index scan cursor initialized"))? {
            runtime.resume();
        } else {
            runtime.finish();
        }
        Ok(())
    }
}

fn ranges_from_lookup<'a>(lookup: IndexLookup, runtime: &mut dyn ExecRuntime<'a>) -> IndexRanges {
    match lookup {
        IndexLookup::Static(Range::SortedRanges(ranges)) => ranges.into(),
        IndexLookup::Static(range) => range.into(),
        IndexLookup::Probe => match runtime.pop_runtime_probe() {
            RuntimeIndexProbe::Eq(value) => Range::Eq(value).into(),
            RuntimeIndexProbe::Scope { min, max } => Range::Scope { min, max }.into(),
        },
    }
}
