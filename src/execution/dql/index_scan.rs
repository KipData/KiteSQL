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
use crate::execution::{ExecArena, ExecId, ExecNode, ExecutionCaches, ExecutorNode, ReadExecutor};
use crate::expression::range_detacher::Range;
use crate::planner::operator::table_scan::TableScanOperator;
use crate::storage::{IndexIter, IndexRanges, Iter, Transaction};
use crate::types::index::{IndexLookup, IndexMetaRef, RuntimeIndexProbe};
use crate::types::serialize::TupleValueSerializableImpl;

pub(crate) struct IndexScan<'a, T: Transaction + 'a> {
    op: Option<TableScanOperator>,
    index_by: IndexMetaRef,
    lookup: Option<IndexLookup>,
    covered_deserializers: Option<Vec<TupleValueSerializableImpl>>,
    cover_mapping: Option<Vec<usize>>,
    iter: Option<IndexIter<'a, T>>,
}

impl<'a, T: Transaction + 'a>
    From<(
        TableScanOperator,
        IndexMetaRef,
        IndexLookup,
        Option<Vec<TupleValueSerializableImpl>>,
        Option<Vec<usize>>,
    )> for IndexScan<'a, T>
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
        IndexScan {
            op: Some(op),
            index_by,
            lookup: Some(lookup),
            covered_deserializers,
            cover_mapping,
            iter: None,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for IndexScan<'a, T> {
    fn into_executor(
        self,
        arena: &mut ExecArena<'a, T>,
        _: ExecutionCaches<'a>,
        _: *mut T,
    ) -> ExecId {
        arena.push(ExecNode::IndexScan(self))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for IndexScan<'a, T> {
    type Input = (
        TableScanOperator,
        IndexMetaRef,
        IndexLookup,
        Option<Vec<TupleValueSerializableImpl>>,
        Option<Vec<usize>>,
    );

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        _: ExecutionCaches<'a>,
        _: *mut T,
    ) -> ExecId {
        arena.push(ExecNode::IndexScan(IndexScan::from(input)))
    }

    fn next_tuple(&mut self, arena: &mut ExecArena<'a, T>) -> Result<(), DatabaseError> {
        IndexScan::next_tuple(self, arena)
    }
}

impl<'a, T: Transaction + 'a> IndexScan<'a, T> {
    fn ranges_from_lookup(lookup: IndexLookup, arena: &mut ExecArena<'a, T>) -> IndexRanges {
        match lookup {
            IndexLookup::Static(Range::SortedRanges(ranges)) => ranges.into(),
            IndexLookup::Static(range) => range.into(),
            IndexLookup::Probe => match arena.pop_runtime_probe() {
                RuntimeIndexProbe::Eq(value) => Range::Eq(value).into(),
                RuntimeIndexProbe::Scope { min, max } => Range::Scope { min, max }.into(),
            },
        }
    }

    pub(crate) fn next_tuple(&mut self, arena: &mut ExecArena<'a, T>) -> Result<(), DatabaseError> {
        if self.iter.is_none() {
            let Some(TableScanOperator {
                table_name,
                columns,
                limit,
                with_pk,
                ..
            }) = self.op.take()
            else {
                arena.finish();
                return Ok(());
            };
            let ranges = Self::ranges_from_lookup(
                self.lookup.take().expect("index scan lookup initialized"),
                arena,
            );
            self.iter = Some(arena.transaction().read_by_index(
                arena.table_cache(),
                table_name,
                limit,
                columns,
                self.index_by.clone(),
                ranges,
                with_pk,
                self.covered_deserializers.take(),
                self.cover_mapping.take(),
            )?);
        }

        if self
            .iter
            .as_mut()
            .expect("index scan iterator initialized")
            .next_tuple_into(arena.result_tuple_mut())?
        {
            arena.resume();
        } else {
            arena.finish();
        }
        Ok(())
    }
}
