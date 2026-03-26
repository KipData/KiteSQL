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
use crate::execution::{ExecArena, ExecId, ExecNode, ExecutionCaches, ReadExecutor};
use crate::expression::range_detacher::Range;
use crate::planner::operator::table_scan::TableScanOperator;
use crate::storage::{IndexIter, Iter, Transaction};
use crate::types::index::IndexMetaRef;
use crate::types::serialize::TupleValueSerializableImpl;

pub(crate) struct IndexScan<'a, T: Transaction + 'a> {
    op: Option<TableScanOperator>,
    index_by: IndexMetaRef,
    ranges: Vec<Range>,
    covered_deserializers: Option<Vec<TupleValueSerializableImpl>>,
    cover_mapping: Option<Vec<usize>>,
    iter: Option<IndexIter<'a, T>>,
}

impl<'a, T: Transaction + 'a>
    From<(
        TableScanOperator,
        IndexMetaRef,
        Range,
        Option<Vec<TupleValueSerializableImpl>>,
        Option<Vec<usize>>,
    )> for IndexScan<'a, T>
{
    fn from(
        (op, index_by, range, covered_deserializers, cover_mapping): (
            TableScanOperator,
            IndexMetaRef,
            Range,
            Option<Vec<TupleValueSerializableImpl>>,
            Option<Vec<usize>>,
        ),
    ) -> Self {
        let ranges = match range {
            Range::SortedRanges(ranges) => ranges,
            range => vec![range],
        };

        IndexScan {
            op: Some(op),
            index_by,
            ranges,
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

impl<'a, T: Transaction + 'a> IndexScan<'a, T> {
    pub(crate) fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        id: ExecId,
    ) -> Result<(), DatabaseError> {
        let _ = id;
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
            self.iter = Some(arena.transaction().read_by_index(
                arena.table_cache(),
                table_name,
                limit,
                columns,
                self.index_by.clone(),
                std::mem::take(&mut self.ranges),
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
