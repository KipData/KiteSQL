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

pub mod add_column;
pub(crate) mod change_column;
pub(crate) mod create_index;
pub(crate) mod create_table;
pub(crate) mod create_view;
pub(crate) mod drop_column;
pub(crate) mod drop_index;
pub(crate) mod drop_table;
pub(crate) mod drop_view;
pub(crate) mod truncate;

use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::storage::table_codec::TableCodec;
use crate::storage::{InnerIter, Transaction};
use crate::types::serialize::TupleValueSerializableImpl;
use crate::types::tuple::{Tuple, TupleId};
use crate::types::LogicalType;
use std::collections::Bound;

const REWRITE_BATCH_SIZE: usize = 1024;

#[allow(clippy::too_many_arguments)]
fn read_tuple_batch<T: Transaction>(
    transaction: &T,
    table_name: &TableName,
    pk_ty: &LogicalType,
    old_deserializers: &[TupleValueSerializableImpl],
    old_values_len: usize,
    old_total_len: usize,
    start_after: Option<&TupleId>,
    batch: &mut Vec<Tuple>,
    batch_size: usize,
) -> Result<usize, DatabaseError> {
    let table_codec = unsafe { &*transaction.table_codec() };
    let lower = if let Some(last_pk) = start_after {
        table_codec.with_tuple_key_unchecked(table_name.as_ref(), last_pk, |key| {
            Ok::<_, DatabaseError>(Bound::Excluded(key.to_vec()))
        })?
    } else {
        Bound::Unbounded
    };

    table_codec.with_tuple_bound(table_name.as_ref(), |min, max| {
        let lower = match &lower {
            Bound::Included(bytes) => Bound::Included(bytes.as_slice()),
            Bound::Excluded(bytes) => Bound::Excluded(bytes.as_slice()),
            Bound::Unbounded => Bound::Included(min),
        };
        let mut iter = transaction.range(lower, Bound::Included(max))?;
        let mut len = 0;

        while len < batch_size {
            let Some((key, value)) = iter.try_next()? else {
                break;
            };
            let tuple_id = TableCodec::decode_tuple_key(key, pk_ty)?;
            let tuple = if len < batch.len() {
                &mut batch[len]
            } else {
                batch.push(Tuple {
                    pk: None,
                    values: Vec::with_capacity(old_values_len),
                });
                batch
                    .last_mut()
                    .expect("batch contains the tuple that was just pushed")
            };
            TableCodec::decode_tuple_into(
                tuple,
                old_deserializers,
                Some(tuple_id),
                value,
                old_total_len,
            )?;
            len += 1;
        }

        Ok(len)
    })
}

pub(crate) fn visit_table_in_batches<T, F>(
    transaction: &T,
    table_name: &TableName,
    pk_ty: &LogicalType,
    old_deserializers: &[TupleValueSerializableImpl],
    old_values_len: usize,
    old_total_len: usize,
    mut visit: F,
) -> Result<(), DatabaseError>
where
    T: Transaction,
    F: FnMut(&Tuple) -> Result<(), DatabaseError>,
{
    let mut last_pk = None;
    let mut batch = Vec::with_capacity(REWRITE_BATCH_SIZE);

    loop {
        let batch_len = read_tuple_batch(
            transaction,
            table_name,
            pk_ty,
            old_deserializers,
            old_values_len,
            old_total_len,
            last_pk.as_ref(),
            &mut batch,
            REWRITE_BATCH_SIZE,
        )?;
        if batch_len == 0 {
            break;
        }
        last_pk = batch.get(batch_len - 1).and_then(|tuple| tuple.pk.clone());

        for tuple in batch.iter().take(batch_len) {
            visit(tuple)?;
        }

        if batch_len < REWRITE_BATCH_SIZE {
            break;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn rewrite_table_in_batches<T, F, G>(
    transaction: &mut T,
    table_name: &TableName,
    pk_ty: &LogicalType,
    old_deserializers: &[TupleValueSerializableImpl],
    old_values_len: usize,
    old_total_len: usize,
    new_serializers: &[TupleValueSerializableImpl],
    mut rewrite: F,
    mut after_write: G,
) -> Result<(), DatabaseError>
where
    T: Transaction,
    F: FnMut(&mut Tuple) -> Result<(), DatabaseError>,
    G: FnMut(&mut T, &Tuple) -> Result<(), DatabaseError>,
{
    let mut last_pk = None;
    let mut batch = Vec::with_capacity(REWRITE_BATCH_SIZE);

    loop {
        let batch_len = read_tuple_batch(
            transaction,
            table_name,
            pk_ty,
            old_deserializers,
            old_values_len,
            old_total_len,
            last_pk.as_ref(),
            &mut batch,
            REWRITE_BATCH_SIZE,
        )?;
        if batch_len == 0 {
            break;
        }
        last_pk = batch.get(batch_len - 1).and_then(|tuple| tuple.pk.clone());

        for tuple in batch.iter_mut().take(batch_len) {
            rewrite(tuple)?;
            transaction.append_tuple(table_name.as_ref(), tuple.clone(), new_serializers, true)?;
            after_write(transaction, tuple)?;
        }

        if batch_len < REWRITE_BATCH_SIZE {
            break;
        }
    }

    Ok(())
}
