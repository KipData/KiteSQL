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

use crate::catalog::{ColumnCatalog, ColumnRef};
use crate::errors::DatabaseError;
use crate::iter_ext::Itertools;
use crate::planner::MetaArena;
use crate::types::serialize::{TupleValueSerializable, TupleValueSerializableImpl};
use crate::types::value::DataValue;
use std::borrow::Borrow;
use std::borrow::Cow;
use std::io::Cursor;
use std::mem::MaybeUninit;
use std::ptr;

const BITS_MAX_INDEX: usize = 8;

pub type TupleId = DataValue;
pub type Schema = Vec<ColumnRef>;

pub struct SchemaView<'a, 'p> {
    schema: &'a Schema,
    arena: &'a (dyn MetaArena + 'p),
}

pub struct SchemaColumnIter<'a, 'p, 's> {
    columns: std::slice::Iter<'s, ColumnRef>,
    arena: &'a (dyn MetaArena + 'p),
}

impl<'a> Iterator for SchemaColumnIter<'a, '_, '_> {
    type Item = &'a ColumnCatalog;

    fn next(&mut self) -> Option<Self::Item> {
        self.columns.next().map(|column| self.arena.column(*column))
    }
}

impl<'a, 'p> SchemaView<'a, 'p> {
    pub fn new(schema: &'a Schema, arena: &'a (dyn MetaArena + 'p)) -> Self {
        Self { schema, arena }
    }

    pub fn iter(&self) -> SchemaColumnIter<'a, 'p, '_> {
        SchemaColumnIter {
            columns: self.schema.iter(),
            arena: self.arena,
        }
    }

    pub fn len(&self) -> usize {
        self.schema.len()
    }

    pub fn is_empty(&self) -> bool {
        self.schema.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<&'a ColumnCatalog> {
        self.schema
            .get(index)
            .map(|column| self.arena.column(*column))
    }

    pub fn position(&self, name: &str) -> Option<usize> {
        self.iter().position(|column| column.name() == name)
    }
}

pub trait Input {
    fn width(&self) -> usize;

    fn column(&self, position: usize) -> Cow<'_, DataValue>;
}

impl<T: TupleLike + ?Sized> Input for T {
    fn width(&self) -> usize {
        TupleLike::len(self)
    }

    fn column(&self, position: usize) -> Cow<'_, DataValue> {
        Cow::Borrowed(self.value_at(position))
    }
}

// Sized adapter for passing any Input through the object-safe function interface.
pub(crate) struct InputRef<'a, T: ?Sized>(pub &'a T);

impl<T: Input + ?Sized> Input for InputRef<'_, T> {
    fn width(&self) -> usize {
        self.0.width()
    }

    fn column(&self, position: usize) -> Cow<'_, DataValue> {
        self.0.column(position)
    }
}

pub trait TupleLike {
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn value_at(&self, index: usize) -> &DataValue;

    #[inline]
    fn as_slice(&self) -> Option<&[DataValue]> {
        None
    }
}

#[derive(Clone, Copy)]
pub struct SplitTupleRef<'a, L: ?Sized, R: ?Sized> {
    left: &'a L,
    right: &'a R,
    left_len: usize,
}

impl<'a, L: TupleLike + ?Sized, R: TupleLike + ?Sized> SplitTupleRef<'a, L, R> {
    pub fn new(left: &'a L, right: &'a R) -> Self {
        Self {
            left,
            right,
            left_len: left.len(),
        }
    }
}

impl<'a> SplitTupleRef<'a, [DataValue], [DataValue]> {
    pub fn from_slices(left: &'a [DataValue], right: &'a [DataValue]) -> Self {
        Self::new(left, right)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct Tuple {
    pub pk: Option<TupleId>,
    pub values: Vec<DataValue>,
}

impl TupleLike for Tuple {
    #[inline]
    fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    fn value_at(&self, index: usize) -> &DataValue {
        &self.values[index]
    }

    #[inline]
    fn as_slice(&self) -> Option<&[DataValue]> {
        Some(self.values.as_slice())
    }
}

impl TupleLike for [DataValue] {
    #[inline]
    fn len(&self) -> usize {
        <[DataValue]>::len(self)
    }

    #[inline]
    fn value_at(&self, index: usize) -> &DataValue {
        &self[index]
    }

    #[inline]
    fn as_slice(&self) -> Option<&[DataValue]> {
        Some(self)
    }
}

impl<T: TupleLike + ?Sized> TupleLike for &T {
    #[inline]
    fn len(&self) -> usize {
        (**self).len()
    }

    #[inline]
    fn value_at(&self, index: usize) -> &DataValue {
        (**self).value_at(index)
    }

    #[inline]
    fn as_slice(&self) -> Option<&[DataValue]> {
        (**self).as_slice()
    }
}

impl<L: TupleLike + ?Sized, R: TupleLike + ?Sized> TupleLike for SplitTupleRef<'_, L, R> {
    #[inline]
    fn len(&self) -> usize {
        self.left_len + self.right.len()
    }

    #[inline]
    fn value_at(&self, index: usize) -> &DataValue {
        if index < self.left_len {
            self.left.value_at(index)
        } else {
            self.right.value_at(index - self.left_len)
        }
    }
}

#[derive(Clone, Debug)]
pub enum TupleImpl {
    Tuple(Tuple),
    Concat(Box<ConcatTuple>),
}

#[derive(Clone, Debug)]
pub struct ConcatTuple {
    pk: Option<TupleId>,
    left: TupleImpl,
    right: TupleImpl,
    left_len: usize,
}

impl ConcatTuple {
    #[inline]
    fn route(&self, position: &mut usize) -> bool {
        if *position < self.left_len {
            true
        } else {
            *position -= self.left_len;
            false
        }
    }
}

#[allow(clippy::vec_box)]
#[derive(Debug, Default)]
pub(crate) struct ConcatPool(Vec<Box<MaybeUninit<ConcatTuple>>>);

impl ConcatPool {
    const CAPACITY: usize = 64;

    pub(crate) fn concat(
        &mut self,
        left: TupleImpl,
        right: TupleImpl,
        pk: Option<TupleId>,
    ) -> TupleImpl {
        let concat = ConcatTuple {
            pk,
            left_len: left.len(),
            left,
            right,
        };
        TupleImpl::Concat(match self.0.pop() {
            Some(slot) => Box::write(slot, concat),
            None => Box::new(concat),
        })
    }

    pub(crate) fn retain_leftmost<'a>(&mut self, tuple: &'a mut TupleImpl) -> &'a mut Tuple {
        self.retain(tuple, true)
    }

    pub(crate) fn retain_rightmost<'a>(&mut self, tuple: &'a mut TupleImpl) -> &'a mut Tuple {
        self.retain(tuple, false)
    }

    fn retain<'a>(&mut self, tuple: &'a mut TupleImpl, keep_left: bool) -> &'a mut Tuple {
        loop {
            match tuple {
                TupleImpl::Tuple(tuple) => return tuple,
                TupleImpl::Concat(_) => *tuple = self.split(std::mem::take(tuple), keep_left),
            }
        }
    }

    fn split(&mut self, tuple: TupleImpl, keep_left: bool) -> TupleImpl {
        match tuple {
            TupleImpl::Tuple(tuple) => TupleImpl::Tuple(tuple),
            TupleImpl::Concat(boxed) => {
                let ConcatTuple { left, right, .. } = self.recycle(boxed);
                let (keep, discard) = if keep_left {
                    (left, right)
                } else {
                    (right, left)
                };
                self.release(discard);
                keep
            }
        }
    }

    pub(crate) fn release(&mut self, tuple: TupleImpl) {
        if let TupleImpl::Concat(boxed) = tuple {
            let ConcatTuple { left, right, .. } = self.recycle(boxed);
            self.release(left);
            self.release(right);
        }
    }

    fn recycle(&mut self, boxed: Box<ConcatTuple>) -> ConcatTuple {
        let raw = Box::into_raw(boxed);
        let (concat, slot) = unsafe { (ptr::read(raw), Box::from_raw(raw.cast())) };
        if self.0.len() < Self::CAPACITY {
            self.0.push(slot);
        }
        concat
    }
}

impl Default for TupleImpl {
    fn default() -> Self {
        TupleImpl::Tuple(Tuple::default())
    }
}

impl From<Tuple> for TupleImpl {
    fn from(tuple: Tuple) -> Self {
        TupleImpl::Tuple(tuple)
    }
}

impl TupleImpl {
    pub fn concat(left: TupleImpl, right: TupleImpl, pk: Option<TupleId>) -> Self {
        let left_len = left.len();
        TupleImpl::Concat(Box::new(ConcatTuple {
            pk,
            left,
            right,
            left_len,
        }))
    }

    #[inline]
    pub fn pk(&self) -> Option<&TupleId> {
        self.pk_slot().as_ref()
    }

    /// The stored `Option` itself, for callers that encode it as a whole.
    #[inline]
    pub(crate) fn pk_slot(&self) -> &Option<TupleId> {
        match self {
            TupleImpl::Tuple(tuple) => &tuple.pk,
            TupleImpl::Concat(concat) => &concat.pk,
        }
    }

    #[inline]
    pub fn take_pk(&mut self) -> Option<TupleId> {
        match self {
            TupleImpl::Tuple(tuple) => tuple.pk.take(),
            TupleImpl::Concat(concat) => concat.pk.take(),
        }
    }

    #[inline]
    pub fn set_pk(&mut self, pk: Option<TupleId>) {
        match self {
            TupleImpl::Tuple(tuple) => tuple.pk = pk,
            TupleImpl::Concat(concat) => concat.pk = pk,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        TupleLike::len(self)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<&DataValue> {
        (index < self.len()).then(|| self.value_at(index))
    }

    #[inline]
    pub fn get_mut(&mut self, index: usize) -> Option<&mut DataValue> {
        if index < self.len() {
            Some(&mut self[index])
        } else {
            None
        }
    }

    pub fn push(&mut self, value: DataValue) {
        self.rightmost_mut().values.push(value);
    }

    pub fn remove(&mut self, mut index: usize) -> DataValue {
        let mut node = self;
        loop {
            match node {
                TupleImpl::Tuple(tuple) => return tuple.values.remove(index),
                TupleImpl::Concat(concat) => {
                    node = if concat.route(&mut index) {
                        concat.left_len -= 1;
                        &mut concat.left
                    } else {
                        &mut concat.right
                    };
                }
            }
        }
    }

    pub fn prepend(&mut self, values: impl IntoIterator<Item = DataValue>) {
        match self {
            TupleImpl::Tuple(tuple) => {
                let len = tuple.values.len();
                tuple.values.extend(values);
                let count = tuple.values.len() - len;
                tuple.values.rotate_right(count);
            }
            TupleImpl::Concat(concat) => {
                concat.left.prepend(values);
                concat.left_len = concat.left.len();
            }
        }
    }

    pub fn resize(&mut self, len: usize, value: DataValue) {
        let prefix: usize = self.len() - self.rightmost_mut().values.len();
        debug_assert!(len >= prefix, "resize cannot shrink into the left prefix");
        self.rightmost_mut()
            .values
            .resize(len.saturating_sub(prefix), value);
    }

    pub(crate) fn rightmost_mut(&mut self) -> &mut Tuple {
        let mut node = self;
        loop {
            match node {
                TupleImpl::Tuple(tuple) => return tuple,
                TupleImpl::Concat(concat) => node = &mut concat.right,
            }
        }
    }

    fn value_at_mut(&mut self, mut index: usize) -> &mut DataValue {
        let mut node = self;
        loop {
            match node {
                TupleImpl::Tuple(tuple) => return &mut tuple.values[index],
                TupleImpl::Concat(concat) => {
                    node = if concat.route(&mut index) {
                        &mut concat.left
                    } else {
                        &mut concat.right
                    };
                }
            }
        }
    }

    /// e.g.: bits(u8)..|data_0(len for utf8_1)|utf8_0|data_1|
    /// Tips: all len is u32
    pub fn serialize_to<I, S>(
        &self,
        serializers: I,
        bytes: &mut Vec<u8>,
    ) -> Result<(), DatabaseError>
    where
        I: IntoIterator<Item = S>,
        S: Borrow<TupleValueSerializableImpl>,
    {
        fn flip_bit(bits: u8, i: usize) -> u8 {
            bits | (1 << (7 - i))
        }

        let values_len = self.len();
        let bits_len = (values_len + BITS_MAX_INDEX) / BITS_MAX_INDEX;
        let values_bytes_len = self
            .iter()
            .map(DataValue::serialized_len_hint)
            .sum::<usize>();
        bytes.clear();
        bytes.reserve(bits_len + values_bytes_len);
        bytes.resize(bits_len, 0u8);

        for (i, (value, serializer)) in self.iter().zip(serializers).enumerate() {
            if value.is_null() {
                bytes[i / BITS_MAX_INDEX] = flip_bit(bytes[i / BITS_MAX_INDEX], i % BITS_MAX_INDEX);
            } else {
                serializer.borrow().to_raw(value, bytes)?;
            }
        }

        Ok(())
    }

    pub fn iter(&self) -> TupleImplIter<'_> {
        match self.as_slice() {
            Some(values) => TupleImplIter::Flat(values.iter()),
            None => TupleImplIter::Joined {
                tuple: self,
                position: 0,
                len: self.len(),
            },
        }
    }

    pub fn take_values(&mut self) -> Vec<DataValue> {
        match self {
            TupleImpl::Tuple(tuple) => std::mem::take(&mut tuple.values),
            TupleImpl::Concat(_) => {
                let mut values = Vec::with_capacity(self.len());
                self.drain_values(|value| values.push(value));
                values
            }
        }
    }

    pub fn drain_values(&mut self, mut f: impl FnMut(DataValue)) {
        self.drain_leaves(&mut f);
        self.output_mut().clear();
    }

    fn drain_leaves(&mut self, f: &mut impl FnMut(DataValue)) {
        match self {
            TupleImpl::Tuple(tuple) => tuple.values.drain(..).for_each(f),
            TupleImpl::Concat(concat) => {
                concat.left.drain_leaves(f);
                concat.right.drain_leaves(f);
            }
        }
    }

    pub fn values_eq(&self, other: &TupleImpl) -> bool {
        match (self.as_slice(), other.as_slice()) {
            (Some(left), Some(right)) => left == right,
            _ => self.len() == other.len() && self.iter().eq(other.iter()),
        }
    }

    pub fn output_mut(&mut self) -> &mut Tuple {
        loop {
            match self {
                TupleImpl::Tuple(tuple) => return tuple,
                TupleImpl::Concat(concat) => *self = std::mem::take(&mut concat.left),
            }
        }
    }
}

impl Extend<DataValue> for TupleImpl {
    fn extend<I: IntoIterator<Item = DataValue>>(&mut self, values: I) {
        self.rightmost_mut().values.extend(values);
    }
}

impl std::ops::Index<usize> for TupleImpl {
    type Output = DataValue;

    #[inline]
    fn index(&self, index: usize) -> &DataValue {
        self.value_at(index)
    }
}

impl std::ops::IndexMut<usize> for TupleImpl {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut DataValue {
        self.value_at_mut(index)
    }
}

pub enum TupleImplIter<'a> {
    Flat(std::slice::Iter<'a, DataValue>),
    Joined {
        tuple: &'a TupleImpl,
        position: usize,
        len: usize,
    },
}

impl<'a> Iterator for TupleImplIter<'a> {
    type Item = &'a DataValue;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            TupleImplIter::Flat(iter) => iter.next(),
            TupleImplIter::Joined {
                tuple,
                position,
                len,
            } => {
                if *position >= *len {
                    return None;
                }
                let value = tuple.value_at(*position);
                *position += 1;
                Some(value)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = match self {
            TupleImplIter::Flat(iter) => iter.len(),
            TupleImplIter::Joined { position, len, .. } => len - position,
        };
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for TupleImplIter<'_> {}

impl PartialEq for TupleImpl {
    fn eq(&self, other: &Self) -> bool {
        self.pk() == other.pk() && self.values_eq(other)
    }
}

impl Eq for TupleImpl {}

impl PartialEq<[DataValue]> for TupleImpl {
    fn eq(&self, other: &[DataValue]) -> bool {
        self.iter().eq(other)
    }
}

impl PartialEq<Vec<DataValue>> for TupleImpl {
    fn eq(&self, other: &Vec<DataValue>) -> bool {
        *self == **other
    }
}

impl<const N: usize> PartialEq<[DataValue; N]> for TupleImpl {
    fn eq(&self, other: &[DataValue; N]) -> bool {
        *self == other[..]
    }
}

impl PartialEq<Tuple> for TupleImpl {
    fn eq(&self, other: &Tuple) -> bool {
        self.pk() == other.pk.as_ref() && *self == other.values
    }
}

impl std::hash::Hash for TupleImpl {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.pk().hash(state);
        state.write_usize(self.len());
        for value in self.iter() {
            value.hash(state);
        }
    }
}

impl TupleLike for TupleImpl {
    #[inline]
    fn len(&self) -> usize {
        match self {
            TupleImpl::Tuple(tuple) => tuple.values.len(),
            TupleImpl::Concat(concat) => concat.left_len + concat.right.len(),
        }
    }

    #[inline]
    fn value_at(&self, mut index: usize) -> &DataValue {
        let mut node = self;
        loop {
            match node {
                TupleImpl::Tuple(tuple) => return &tuple.values[index],
                TupleImpl::Concat(concat) => {
                    node = if concat.route(&mut index) {
                        &concat.left
                    } else {
                        &concat.right
                    };
                }
            }
        }
    }

    #[inline]
    fn as_slice(&self) -> Option<&[DataValue]> {
        match self {
            TupleImpl::Tuple(tuple) => Some(tuple.values.as_slice()),
            TupleImpl::Concat(_) => None,
        }
    }
}

impl<'a> From<&'a Tuple> for &'a [DataValue] {
    fn from(val: &'a Tuple) -> Self {
        val.values.as_slice()
    }
}

impl Tuple {
    pub fn clear(&mut self) {
        self.pk = None;
        self.values.clear();
    }

    pub fn new(pk: Option<TupleId>, values: Vec<DataValue>) -> Self {
        Tuple { pk, values }
    }

    #[inline]
    pub fn deserialize_from_into<I, S>(
        &mut self,
        deserializers: I,
        bytes: &[u8],
        total_len: usize,
    ) -> Result<(), DatabaseError>
    where
        I: IntoIterator<Item = S>,
        S: Borrow<TupleValueSerializableImpl>,
    {
        fn is_null(bits: u8, i: usize) -> bool {
            bits & (1 << (7 - i)) > 0
        }

        let bits_len = (total_len + BITS_MAX_INDEX) / BITS_MAX_INDEX;
        self.values.clear();
        self.values.reserve(total_len);

        let mut cursor = Cursor::new(&bytes[bits_len..]);

        for (i, deserializer) in deserializers.into_iter().enumerate() {
            if is_null(bytes[i / BITS_MAX_INDEX], i % BITS_MAX_INDEX) {
                // A skipped (unprojected) column yields no value, NULL or not;
                // pushing one here would shift every later projected column.
                if !matches!(
                    deserializer.borrow(),
                    TupleValueSerializableImpl::SkipFixed(_)
                        | TupleValueSerializableImpl::SkipVariable
                ) {
                    self.values.push(DataValue::Null);
                }
                continue;
            }
            deserializer
                .borrow()
                .filling_value(&mut cursor, &mut self.values)?;
        }
        Ok(())
    }

    pub fn primary_projection<V: TupleLike + ?Sized>(pk_indices: &[usize], values: &V) -> TupleId {
        if pk_indices.len() > 1 {
            DataValue::Tuple(
                pk_indices
                    .iter()
                    .map(|i| values.value_at(*i).clone())
                    .collect_vec(),
            )
        } else {
            values.value_at(pk_indices[0]).clone()
        }
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::TupleLike;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::iter_ext::Itertools;
    use crate::planner::{PlanArena, TableArenaCell};
    use crate::types::serialize::TupleValueSerializable;
    use crate::types::tuple::{Tuple, TupleImpl};
    use crate::types::value::{DataValue, Utf8Type};
    use crate::types::CharLengthUnits;
    use crate::types::LogicalType;
    use ordered_float::OrderedFloat;
    use rust_decimal::Decimal;
    use std::sync::Arc;

    struct OneValueTupleLike(DataValue);

    impl super::TupleLike for OneValueTupleLike {
        fn len(&self) -> usize {
            1
        }

        fn value_at(&self, index: usize) -> &DataValue {
            assert_eq!(index, 0);
            &self.0
        }
    }

    #[test]
    fn clear_removes_pk_and_values_without_reallocating() {
        let mut values = Vec::with_capacity(8);
        values.push(DataValue::Int32(1));
        values.push(DataValue::Null);
        let mut tuple = Tuple::new(Some(DataValue::Int32(1)), values);
        let allocation = (tuple.values.as_ptr(), tuple.values.capacity());
        for _ in 0..2 {
            tuple.clear();
            assert!(tuple.pk.is_none());
            assert!(tuple.values.is_empty());
            assert_eq!((tuple.values.as_ptr(), tuple.values.capacity()), allocation);
        }
        tuple.values.push(DataValue::Int32(2));
        assert_eq!((tuple.values.as_ptr(), tuple.values.capacity()), allocation);
    }

    #[test]
    fn test_tuple_serialize_to_and_deserialize_from() {
        let columns = Arc::new(vec![
            ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, Some(0), false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::UInteger, None, false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c3".to_string(),
                false,
                ColumnDesc::new(
                    LogicalType::Varchar(Some(2), CharLengthUnits::Characters),
                    None,
                    false,
                    None,
                )
                .unwrap(),
            ),
            ColumnCatalog::new(
                "c4".to_string(),
                false,
                ColumnDesc::new(LogicalType::Smallint, None, false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c5".to_string(),
                false,
                ColumnDesc::new(LogicalType::USmallint, None, false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c6".to_string(),
                false,
                ColumnDesc::new(LogicalType::Float, None, false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c7".to_string(),
                false,
                ColumnDesc::new(LogicalType::Double, None, false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c8".to_string(),
                false,
                ColumnDesc::new(LogicalType::Tinyint, None, false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c9".to_string(),
                false,
                ColumnDesc::new(LogicalType::UTinyint, None, false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c10".to_string(),
                false,
                ColumnDesc::new(LogicalType::Boolean, None, false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c11".to_string(),
                false,
                ColumnDesc::new(LogicalType::DateTime, None, false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c12".to_string(),
                false,
                ColumnDesc::new(LogicalType::Date, None, false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c13".to_string(),
                false,
                ColumnDesc::new(LogicalType::Decimal(None, None), None, false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c14".to_string(),
                false,
                ColumnDesc::new(
                    LogicalType::Char(1, CharLengthUnits::Characters),
                    None,
                    false,
                    None,
                )
                .unwrap(),
            ),
            ColumnCatalog::new(
                "c15".to_string(),
                false,
                ColumnDesc::new(
                    LogicalType::Varchar(Some(2), CharLengthUnits::Octets),
                    None,
                    false,
                    None,
                )
                .unwrap(),
            ),
            ColumnCatalog::new(
                "c16".to_string(),
                false,
                ColumnDesc::new(
                    LogicalType::Char(10, CharLengthUnits::Octets),
                    None,
                    false,
                    None,
                )
                .unwrap(),
            ),
            ColumnCatalog::new(
                "c17".to_string(),
                false,
                ColumnDesc::new(
                    LogicalType::Char(3, CharLengthUnits::Characters),
                    None,
                    false,
                    None,
                )
                .unwrap(),
            ),
        ]);

        let tuples = [
            Tuple::new(
                Some(DataValue::Int32(0)),
                vec![
                    DataValue::Int32(0),
                    DataValue::UInt32(1),
                    DataValue::Utf8 {
                        value: "LOL".to_string(),
                        ty: Utf8Type::Variable(Some(2)),
                        unit: CharLengthUnits::Characters,
                    },
                    DataValue::Int16(1),
                    DataValue::UInt16(1),
                    DataValue::Float32(OrderedFloat(0.1)),
                    DataValue::Float64(OrderedFloat(0.1)),
                    DataValue::Int8(1),
                    DataValue::UInt8(1),
                    DataValue::Boolean(true),
                    DataValue::Date64(0),
                    DataValue::Date32(0),
                    DataValue::Decimal(Decimal::new(0, 3)),
                    DataValue::Utf8 {
                        value: "K".to_string(),
                        ty: Utf8Type::Fixed(1),
                        unit: CharLengthUnits::Characters,
                    },
                    DataValue::Utf8 {
                        value: "LOL".to_string(),
                        ty: Utf8Type::Variable(Some(2)),
                        unit: CharLengthUnits::Octets,
                    },
                    DataValue::Utf8 {
                        value: "K".to_string(),
                        ty: Utf8Type::Fixed(10),
                        unit: CharLengthUnits::Octets,
                    },
                    DataValue::Utf8 {
                        value: "你".to_string(),
                        ty: Utf8Type::Fixed(3),
                        unit: CharLengthUnits::Characters,
                    },
                ],
            ),
            Tuple::new(
                Some(DataValue::Int32(1)),
                vec![
                    DataValue::Int32(1),
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                    DataValue::Null,
                ],
            ),
        ]
        .map(TupleImpl::from);
        let serializers = columns
            .iter()
            .map(|column| column.datatype().serializable())
            .collect_vec();
        let mut bytes = Vec::new();
        {
            let mut tuple_0 = Tuple {
                pk: tuples[0].pk().cloned(),
                values: Vec::with_capacity(serializers.len()),
            };
            tuples[0].serialize_to(&serializers, &mut bytes).unwrap();
            tuple_0
                .deserialize_from_into(&serializers, &bytes, columns.len())
                .unwrap();

            assert_eq!(tuples[0], TupleImpl::from(tuple_0));
        }
        {
            let mut tuple_1 = Tuple {
                pk: tuples[1].pk().cloned(),
                values: Vec::with_capacity(serializers.len()),
            };
            tuples[1].serialize_to(&serializers, &mut bytes).unwrap();
            tuple_1
                .deserialize_from_into(&serializers, &bytes, columns.len())
                .unwrap();

            assert_eq!(tuples[1], TupleImpl::from(tuple_1));
        }
        // projection
        {
            let projection_serializers = vec![
                columns[0].datatype().serializable(),
                columns[1].datatype().skip_serializable(),
                columns[2].datatype().skip_serializable(),
                columns[3].datatype().serializable(),
            ];
            let mut tuple_2 = Tuple {
                pk: tuples[0].pk().cloned(),
                values: Vec::with_capacity(2),
            };
            tuples[0].serialize_to(&serializers, &mut bytes).unwrap();
            tuple_2
                .deserialize_from_into(&projection_serializers, &bytes, columns.len())
                .unwrap();

            assert_eq!(
                tuple_2,
                Tuple {
                    pk: Some(DataValue::Int32(0)),
                    values: vec![DataValue::Int32(0), DataValue::Int16(1)],
                }
            );

            // A NULL in a skipped column must not shift later projected columns.
            let mut null_skipped = tuples[0].clone();
            null_skipped[1] = DataValue::Null;
            null_skipped.serialize_to(&serializers, &mut bytes).unwrap();
            tuple_2
                .deserialize_from_into(&projection_serializers, &bytes, columns.len())
                .unwrap();

            assert_eq!(
                tuple_2.values,
                vec![DataValue::Int32(0), DataValue::Int16(1)]
            );
        }
        // multiple pk
        {
            let multiple_pk_serializers = columns
                .iter()
                .take(5)
                .map(|column| column.datatype().serializable())
                .collect_vec();
            let mut multi_pk_tuple = tuples[0].clone();
            let pk = DataValue::Tuple(vec![multi_pk_tuple[4].clone(), multi_pk_tuple[2].clone()]);
            multi_pk_tuple.set_pk(Some(pk));

            let mut tuple_3 = Tuple {
                pk: multi_pk_tuple.pk().cloned(),
                values: Vec::with_capacity(serializers.len()),
            };
            multi_pk_tuple
                .serialize_to(&serializers, &mut bytes)
                .unwrap();
            tuple_3
                .deserialize_from_into(&multiple_pk_serializers, &bytes, columns.len())
                .unwrap();

            assert_eq!(
                tuple_3,
                Tuple {
                    pk: multi_pk_tuple.pk().cloned(),
                    values: vec![
                        DataValue::Int32(0),
                        DataValue::UInt32(1),
                        DataValue::Utf8 {
                            value: "LOL".to_string(),
                            ty: Utf8Type::Variable(Some(2)),
                            unit: CharLengthUnits::Characters,
                        },
                        DataValue::Int16(1),
                        DataValue::UInt16(1),
                    ],
                }
            );
        }
    }

    #[test]
    fn test_schema_view_and_tuple_like_impls() {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let schema = vec![
            arena.alloc_column(ColumnCatalog::new(
                "id".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, Some(0), false, None).unwrap(),
            )),
            arena.alloc_column(ColumnCatalog::new(
                "name".to_string(),
                true,
                ColumnDesc::new(
                    LogicalType::Varchar(None, CharLengthUnits::Characters),
                    None,
                    true,
                    None,
                )
                .unwrap(),
            )),
        ];
        let view = super::SchemaView::new(&schema, &arena);

        assert_eq!(view.len(), 2);
        assert!(!view.is_empty());
        assert_eq!(view.get(0).unwrap().name(), "id");
        assert_eq!(view.position("name"), Some(1));
        assert_eq!(
            view.iter()
                .map(|column| column.name().to_string())
                .collect_vec(),
            vec!["id".to_string(), "name".to_string()]
        );

        let tuple = Tuple::new(None, vec![DataValue::Int32(1), DataValue::Int32(2)]);
        assert_eq!(super::TupleLike::value_at(&tuple, 1), &DataValue::Int32(2));
        assert_eq!(
            super::TupleLike::as_slice(&tuple).unwrap(),
            tuple.values.as_slice()
        );
        assert_eq!(
            super::TupleLike::value_at(&tuple.values[..], 0),
            &DataValue::Int32(1)
        );
        assert_eq!(super::TupleLike::value_at(&&tuple, 0), &DataValue::Int32(1));
        assert_eq!(
            super::TupleLike::as_slice(&&tuple).unwrap(),
            tuple.values.as_slice()
        );
        assert_eq!(
            super::TupleLike::value_at(&tuple.values.as_slice(), 1),
            &DataValue::Int32(2)
        );
        let tuple_like: &dyn super::TupleLike = &tuple;
        assert_eq!(
            super::TupleLike::value_at(&tuple_like, 1),
            &DataValue::Int32(2)
        );
        assert_eq!(
            super::TupleLike::as_slice(&tuple_like).unwrap(),
            tuple.values.as_slice()
        );
        let one = OneValueTupleLike(DataValue::Int32(9));
        assert_eq!(super::TupleLike::as_slice(&one), None);
        assert_eq!(super::TupleLike::value_at(&one, 0), &DataValue::Int32(9));

        let left = Tuple::new(None, vec![DataValue::Int32(1)]);
        let right = Tuple::new(None, vec![DataValue::Int32(2)]);
        let split = super::SplitTupleRef::new(&left, &right);
        assert_eq!(split.value_at(0), &DataValue::Int32(1));
        assert_eq!(split.value_at(1), &DataValue::Int32(2));
        let split = super::SplitTupleRef::from_slices(&left.values, &right.values);
        assert_eq!(split.value_at(1), &DataValue::Int32(2));
        assert_eq!(split.len(), 2);
        assert!(!split.is_empty());
        assert_eq!(tuple_like.len(), 2);
        assert_eq!(one.len(), 1);
        let empty: &[DataValue] = &[];
        assert!(empty.is_empty());
        let tail = OneValueTupleLike(DataValue::Int32(9));
        let nested = super::SplitTupleRef::new(&split, &tail);
        assert_eq!(nested.len(), 3);
        assert_eq!(nested.value_at(0), &DataValue::Int32(1));
        assert_eq!(nested.value_at(1), &DataValue::Int32(2));
        assert_eq!(nested.value_at(2), &DataValue::Int32(9));
        assert_eq!(nested.as_slice(), None);
        let empty_left = super::SplitTupleRef::new(empty, &tail);
        assert_eq!(empty_left.len(), 1);
        assert_eq!(empty_left.value_at(0), &DataValue::Int32(9));
        let empty_right = super::SplitTupleRef::new(&tail, empty);
        assert_eq!(empty_right.len(), 1);
        assert_eq!(empty_right.value_at(0), &DataValue::Int32(9));
        let empty_split = super::SplitTupleRef::new(empty, empty);
        assert!(super::TupleLike::is_empty(&empty_split));
        let tuple_slice: &[DataValue] = (&tuple).into();
        assert_eq!(tuple_slice, tuple.values.as_slice());
        assert_eq!(
            super::TupleLike::as_slice(&tuple.values[..]).unwrap(),
            tuple.values.as_slice()
        );
        assert_eq!(
            super::TupleLike::as_slice(&tuple.values.as_slice()).unwrap(),
            tuple.values.as_slice()
        );
    }

    #[test]
    fn test_tuple_deserialize_null_bits_and_primary_projection() {
        let serializers = [
            LogicalType::Integer.serializable(),
            LogicalType::Integer.serializable(),
        ];
        let mut bytes = vec![0b1000_0000];
        serializers[1]
            .to_raw(&DataValue::Int32(7), &mut bytes)
            .unwrap();

        let mut tuple = Tuple::default();
        tuple
            .deserialize_from_into(serializers.iter(), &bytes, 2)
            .unwrap();
        assert_eq!(tuple.values, vec![DataValue::Null, DataValue::Int32(7)]);

        assert_eq!(
            Tuple::primary_projection(&[1], tuple.values.as_slice()),
            DataValue::Int32(7)
        );
        assert_eq!(
            Tuple::primary_projection(&[0, 1], tuple.values.as_slice()),
            DataValue::Tuple(vec![DataValue::Null, DataValue::Int32(7)])
        );
    }

    fn ints(pk: Option<i32>, values: &[i32]) -> super::TupleImpl {
        Tuple::new(
            pk.map(DataValue::Int32),
            values.iter().copied().map(DataValue::Int32).collect(),
        )
        .into()
    }

    fn values_of(tuple: &super::TupleImpl) -> Vec<DataValue> {
        (0..tuple.len())
            .map(|i| tuple.value_at(i).clone())
            .collect()
    }

    #[test]
    fn tuple_impl_concat_reads_as_contiguous() {
        use super::TupleImpl;

        let left = TupleImpl::concat(ints(Some(9), &[0, 1]), ints(None, &[2]), None);
        let right = TupleImpl::concat(ints(None, &[3]), ints(None, &[4, 5]), None);
        let mut tuple = TupleImpl::concat(left, right, Some(DataValue::Int32(7)));

        assert_eq!(tuple.len(), 6);
        assert!(tuple.as_slice().is_none());
        assert_eq!(
            values_of(&tuple),
            (0..6).map(DataValue::Int32).collect_vec()
        );
        assert_eq!(tuple.pk(), Some(&DataValue::Int32(7)));

        tuple.push(DataValue::Int32(6));
        assert_eq!(tuple.len(), 7);
        assert_eq!(tuple.value_at(6), &DataValue::Int32(6));

        assert_eq!(tuple, (0..7).map(DataValue::Int32).collect_vec());
        assert_eq!(tuple[6], DataValue::Int32(6));
        assert_eq!(tuple.get(7), None);

        tuple[1] = DataValue::Int32(10);
        assert_eq!(tuple.get(1), Some(&DataValue::Int32(10)));
        tuple.set_pk(None);
        assert_eq!(tuple.pk(), None);

        let tuple = TupleImpl::concat(ints(None, &[]), ints(None, &[1, 2]), None);
        assert_eq!(tuple, [1, 2].map(DataValue::Int32));
    }

    #[test]
    fn tuple_impl_positional_access_ignores_shape() {
        use super::TupleImpl;
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let hash = |tuple: &TupleImpl| {
            let mut hasher = DefaultHasher::new();
            tuple.hash(&mut hasher);
            hasher.finish()
        };
        let mut joined = TupleImpl::concat(
            TupleImpl::concat(ints(None, &[0]), ints(None, &[1, 2]), None),
            ints(None, &[3]),
            Some(DataValue::Int32(9)),
        );
        let flat = ints(Some(9), &[0, 1, 2, 3]);

        assert_eq!(joined.iter().len(), 4);
        assert_eq!(joined, (0..4).map(DataValue::Int32).collect_vec());
        assert!(joined.values_eq(&flat));
        assert_eq!(joined, flat);
        assert_eq!(hash(&joined), hash(&flat));
        assert_ne!(joined, ints(None, &[0, 1, 2, 3]));
        assert!(!joined.values_eq(&ints(None, &[0, 1, 2])));

        *joined.value_at_mut(2) = DataValue::Int32(20);
        assert_eq!(joined.value_at(2), &DataValue::Int32(20));
        joined.resize(6, DataValue::Null);
        assert_eq!(joined.len(), 6);
        assert_eq!(joined.value_at(5), &DataValue::Null);

        let mut drained = Vec::new();
        joined.drain_values(|value| drained.push(value));
        assert_eq!(
            drained,
            [0, 1, 20, 3]
                .map(DataValue::Int32)
                .into_iter()
                .chain([DataValue::Null, DataValue::Null])
                .collect_vec()
        );
        assert!(matches!(joined, TupleImpl::Tuple(_)));
        assert_eq!(joined.len(), 0);
        assert_eq!(joined.pk(), None);
    }

    #[test]
    fn tuple_impl_output_keeps_leftmost_buffer() {
        use super::TupleImpl;

        let mut tuple = TupleImpl::concat(ints(None, &[0, 1, 2, 3]), ints(None, &[4]), None);
        let output = tuple.output_mut();
        assert!(output.values.capacity() >= 4);
        assert_eq!(output.values.len(), 4);
        assert!(matches!(tuple, TupleImpl::Tuple(_)));
    }

    #[test]
    fn concat_pool_recycles_and_reuses_boxes() {
        use super::{ConcatPool, TupleImpl};

        let mut pool = ConcatPool::default();
        let inner = pool.concat(ints(None, &[0]), ints(None, &[1]), None);
        let mut tuple = pool.concat(inner, ints(None, &[2, 3]), Some(DataValue::Int32(9)));
        assert_eq!(tuple, [0, 1, 2, 3].map(DataValue::Int32));

        assert_eq!(
            pool.retain_rightmost(&mut tuple).values,
            [2, 3].map(DataValue::Int32)
        );
        assert_eq!(pool.0.len(), 2);

        let inner = pool.concat(ints(None, &[4]), ints(None, &[5]), None);
        let mut tuple = pool.concat(ints(None, &[6]), inner, None);
        assert!(pool.0.is_empty());
        assert_eq!(tuple, [6, 4, 5].map(DataValue::Int32));

        assert_eq!(
            pool.retain_leftmost(&mut tuple).values,
            [6].map(DataValue::Int32)
        );
        assert!(matches!(tuple, TupleImpl::Tuple(_)));
        assert_eq!(pool.0.len(), 2);

        let dropped = pool.concat(ints(None, &[7]), ints(None, &[8]), None);
        drop(dropped);
        assert_eq!(pool.0.len(), 1);
    }
}
// GRCOV_EXCL_STOP
