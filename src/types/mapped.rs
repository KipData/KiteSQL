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

use std::borrow::{Borrow, Cow};
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::ptr::NonNull;
#[cfg(debug_assertions)]
use std::sync::{Arc, Weak};
use std::{slice, str};

/// Held by a transaction whose reads stay valid, unchanged and at the same address until it
/// ends, such as an LMDB read-only snapshot. Strings and binaries decoded from those reads point
/// into them instead of being copied.
#[derive(Debug)]
pub struct MappedScope {
    #[cfg(debug_assertions)]
    live: Arc<()>,
}

impl MappedScope {
    pub unsafe fn new() -> Self {
        Self {
            #[cfg(debug_assertions)]
            live: Arc::new(()),
        }
    }
}

mod sealed {
    pub trait Sealed {}

    impl Sealed for str {}
    impl Sealed for [u8] {}
}

pub trait Mappable: sealed::Sealed + ToOwned {
    #[doc(hidden)]
    fn as_raw(&self) -> &[u8];
    #[doc(hidden)]
    unsafe fn from_raw(bytes: &[u8]) -> &Self;

    #[doc(hidden)]
    fn owned_into_raw(owned: Self::Owned) -> Vec<u8>;

    #[doc(hidden)]
    unsafe fn owned_from_raw(raw: Vec<u8>) -> Self::Owned;

    #[doc(hidden)]
    fn owned_capacity(owned: &Self::Owned) -> usize;
}

impl Mappable for str {
    fn as_raw(&self) -> &[u8] {
        self.as_bytes()
    }

    unsafe fn from_raw(bytes: &[u8]) -> &Self {
        unsafe { str::from_utf8_unchecked(bytes) }
    }

    fn owned_into_raw(owned: String) -> Vec<u8> {
        owned.into_bytes()
    }

    unsafe fn owned_from_raw(raw: Vec<u8>) -> String {
        unsafe { String::from_utf8_unchecked(raw) }
    }

    fn owned_capacity(owned: &String) -> usize {
        owned.capacity()
    }
}

impl Mappable for [u8] {
    fn as_raw(&self) -> &[u8] {
        self
    }

    unsafe fn from_raw(bytes: &[u8]) -> &Self {
        bytes
    }

    fn owned_into_raw(owned: Vec<u8>) -> Vec<u8> {
        owned
    }

    unsafe fn owned_from_raw(raw: Vec<u8>) -> Vec<u8> {
        raw
    }

    fn owned_capacity(owned: &Vec<u8>) -> usize {
        owned.capacity()
    }
}

pub type Utf8Str = MaybeMapped<str>;
pub type BinaryBytes = MaybeMapped<[u8]>;

/// A value owned, or pointing into a [`MappedScope`]'s reads.
///
/// A mapped value is only valid until its transaction ends, so it never leaves the engine as is:
/// cloning one copies it into an owned value, a result row hands its values out only through
/// copying methods, and an executor must not read a value kept from an earlier execution (see
/// `ExecutorNode::reset`).
pub struct MaybeMapped<B: ?Sized + Mappable>(Repr<B>);

enum Repr<B: ?Sized + Mappable> {
    Owned(B::Owned),
    Mapped(MappedRead),
}

#[derive(Clone)]
struct MappedRead {
    ptr: NonNull<u8>,
    len: usize,
    #[cfg(debug_assertions)]
    live: Weak<()>,
}

unsafe impl Send for MappedRead {}
unsafe impl Sync for MappedRead {}

impl<B: ?Sized + Mappable> MaybeMapped<B> {
    #[cfg_attr(not(debug_assertions), expect(unused_variables))]
    pub(crate) unsafe fn mapped(value: &B, scope: &MappedScope) -> Self {
        let raw = value.as_raw();
        MaybeMapped(Repr::Mapped(MappedRead {
            ptr: NonNull::from(raw).cast(),
            len: raw.len(),
            #[cfg(debug_assertions)]
            live: Arc::downgrade(&scope.live),
        }))
    }

    pub fn into_owned(self) -> B::Owned {
        match self.0 {
            Repr::Owned(value) => value,
            Repr::Mapped(_) => (*self).to_owned(),
        }
    }

    pub fn to_mut(&mut self) -> &mut B::Owned {
        self.materialize();
        let Repr::Owned(value) = &mut self.0 else {
            unreachable!("materialize leaves an owned value")
        };
        value
    }

    #[cfg(feature = "spill")]
    pub(crate) fn heap_size(&self) -> usize {
        match &self.0 {
            Repr::Owned(value) => B::owned_capacity(value),
            Repr::Mapped(_) => 0,
        }
    }

    pub(crate) unsafe fn into_cow<'a>(self) -> Cow<'a, B> {
        match self.0 {
            Repr::Owned(value) => Cow::Owned(value),
            Repr::Mapped(mapped) => {
                #[cfg(debug_assertions)]
                assert!(
                    mapped.live.strong_count() > 0,
                    "mapped value read after its transaction ended"
                );
                // SAFETY: the read stays valid during `'a`, as the caller guarantees.
                Cow::Borrowed(unsafe {
                    B::from_raw(slice::from_raw_parts(mapped.ptr.as_ptr(), mapped.len))
                })
            }
        }
    }

    #[inline]
    pub(crate) fn materialize(&mut self) {
        if let Repr::Mapped(_) = self.0 {
            self.0 = Repr::Owned((**self).to_owned());
        }
    }

    pub(crate) unsafe fn clone_with(&self, copy: impl FnOnce(&[u8]) -> Vec<u8>) -> Self {
        MaybeMapped(match &self.0 {
            // SAFETY: the caller guarantees `copy` returns the bytes of a `B`.
            Repr::Owned(_) => Repr::Owned(unsafe { B::owned_from_raw(copy((**self).as_raw())) }),
            Repr::Mapped(mapped) => Repr::Mapped(mapped.clone()),
        })
    }

    pub(crate) fn into_owned_raw(self) -> Option<Vec<u8>> {
        match self.0 {
            Repr::Owned(value) => Some(B::owned_into_raw(value)),
            Repr::Mapped(_) => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn is_mapped(&self) -> bool {
        matches!(self.0, Repr::Mapped(_))
    }
}

impl Utf8Str {
    #[inline]
    pub fn as_str(&self) -> &str {
        self
    }

    pub fn into_string(self) -> String {
        self.into_owned()
    }
}

impl BinaryBytes {
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.into_owned()
    }
}

impl<B: ?Sized + Mappable> Deref for MaybeMapped<B> {
    type Target = B;

    #[inline]
    fn deref(&self) -> &B {
        match &self.0 {
            Repr::Owned(value) => value.borrow(),
            Repr::Mapped(mapped) => {
                #[cfg(debug_assertions)]
                assert!(
                    mapped.live.strong_count() > 0,
                    "mapped value read after its transaction ended"
                );
                // SAFETY: `mapped` was built from a `B` that stays valid while its transaction
                // runs, which the caller of `MaybeMapped::mapped` guaranteed.
                unsafe { B::from_raw(slice::from_raw_parts(mapped.ptr.as_ptr(), mapped.len)) }
            }
        }
    }
}

impl<B: ?Sized + Mappable> Clone for MaybeMapped<B> {
    fn clone(&self) -> Self {
        MaybeMapped(Repr::Owned((**self).to_owned()))
    }
}

impl<B: ?Sized + Mappable> Default for MaybeMapped<B>
where
    B::Owned: Default,
{
    fn default() -> Self {
        MaybeMapped(Repr::Owned(B::Owned::default()))
    }
}

impl<B: ?Sized + Mappable> AsRef<B> for MaybeMapped<B> {
    fn as_ref(&self) -> &B {
        self
    }
}

impl<B: ?Sized + Mappable> Borrow<B> for MaybeMapped<B> {
    fn borrow(&self) -> &B {
        self
    }
}

impl<B: ?Sized + Mappable + PartialEq> PartialEq for MaybeMapped<B> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl<B: ?Sized + Mappable + Eq> Eq for MaybeMapped<B> {}

impl<B: ?Sized + Mappable + Ord> PartialOrd for MaybeMapped<B> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<B: ?Sized + Mappable + Ord> Ord for MaybeMapped<B> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        (**self).cmp(&**other)
    }
}

impl<B: ?Sized + Mappable + Hash> Hash for MaybeMapped<B> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        (**self).hash(state)
    }
}

impl<B: ?Sized + Mappable + fmt::Debug> fmt::Debug for MaybeMapped<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl fmt::Display for Utf8Str {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.as_str(), f)
    }
}

macro_rules! impl_owned_conversions {
    ($borrowed:ty, $owned:ty) => {
        impl From<$owned> for MaybeMapped<$borrowed> {
            fn from(value: $owned) -> Self {
                MaybeMapped(Repr::Owned(value))
            }
        }

        impl From<&$borrowed> for MaybeMapped<$borrowed> {
            fn from(value: &$borrowed) -> Self {
                MaybeMapped(Repr::Owned(value.to_owned()))
            }
        }

        impl From<MaybeMapped<$borrowed>> for $owned {
            fn from(value: MaybeMapped<$borrowed>) -> Self {
                value.into_owned()
            }
        }

        impl PartialEq<$borrowed> for MaybeMapped<$borrowed> {
            fn eq(&self, other: &$borrowed) -> bool {
                **self == *other
            }
        }

        impl PartialEq<&$borrowed> for MaybeMapped<$borrowed> {
            fn eq(&self, other: &&$borrowed) -> bool {
                **self == **other
            }
        }

        impl PartialEq<$owned> for MaybeMapped<$borrowed> {
            fn eq(&self, other: &$owned) -> bool {
                **self == **other
            }
        }
    };
}

impl_owned_conversions!(str, String);
impl_owned_conversions!([u8], Vec<u8>);

// GRCOV_EXCL_START
#[cfg(test)]
mod tests {
    use super::{BinaryBytes, Mappable, MappedScope, MaybeMapped, Utf8Str};
    use std::borrow::Borrow;
    use std::collections::hash_map::DefaultHasher;
    use std::fmt::Debug;
    use std::hash::{Hash, Hasher};

    fn hash<T: Hash + ?Sized>(value: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    // `read` stands for a read of the scope's transaction; `next` sorts right after it.
    fn check_mapped<B>(read: &B, next: &B)
    where
        B: ?Sized + Mappable + Ord + Hash + Debug,
        B::Owned: Debug,
    {
        // SAFETY: `read` stays unchanged and outlives the scope.
        let scope = unsafe { MappedScope::new() };
        // SAFETY: as above.
        let mapped = unsafe { MaybeMapped::mapped(read, &scope) };
        let owned = MaybeMapped::<B>(super::Repr::Owned(read.to_owned()));

        assert!(mapped.is_mapped() && !owned.is_mapped());
        assert_eq!(mapped, owned);
        assert_eq!(hash(&mapped), hash(read));
        assert_eq!(format!("{mapped:?}"), format!("{read:?}"));
        let next = MaybeMapped::<B>(super::Repr::Owned(next.to_owned()));
        assert!(mapped < next);
        // A clone may leave the transaction, so it is copied; a shared value keeps pointing into
        // the read and has no buffer to recycle.
        let clone = mapped.clone();
        assert!(!clone.is_mapped() && clone == owned);
        // SAFETY: `to_vec` copies what it is given.
        let shared = unsafe { mapped.clone_with(<[u8]>::to_vec) };
        assert!(shared.is_mapped());
        assert_eq!(shared.into_owned_raw(), None);

        // SAFETY: as above.
        let mut materialized = unsafe { mapped.clone_with(<[u8]>::to_vec) };
        materialized.materialize();
        assert!(!materialized.is_mapped());
        assert_eq!(materialized, owned);
        // SAFETY: as above.
        let mut edited = unsafe { mapped.clone_with(<[u8]>::to_vec) };
        let _ = edited.to_mut();
        assert!(!edited.is_mapped());
        assert!(Borrow::<B>::borrow(&mapped.into_owned()) == read);
    }

    #[test]
    fn mapped_values_behave_like_owned_ones() {
        check_mapped("kite", "kitf");
        check_mapped(&b"kite"[..], &b"kitf"[..]);
        assert_eq!(Utf8Str::from("kite").to_string(), "kite");
        assert_eq!(BinaryBytes::from(vec![1, 2]).into_vec(), [1, 2]);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "mapped value read after its transaction ended")]
    fn reading_after_the_scope_ends_panics() {
        let read = String::from("kite");
        // SAFETY: `read` stays unchanged and outlives the scope.
        let scope = unsafe { MappedScope::new() };
        // SAFETY: as above; the read below is caught before it dereferences.
        let mapped = unsafe { Utf8Str::mapped(read.as_str(), &scope) };
        drop(scope);
        let _ = mapped.as_str();
    }
}
// GRCOV_EXCL_STOP
