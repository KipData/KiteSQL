//! Counts allocation calls and requested bytes while enabled, and tracks the live heap always.

use std::ffi::{c_int, c_void};
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering::Relaxed};

static COUNTING: AtomicBool = AtomicBool::new(false);
static CALLS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);
static LIVE: AtomicIsize = AtomicIsize::new(0);

extern "C" {
    fn __libc_malloc(size: usize) -> *mut c_void;
    fn __libc_calloc(count: usize, size: usize) -> *mut c_void;
    fn __libc_realloc(ptr: *mut c_void, size: usize) -> *mut c_void;
    fn __libc_memalign(align: usize, size: usize) -> *mut c_void;
    fn __libc_free(ptr: *mut c_void);
    fn malloc_usable_size(ptr: *mut c_void) -> usize;
}

/// Bytes currently allocated.
#[allow(dead_code)] // Not every example measures the live heap.
pub fn live() -> isize {
    LIVE.load(Relaxed)
}

pub fn start() {
    CALLS.store(0, Relaxed);
    BYTES.store(0, Relaxed);
    COUNTING.store(true, Relaxed);
}

/// Stops counting and returns the allocation calls and the bytes they requested since `start`.
pub fn stop() -> (usize, usize) {
    COUNTING.store(false, Relaxed);
    (CALLS.load(Relaxed), BYTES.load(Relaxed))
}

unsafe fn track(ptr: *mut c_void, size: usize) -> *mut c_void {
    if COUNTING.load(Relaxed) {
        CALLS.fetch_add(1, Relaxed);
        BYTES.fetch_add(size, Relaxed);
    }
    if !ptr.is_null() {
        LIVE.fetch_add(malloc_usable_size(ptr) as isize, Relaxed);
    }
    ptr
}

unsafe fn untrack(ptr: *mut c_void) {
    if !ptr.is_null() {
        LIVE.fetch_sub(malloc_usable_size(ptr) as isize, Relaxed);
    }
}

#[no_mangle]
unsafe extern "C" fn malloc(size: usize) -> *mut c_void {
    track(__libc_malloc(size), size)
}

#[no_mangle]
unsafe extern "C" fn calloc(count: usize, size: usize) -> *mut c_void {
    track(__libc_calloc(count, size), count.saturating_mul(size))
}

#[no_mangle]
unsafe extern "C" fn realloc(ptr: *mut c_void, size: usize) -> *mut c_void {
    untrack(ptr);
    track(__libc_realloc(ptr, size), size)
}

#[no_mangle]
unsafe extern "C" fn memalign(align: usize, size: usize) -> *mut c_void {
    track(__libc_memalign(align, size), size)
}

#[no_mangle]
unsafe extern "C" fn aligned_alloc(align: usize, size: usize) -> *mut c_void {
    track(__libc_memalign(align, size), size)
}

#[no_mangle]
unsafe extern "C" fn posix_memalign(out: *mut *mut c_void, align: usize, size: usize) -> c_int {
    let ptr = track(__libc_memalign(align, size), size);
    if ptr.is_null() {
        return 12; // ENOMEM
    }
    *out = ptr;
    0
}

#[no_mangle]
unsafe extern "C" fn free(ptr: *mut c_void) {
    untrack(ptr);
    __libc_free(ptr)
}
