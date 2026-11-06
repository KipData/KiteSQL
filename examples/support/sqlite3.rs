//! The parts of the SQLite C API the examples call, from the system library that the `sqlite`
//! dev-dependency also links. The raw API lets the caller read the string SQLite hands out in
//! place, with no further copy; SQLite itself may still have copied it there. Not every
//! example calls every function.
#![allow(dead_code)]

use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::ptr;

pub const ROW: c_int = 100;
pub const DONE: c_int = 101;
pub const DBSTATUS_CACHE_USED: c_int = 1;
pub const DBSTATUS_LOOKASIDE_HIT: c_int = 4;
pub const STMTSTATUS_MEMUSED: c_int = 99;

#[link(name = "sqlite3")]
extern "C" {
    pub fn sqlite3_libversion() -> *const c_char;
    pub fn sqlite3_open(path: *const c_char, db: *mut *mut c_void) -> c_int;
    pub fn sqlite3_close(db: *mut c_void) -> c_int;
    pub fn sqlite3_exec(
        db: *mut c_void,
        sql: *const c_char,
        callback: *const c_void,
        arg: *mut c_void,
        err: *mut *mut c_char,
    ) -> c_int;
    pub fn sqlite3_prepare_v2(
        db: *mut c_void,
        sql: *const c_char,
        len: c_int,
        stmt: *mut *mut c_void,
        tail: *mut *const c_char,
    ) -> c_int;
    pub fn sqlite3_finalize(stmt: *mut c_void) -> c_int;
    pub fn sqlite3_reset(stmt: *mut c_void) -> c_int;
    pub fn sqlite3_bind_int64(stmt: *mut c_void, index: c_int, value: i64) -> c_int;
    pub fn sqlite3_bind_text(
        stmt: *mut c_void,
        index: c_int,
        value: *const c_char,
        len: c_int,
        destructor: Option<unsafe extern "C" fn(*mut c_void)>,
    ) -> c_int;
    pub fn sqlite3_step(stmt: *mut c_void) -> c_int;
    pub fn sqlite3_column_bytes(stmt: *mut c_void, column: c_int) -> c_int;
    pub fn sqlite3_column_int64(stmt: *mut c_void, column: c_int) -> i64;
    pub fn sqlite3_column_text(stmt: *mut c_void, column: c_int) -> *const u8;
    pub fn sqlite3_db_status(
        db: *mut c_void,
        op: c_int,
        cur: *mut c_int,
        hi: *mut c_int,
        reset: c_int,
    ) -> c_int;
    pub fn sqlite3_stmt_status(stmt: *mut c_void, op: c_int, reset: c_int) -> c_int;
}

pub unsafe fn sqlite_exec(db: *mut c_void, sql: &str) {
    let sql = CString::new(sql).unwrap();
    let code = sqlite3_exec(
        db,
        sql.as_ptr(),
        ptr::null(),
        ptr::null_mut(),
        ptr::null_mut(),
    );
    assert_eq!(code, 0, "{sql:?}");
}

pub unsafe fn sqlite_prepare(db: *mut c_void, sql: &CStr) -> *mut c_void {
    let mut stmt = ptr::null_mut();
    let code = sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut());
    assert_eq!(code, 0, "{sql:?}");
    stmt
}

/// Returns a counter's current value and highwater mark, and resets the highwater mark.
pub unsafe fn db_status(db: *mut c_void, op: c_int) -> (c_int, c_int) {
    let (mut current, mut highwater) = (0, 0);
    assert_eq!(
        sqlite3_db_status(db, op, &mut current, &mut highwater, 1),
        0
    );
    (current, highwater)
}
