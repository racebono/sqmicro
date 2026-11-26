//! Races and I/O failures that need to be provoked at an exact point inside SQLite's VFS calls.

use rusqlite::{Connection, OpenFlags, ffi};
use sqmicro::{VFS_NAME, register, uri};
use std::cell::{Cell, RefCell};
use std::ffi::{CString, c_char, c_int, c_void};
use std::path::Path;
use std::rc::Rc;
use std::sync::{Mutex, OnceLock};

type Hook = Box<dyn FnOnce()>;

thread_local! {
    /// Fails this many of this thread's writes to main database files with SQLITE_FULL.
    static FAIL_WRITES: Cell<u32> = const { Cell::new(0) };
    /// Fails this thread's next sync of a main database file.
    static FAIL_NEXT_SYNC: Cell<bool> = const { Cell::new(false) };
    /// Fails this thread's syncs of main database files whose bit is set, counting from bit 0.
    static FAIL_SYNCS: Cell<u32> = const { Cell::new(0) };
    /// Lets this many of this thread's syncs of main database files through, then fails them.
    static SYNCS_LEFT: Cell<u32> = const { Cell::new(u32::MAX) };
    /// Runs once, right after this thread's connection has locked a WAL read mark.
    static AFTER_READ_MARK: RefCell<Option<Hook>> = const { RefCell::new(None) };
    /// Runs once, after this thread has passed this many other points where another connection
    /// could do something between two steps of sqmicro or SQLite.
    static AT_POINT: RefCell<Option<(u32, Hook)>> = const { RefCell::new(None) };
}

type OpenFn = unsafe extern "C" fn(
    *mut ffi::sqlite3_vfs,
    *const c_char,
    *mut ffi::sqlite3_file,
    c_int,
    *mut c_int,
) -> c_int;

fn point() {
    let hook = AT_POINT.with_borrow_mut(|at| match at {
        Some((0, _)) => at.take().map(|(_, hook)| hook),
        Some((left, _)) => {
            *left -= 1;
            None
        }
        None => None,
    });
    if let Some(hook) = hook {
        hook();
    }
}

/// What a patched VFS originally had, and the method table it now gives main database files.
struct Patch {
    open: OpenFn,
    original: OnceLock<&'static ffi::sqlite3_io_methods>,
    patched: OnceLock<ffi::sqlite3_io_methods>,
}

static OS: OnceLock<Patch> = OnceLock::new();
static SQMICRO: OnceLock<Patch> = OnceLock::new();

unsafe fn open_with(
    patch: &'static Patch,
    modify: fn(&mut ffi::sqlite3_io_methods),
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    unsafe {
        let rc = (patch.open)(vfs, name, file, flags, out_flags);
        if rc == ffi::SQLITE_OK && flags & ffi::SQLITE_OPEN_MAIN_DB != 0 {
            let original = *patch.original.get_or_init(|| &*(*file).pMethods);
            (*file).pMethods = patch.patched.get_or_init(|| {
                let mut methods = *original;
                modify(&mut methods);
                methods
            });
        }
        rc
    }
}

unsafe extern "C" fn os_open(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    let modify = |m: &mut ffi::sqlite3_io_methods| {
        m.xRead = Some(os_read);
        m.xWrite = Some(os_write);
        m.xSync = Some(os_sync);
    };
    unsafe { open_with(OS.get().unwrap(), modify, vfs, name, file, flags, out_flags) }
}

unsafe extern "C" fn os_read(
    file: *mut ffi::sqlite3_file,
    data: *mut c_void,
    len: c_int,
    offset: ffi::sqlite3_int64,
) -> c_int {
    let original = OS.get().unwrap().original.get().unwrap();
    let rc = unsafe { original.xRead.unwrap()(file, data, len, offset) };
    point();
    rc
}

unsafe extern "C" fn os_write(
    file: *mut ffi::sqlite3_file,
    data: *const c_void,
    len: c_int,
    offset: ffi::sqlite3_int64,
) -> c_int {
    match FAIL_WRITES.get() {
        0 => {}
        n => {
            FAIL_WRITES.set(if n == u32::MAX { n } else { n - 1 });
            return ffi::SQLITE_FULL;
        }
    }
    let original = OS.get().unwrap().original.get().unwrap();
    unsafe { original.xWrite.unwrap()(file, data, len, offset) }
}

unsafe extern "C" fn os_sync(file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    if FAIL_NEXT_SYNC.take() {
        return ffi::SQLITE_IOERR_FSYNC;
    }
    let mask = FAIL_SYNCS.get();
    FAIL_SYNCS.set(mask >> 1);
    if mask & 1 != 0 {
        return ffi::SQLITE_IOERR_FSYNC;
    }
    match SYNCS_LEFT.get() {
        u32::MAX => {}
        0 => return ffi::SQLITE_IOERR_FSYNC,
        n => SYNCS_LEFT.set(n - 1),
    }
    let original = OS.get().unwrap().original.get().unwrap();
    unsafe { original.xSync.unwrap()(file, flags) }
}

unsafe extern "C" fn sqmicro_open(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    let modify = |m: &mut ffi::sqlite3_io_methods| {
        m.xRead = Some(sqmicro_read);
        m.xFileSize = Some(sqmicro_file_size);
        m.xLock = Some(sqmicro_lock);
        m.xShmLock = Some(sqmicro_shm_lock);
    };
    unsafe {
        open_with(
            SQMICRO.get().unwrap(),
            modify,
            vfs,
            name,
            file,
            flags,
            out_flags,
        )
    }
}

unsafe extern "C" fn sqmicro_read(
    file: *mut ffi::sqlite3_file,
    data: *mut c_void,
    len: c_int,
    offset: ffi::sqlite3_int64,
) -> c_int {
    let original = SQMICRO.get().unwrap().original.get().unwrap();
    let rc = unsafe { original.xRead.unwrap()(file, data, len, offset) };
    point();
    rc
}

unsafe extern "C" fn sqmicro_file_size(
    file: *mut ffi::sqlite3_file,
    size: *mut ffi::sqlite3_int64,
) -> c_int {
    let original = SQMICRO.get().unwrap().original.get().unwrap();
    let rc = unsafe { original.xFileSize.unwrap()(file, size) };
    point();
    rc
}

unsafe extern "C" fn sqmicro_lock(file: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    let original = SQMICRO.get().unwrap().original.get().unwrap();
    let rc = unsafe { original.xLock.unwrap()(file, level) };
    point();
    rc
}

unsafe extern "C" fn sqmicro_shm_lock(
    file: *mut ffi::sqlite3_file,
    offset: c_int,
    n: c_int,
    flags: c_int,
) -> c_int {
    let original = SQMICRO.get().unwrap().original.get().unwrap();
    let rc = unsafe { original.xShmLock.unwrap()(file, offset, n, flags) };
    // Slot 3 is the mark for readers that ignore the WAL, which checkpoints wait for.
    if rc == ffi::SQLITE_OK
        && offset > 3
        && flags == ffi::SQLITE_SHM_LOCK | ffi::SQLITE_SHM_SHARED
        && let Some(hook) = AFTER_READ_MARK.take()
    {
        hook();
    }
    point();
    rc
}

fn setup() {
    static DONE: Mutex<bool> = Mutex::new(false);
    let mut done = DONE.lock().unwrap();
    if *done {
        return;
    }
    unsafe {
        let patch = |vfs: *mut ffi::sqlite3_vfs, slot: &OnceLock<Patch>, open: OpenFn| {
            let saved = Patch {
                open: (*vfs).xOpen.unwrap(),
                original: OnceLock::new(),
                patched: OnceLock::new(),
            };
            assert!(slot.set(saved).is_ok());
            (*vfs).xOpen = Some(open);
        };
        ffi::sqlite3_initialize();
        patch(ffi::sqlite3_vfs_find(std::ptr::null()), &OS, os_open);
        register().unwrap();
        let name = CString::new(VFS_NAME).unwrap();
        patch(ffi::sqlite3_vfs_find(name.as_ptr()), &SQMICRO, sqmicro_open);
    }
    *done = true;
}

fn open(path: &Path) -> Connection {
    open_uri(path, "")
}

/// Opens with more URI parameters, such as `&cache_mib=0`.
fn open_uri(path: &Path, params: &str) -> Connection {
    setup();
    Connection::open_with_flags(
        format!("{}?vfs={VFS_NAME}{params}", uri(path).unwrap()),
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI,
    )
    .unwrap()
}

fn int(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get(0)).unwrap()
}

fn assert_intact(conn: &Connection) {
    let res: String = conn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(res, "ok");
}

/// A WAL database with `t(v)` holding one row, 1, all of it checkpointed, and no automatic
/// checkpoints on the returned connection.
fn wal_database(path: &Path) -> Connection {
    let conn = open(path);
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA wal_autocheckpoint = 0;
         CREATE TABLE t (v INTEGER);
         INSERT INTO t VALUES (1);
         CREATE TABLE other (v INTEGER);
         PRAGMA wal_checkpoint(TRUNCATE);",
    )
    .unwrap();
    conn
}

#[test]
fn checkpoint_while_a_reader_starts_does_not_make_it_read_old_data() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("a.db");
    let writer = wal_database(&db);
    let reader = open(&db);
    assert_eq!(int(&reader, "SELECT v FROM t"), 1);

    writer.execute_batch("UPDATE t SET v = 2").unwrap();
    AFTER_READ_MARK.set(Some(Box::new(move || {
        let (busy, log, done) = writer
            .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            })
            .unwrap();
        assert!(busy == 0 && log > 0 && log == done, "not a full checkpoint");
    })));
    assert_eq!(int(&reader, "SELECT v FROM t"), 2);
    assert!(
        AFTER_READ_MARK.take().is_none(),
        "the race was not provoked"
    );
    assert_eq!(int(&reader, "SELECT v FROM t"), 2);
}

#[test]
fn failed_partial_checkpoint_does_not_hide_committed_data() {
    for writer_survives in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("a.db");
        let writer = wal_database(&db);

        writer.execute_batch("UPDATE t SET v = 2").unwrap();
        let old_reader = open(&db);
        old_reader.execute_batch("BEGIN").unwrap();
        assert_eq!(int(&old_reader, "SELECT v FROM t"), 2);
        writer
            .execute_batch("INSERT INTO other VALUES (1)")
            .unwrap();

        // Partial, because of old_reader. SQLite reports success regardless.
        FAIL_WRITES.set(u32::MAX);
        writer
            .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |_| Ok(()))
            .unwrap();
        let other = open(&db);
        assert_eq!(int(&other, "SELECT v FROM t"), 2);
        assert_eq!(int(&writer, "SELECT v FROM t"), 2);
        let writer = if writer_survives {
            FAIL_WRITES.set(0);
            Some(writer)
        } else {
            drop(writer);
            FAIL_WRITES.set(0);
            None
        };

        old_reader.execute_batch("COMMIT").unwrap();
        other
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
                assert_eq!(r.get::<_, i64>(0)?, 0);
                Ok(())
            })
            .unwrap();
        assert_eq!(int(&other, "SELECT v FROM t"), 2);
        drop((writer, old_reader, other));

        let reopened = open(&db);
        assert_eq!(int(&reopened, "SELECT v FROM t"), 2);
        assert_eq!(int(&reopened, "SELECT count(*) FROM other"), 1);
        assert_intact(&reopened);
    }
}

#[test]
fn failed_checkpoint_never_overwrites_later_commits() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("a.db");
    let a = wal_database(&db);
    a.execute_batch("UPDATE t SET v = 2").unwrap();
    FAIL_WRITES.set(u32::MAX);
    let failed = a.query_row("PRAGMA wal_checkpoint(FULL)", [], |_| Ok(()));
    FAIL_WRITES.set(0);
    assert!(failed.is_err());

    let b = open(&db);
    b.execute_batch(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 3000)
         INSERT INTO other SELECT i FROM n;
         PRAGMA wal_checkpoint(TRUNCATE);",
    )
    .unwrap();
    assert_eq!(int(&a, "SELECT count(*) FROM other"), 3000);
    drop(a);
    drop(b);

    let reopened = open(&db);
    assert_eq!(int(&reopened, "SELECT v FROM t"), 2);
    assert_eq!(int(&reopened, "SELECT count(*) FROM other"), 3000);
    assert_intact(&reopened);
}

#[test]
fn failed_rollback_mode_commit_leaves_the_previous_state() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("a.db");
    let a = open(&db);
    a.execute_batch(
        "PRAGMA journal_mode = DELETE;
         CREATE TABLE t (v INTEGER);
         INSERT INTO t VALUES (1);",
    )
    .unwrap();
    FAIL_WRITES.set(u32::MAX);
    let failed = a.execute_batch("UPDATE t SET v = 2");
    FAIL_WRITES.set(0);
    assert!(failed.is_err());

    let b = open(&db);
    assert_eq!(int(&b, "SELECT v FROM t"), 1);
    b.execute_batch("UPDATE t SET v = 3").unwrap();
    assert_eq!(int(&a, "SELECT v FROM t"), 3);
    drop(a);
    assert_eq!(int(&b, "SELECT v FROM t"), 3);
    assert_intact(&b);
}

#[test]
fn commits_reach_the_file_even_if_sqlite_neither_syncs_nor_unlocks() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("a.db");
    let conn = open(&db);
    conn.execute_batch(
        "PRAGMA locking_mode = EXCLUSIVE;
         PRAGMA journal_mode = DELETE;
         PRAGMA synchronous = OFF;
         CREATE TABLE t (v INTEGER);",
    )
    .unwrap();
    for i in 1..=20 {
        conn.execute("INSERT INTO t VALUES (?1)", [i]).unwrap();
    }
    // What a crash of this process would leave behind.
    let copy = dir.path().join("copy.db");
    std::fs::copy(&db, &copy).unwrap();
    // Exclusive mode leaves a journal with a zeroed header, which isn't hot.
    let journal = std::fs::read(dir.path().join("a.db-journal")).unwrap_or_default();
    assert!(journal.first().is_none_or(|&b| b == 0));
    let copied = open(&copy);
    assert_eq!(int(&copied, "SELECT count(*) FROM t"), 20);
    assert_intact(&copied);
}

#[test]
fn rollback_mode_transaction_survives_failure_at_any_sync() {
    for (change, grows) in [
        ("UPDATE t SET v = 'new' || v", false),
        ("PRAGMA page_size = 4096; VACUUM", false),
        ("PRAGMA page_size = 65536; VACUUM", false),
        (
            "INSERT INTO t WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n              WHERE i < 12000) SELECT i + 12000, printf('row-%0100d', i + 12000) FROM n",
            true,
        ),
    ] {
        let mut failures = 0;
        for syncs in 0..8 {
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("a.db");
            let conn = open(&db);
            conn.execute_batch(
                "PRAGMA page_size = 16384;
                 PRAGMA journal_mode = DELETE;
                 CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
                 WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 12000)
                 INSERT INTO t SELECT i, printf('row-%0100d', i) FROM n;",
            )
            .unwrap();
            SYNCS_LEFT.set(syncs);
            let res = conn.execute_batch(change);
            SYNCS_LEFT.set(u32::MAX);
            failures += res.is_err() as u32;

            let other = open(&db);
            for conn in [&other, &conn] {
                let rows = int(conn, "SELECT count(*) FROM t");
                let old = int(conn, "SELECT count(*) FROM t WHERE v LIKE 'row-%'");
                assert!(
                    rows == 12000 || (grows && rows == 24000),
                    "{change}: {rows} rows"
                );
                assert!(old == rows || old == 0, "{change}: {old} of {rows}");
                if res.is_ok() {
                    assert!(old == 0 || change != "UPDATE t SET v = 'new' || v");
                }
                assert_intact(conn);
            }
            other
                .execute_batch("INSERT INTO t VALUES (100000, 'row-')")
                .unwrap();
            assert_eq!(int(&conn, "SELECT max(id) FROM t"), 100_000);
            assert_intact(&conn);
        }
        assert!(failures >= 2, "{change}: only {failures} failures");
    }
}

#[test]
fn checkpoint_does_not_retry_after_a_failed_sync() {
    for exclusive in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("a.db");
        let conn = wal_database(&db);
        if exclusive {
            conn.execute_batch("PRAGMA locking_mode = EXCLUSIVE")
                .unwrap();
        }
        conn.execute_batch("UPDATE t SET v = 2").unwrap();

        // The pages may never reach the disk after this, unless they are written again.
        FAIL_NEXT_SYNC.set(true);
        let res = conn.query_row("PRAGMA wal_checkpoint(FULL)", [], |_| Ok(()));
        assert!(!FAIL_NEXT_SYNC.take());
        assert!(res.is_err());
        assert_eq!(int(&conn, "SELECT v FROM t"), 2);

        conn.execute_batch("INSERT INTO other VALUES (1)").unwrap();
        let before = std::fs::metadata(&db).unwrap().len();
        conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            assert_eq!(r.get::<_, i64>(0)?, 0);
            Ok(())
        })
        .unwrap();
        assert!(std::fs::metadata(&db).unwrap().len() > before);
        drop(conn);
        let reopened = open(&db);
        assert_eq!(int(&reopened, "SELECT v FROM t"), 2);
        assert_eq!(int(&reopened, "SELECT count(*) FROM other"), 1);
        assert_intact(&reopened);
    }
}

#[test]
fn switching_to_exclusive_locking_keeps_other_connections_commits() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("a.db");
    let a = wal_database(&db);
    let b = open(&db);
    b.execute_batch("UPDATE t SET v = 2").unwrap();
    a.execute_batch("PRAGMA locking_mode = EXCLUSIVE; BEGIN")
        .unwrap();
    assert_eq!(int(&a, "SELECT count(*) FROM other"), 0);
    b.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |r| {
        assert!(r.get::<_, i64>(1)? > 0);
        assert_eq!(r.get::<_, i64>(1)?, r.get::<_, i64>(2)?);
        Ok(())
    })
    .unwrap();
    drop(b);
    // From here on, a takes no more locks that would tell it of b's checkpoint.
    a.execute_batch("INSERT INTO other VALUES (1); COMMIT")
        .unwrap();
    assert_eq!(int(&a, "SELECT v FROM t"), 2);
    a.execute_batch("UPDATE t SET v = v + 100").unwrap();
    drop(a);
    let reopened = open(&db);
    assert_eq!(int(&reopened, "SELECT v FROM t"), 102);
    assert_intact(&reopened);
}

#[test]
fn failed_transaction_without_a_journal_file_is_not_published() {
    let mut failures = 0;
    for writes in 1..=4 {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("a.db");
        let conn = open(&db);
        conn.execute_batch(
            "PRAGMA page_size = 16384;
             PRAGMA journal_mode = MEMORY;
             PRAGMA cache_size = 10;
             CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 2000)
             INSERT INTO t SELECT i, zeroblob(12000) FROM n;",
        )
        .unwrap();
        FAIL_WRITES.set(writes);
        let failed = conn
            .execute_batch("UPDATE t SET v = randomblob(12000)")
            .is_err();
        FAIL_WRITES.set(0);
        failures += failed as u32;
        drop(conn);
        let reopened = open(&db);
        let new = int(
            &reopened,
            "SELECT count(*) FROM t WHERE v <> zeroblob(12000)",
        );
        assert_eq!(new, if failed { 0 } else { 2000 }, "{writes} failed writes");
        assert_intact(&reopened);
    }
    assert!(failures >= 3, "only {failures} failures");
}

#[test]
fn failed_rollback_without_a_journal_file_publishes_nothing() {
    let mut failures = 0;
    for syncs in 0..4 {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("a.db");
        let conn = open(&db);
        conn.execute_batch(
            "PRAGMA journal_mode = MEMORY;
             PRAGMA cache_size = 10;
             CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 2000)
             INSERT INTO t SELECT i, zeroblob(500) FROM n;
             BEGIN;
             UPDATE t SET v = randomblob(500);
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 500)
             INSERT INTO t SELECT i + 2000, randomblob(500) FROM n;",
        )
        .unwrap();
        SYNCS_LEFT.set(syncs);
        failures += conn.execute_batch("ROLLBACK").is_err() as u32;
        SYNCS_LEFT.set(u32::MAX);
        let other = open(&db);
        for conn in [&other, &conn] {
            assert_eq!(int(conn, "SELECT count(*) FROM t"), 2000, "{syncs} syncs");
            assert_eq!(
                int(conn, "SELECT count(*) FROM t WHERE v <> zeroblob(500)"),
                0
            );
            assert_intact(conn);
        }
    }
    // SQLite reports success regardless.
    assert_eq!(failures, 0);
}

/// Readers in a transaction keep compaction from moving the pages they are reading, and those that
/// keep their connection read the moved pages afterwards.
#[cfg(windows)]
#[test]
fn compaction_waits_for_readers_and_moves_pages_under_idle_ones() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("a.db");
    let writer = open(&db);
    writer
        .execute_batch(
            "PRAGMA auto_vacuum = FULL;
             PRAGMA journal_mode = WAL;
             PRAGMA wal_autocheckpoint = 0;
             CREATE TABLE keep (id INTEGER PRIMARY KEY, v BLOB);
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 300)
             INSERT INTO keep SELECT i, randomblob(2000) FROM n;
             CREATE TABLE junk (v BLOB);
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 800)
             INSERT INTO junk SELECT randomblob(8000) FROM n;
             PRAGMA wal_checkpoint(TRUNCATE);",
        )
        .unwrap();
    let digest = "SELECT group_concat(hex(v), '') FROM keep";
    let want: String = writer.query_row(digest, [], |r| r.get(0)).unwrap();
    let size = || std::fs::metadata(&db).unwrap().len();
    let before = size();

    let reading = open_uri(&db, "&cache_mib=0");
    let idle = open(&db);
    let got: String = idle.query_row(digest, [], |r| r.get(0)).unwrap();
    assert_eq!(got, want);

    writer.execute_batch("DELETE FROM junk").unwrap();
    reading.execute_batch("BEGIN").unwrap();
    assert_eq!(int(&reading, "SELECT count(*) FROM keep WHERE id = 1"), 1);
    // Copies everything back, and would compact if it weren't for `reading`.
    writer
        .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |r| {
            assert_eq!(r.get::<_, i64>(1)?, r.get::<_, i64>(2)?);
            Ok(())
        })
        .unwrap();
    let got: String = reading.query_row(digest, [], |r| r.get(0)).unwrap();
    assert_eq!(got, want);
    reading.execute_batch("COMMIT").unwrap();
    assert!(size() >= before);

    writer
        .execute_batch("DELETE FROM keep WHERE id > 250; PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    assert!(
        size() < before / 4,
        "not compacted: {before} -> {} bytes",
        size()
    );
    let want: String = writer.query_row(digest, [], |r| r.get(0)).unwrap();
    for conn in [&idle, &reading] {
        let got: String = conn.query_row(digest, [], |r| r.get(0)).unwrap();
        assert_eq!(got, want);
        assert_intact(conn);
    }
}

/// Rollback-mode commits shrink the file once the journal is finalized, and compact after it.
#[cfg(windows)]
#[test]
fn rollback_mode_commits_compact_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("a.db");
    let conn = open(&db);
    conn.execute_batch(
        "PRAGMA auto_vacuum = FULL;
         PRAGMA journal_mode = DELETE;
         CREATE TABLE t (v BLOB);
         WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 700)
         INSERT INTO t SELECT randomblob(8000) FROM n;",
    )
    .unwrap();
    let before = std::fs::metadata(&db).unwrap().len();
    conn.execute_batch("DELETE FROM t").unwrap();
    let after = std::fs::metadata(&db).unwrap().len();
    assert!(after < before / 10, "{before} -> {after} bytes");
    drop(conn);
    let reopened = open(&db);
    assert_eq!(int(&reopened, "SELECT count(*) FROM t"), 0);
    assert_intact(&reopened);
}

#[test]
fn commit_without_a_journal_file_is_whole_or_absent_after_failed_syncs() {
    // Always the one after the commit has published (bit 1), and any others among the first few,
    // such as two in its rollback.
    for mask in (0..128u32).filter(|mask| mask & 2 != 0) {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("a.db");
        let conn = open(&db);
        conn.execute_batch(
            "PRAGMA journal_mode = MEMORY;
             CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 200)
             INSERT INTO t SELECT i, zeroblob(500) FROM n;",
        )
        .unwrap();
        FAIL_SYNCS.set(mask);
        let res = conn.execute_batch(
            "BEGIN;
             UPDATE t SET v = randomblob(500);
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 2000)
             INSERT INTO t SELECT i + 200, randomblob(500) FROM n;
             COMMIT;",
        );
        FAIL_SYNCS.set(0);
        if res.is_err() {
            let _ = conn.execute_batch("ROLLBACK");
        }
        let other = open(&db);
        let rows = int(&other, "SELECT count(*) FROM t");
        let old = int(&other, "SELECT count(*) FROM t WHERE v = zeroblob(500)");
        assert!(
            (rows, old) == (200, 200) || (rows, old) == (2200, 0),
            "{mask:b}: {rows} {old}"
        );
        assert_intact(&other);
    }
}

#[test]
fn commit_across_databases_is_whole_or_absent_after_failed_syncs() {
    let mut failures = 0;
    for mask in 1..256u32 {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("a.db");
        let conn = open(&db);
        conn.execute_batch(&format!(
            "PRAGMA journal_mode = MEMORY;
             CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 200)
             INSERT INTO t SELECT i, zeroblob(500) FROM n;
             ATTACH '{}?vfs={VFS_NAME}' AS b;
             CREATE TABLE b.u (v INTEGER);",
            uri(&dir.path().join("b.db")).unwrap()
        ))
        .unwrap();
        // Among them those of the other database, which can fail after this one has committed.
        FAIL_SYNCS.set(mask);
        let res = conn.execute_batch(
            "BEGIN;
             UPDATE t SET v = randomblob(500);
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 2000)
             INSERT INTO t SELECT i + 200, randomblob(500) FROM n;
             INSERT INTO b.u VALUES (1);
             COMMIT;",
        );
        FAIL_SYNCS.set(0);
        if res.is_err() {
            failures += 1;
            let _ = conn.execute_batch("ROLLBACK");
        }
        drop(conn);
        let other = open(&db);
        let rows = int(&other, "SELECT count(*) FROM t");
        let old = int(&other, "SELECT count(*) FROM t WHERE v = zeroblob(500)");
        let whole = (rows, old) == (200, 200) || (rows, old) == (2200, 0);
        assert!(whole, "{mask:b}: {rows} {old}");
        assert_intact(&other);
    }
    assert!(failures > 100, "only {failures} failures");
}

#[test]
fn failed_sync_in_exclusive_locking_mode_is_rolled_back_at_once() {
    for journal in ["DELETE", "MEMORY", "OFF"] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("a.db");
        let conn = open(&db);
        conn.execute_batch(&format!(
            "PRAGMA locking_mode = EXCLUSIVE;
             PRAGMA journal_mode = {journal};
             CREATE TABLE t (v INTEGER);
             INSERT INTO t VALUES (1);"
        ))
        .unwrap();
        FAIL_NEXT_SYNC.set(true);
        assert!(conn.execute_batch("UPDATE t SET v = 2").is_err());
        assert!(!FAIL_NEXT_SYNC.take());
        // SQLite never unlocks the file in this mode.
        assert_eq!(int(&conn, "SELECT v FROM t"), 1, "{journal}");
        conn.execute_batch("UPDATE t SET v = 3").unwrap();
        drop(conn);
        let reopened = open(&db);
        assert_eq!(int(&reopened, "SELECT v FROM t"), 3);
        assert_intact(&reopened);
    }
}

#[test]
fn failed_commit_does_not_leave_the_connection_unable_to_read() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("a.db");
    let a = open(&db);
    a.busy_timeout(std::time::Duration::ZERO).unwrap();
    a.execute_batch(
        "PRAGMA journal_mode = DELETE;
         CREATE TABLE t (v INTEGER);
         INSERT INTO t VALUES (1);",
    )
    .unwrap();
    FAIL_WRITES.set(u32::MAX);
    assert!(a.execute_batch("UPDATE t SET v = 2").is_err());
    FAIL_WRITES.set(0);
    let b = open(&db);
    b.execute_batch("BEGIN").unwrap();
    assert_eq!(int(&b, "SELECT v FROM t"), 1);
    assert_eq!(int(&a, "SELECT v FROM t"), 1);
}

/// Runs `change` on a second connection at every point of `query` on the first. Returns how often
/// that shrank the file to less than half.
fn change_at_every_point(
    template: &Path,
    already_open: bool,
    change: &'static str,
    query: &str,
) -> u32 {
    let mut shrunk = 0;
    for at in 0.. {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("a.db");
        std::fs::copy(template, &db).unwrap();
        let size = || std::fs::metadata(&db).unwrap().len();
        let before = size();
        let writer = Rc::new(open(&db));
        writer.busy_timeout(std::time::Duration::ZERO).unwrap();
        writer
            .execute_batch("PRAGMA wal_autocheckpoint = 0; INSERT INTO other VALUES (1)")
            .unwrap();
        let old = int(&writer, query);
        let reader = already_open.then(|| {
            let reader = open(&db);
            assert_eq!(int(&reader, query), old);
            reader.execute_batch("PRAGMA shrink_memory").unwrap();
            reader
        });

        let other = writer.clone();
        let hook: Hook = Box::new(move || {
            // The checkpoint may be busy or partial.
            let _ = other.execute_batch(change);
        });
        AT_POINT.set(Some((at, hook)));
        let reader = reader.unwrap_or_else(|| open(&db));
        let got = reader.query_row(query, [], |r| r.get::<_, i64>(0));
        let fired = AT_POINT.take().is_none();
        let new = int(&writer, query);
        match got {
            Ok(got) => assert!(got == old || got == new, "point {at}: {got}"),
            Err(e) => assert_eq!(
                e.sqlite_error_code(),
                Some(rusqlite::ErrorCode::DatabaseBusy),
                "point {at}: {e}"
            ),
        }
        assert_eq!(int(&reader, query), new, "point {at}");
        shrunk += (size() < before / 2) as u32;
        if !fired {
            assert!(at >= 5, "only {at} points");
            assert_intact(&reader);
            return shrunk;
        }
    }
    unreachable!()
}

#[test]
fn queries_survive_changes_by_others_at_any_point() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("template.db");
    let conn = wal_database(&db);
    conn.execute_batch(
        "CREATE TABLE blobs (b BLOB);
         WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 700)
         INSERT INTO blobs SELECT randomblob(8000) FROM n;
         UPDATE t SET v = 1;
         PRAGMA wal_checkpoint(TRUNCATE);",
    )
    .unwrap();
    drop(conn);
    let mut compactions = 0;
    for already_open in [false, true] {
        for change in [
            "UPDATE t SET v = 2; PRAGMA wal_checkpoint(PASSIVE)",
            "UPDATE t SET v = 2; PRAGMA wal_checkpoint(TRUNCATE); UPDATE t SET v = 3",
            "DELETE FROM blobs; VACUUM; PRAGMA wal_checkpoint(TRUNCATE); UPDATE t SET v = 2",
        ] {
            compactions += change_at_every_point(&db, already_open, change, "SELECT v FROM t");
        }
    }
    assert!(compactions >= 3, "only {compactions} live compactions");
}
