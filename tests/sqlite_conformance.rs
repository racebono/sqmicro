use rusqlite::{Connection, OpenFlags};
use sqmicro::{CompactOptions, VFS_NAME, compact, register, uri};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};

fn open_rw(path: &std::path::Path, extra_query: &str) -> rusqlite::Result<Connection> {
    let mut u = format!("{}?vfs={VFS_NAME}", uri(path).unwrap());
    if !extra_query.is_empty() {
        u.push('&');
        u.push_str(extra_query);
    }
    Connection::open_with_flags(
        u,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
}

fn open_ro(path: &std::path::Path) -> rusqlite::Result<Connection> {
    let u = format!("{}?vfs={VFS_NAME}&mode=ro", uri(path).unwrap());
    Connection::open_with_flags(
        u,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
}

fn check_integrity(conn: &Connection) {
    let res: String = conn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(res, "ok", "integrity_check failed: {res}");
}

#[test]
fn wal_mode_crud_without_rowid_blobs_and_integrity_check() {
    register().unwrap();
    register().unwrap(); // idempotent

    for page_size in [512i64, 1024, 2048, 4096, 8192, 16384, 32768, 65536] {
        let dir = tempfile::tempdir().unwrap();
        let compressed = dir.path().join(format!("comp_{page_size}.db"));
        let plain = dir.path().join(format!("plain_{page_size}.db"));

        let setup_sql = format!(
            "PRAGMA page_size = {page_size};
             PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             CREATE TABLE telemetry (id INTEGER PRIMARY KEY, vehicle_id INTEGER, speed REAL, note TEXT, payload BLOB);
             CREATE TABLE wheel (telemetry_id INTEGER NOT NULL, w INTEGER NOT NULL, temp REAL,
                 PRIMARY KEY (telemetry_id, w)) WITHOUT ROWID;
             CREATE INDEX telemetry_by_vehicle ON telemetry (vehicle_id);
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 4000)
             INSERT INTO telemetry SELECT i, i % 20, i * 0.125, 'car ' || (i % 20), zeroblob(32) FROM n;
             INSERT INTO wheel SELECT id, w, speed + w FROM telemetry,
                 (SELECT 0 AS w UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3);
             UPDATE telemetry SET speed = speed + 1.0 WHERE id % 50 = 0;
             DELETE FROM telemetry WHERE id > 3800;
             DELETE FROM wheel WHERE telemetry_id > 3800;
             PRAGMA wal_checkpoint(TRUNCATE);"
        );

        let conn_c = open_rw(&compressed, "").unwrap();
        conn_c.execute_batch(&setup_sql).unwrap();
        drop(conn_c);

        let conn_p = Connection::open(&plain).unwrap();
        conn_p.execute_batch(&setup_sql).unwrap();
        drop(conn_p);

        let comp_len = std::fs::metadata(&compressed).unwrap().len();
        let plain_len = std::fs::metadata(&plain).unwrap().len();
        assert!(
            comp_len < plain_len,
            "page_size={page_size}: compressed ({comp_len}) not smaller than plain ({plain_len})"
        );

        let ro = open_ro(&compressed).unwrap();
        check_integrity(&ro);
        let (t_cnt, w_cnt, actual_ps): (i64, i64, i64) = ro
            .query_row(
                "SELECT (SELECT count(*) FROM telemetry), (SELECT count(*) FROM wheel), \
                 (SELECT page_size FROM pragma_page_size)",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!((t_cnt, w_cnt, actual_ps), (3800, 3800 * 4, page_size));
    }
}

#[test]
fn rollback_mode_and_synchronous_off_persists_writes() {
    register().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("rollback.db");

    {
        let conn = open_rw(&db, "").unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode = DELETE;
             PRAGMA synchronous = OFF;
             CREATE TABLE t (id INTEGER PRIMARY KEY, x REAL, s TEXT);
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 5000)
             INSERT INTO t SELECT i, i * 0.5, 'row ' || (i % 100) FROM n;",
        )
        .unwrap();
    }

    let ro = open_ro(&db).unwrap();
    check_integrity(&ro);
    let cnt: i64 = ro.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(cnt, 5000);
}

#[test]
fn vacuum_and_vacuum_into_compressed_db() {
    register().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("vac.db");
    let into_db = dir.path().join("vac_into.db");

    let conn = open_rw(&db, "").unwrap();
    conn.execute_batch(
        "PRAGMA page_size = 8192;
         PRAGMA journal_mode = WAL;
         CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT);
         WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 5000)
         INSERT INTO t SELECT i, printf('val-%05d-%05d', i, i % 7) FROM n;
         DELETE FROM t WHERE id > 1000;
         VACUUM;
         PRAGMA wal_checkpoint(TRUNCATE);",
    )
    .unwrap();
    check_integrity(&conn);

    conn.execute(
        "VACUUM INTO ?1",
        [format!("{}?vfs={VFS_NAME}", uri(&into_db).unwrap())],
    )
    .unwrap();
    drop(conn);

    let ro = open_ro(&into_db).unwrap();
    check_integrity(&ro);
    let cnt: i64 = ro.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(cnt, 1000);
}

#[test]
fn rejects_plain_sqlite_database() {
    register().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let plain = dir.path().join("plain.db");

    {
        let conn = Connection::open(&plain).unwrap();
        conn.execute_batch("CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1), (2);")
            .unwrap();
    }
    let before = std::fs::read(&plain).unwrap();

    // Opening a plain SQLite DB through sqmicro must fail without modifying it.
    let res = open_ro(&plain).and_then(|c| c.query_row("SELECT count(*) FROM t", [], |r| r.get::<_, i64>(0)));
    assert!(res.is_err(), "expected error opening plain DB via sqmicro");
    assert_eq!(std::fs::read(&plain).unwrap(), before);
}

#[test]
fn torn_superblock_and_trailing_garbage_recover_cleanly() {
    register().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("torn.db");

    {
        let conn = open_rw(&db, "").unwrap();
        conn.execute_batch(
            "PRAGMA page_size = 8192;
             PRAGMA journal_mode = WAL;
             CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t VALUES (1, 'gen1'), (2, 'gen1');
             PRAGMA wal_checkpoint(TRUNCATE);",
        )
        .unwrap();
        // Generation N+1 checkpoint
        conn.execute_batch(
            "INSERT INTO t VALUES (3, 'gen2'), (4, 'gen2');
             PRAGMA wal_checkpoint(TRUNCATE);",
        )
        .unwrap();
    }

    // Determine which superblock slot holds the higher generation and corrupt it + append trailing garbage.
    {
        let mut f = OpenOptions::new().read(true).write(true).open(&db).unwrap();
        let mut sb_region = [0u8; 8192];
        f.read_exact(&mut sb_region).unwrap();
        let gen_a = u64::from_le_bytes(sb_region[16..24].try_into().unwrap());
        let gen_b = u64::from_le_bytes(sb_region[4096 + 16..4096 + 24].try_into().unwrap());
        let newer_slot_offset = if gen_b > gen_a { 4096u64 } else { 0u64 };
        f.seek(SeekFrom::Start(newer_slot_offset + 100)).unwrap();
        f.write_all(&[0xDE, 0xAD, 0xBE, 0xEF]).unwrap();
        f.seek(SeekFrom::End(0)).unwrap();
        f.write_all(&vec![0x55u8; 10 * 1024]).unwrap();
        f.sync_all().unwrap();
    }

    let ro = open_ro(&db).unwrap();
    check_integrity(&ro);
    let cnt: i64 = ro.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap();
    assert!(cnt >= 2, "expected prior valid generation intact, got {cnt}");
    drop(ro);

    let rw = open_rw(&db, "").unwrap();
    rw.execute_batch(
        "WITH RECURSIVE n(i) AS (SELECT 100 UNION ALL SELECT i + 1 FROM n WHERE i < 2000)
         INSERT INTO t SELECT i, 'after' FROM n;
         PRAGMA wal_checkpoint(TRUNCATE);",
    )
    .unwrap();
    drop(rw);
    let ro = open_ro(&db).unwrap();
    check_integrity(&ro);
    let after: i64 = ro
        .query_row("SELECT count(*) FROM t WHERE v = 'after'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(after, 1901);
}

#[test]
fn closed_connection_allows_file_deletion() {
    register().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("cleanup.db");

    {
        let w = open_rw(&db, "").unwrap();
        w.execute_batch(
            "PRAGMA journal_mode = WAL;
             CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t VALUES (1, 'hello');",
        )
        .unwrap();
        let r = open_ro(&db).unwrap();
        let v: String = r.query_row("SELECT v FROM t WHERE id = 1", [], |r| r.get(0)).unwrap();
        assert_eq!(v, "hello");
    }

    for suffix in ["", "-wal", "-shm"] {
        let mut os = db.as_os_str().to_owned();
        os.push(suffix);
        let p = std::path::PathBuf::from(os);
        if p.exists() {
            std::fs::remove_file(&p).unwrap_or_else(|e| panic!("failed to delete {}: {e}", p.display()));
        }
    }
}

#[test]
fn compact_api_roundtrip_after_wal_checkpoint() {
    register().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("live.db");
    let dst = dir.path().join("saved.db");

    {
        let conn = open_rw(&src, "").unwrap();
        conn.execute_batch(
            "PRAGMA page_size = 16384;
             PRAGMA journal_mode = WAL;
             CREATE TABLE telemetry (id INTEGER PRIMARY KEY, speed REAL);
             WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 6000)
             INSERT INTO telemetry SELECT i, i * 0.25 FROM n;
             PRAGMA wal_checkpoint(TRUNCATE);
             UPDATE telemetry SET speed = speed * 2.0 WHERE id <= 3000;
             PRAGMA wal_checkpoint(TRUNCATE);",
        )
        .unwrap();
    }

    let stats = compact(&src, &dst, CompactOptions::default()).unwrap();
    assert!(stats.page_count > 0);
    assert!(stats.output_bytes < stats.input_bytes);

    let ro = open_ro(&dst).unwrap();
    check_integrity(&ro);
    let (cnt, sum): (i64, f64) = ro
        .query_row("SELECT count(*), sum(speed) FROM telemetry", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!(cnt, 6000);
    assert!(sum > 0.0);
}

#[test]
fn pragma_page_size_change_with_vacuum_works_both_directions() {
    register().unwrap();
    for (from, to) in [(16384, 4096), (8192, 4096), (65536, 512), (512, 65536), (4096, 16384)] {
        for rows in [100i64, 12_000] {
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("resize.db");
            let saved = dir.path().join("saved.db");
            let check = |conn: &Connection, page_size: i64, want: i64| {
                check_integrity(conn);
                let got: (i64, i64, i64) = conn
                    .query_row(
                        "SELECT (SELECT page_size FROM pragma_page_size), count(*), sum(id) FROM t \
                         WHERE v = printf('row-%0100d', id)",
                        [],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                    )
                    .unwrap();
                assert_eq!(got, (page_size, want, want * (want + 1) / 2), "{from}->{to}");
            };

            let conn = open_rw(&db, "").unwrap();
            conn.execute_batch(&format!(
                "PRAGMA page_size = {from};
                 PRAGMA journal_mode = DELETE;
                 CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
                 WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {rows})
                 INSERT INTO t SELECT i, printf('row-%0100d', i) FROM n;
                 PRAGMA page_size = {to};
                 VACUUM;"
            ))
            .unwrap();
            check(&conn, to, rows);
            // The size sticks through later writes, in either journal mode, and back again.
            conn.execute_batch(&format!(
                "INSERT INTO t VALUES ({rows} + 1, printf('row-%0100d', {rows} + 1));
                 PRAGMA journal_mode = WAL;
                 INSERT INTO t VALUES ({rows} + 2, printf('row-%0100d', {rows} + 2));
                 PRAGMA wal_checkpoint(TRUNCATE);"
            ))
            .unwrap();
            drop(conn);
            check(&open_ro(&db).unwrap(), to, rows + 2);
            compact(&db, &saved, CompactOptions::default()).unwrap();
            check(&open_ro(&saved).unwrap(), to, rows + 2);

            let conn = open_rw(&db, "").unwrap();
            conn.execute_batch(&format!(
                "PRAGMA journal_mode = DELETE;
                 PRAGMA page_size = {from};
                 VACUUM;
                 INSERT INTO t VALUES ({rows} + 3, printf('row-%0100d', {rows} + 3));"
            ))
            .unwrap();
            drop(conn);
            check(&open_ro(&db).unwrap(), from, rows + 3);
        }
    }
}

#[test]
fn new_databases_larger_than_the_page_cache_work_without_wal() {
    register().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let plain = dir.path().join("plain.db");
    const FILL: &str = "CREATE TABLE t (x);
         WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 400000)
         INSERT INTO t SELECT i FROM n;";
    let check = |conn: &Connection, want: i64| {
        check_integrity(conn);
        let cnt: i64 = conn.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap();
        assert_eq!(cnt, want);
    };

    // VACUUM INTO, both ways.
    let conn = Connection::open(&plain).unwrap();
    conn.execute_batch(FILL).unwrap();
    let into = dir.path().join("into.db");
    conn.execute("VACUUM INTO ?1", [format!("{}?vfs={VFS_NAME}", uri(&into).unwrap())])
        .unwrap();
    let back = dir.path().join("back.db");
    let os_vfs = if cfg!(windows) { "win32" } else { "unix" };
    let ro = open_ro(&into).unwrap();
    check(&ro, 400_000);
    ro.execute("VACUUM INTO ?1", [format!("{}?vfs={os_vfs}", uri(&back).unwrap())])
        .unwrap();
    check(&Connection::open(&back).unwrap(), 400_000);

    // One big transaction, rolled back and committed.
    let direct = dir.path().join("direct.db");
    let conn = open_rw(&direct, "").unwrap();
    conn.execute_batch(&format!("BEGIN; {FILL} ROLLBACK;")).unwrap();
    conn.execute_batch(&format!("BEGIN; {FILL} COMMIT;")).unwrap();
    drop(conn);
    check(&open_ro(&direct).unwrap(), 400_000);
}

#[test]
fn compact_refuses_leftover_wal_and_journal_files() {
    register().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("live.db");
    let dst = dir.path().join("saved.db");
    open_rw(&src, "").unwrap().execute_batch("CREATE TABLE t (x);").unwrap();
    for (name, content, ok) in [
        ("live.db-wal", &b"frames"[..], false),
        ("live.db-journal", b"hot", false),
        ("saved.db-wal", b"frames", false),
        ("saved.db-journal", b"hot", false),
        ("live.db-wal", b"", true),
        ("live.db-journal", b"\0persisted", true),
        ("saved.db-journal", b"", true),
    ] {
        let leftover = dir.path().join(name);
        std::fs::write(&leftover, content).unwrap();
        let res = compact(&src, &dst, CompactOptions::default());
        assert_eq!(res.is_ok(), ok, "{name}: {res:?}");
        assert_eq!(dst.exists(), ok, "{name}");
        assert_eq!(std::fs::read(&leftover).unwrap(), content);
        std::fs::remove_file(&leftover).unwrap();
        let _ = std::fs::remove_file(&dst);
    }
}

#[test]
fn open_without_uri_flag_uses_default_config_safely() {
    register().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("no_uri.db");

    {
        let conn = Connection::open_with_flags_and_vfs(
            &db,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            VFS_NAME,
        )
        .unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             CREATE TABLE t (id INTEGER PRIMARY KEY);
             INSERT INTO t VALUES (1), (2), (3);",
        )
        .unwrap();
    }

    let ro = Connection::open_with_flags_and_vfs(
        &db,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        VFS_NAME,
    )
    .unwrap();
    check_integrity(&ro);
    let cnt: i64 = ro.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(cnt, 3);
}

#[test]
fn concurrent_opens_and_reads_during_live_compaction_do_not_corrupt() {
    register().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("compact_race.db");

    // Pre-populate >4 MiB of incompressible data in WAL mode.
    let writer = open_rw(&db, "").unwrap();
    writer.busy_timeout(std::time::Duration::from_secs(5)).unwrap();
    writer
        .execute_batch(
            "PRAGMA page_size = 16384;
             PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             CREATE TABLE blobs (id INTEGER PRIMARY KEY, b BLOB);",
        )
        .unwrap();

    let mut rng = 0xfeed_face_dead_beefu64;
    let mut make_blob = |len: usize| -> Vec<u8> {
        (0..len)
            .map(|_| {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                rng as u8
            })
            .collect()
    };

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let db_reader = db.clone();
    let stop_reader = stop.clone();
    let reader_thread = std::thread::spawn(move || {
        // Repeatedly open fresh connections & query while writer triggers live compaction cycles.
        while !stop_reader.load(std::sync::atomic::Ordering::Relaxed) {
            let res = open_ro(&db_reader).and_then(|conn| {
                conn.busy_timeout(std::time::Duration::from_millis(200))?;
                conn.query_row("SELECT count(*), total(length(b)) FROM blobs", [], |r| {
                    Ok((r.get::<_, i64>(0)?, r.get::<_, f64>(1)?))
                })
            });
            match res {
                Ok((rows, bytes)) => {
                    assert!(rows == 0 || rows == 350, "saw {rows} rows");
                    assert_eq!(bytes, rows as f64 * 15_000.0);
                }
                Err(e) => assert!(
                    matches!(
                        e.sqlite_error_code(),
                        Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
                    ),
                    "reader hit fatal error during live compaction: {e}"
                ),
            }
        }
    });

    for _round in 0..3 {
        let tx = writer.unchecked_transaction().unwrap();
        {
            let mut ins = tx.prepare("INSERT INTO blobs (b) VALUES (?1)").unwrap();
            for _ in 0..350 {
                ins.execute([make_blob(15_000)]).unwrap();
            }
        }
        tx.commit().unwrap();
        writer.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();

        writer
            .execute_batch("DELETE FROM blobs; VACUUM; PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    reader_thread.join().unwrap();
    check_integrity(&writer);

    // With nobody reading, the space of a cleared recording must come back (on Windows only).
    writer
        .execute_batch("DELETE FROM blobs; VACUUM; PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    let mut superblocks = [0u8; 8192];
    std::fs::File::open(&db).unwrap().read_exact(&mut superblocks).unwrap();
    let compact_id = |slot: usize| {
        u64::from_le_bytes(superblocks[slot + 72..slot + 80].try_into().unwrap())
    };
    if cfg!(windows) {
        assert!(compact_id(0).max(compact_id(4096)) > 0, "live compaction never ran");
    }
}
