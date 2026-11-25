use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use sqmicro::{VFS_NAME, register, uri};
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

const ENV_CHILD_DB: &str = "SQMICRO_CHILD_WRITER_DB";
const ENV_CHILD_READY: &str = "SQMICRO_CHILD_READY_FILE";
const ENV_CHILD_CLEARED: &str = "SQMICRO_CHILD_CLEARED_FILE";

fn open_rw(path: &std::path::Path) -> rusqlite::Result<Connection> {
    let u = format!("{}?vfs={VFS_NAME}", uri(path).unwrap());
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

fn create_bono_schema(conn: &Connection) {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS epoch (
            id INTEGER PRIMARY KEY, reason TEXT NOT NULL,
            telemetry_from INTEGER NOT NULL, session_from INTEGER NOT NULL, standings_from INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS telemetry (
            id INTEGER PRIMARY KEY, epoch INTEGER, vehicle_id INTEGER, is_player INTEGER,
            session_time REAL, lap INTEGER, lap_start_time REAL, lap_distance REAL,
            speed REAL, throttle REAL, brake REAL, gear INTEGER, in_pit_lane INTEGER,
            raw_blob BLOB
        );
        CREATE TABLE IF NOT EXISTS telemetry_wheel (
            telemetry_id INTEGER NOT NULL REFERENCES telemetry(id),
            wheel INTEGER NOT NULL,
            tire_surface_temperature_center REAL,
            tire_surface_temperature_left REAL,
            brake_temperature REAL,
            PRIMARY KEY (telemetry_id, wheel)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS car_state (
            id INTEGER PRIMARY KEY, telemetry_id INTEGER NOT NULL REFERENCES telemetry(id),
            vehicle_id INTEGER, session_time REAL, fuel REAL, tc INTEGER, abs INTEGER
        );
        CREATE INDEX IF NOT EXISTS car_state_by_vehicle ON car_state (vehicle_id, telemetry_id);
        CREATE TABLE IF NOT EXISTS session (
            id INTEGER PRIMARY KEY, epoch INTEGER, session_time REAL, track TEXT
        );
        CREATE TABLE IF NOT EXISTS standings (
            id INTEGER PRIMARY KEY, epoch INTEGER, vehicle_id INTEGER, is_player INTEGER,
            position INTEGER, lap INTEGER
        );",
    )
    .unwrap();
}

/// Child entry point invoked by the parent test process.
#[test]
fn cross_process_child_writer_entry() {
    let Ok(db_str) = std::env::var(ENV_CHILD_DB) else {
        return; // Only runs when spawned as child process.
    };
    let ready_path = PathBuf::from(std::env::var(ENV_CHILD_READY).unwrap());
    let cleared_path = PathBuf::from(std::env::var(ENV_CHILD_CLEARED).unwrap());
    let db_path = PathBuf::from(db_str);

    register().unwrap();
    let conn = open_rw(&db_path).unwrap();
    conn.busy_timeout(Duration::from_secs(5)).unwrap();
    conn.execute_batch(
        "PRAGMA page_size = 16384;
         PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA wal_autocheckpoint = 64;",
    )
    .unwrap();
    create_bono_schema(&conn);
    conn.execute(
        "INSERT INTO epoch (id, reason, telemetry_from, session_from, standings_from) VALUES (1, 'start', 1, 1, 1)",
        [],
    )
    .unwrap();

    let mut insert_tel = conn
        .prepare(
            "INSERT INTO telemetry (epoch, vehicle_id, is_player, session_time, lap, lap_start_time, \
             lap_distance, speed, throttle, brake, gear, in_pit_lane, raw_blob) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 0, ?12)",
        )
        .unwrap();
    let mut insert_wheel = conn
        .prepare(
            "INSERT INTO telemetry_wheel (telemetry_id, wheel, tire_surface_temperature_center, \
             tire_surface_temperature_left, brake_temperature) VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .unwrap();
    let mut insert_state = conn
        .prepare(
            "INSERT INTO car_state (telemetry_id, vehicle_id, session_time, fuel, tc, abs) \
             VALUES (?1, ?2, ?3, ?4, 3, 2)",
        )
        .unwrap();
    let mut insert_standing = conn
        .prepare(
            "INSERT INTO standings (epoch, vehicle_id, is_player, position, lap) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .unwrap();

    // Deterministic pseudo-random blob generator, so part of phase 1 doesn't compress.
    let mut rng = 0x9e37_79b9_7f4a_7c15u64;
    let mut next_blob = |len: usize| -> Vec<u8> {
        (0..len)
            .map(|_| {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                rng as u8
            })
            .collect()
    };

    // Phase 1: write 20 batches (~400 telemetry + 1600 wheel rows per batch = 8,000 telemetry rows)
    // with checkpoints triggering continuously while parent reads concurrently.
    for batch in 0..20i64 {
        let tx = conn.unchecked_transaction().unwrap();
        for tick in 0..80i64 {
            let t = (batch * 80 + tick) as f64 / 50.0;
            for car in 0..20i64 {
                if car != 0 && tick % 5 != car % 5 {
                    continue;
                }
                let lap = (t / 95.0) as i64;
                // Attach 512B high-entropy blob to 1 in 4 rows in phase 1
                let blob = if tick % 4 == 0 {
                    next_blob(512)
                } else {
                    vec![(car as u8).wrapping_add(batch as u8); 64]
                };
                insert_tel
                    .execute(params![
                        1i64,
                        car,
                        i64::from(car == 0),
                        t,
                        lap,
                        lap as f64 * 95.0,
                        (t * 50.0) % 5000.0,
                        60.0 + (t + car as f64).sin() * 20.0,
                        0.85,
                        0.0,
                        4i64,
                        blob
                    ])
                    .unwrap();
                let tid = conn.last_insert_rowid();
                for w in 0..4i64 {
                    insert_wheel
                        .execute(params![tid, w, 85.0 + w as f64, 84.0, 320.0 + t])
                        .unwrap();
                }
                if tick == 0 {
                    insert_state
                        .execute(params![tid, car, t, 60.0 - batch as f64 * 0.5])
                        .unwrap();
                }
            }
        }
        for car in 0..20i64 {
            insert_standing
                .execute(params![1i64, car, i64::from(car == 0), car + 1, batch / 5])
                .unwrap();
        }
        tx.commit().unwrap();
        if batch == 1 {
            std::fs::write(&ready_path, b"ready").unwrap();
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    // Mid-run Bono `clear_tables` sequence: DELETE all tables + new epoch + VACUUM + wal_checkpoint(TRUNCATE).
    {
        let tx = conn.unchecked_transaction().unwrap();
        tx.pragma_update(None, "defer_foreign_keys", true).unwrap();
        for tbl in [
            "standings",
            "session",
            "car_state",
            "telemetry_wheel",
            "telemetry",
            "epoch",
        ] {
            tx.execute(&format!("DELETE FROM {tbl}"), []).unwrap();
        }
        tx.execute(
            "INSERT INTO epoch (id, reason, telemetry_from, session_from, standings_from) VALUES (2, 'cleared', 1, 1, 1)",
            [],
        )
        .unwrap();
        tx.commit().unwrap();
        // Retry VACUUM + TRUNCATE briefly if a reader holds a snapshot right now.
        for _ in 0..10 {
            if conn
                .execute_batch("VACUUM; PRAGMA wal_checkpoint(TRUNCATE);")
                .is_ok()
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }
    std::fs::write(&cleared_path, b"cleared").unwrap();

    // Phase 2: write 8 post-clear batches in epoch 2.
    for batch in 0..8i64 {
        let tx = conn.unchecked_transaction().unwrap();
        for tick in 0..50i64 {
            let t = (batch * 50 + tick) as f64 / 50.0;
            for car in 0..20i64 {
                if car != 0 && tick % 5 != car % 5 {
                    continue;
                }
                let lap = (t / 95.0) as i64;
                insert_tel
                    .execute(params![
                        2i64,
                        car,
                        i64::from(car == 0),
                        t,
                        lap,
                        lap as f64 * 95.0,
                        (t * 50.0) % 5000.0,
                        72.0 + (t + car as f64).cos() * 15.0,
                        0.9,
                        0.0,
                        5i64,
                        vec![2u8; 32]
                    ])
                    .unwrap();
                let tid = conn.last_insert_rowid();
                for w in 0..4i64 {
                    insert_wheel
                        .execute(params![tid, w, 90.0 + w as f64, 89.0, 350.0])
                        .unwrap();
                }
            }
        }
        for car in 0..20i64 {
            insert_standing
                .execute(params![2i64, car, i64::from(car == 0), car + 1, 0i64])
                .unwrap();
        }
        tx.commit().unwrap();
        std::thread::sleep(Duration::from_millis(20));
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
}

#[test]
fn cross_process_wal_writer_with_spotter_engineer_and_clear() {
    register().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("telemetry.db");
    let ready_path = dir.path().join("ready");
    let cleared_path = dir.path().join("cleared");

    let exe = std::env::current_exe().unwrap();
    let mut child = Command::new(exe)
        .args(["--exact", "cross_process_child_writer_entry", "--nocapture"])
        .env(ENV_CHILD_DB, &db_path)
        .env(ENV_CHILD_READY, &ready_path)
        .env(ENV_CHILD_CLEARED, &cleared_path)
        .spawn()
        .expect("failed to spawn child writer process");

    // Wait for child to initialize schema and commit initial rows.
    let deadline = Instant::now() + Duration::from_secs(15);
    while !ready_path.exists() {
        assert!(Instant::now() < deadline, "child writer never became ready");
        std::thread::sleep(Duration::from_millis(10));
    }

    let stop = Arc::new(AtomicBool::new(false));
    let spotter_polls = Arc::new(AtomicU64::new(0));
    let engineer_scans = Arc::new(AtomicU64::new(0));
    let max_spotter_us = Arc::new(AtomicU64::new(0));

    // Spotter reader thread (~30 Hz polling hot tail with 200ms busy timeout).
    let spotter_handle = {
        let db = db_path.clone();
        let stop = stop.clone();
        let polls = spotter_polls.clone();
        let max_us = max_spotter_us.clone();
        std::thread::spawn(move || {
            let conn = open_ro(&db).unwrap();
            conn.busy_timeout(Duration::from_millis(200)).unwrap();
            while !stop.load(Ordering::Relaxed) {
                let start = Instant::now();
                let res: rusqlite::Result<()> = (|| {
                    let player: Option<(i64, i64)> = conn
                        .query_row(
                            "SELECT id, vehicle_id FROM telemetry WHERE is_player = 1 ORDER BY id DESC LIMIT 1",
                            [],
                            |r| Ok((r.get(0)?, r.get(1)?)),
                        )
                        .optional()?;
                    if let Some((pid, _)) = player {
                        let mut stmt = conn.prepare_cached(
                            "SELECT id, vehicle_id, speed, lap_distance FROM telemetry WHERE id BETWEEN ?1 AND ?2",
                        )?;
                        let _rows = stmt
                            .query_map(params![(pid - 110).max(1), pid + 10], |r| {
                                Ok((
                                    r.get::<_, i64>(0)?,
                                    r.get::<_, i64>(1)?,
                                    r.get::<_, f64>(2)?,
                                    r.get::<_, f64>(3)?,
                                ))
                            })?
                            .collect::<rusqlite::Result<Vec<_>>>()?;
                    }
                    let mut st = conn.prepare_cached(
                        "SELECT vehicle_id, position FROM standings WHERE id IN (SELECT MAX(id) FROM standings GROUP BY vehicle_id)",
                    )?;
                    let _s = st
                        .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    Ok(())
                })();
                match res {
                    Ok(()) => {
                        let us = start.elapsed().as_micros() as u64;
                        max_us.fetch_max(us, Ordering::Relaxed);
                        polls.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        // A brief SQLITE_BUSY during child VACUUM is acceptable; CORRUPT/IOERR is never acceptable.
                        let code = e.sqlite_error_code();
                        assert!(
                            matches!(
                                code,
                                Some(rusqlite::ErrorCode::DatabaseBusy)
                                    | Some(rusqlite::ErrorCode::DatabaseLocked)
                            ),
                            "spotter hit fatal SQLite error: {e}"
                        );
                    }
                }
                std::thread::sleep(Duration::from_millis(30));
            }
        })
    };

    // Engineer reader thread (historical lap range scans & wheel joins).
    let engineer_handle = {
        let db = db_path.clone();
        let stop = stop.clone();
        let scans = engineer_scans.clone();
        std::thread::spawn(move || {
            let conn = open_ro(&db).unwrap();
            conn.busy_timeout(Duration::from_millis(500)).unwrap();
            while !stop.load(Ordering::Relaxed) {
                let res: rusqlite::Result<()> = (|| {
                    let mut laps = conn.prepare_cached(
                        "SELECT vehicle_id, lap, MIN(id), MAX(id), MAX(speed) FROM telemetry GROUP BY vehicle_id, lap",
                    )?;
                    let _ = laps
                        .query_map([], |r| {
                            Ok((
                                r.get::<_, i64>(0)?,
                                r.get::<_, i64>(1)?,
                                r.get::<_, i64>(2)?,
                                r.get::<_, i64>(3)?,
                                r.get::<_, f64>(4)?,
                            ))
                        })?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    let mut tyres = conn.prepare_cached(
                        "SELECT w.wheel, AVG(w.tire_surface_temperature_center) \
                         FROM telemetry t JOIN telemetry_wheel w ON w.telemetry_id = t.id \
                         WHERE t.is_player = 1 GROUP BY w.wheel",
                    )?;
                    let _ = tyres
                        .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, f64>(1)?)))?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    Ok(())
                })();
                match res {
                    Ok(()) => {
                        scans.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        let code = e.sqlite_error_code();
                        assert!(
                            matches!(
                                code,
                                Some(rusqlite::ErrorCode::DatabaseBusy)
                                    | Some(rusqlite::ErrorCode::DatabaseLocked)
                            ),
                            "engineer hit fatal SQLite error: {e}"
                        );
                    }
                }
                std::thread::sleep(Duration::from_millis(60));
            }
        })
    };

    let status = child.wait().expect("failed waiting for child writer");
    stop.store(true, Ordering::Relaxed);
    spotter_handle.join().expect("spotter thread panicked");
    engineer_handle.join().expect("engineer thread panicked");
    assert!(status.success(), "child writer exited with {status}");
    assert!(cleared_path.exists(), "child did not reach clear_tables");

    let polls = spotter_polls.load(Ordering::Relaxed);
    let scans = engineer_scans.load(Ordering::Relaxed);
    assert!(polls >= 5, "expected multiple spotter polls, got {polls}");
    assert!(scans >= 2, "expected multiple engineer scans, got {scans}");

    // Final verification: integrity check, post-clear epoch 2 data intact, and file deletion works.
    {
        let ro = open_ro(&db_path).unwrap();
        let integrity: String = ro
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .unwrap();
        assert_eq!(integrity, "ok");
        let (epoch_id, tel_cnt, wheel_cnt): (i64, i64, i64) = ro
            .query_row(
                "SELECT (SELECT MAX(id) FROM epoch), (SELECT count(*) FROM telemetry), \
                 (SELECT count(*) FROM telemetry_wheel)",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(epoch_id, 2);
        assert!(tel_cnt > 1000, "expected phase 2 rows, got {tel_cnt}");
        assert_eq!(wheel_cnt, tel_cnt * 4);
    }

    eprintln!(
        "cross-process stress passed: spotter_polls={polls}, max_spotter_ms={:.2}, engineer_scans={scans}, final_db_kib={}",
        max_spotter_us.load(Ordering::Relaxed) as f64 / 1000.0,
        std::fs::metadata(&db_path).unwrap().len() / 1024
    );

    for suffix in ["", "-wal", "-shm"] {
        let mut os = db_path.as_os_str().to_owned();
        os.push(suffix);
        let p = PathBuf::from(os);
        if p.exists() {
            std::fs::remove_file(&p)
                .unwrap_or_else(|e| panic!("failed to delete {}: {e}", p.display()));
        }
    }
}
