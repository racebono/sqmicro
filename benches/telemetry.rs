//! Bono live telemetry benchmark comparing plain WAL SQLite against `sqmicro`.
//!
//! Environment variables:
//! - `SQMICRO_BENCH_MINUTES`: synthetic 20-car race duration in minutes (default `5.0`).
//! - `SQMICRO_BENCH_DB`: optional existing plain Bono `telemetry.db` to replay into each target.

use rusqlite::{Connection, OpenFlags, params, types::Value};
use sqmicro::{CompactOptions, VFS_NAME, compact, register, uri};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const CARS: i64 = 20;
const HZ: i64 = 50;
const OPPONENT_EVERY: i64 = 5;
const LAP_SECONDS: f64 = 95.0;
const LAP_METRES: f64 = 5000.0;

struct Variant {
    label: &'static str,
    use_sqmicro: bool,
    page_size: i64,
    chunk_kib: usize,
    level: i32,
}

const VARIANTS: &[Variant] = &[
    Variant { label: "plain-wal (4K)", use_sqmicro: false, page_size: 4096, chunk_kib: 0, level: 0 },
    Variant { label: "plain-wal (16K)", use_sqmicro: false, page_size: 16384, chunk_kib: 0, level: 0 },
    Variant { label: "sqmicro 4K/128K/L3", use_sqmicro: true, page_size: 4096, chunk_kib: 128, level: 3 },
    Variant { label: "sqmicro 8K/128K/L3", use_sqmicro: true, page_size: 8192, chunk_kib: 128, level: 3 },
    Variant { label: "sqmicro 16K/64K/L3", use_sqmicro: true, page_size: 16384, chunk_kib: 64, level: 3 },
    Variant { label: "sqmicro 16K/128K/L1", use_sqmicro: true, page_size: 16384, chunk_kib: 128, level: 1 },
    Variant { label: "sqmicro 16K/128K/L3", use_sqmicro: true, page_size: 16384, chunk_kib: 128, level: 3 },
    Variant { label: "sqmicro 16K/256K/L3", use_sqmicro: true, page_size: 16384, chunk_kib: 256, level: 3 },
    Variant { label: "sqmicro 16K/128K/L6", use_sqmicro: true, page_size: 16384, chunk_kib: 128, level: 6 },
];

fn main() {
    register().expect("failed to register sqmicro VFS");

    let minutes: f64 = std::env::var("SQMICRO_BENCH_MINUTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5.0);
    let source_db: Option<PathBuf> = std::env::var_os("SQMICRO_BENCH_DB").map(PathBuf::from);

    let dir = tempfile::tempdir().expect("tempdir");
    if let Some(src) = &source_db {
        println!("Benchmarking replay of real DB: {}\n", src.display());
    } else {
        println!("Benchmarking synthetic {minutes:.1}-minute 20-car race @ {HZ} Hz (opponents every {OPPONENT_EVERY}th tick)\n");
    }

    struct RowResult {
        label: &'static str,
        live_bytes: u64,
        write_s: f64,
        compact_bytes: Option<u64>,
        compact_s: Option<f64>,
        spotter_p50_ms: f64,
        spotter_p99_ms: f64,
        cold_queries: Vec<(&'static str, Duration)>,
        warm_queries: Vec<(&'static str, Duration)>,
    }

    let mut results = Vec::new();
    for (idx, v) in VARIANTS.iter().enumerate() {
        let db_path = dir.path().join(format!("bench_{idx}.db"));
        let start = Instant::now();
        let conn = open_rw(&db_path, v);
        if let Some(src) = &source_db {
            replay_real_db(src, &conn);
        } else {
            generate_race(&conn, minutes);
        }
        let write_s = start.elapsed().as_secs_f64();
        let live_bytes = size_with_wal(&db_path);

        // Measure 30 Hz spotter tail latency while WAL has active hot tail frames.
        let (spotter_p50_ms, spotter_p99_ms) = bench_spotter(&db_path, v);

        // Checkpoint so historical scan benchmarks read from main DB (compressed for sqmicro).
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
        drop(conn);

        let cold_conn = open_ro(&db_path, v);
        let cold_queries = run_engineer_workload(&cold_conn);
        let mut warm_queries = run_engineer_workload(&cold_conn);
        for _ in 0..4 {
            let run = run_engineer_workload(&cold_conn);
            for (best, (_, d)) in warm_queries.iter_mut().zip(run) {
                best.1 = best.1.min(d);
            }
        }
        drop(cold_conn);

        let (compact_bytes, compact_s) = if v.use_sqmicro {
            let saved = dir.path().join(format!("bench_{idx}_compact.db"));
            let c0 = Instant::now();
            let stats = compact(
                &db_path,
                &saved,
                CompactOptions {
                    level: 9,
                    chunk_kib: v.chunk_kib,
                    threads: 2,
                },
            )
            .unwrap();
            (Some(stats.output_bytes), Some(c0.elapsed().as_secs_f64()))
        } else {
            (None, None)
        };

        results.push(RowResult {
            label: v.label,
            live_bytes,
            write_s,
            compact_bytes,
            compact_s,
            spotter_p50_ms,
            spotter_p99_ms,
            cold_queries,
            warm_queries,
        });
    }

    let baseline = results[0].live_bytes as f64;
    println!("### Storage, Write Speed & Live 30 Hz Spotter Latency\n");
    println!("| Variant | Live Size | vs plain-4K | Write | Compact (L9) | vs plain-4K | Compact Time | Spotter p50 / p99 |");
    println!("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |");
    for r in &results {
        let comp_sz = r
            .compact_bytes
            .map_or("-".into(), |b| format!("{:.2} MiB", mib(b)));
        let comp_pct = r
            .compact_bytes
            .map_or("-".into(), |b| format!("{:.1}%", b as f64 / baseline * 100.0));
        let comp_t = r
            .compact_s
            .map_or("-".into(), |s| format!("{s:.2} s"));
        println!(
            "| {} | {:.2} MiB | {:.1}% | {:.2} s | {} | {} | {} | {:.3} / {:.3} ms |",
            r.label,
            mib(r.live_bytes),
            r.live_bytes as f64 / baseline * 100.0,
            r.write_s,
            comp_sz,
            comp_pct,
            comp_t,
            r.spotter_p50_ms,
            r.spotter_p99_ms,
        );
    }

    println!("\n### Historical Engineer Queries (cold / warm best-of-5, ms)\n");
    print!("| Variant |");
    for (name, _) in &results[0].cold_queries {
        print!(" {name} |");
    }
    println!();
    println!("| --- {}|", "| ---: ".repeat(results[0].cold_queries.len()));
    for r in &results {
        print!("| {} |", r.label);
        for ((_, cold), (_, warm)) in r.cold_queries.iter().zip(&r.warm_queries) {
            print!(" {:.2} / {:.2} |", ms(*cold), ms(*warm));
        }
        println!();
    }
}

fn open_rw(path: &Path, v: &Variant) -> Connection {
    let conn = if v.use_sqmicro {
        let u = format!(
            "{}?vfs={VFS_NAME}&level={}&chunk_kib={}&threads=2",
            uri(path).unwrap(),
            v.level,
            v.chunk_kib
        );
        Connection::open_with_flags(
            u,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .unwrap()
    } else {
        Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .unwrap()
    };
    conn.execute_batch(&format!(
        "PRAGMA page_size = {};
         PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;",
        v.page_size
    ))
    .unwrap();
    conn
}

fn open_ro(path: &Path, v: &Variant) -> Connection {
    if v.use_sqmicro {
        let u = format!("{}?vfs={VFS_NAME}&mode=ro&cache_mib=16", uri(path).unwrap());
        Connection::open_with_flags(
            u,
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .unwrap()
    } else {
        Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .unwrap()
    }
}

fn create_schema(conn: &Connection) {
    conn.execute_batch(
        "CREATE TABLE epoch (
            id INTEGER PRIMARY KEY, reason TEXT NOT NULL,
            telemetry_from INTEGER NOT NULL, session_from INTEGER NOT NULL, standings_from INTEGER NOT NULL
        );
        CREATE TABLE telemetry (
            id INTEGER PRIMARY KEY, epoch INTEGER, vehicle_id INTEGER, is_player INTEGER,
            session_time REAL, lap INTEGER, lap_start_time REAL, lap_distance REAL, in_pit_lane INTEGER,
            speed REAL, rpm REAL, max_rpm REAL, gear INTEGER, throttle REAL, brake REAL, clutch REAL,
            steering REAL, fuel REAL, fuel_capacity REAL, water_temp REAL, oil_temp REAL,
            pos_x REAL, pos_y REAL, pos_z REAL, vel_x REAL, vel_y REAL, vel_z REAL,
            accel_x REAL, accel_y REAL, accel_z REAL, pitch REAL, roll REAL, yaw REAL,
            battery_charge REAL, electric_boost REAL, aero_front REAL, aero_rear REAL,
            drs_active INTEGER, pit_limiter INTEGER, flags INTEGER, track_surface TEXT
        );
        CREATE TABLE telemetry_wheel (
            telemetry_id INTEGER NOT NULL REFERENCES telemetry(id),
            wheel INTEGER NOT NULL,
            tire_surface_temperature_left REAL, tire_surface_temperature_center REAL, tire_surface_temperature_right REAL,
            tire_inner_temperature_left REAL, tire_inner_temperature_center REAL, tire_inner_temperature_right REAL,
            tire_pressure REAL, tire_wear REAL, brake_temperature REAL, brake_pressure REAL,
            suspension_deflection REAL, ride_height REAL, wheel_rotation REAL, wheel_load REAL,
            slip_angle REAL, slip_ratio REAL, compound TEXT,
            PRIMARY KEY (telemetry_id, wheel)
        ) WITHOUT ROWID;
        CREATE TABLE car_state (
            id INTEGER PRIMARY KEY, telemetry_id INTEGER NOT NULL REFERENCES telemetry(id),
            vehicle_id INTEGER, session_time REAL,
            tc INTEGER, tc_cut INTEGER, abs INTEGER, brake_bias REAL, engine_map INTEGER,
            damage_front REAL, damage_rear REAL, damage_left REAL, damage_right REAL
        );
        CREATE INDEX car_state_by_vehicle ON car_state (vehicle_id, telemetry_id);
        CREATE TABLE session (
            id INTEGER PRIMARY KEY, epoch INTEGER, session_time REAL, track TEXT, session_type TEXT,
            track_length REAL, ambient_temp REAL, track_temp REAL, rain_intensity REAL
        );
        CREATE TABLE session_sector (
            session_id INTEGER NOT NULL REFERENCES session(id), sector INTEGER NOT NULL,
            local_yellow INTEGER, track_wetness REAL,
            PRIMARY KEY (session_id, sector)
        ) WITHOUT ROWID;
        CREATE TABLE standings (
            id INTEGER PRIMARY KEY, epoch INTEGER, vehicle_id INTEGER, is_player INTEGER,
            driver TEXT, position INTEGER, lap INTEGER, best_lap REAL, last_lap REAL, gap_ahead REAL
        );
        CREATE TABLE standing_sector (
            standing_id INTEGER NOT NULL REFERENCES standings(id), sector INTEGER NOT NULL,
            last_lap REAL, best_lap REAL,
            PRIMARY KEY (standing_id, sector)
        ) WITHOUT ROWID;
        CREATE TABLE forecast (
            id INTEGER PRIMARY KEY, generation INTEGER, session_time REAL, rain REAL, temp REAL
        );",
    )
    .unwrap();
}

fn generate_race(conn: &Connection, minutes: f64) {
    create_schema(conn);
    conn.execute(
        "INSERT INTO epoch (id, reason, telemetry_from, session_from, standings_from) VALUES (1, 'start', 1, 1, 1)",
        [],
    )
    .unwrap();

    let mut telemetry = Insert::new(conn, "telemetry");
    let mut wheels = Insert::new(conn, "telemetry_wheel");
    let mut states = Insert::new(conn, "car_state");
    let mut standings = Insert::new(conn, "standings");
    let mut session = Insert::new(conn, "session");
    let mut forecast = Insert::new(conn, "forecast");
    for point in 0..20 {
        forecast.row(point, 0.0, &[("generation", 1.into())]);
    }

    let ticks = (minutes * 60.0) as i64 * HZ;
    let mut tx = conn.unchecked_transaction().unwrap();
    for tick in 0..ticks {
        let t = tick as f64 / HZ as f64;
        for car in 0..CARS {
            if car != 0 && tick % OPPONENT_EVERY != car % OPPONENT_EVERY {
                continue;
            }
            let along = t + car as f64 * 3.0;
            let lap = (along / LAP_SECONDS) as i64;
            let id = telemetry.row(
                car,
                t,
                &[
                    ("epoch", 1.into()),
                    ("vehicle_id", car.into()),
                    ("is_player", i64::from(car == 0).into()),
                    ("session_time", t.into()),
                    ("lap", lap.into()),
                    ("lap_start_time", (lap as f64 * LAP_SECONDS - car as f64 * 3.0).into()),
                    ("lap_distance", (along % LAP_SECONDS / LAP_SECONDS * LAP_METRES).into()),
                    ("in_pit_lane", 0.into()),
                ],
            );
            for wheel in 0..4 {
                wheels.row(
                    car * 4 + wheel,
                    t,
                    &[("telemetry_id", id.into()), ("wheel", wheel.into())],
                );
            }
            if tick % (60 * HZ) < OPPONENT_EVERY {
                states.row(
                    car,
                    t,
                    &[
                        ("telemetry_id", id.into()),
                        ("vehicle_id", car.into()),
                        ("session_time", t.into()),
                    ],
                );
            }
        }
        if tick % HZ == 0 {
            for car in 0..CARS {
                if (tick / HZ + car) % 30 == 0 {
                    standings.row(
                        car,
                        t,
                        &[
                            ("epoch", 1.into()),
                            ("vehicle_id", car.into()),
                            ("is_player", i64::from(car == 0).into()),
                        ],
                    );
                }
            }
            if tick % (3 * HZ) == 0 {
                session.row(0, t, &[("epoch", 1.into()), ("session_time", t.into())]);
            }
        }
        if tick % HZ == HZ - 1 {
            tx.commit().unwrap();
            tx = conn.unchecked_transaction().unwrap();
        }
    }
    tx.commit().unwrap();
}

fn replay_real_db(src_path: &Path, dst: &Connection) {
    let src = Connection::open_with_flags(
        src_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .unwrap();
    let ddl: Vec<String> = src
        .prepare("SELECT sql FROM sqlite_schema WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY CASE type WHEN 'table' THEN 0 ELSE 1 END, rowid")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    for sql in ddl {
        dst.execute_batch(&sql).unwrap();
    }
    let tables: Vec<String> = src
        .prepare("SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();

    dst.pragma_update(None, "foreign_keys", false).unwrap();
    for table in tables {
        let cols: Vec<String> = src
            .prepare(&format!("PRAGMA table_info(\"{table}\")"))
            .unwrap()
            .query_map([], |r| r.get(1))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        let col_list = cols.iter().map(|c| format!("\"{c}\"")).collect::<Vec<_>>().join(", ");
        let placeholders = vec!["?"; cols.len()].join(", ");
        let mut sel = src.prepare(&format!("SELECT {col_list} FROM \"{table}\"")).unwrap();
        let mut ins = dst
            .prepare(&format!("INSERT INTO \"{table}\" ({col_list}) VALUES ({placeholders})"))
            .unwrap();
        let mut rows = sel.query([]).unwrap();
        let mut tx = dst.unchecked_transaction().unwrap();
        let mut n = 0usize;
        while let Some(row) = rows.next().unwrap() {
            let vals: Vec<Value> = (0..cols.len()).map(|i| row.get::<_, Value>(i).unwrap()).collect();
            ins.execute(rusqlite::params_from_iter(vals)).unwrap();
            n += 1;
            if n % 2000 == 0 {
                tx.commit().unwrap();
                tx = dst.unchecked_transaction().unwrap();
            }
        }
        tx.commit().unwrap();
    }
}

fn bench_spotter(path: &Path, v: &Variant) -> (f64, f64) {
    let conn = open_ro(path, v);
    let mut times = Vec::with_capacity(60);
    for _ in 0..60 {
        let t0 = Instant::now();
        let player: (i64, i64) = conn
            .query_row(
                "SELECT id, vehicle_id FROM telemetry WHERE is_player = 1 ORDER BY id DESC LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        let mut stmt = conn
            .prepare_cached("SELECT id, vehicle_id, speed, lap_distance FROM telemetry WHERE id BETWEEN ?1 AND ?2")
            .unwrap();
        let _rows: Vec<(i64, i64, f64, f64)> = stmt
            .query_map(params![(player.0 - 110).max(1), player.0 + 10], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        times.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.total_cmp(b));
    let p50 = times[times.len() / 2];
    let p99 = times[(times.len() * 99) / 100];
    (p50, p99)
}

fn run_engineer_workload(conn: &Connection) -> Vec<(&'static str, Duration)> {
    let mut out = Vec::new();
    let mut time = |name: &'static str, f: &mut dyn FnMut()| {
        let t0 = Instant::now();
        f();
        out.push((name, t0.elapsed()));
    };

    let mut player = (0i64, 0i64);
    time("player last", &mut || {
        player = conn
            .query_row(
                "SELECT id, vehicle_id FROM telemetry WHERE is_player = 1 ORDER BY id DESC LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
    });

    let mut laps = Vec::<(i64, i64, i64)>::new();
    time("all laps", &mut || {
        laps = conn
            .prepare(
                "SELECT vehicle_id, lap, MIN(id), MAX(id), MAX(lap_start_time), MAX(in_pit_lane), MAX(speed) \
                 FROM telemetry WHERE id BETWEEN ?1 AND ?2 GROUP BY vehicle_id, lap",
            )
            .unwrap()
            .query_map(params![0, player.0], |r| {
                Ok((r.get::<_, Option<i64>>(0)?, r.get(2)?, r.get(3)?))
            })
            .unwrap()
            .filter_map(|r| match r.unwrap() {
                (Some(v), f, l) if v == player.1 => Some((v, f, l)),
                _ => None,
            })
            .collect();
    });

    time("laps trace", &mut || {
        let mut stmt = conn
            .prepare(
                "SELECT session_time, lap_distance, speed, brake, throttle, gear FROM telemetry \
                 WHERE vehicle_id = ?1 AND id BETWEEN ?2 AND ?3 ORDER BY id",
            )
            .unwrap();
        for &(v, f, l) in &laps {
            let _: Vec<()> = stmt
                .query_map(params![v, f, l], |_| Ok(()))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
        }
    });

    time("tyre heat", &mut || {
        let mut stmt = conn
            .prepare(
                "SELECT w.wheel, AVG(w.tire_surface_temperature_center), \
                        MAX(w.tire_surface_temperature_left), AVG(w.brake_temperature) \
                 FROM telemetry t JOIN telemetry_wheel w ON w.telemetry_id = t.id \
                 WHERE t.vehicle_id = ?1 AND t.id BETWEEN ?2 AND ?3 GROUP BY w.wheel",
            )
            .unwrap();
        for &(v, f, l) in &laps {
            let _: Vec<()> = stmt
                .query_map(params![v, f, l], |_| Ok(()))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
        }
    });

    let mut rng = 42u64;
    let mut below = |n: u64| {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (rng >> 33) % n.max(1)
    };
    let vehicles: Vec<i64> = conn
        .prepare("SELECT DISTINCT vehicle_id FROM car_state WHERE vehicle_id IS NOT NULL")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    time("500 states", &mut || {
        let mut stmt = conn
            .prepare(
                "SELECT * FROM car_state WHERE vehicle_id = ?1 AND telemetry_id <= ?2 \
                 ORDER BY telemetry_id DESC LIMIT 1",
            )
            .unwrap();
        for _ in 0..if vehicles.is_empty() { 0 } else { 500 } {
            let v = vehicles[below(vehicles.len() as u64) as usize];
            let id = below(player.0 as u64) as i64;
            let _: Vec<()> = stmt
                .query_map(params![v, id], |_| Ok(()))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
        }
    });

    time("500 rows+w", &mut || {
        let mut row = conn.prepare("SELECT * FROM telemetry WHERE id = ?1").unwrap();
        let mut wheels = conn
            .prepare("SELECT * FROM telemetry_wheel WHERE telemetry_id = ?1 ORDER BY wheel")
            .unwrap();
        for _ in 0..500 {
            let id = 1 + below(player.0 as u64) as i64;
            let _: Vec<()> = row.query_map([id], |_| Ok(())).unwrap().collect::<rusqlite::Result<_>>().unwrap();
            let _: Vec<()> = wheels.query_map([id], |_| Ok(())).unwrap().collect::<rusqlite::Result<_>>().unwrap();
        }
    });

    time("standings", &mut || {
        let _: Vec<()> = conn
            .prepare("SELECT * FROM standings WHERE id IN (SELECT MAX(id) FROM standings GROUP BY vehicle_id)")
            .unwrap()
            .query_map([], |_| Ok(()))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
    });

    out
}

struct Insert<'conn> {
    conn: &'conn Connection,
    stmt: rusqlite::Statement<'conn>,
    columns: Vec<(String, String)>,
}

impl<'conn> Insert<'conn> {
    fn new(conn: &'conn Connection, table: &str) -> Self {
        let columns: Vec<(String, String)> = conn
            .prepare("SELECT name, type FROM pragma_table_info(?1) WHERE name != 'id' ORDER BY cid")
            .unwrap()
            .query_map([table], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        let names = columns.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>();
        let stmt = conn
            .prepare(&format!(
                "INSERT INTO {table} ({}) VALUES ({})",
                names.join(", "),
                vec!["?"; names.len()].join(", ")
            ))
            .unwrap();
        Self { conn, stmt, columns }
    }

    fn row(&mut self, key: i64, t: f64, given: &[(&str, Value)]) -> i64 {
        let values: Vec<Value> = self
            .columns
            .iter()
            .enumerate()
            .map(|(k, (name, kind))| {
                if let Some((_, val)) = given.iter().find(|(g, _)| g == name) {
                    return val.clone();
                }
                if k % 6 == 5 {
                    return Value::Null;
                }
                match kind.as_str() {
                    "REAL" => curve(k, key, t).into(),
                    "INTEGER" => {
                        (((t * 0.01 * (k % 5 + 1) as f64) as i64 + key) % (2 + k as i64 % 9)).into()
                    }
                    _ if k % 3 == 0 => format!("{name} {key}").into(),
                    _ => Value::Null,
                }
            })
            .collect();
        self.stmt.execute(rusqlite::params_from_iter(values)).unwrap();
        self.conn.last_insert_rowid()
    }
}

fn curve(k: usize, key: i64, t: f64) -> f64 {
    let rate = 0.02 + 0.013 * (k % 11) as f64;
    let scale = 10f64.powi((k % 4) as i32);
    (((t * rate + key as f64 * 0.7 + k as f64).sin() + 1.5) * scale) as f32 as f64
}

fn size_with_wal(path: &Path) -> u64 {
    let mut wal = path.as_os_str().to_owned();
    wal.push("-wal");
    let main = std::fs::metadata(path).map_or(0, |m| m.len());
    let wal_len = std::fs::metadata(wal).map_or(0, |m| m.len());
    main + wal_len
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}
