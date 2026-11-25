# sqmicro

A SQLite VFS, written in Rust, that keeps a live database compressed on disk with Zstandard while it is being written and read.

sqmicro was built for recording sim-racing telemetry with [Bono](https://github.com/racebono): one process appends rows continuously for hours while other processes poll the newest rows and run queries over the whole session. On a real 106-minute race it stores the recording in about a third of the space of plain SQLite.

- **Compressed while live.** Pages are grouped into chunks (128 KiB by default) and compressed as SQLite checkpoints them. You don't need a separate compression step.
- **Fast access to the newest rows.** The `-wal` and `-shm` files go straight to the operating system's VFS, so rows that haven't been checkpointed yet are read without any decompression.
- **Cross-process.** One writer and many readers, in different processes, using SQLite's own WAL locking.
- **One file at rest.** After a truncating checkpoint, the database is a single file you can copy or move.
- **Ordinary SQL.** Schemas, queries, indexes and transactions work as usual.

> [!WARNING]
> sqmicro is at version 0.1 and its file format may still change. Read [Limitations](#limitations) before trusting it with data you can't recreate.

## Getting started

sqmicro isn't on crates.io, so depend on it through git:

```toml
[dependencies]
sqmicro = { git = "https://github.com/racebono/sqmicro" }
rusqlite = { version = "0.40.2", features = ["bundled"] }
```

It needs Rust 1.89 or newer and a C compiler (for zstd). sqmicro uses `libsqlite3-sys` 0.38 without choosing how SQLite is linked, so your application decides that. The `bundled` feature above is the simplest choice.

Register the VFS once per process, then open databases through it:

```rust
use rusqlite::{Connection, OpenFlags};
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    sqmicro::register()?; // safe to call more than once

    let path = sqmicro::uri(Path::new("telemetry.db"))?;
    let conn = Connection::open_with_flags(
        format!("{path}?vfs={}", sqmicro::VFS_NAME),
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI,
    )?;

    conn.execute_batch(
        "PRAGMA page_size = 16384; -- must come before the first table
         PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         CREATE TABLE IF NOT EXISTS lap (id INTEGER PRIMARY KEY, seconds REAL);
         INSERT INTO lap (seconds) VALUES (94.812), (94.377);",
    )?;

    let best: f64 = conn.query_row("SELECT min(seconds) FROM lap", [], |r| r.get(0))?;
    println!("best lap: {best}");
    Ok(())
}
```

Every process that opens the file must do so through sqmicro. Plain SQLite doesn't recognise it.

WAL mode with 16 KiB pages is the recommended setup. All SQLite page sizes work, but set it before the first table: after a later `PRAGMA page_size` and `VACUUM`, the file keeps storing pages of the original size, which makes smaller pages slower to write.

## Configuration

Set options per connection in the URI, for example `?vfs=sqmicro&level=1&cache_mib=32`:

| Option | Default | Meaning |
| --- | ---: | --- |
| `level` | `3` | Zstandard compression level. |
| `chunk_kib` | `128` | Target uncompressed chunk size, limited to between one page and 256 KiB. At most 64 pages per chunk. |
| `cache_mib` | `16` | Cache of decompressed chunks, per connection. `0` disables it. |
| `threads` | `2` | Threads used to compress chunks. |
| `compact_threshold` | `50` | Percentage of dead space that triggers [automatic compaction](#reclaiming-space). `0` turns the threshold off. |

Options that a connection can't use, such as `level` on a read-only one, have no effect. URI options are only read when the connection is opened with `SQLITE_OPEN_URI`.

To change the defaults, register another VFS with its own name:

```rust
use sqmicro::Config;

sqmicro::register_with("sqmicro-fast", Config { level: 1, ..Config::default() })?;
```

### Choosing values

- **Larger chunks** compress better and make range scans cheap, but a lookup that misses the cache decompresses a whole chunk to read one page.
- **Higher levels** cost write time for little gain while live. Level 6 took about 3× as long as level 3 and saved 2% in the benchmark below. Use a high level when [compacting](#reclaiming-space) instead.

## Reclaiming space

sqmicro never overwrites live data: changed pages are appended and the old copies become dead space. That space is **not reused**, so a database that is updated heavily keeps growing until it is compacted. `VACUUM` alone doesn't prevent this.

**Offline.** `compact` rewrites a database into a new, fully packed file, at level 9 by default. This suits saving a session once it's finished:

```rust
use sqmicro::{CompactOptions, compact};
use std::path::Path;

// First, on the last connection: PRAGMA wal_checkpoint(TRUNCATE);
let stats = compact(
    Path::new("telemetry.db"),
    Path::new("saved.db"),
    CompactOptions::default(),
)?;
println!("{} -> {} bytes", stats.input_bytes, stats.output_bytes);
```

Close every connection to both files first. `compact` returns an error if either has a non-empty `-wal` file or a hot `-journal` next to it, but it doesn't check whether another connection is writing. It replaces the destination if it already exists.

**Automatic (Windows only).** When SQLite shrinks the database, as happens after `DELETE`, `VACUUM` and a checkpoint, sqmicro compacts it in place if all of these hold:

- no reader is in the middle of a transaction (otherwise it skips compaction instead of waiting);
- the remaining data is at most 4 MiB compressed and 16 MiB uncompressed;
- at least 4 MiB, and at least `compact_threshold` percent, of the file is dead.

A database that shrinks to nothing is always reset, readers permitting. In practice this covers clearing a recording between sessions. Larger databases need `compact`. Readers that start during automatic compaction wait for it to finish.

## Converting databases

`VACUUM INTO` converts in both directions:

```sql
-- Plain to sqmicro, from a connection to the plain database.
VACUUM INTO 'file:///C:/data/compressed.db?vfs=sqmicro';

-- sqmicro to plain, from a connection to the sqmicro database.
VACUUM INTO 'file:///C:/data/plain.db?vfs=win32'; -- vfs=unix on Linux and macOS
```

Open the source connection with `SQLITE_OPEN_URI`, and register sqmicro first.

## Benchmarks

These results come from replaying a real 106-minute race (14 cars, 13.3 million rows, 2.7 GiB) on one Windows machine. They predate format version 2, and the extra fsync that each checkpoint has needed since to survive a power loss.

| | Size | vs original | Write time | After `compact` |
| --- | ---: | ---: | ---: | ---: |
| Original SQLite, 4 KiB pages | 2,739 MiB | 100% | – | – |
| Plain SQLite, 16 KiB pages | 2,730 MiB | 99.7% | 138 s | – |
| sqmicro, 16 KiB / 128 KiB / level 1 | 941 MiB | 34.4% | 174 s | 869 MiB |
| **sqmicro, 16 KiB / 128 KiB / level 3** | **933 MiB** | **34.1%** | **190 s** | **869 MiB** |
| sqmicro, 16 KiB / 256 KiB / level 3 | 926 MiB | 33.8% | 170 s | 860 MiB |
| sqmicro, 16 KiB / 128 KiB / level 6 | 916 MiB | 33.4% | 594 s | 869 MiB |

Two shorter recordings compressed to 27% live, and to 19% and 21% after `compact`.

Read times with a warm cache, in milliseconds. sqmicro uses 16 KiB / 128 KiB / level 3:

| Query | Original, 4 KiB | Plain, 16 KiB | sqmicro |
| --- | ---: | ---: | ---: |
| Newest player row | 0.02 | 0.02 | 0.15 |
| Standings | 1.55 | 0.87 | 1.41 |
| Summary of every lap (full scan) | 2,474 | 1,930 | 2,418 |
| Tyre temperatures (full scan) | 3,278 | 2,068 | 3,346 |
| Lap traces | 797 | 404 | 1,063 |
| 500 random car states | 1.90 | 3.67 | 21.00 |
| 500 random rows with wheel data | 9.28 | 12.79 | 155.90 |

Full scans are close to the original. Random lookups are where the cost shows: about 0.3 ms per row with its wheel data. Polling for the newest rows, on an otherwise idle database, took 0.05 ms at the median and 8.8 ms for the slowest of 60 polls.

Run the benchmark yourself:

```sh
cargo bench --bench telemetry
```

| Variable | Meaning |
| --- | --- |
| `SQMICRO_BENCH_MINUTES` | Length of the generated 20-car race (default `5.0`). |
| `SQMICRO_BENCH_DB` | Replay an existing plain telemetry database instead. |
| `SQMICRO_BENCH_VARIANTS` | Comma-separated filter on variant names, such as `plain-wal (16k),16k/128k/l3`. |
| `SQMICRO_BENCH_DIR` | Where to put temporary databases. |

## How it works

```text
         writer process                         reader processes
               │                                       │
          SQLite (WAL)                            SQLite (WAL)
               │                                       │
          sqmicro VFS                             sqmicro VFS
            │       │                               │       │
            │       └──── .db-wal / .db-shm ────────┘       │
            │         uncompressed, normal OS files         │
            │                                               │
            └───────────────── main .db ────────────────────┘
                   compressed, append-only chunks
```

Only the main database file is intercepted. When SQLite checkpoints the WAL, runs of neighbouring pages are grouped into chunks, compressed in parallel and appended to the file.

The file has three kinds of content:

- **Superblocks.** Two 4 KiB slots at the start of the file, each with a generation number and a CRC-32. Writes alternate between them, and everything a superblock points to is synced before it is written, so a crash or power loss at any point leaves one that describes intact data.
- **Page directory.** A two-level table that maps each SQLite page to its chunk. Updated parts are appended, then the next superblock points to them.
- **Chunks.** Compressed with zstd and its content checksum. Page 1, and any chunk that doesn't shrink, is stored uncompressed, without a checksum.

Nothing is ever allocated in the 4 KiB at the 1 GiB mark, because SQLite takes mandatory locks there on Windows.

Files start with the bytes `SQMICRO1`, followed by a format version. This release reads and writes **version 2** only. Opening another version, or a plain SQLite database, fails with `SQLITE_NOTADB` and leaves the file unchanged.

## Limitations

- **Damage isn't always detected.** Superblocks and compressed data have checksums; the page directory, chunk headers and uncompressed chunks don't. A damaged directory can make pages read as zeros or as other pages.
- **Windows is the main target.** Unix code paths exist but are experimental, and never compact automatically.
- **I/O errors during a checkpoint.** SQLite ignores the result of a checkpoint's final flush. If that fails while a reader keeps the checkpoint from completing, sqmicro corrects SQLite's WAL index so that the data is still read from the WAL. A connection that starts a transaction in the instant before that can read old data until the next commit, and if it writes, base its changes on it.
- **`synchronous=OFF` still syncs** whenever a superblock is written. `exclusive=1` in the URI isn't supported on Windows.
- **Rust library only.** There is no loadable extension, so tools such as the `sqlite3` shell can't open the files. [Convert](#converting-databases) them first.
- **No memory mapping.** `PRAGMA mmap_size` has no effect.

## Development

```sh
cargo test
```

The tests cover SQLite behaviour across page sizes, simulated power loss and I/O errors, compaction alongside readers, and a writer and readers in separate processes.

| Path | Contents |
| --- | --- |
| `src/vfs.rs` | The SQLite VFS and file methods. |
| `src/store.rs` | The page store: reads, writes, flushing, truncation, automatic compaction. |
| `src/format.rs` | The on-disk format. |
| `src/codec.rs` | Chunk compression. |
| `src/compact.rs` | Offline compaction. |
| `src/cache.rs`, `src/lock.rs` | Chunk cache and compaction lock. |

## License

MIT
