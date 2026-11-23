pub(crate) mod cache;
pub(crate) mod codec;
pub mod compact;
pub mod error;
pub(crate) mod format;
pub(crate) mod lock;
pub(crate) mod store;
mod vfs;

pub use compact::{CompactOptions, CompactStats, compact};
pub use error::{Error, Result};
pub use vfs::{VFS_NAME, register, register_with, uri};

/// Default and per-connection configuration for the `sqmicro` SQLite VFS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    /// Zstandard compression level (default `3`).
    pub level: i32,
    /// Target uncompressed chunk size in KiB (default `128`, clamped to `[page_size, 256 KiB]`).
    pub chunk_kib: usize,
    /// Per-connection decompressed chunk LRU cache size in MiB (default `16`).
    pub cache_mib: usize,
    /// Worker threads used to compress chunks in parallel during flush (default `2`).
    pub threads: usize,
    /// Auto-compaction dead-space ratio percentage on truncate (`0` disables, default `50`).
    pub compact_threshold: u8,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            level: 3,
            chunk_kib: 128,
            cache_mib: 16,
            threads: 2,
            compact_threshold: 50,
        }
    }
}
