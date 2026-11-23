use crate::Config;
use crate::error::{Error, Result};
use crate::store::{FileBlockIo, PageStore};
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactOptions {
    pub level: i32,
    pub chunk_kib: usize,
    pub threads: usize,
}

impl Default for CompactOptions {
    fn default() -> Self {
        Self {
            level: 9,
            chunk_kib: 128,
            threads: 2,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactStats {
    pub logical_bytes: u64,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub page_count: u64,
}

pub fn compact(src: &Path, dst: &Path, opts: CompactOptions) -> Result<CompactStats> {
    // Reject uncheckpointed non-empty WAL so callers never lose frames silently.
    let mut wal_os = src.as_os_str().to_owned();
    wal_os.push("-wal");
    let wal_path = PathBuf::from(wal_os);
    if let Ok(meta) = std::fs::metadata(&wal_path)
        && meta.len() > 0
    {
        return Err(Error::InvalidConfig(
            "source database has an uncheckpointed WAL; run PRAGMA wal_checkpoint(TRUNCATE) first"
                .into(),
        ));
    }

    let input_bytes = std::fs::metadata(src)?.len();
    let src_file = OpenOptions::new().read(true).open(src)?;
    let mut reader = PageStore::open(
        FileBlockIo(src_file),
        Config {
            cache_mib: 16,
            ..Config::default()
        },
        true,
    )?;

    let (page_size, page_count) = match reader.superblock() {
        Some(sb) => (sb.page_size, sb.page_count),
        None => (0, 0),
    };
    let logical_bytes = page_count.saturating_mul(page_size as u64);

    let dst_dir = dst
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(dst_dir)?;

    let (tmp_path, tmp_file) = create_temp_file(dst_dir)?;
    struct TempCleanup(Option<PathBuf>);
    impl Drop for TempCleanup {
        fn drop(&mut self) {
            if let Some(p) = self.0.take() {
                let _ = std::fs::remove_file(p);
            }
        }
    }
    let mut cleanup = TempCleanup(Some(tmp_path.clone()));

    let writer_cfg = Config {
        level: opts.level,
        chunk_kib: opts.chunk_kib,
        cache_mib: 4,
        threads: opts.threads,
        compact_threshold: 0,
    };
    let mut writer = PageStore::open(FileBlockIo(tmp_file), writer_cfg, false)?;

    if page_size > 0 && page_count > 0 {
        let ps = page_size as usize;
        let mut buf = vec![0u8; ps];
        for pgno in 1..=page_count {
            // Preserve interior sparse pages without allocating chunks; always write page 1 and last page.
            if pgno > 1 && pgno < page_count && reader.is_page_empty(pgno)? {
                continue;
            }
            reader.read((pgno - 1) * page_size as u64, &mut buf)?;
            writer.write((pgno - 1) * page_size as u64, &buf)?;
        }
        writer.flush(true)?;
    }

    let tmp_file = writer.into_io().0;
    tmp_file.sync_all()?;
    let output_bytes = tmp_file.metadata()?.len();
    drop(tmp_file);
    drop(reader);

    replace_file(&tmp_path, dst)?;
    cleanup.0 = None;

    Ok(CompactStats {
        logical_bytes,
        input_bytes,
        output_bytes,
        page_count,
    })
}

fn create_temp_file(dir: &Path) -> Result<(PathBuf, std::fs::File)> {
    let pid = std::process::id();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    for attempt in 0..64u32 {
        let path = dir.join(format!(".sqmicro-compact-{pid}-{nanos}-{attempt}.tmp"));
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(file) => return Ok((path, file)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "could not allocate unique temp file for compaction",
    )
    .into())
}

fn replace_file(src: &Path, dst: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::{
            MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
        };
        let to_wide = |p: &Path| -> Vec<u16> {
            p.as_os_str()
                .encode_wide()
                .chain(std::iter::once(0))
                .collect()
        };
        let src_w = to_wide(src);
        let dst_w = to_wide(dst);
        let ok = unsafe {
            MoveFileExW(
                src_w.as_ptr(),
                dst_w.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        };
        if ok == 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(())
    }
    #[cfg(not(windows))]
    {
        std::fs::rename(src, dst)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;
    use crate::store::{FileBlockIo, PageStore};

    #[test]
    fn offline_compact_reclaims_overwritten_pages_and_preserves_every_page() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src.db");
        let dst = dir.path().join("dst.db");
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&src)
            .unwrap();
        let mut store = PageStore::open(FileBlockIo(f), Config::default(), false).unwrap();
        let page_size = 8192usize;
        let mut p1 = vec![0u8; page_size];
        p1[16..18].copy_from_slice(&(page_size as u16).to_be_bytes());
        store.write(0, &p1).unwrap();
        for round in 0..5u8 {
            for pgno in 2..=20u64 {
                let mut buf = vec![round.wrapping_add(pgno as u8); page_size];
                buf[..8].copy_from_slice(&pgno.to_le_bytes());
                store.write((pgno - 1) * page_size as u64, &buf).unwrap();
            }
            store.flush(true).unwrap();
        }
        drop(store);

        let stats = compact(&src, &dst, CompactOptions::default()).unwrap();
        assert_eq!(stats.page_count, 20);
        assert!(stats.output_bytes < stats.input_bytes);

        let f2 = std::fs::OpenOptions::new().read(true).open(&dst).unwrap();
        let mut reader = PageStore::open(FileBlockIo(f2), Config::default(), true).unwrap();
        for pgno in 2..=20u64 {
            let mut out = vec![0u8; page_size];
            reader.read((pgno - 1) * page_size as u64, &mut out).unwrap();
            assert_eq!(&out[..8], &pgno.to_le_bytes());
            assert_eq!(out[8], 4u8.wrapping_add(pgno as u8));
        }
    }
}
