#![allow(dead_code)]

use crate::Config;
use crate::cache::LruCache;
use crate::codec::{ChunkInput, Decompressor, compress_chunks_parallel};
use crate::error::{Error, Result};
use crate::format::{
    CHUNK_HEADER_SIZE, ChunkHeader, DirEntry, L2_ENTRIES, L2_PAGE_SIZE, L2Page, MAX_CHUNK_BYTES,
    MAX_CHUNK_PAGES, SUPERBLOCK_REGION, SUPERBLOCK_SIZE, Superblock, align16, alloc_offset,
    is_valid_page_size, parse_sqlite_page_size,
};
use std::collections::{BTreeMap, HashMap};

const L2_CACHE_MAX_BYTES: usize = 1024 * 1024; // 1 MiB (256 L2 pages = 131,072 inner pages)
const PENDING_SPILL_CAP_BYTES: usize = 16 * 1024 * 1024; // 16 MiB

pub trait BlockIo {
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize>;
    fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<()>;
    fn sync(&mut self) -> Result<()>;
    fn file_size(&mut self) -> Result<u64>;
    fn truncate(&mut self, size: u64) -> Result<()>;
}

pub struct FileBlockIo(pub std::fs::File);

impl BlockIo for FileBlockIo {
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let mut total = 0;
        while total < buf.len() {
            let n = {
                #[cfg(windows)]
                {
                    use std::os::windows::fs::FileExt;
                    self.0.seek_read(&mut buf[total..], offset + total as u64)?
                }
                #[cfg(unix)]
                {
                    use std::os::unix::fs::FileExt;
                    self.0.read_at(&mut buf[total..], offset + total as u64)?
                }
            };
            if n == 0 {
                break;
            }
            total += n;
        }
        Ok(total)
    }

    fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<()> {
        let mut total = 0;
        while total < buf.len() {
            let n = {
                #[cfg(windows)]
                {
                    use std::os::windows::fs::FileExt;
                    self.0.seek_write(&buf[total..], offset + total as u64)?
                }
                #[cfg(unix)]
                {
                    use std::os::unix::fs::FileExt;
                    self.0.write_at(&buf[total..], offset + total as u64)?
                }
            };
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "short write to file",
                )
                .into());
            }
            total += n;
        }
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        self.0.sync_data()?;
        Ok(())
    }

    fn file_size(&mut self) -> Result<u64> {
        Ok(self.0.metadata()?.len())
    }

    fn truncate(&mut self, size: u64) -> Result<()> {
        self.0.set_len(size)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadOutcome {
    Full,
    ShortRead,
}

pub struct PageStore<B: BlockIo> {
    io: B,
    config: Config,
    read_only: bool,

    // Committed on-disk state
    superblock: Option<Superblock>,
    l1: Vec<u64>,
    l2_cache: LruCache<u32, L2Page>,
    chunk_cache: LruCache<u64, Vec<u8>>,
    decompressor: Decompressor,

    // Staged writer state (uncommitted until flush)
    staged_page_size: u32,
    staged_page_count: u64,
    staged_tail_offset: u64,
    staged_live_bytes: u64,
    staged_header: Option<[u8; 100]>,
    pending_writes: BTreeMap<u64, Vec<u8>>,
    pending_bytes: usize,
    dirty_l2: HashMap<u32, L2Page>,
    has_unflushed_spills: bool,
}

impl<B: BlockIo> PageStore<B> {
    pub fn open(mut io: B, config: Config, read_only: bool) -> Result<Self> {
        let decompressor = Decompressor::new()?;
        let chunk_cache_bytes = config.cache_mib.saturating_mul(1024 * 1024);
        let file_len = io.file_size()?;
        let (superblock, l1) = if file_len == 0 {
            (None, Vec::new())
        } else {
            let sb = read_superblock(&mut io)?.ok_or(Error::NotASqmicroDb)?;
            let l1 = read_l1(&mut io, &sb)?;
            (Some(sb), l1)
        };

        let staged_page_size = superblock.as_ref().map_or(0, |sb| sb.page_size);
        let staged_page_count = superblock.as_ref().map_or(0, |sb| sb.page_count);
        let staged_tail_offset = superblock
            .as_ref()
            .map_or(SUPERBLOCK_REGION, |sb| sb.tail_offset);
        let staged_live_bytes = superblock.as_ref().map_or(0, |sb| sb.live_bytes);

        Ok(Self {
            io,
            config,
            read_only,
            superblock,
            l1,
            l2_cache: LruCache::new(L2_CACHE_MAX_BYTES),
            chunk_cache: LruCache::new(chunk_cache_bytes),
            decompressor,
            staged_page_size,
            staged_page_count,
            staged_tail_offset,
            staged_live_bytes,
            staged_header: None,
            pending_writes: BTreeMap::new(),
            pending_bytes: 0,
            dirty_l2: HashMap::new(),
            has_unflushed_spills: false,
        })
    }

    pub fn refresh(&mut self) -> Result<()> {
        if self.has_pending_writes() {
            return Ok(());
        }
        let file_len = self.io.file_size()?;
        if file_len == 0 {
            if self.superblock.is_some() {
                self.superblock = None;
                self.l1.clear();
                self.l2_cache.clear();
                self.chunk_cache.clear();
                self.staged_page_size = 0;
                self.staged_page_count = 0;
                self.staged_tail_offset = SUPERBLOCK_REGION;
                self.staged_live_bytes = 0;
            }
            return Ok(());
        }
        let Some(new_sb) = read_superblock(&mut self.io)? else {
            return Err(Error::NotASqmicroDb);
        };

        let same_gen = self
            .superblock
            .as_ref()
            .is_some_and(|old| old.generation == new_sb.generation && old.compact_id == new_sb.compact_id);
        if same_gen {
            return Ok(());
        }

        let compacted = self
            .superblock
            .as_ref()
            .is_some_and(|old| old.compact_id != new_sb.compact_id);
        let new_l1 = read_l1(&mut self.io, &new_sb)?;

        if compacted {
            self.l2_cache.clear();
            self.chunk_cache.clear();
        } else {
            let max_len = self.l1.len().max(new_l1.len());
            for idx in 0..max_len {
                let old_off = self.l1.get(idx).copied().unwrap_or(0);
                let new_off = new_l1.get(idx).copied().unwrap_or(0);
                if old_off != new_off {
                    self.l2_cache.remove(&(idx as u32));
                }
            }
        }

        self.staged_page_size = new_sb.page_size;
        self.staged_page_count = new_sb.page_count;
        self.staged_tail_offset = new_sb.tail_offset;
        self.staged_live_bytes = new_sb.live_bytes;
        self.l1 = new_l1;
        self.superblock = Some(new_sb);
        Ok(())
    }

    pub fn logical_size(&mut self) -> Result<u64> {
        let (ps, pc) = self.current_geometry();
        Ok(pc.saturating_mul(ps as u64))
    }

    pub fn read(&mut self, offset: u64, buf: &mut [u8]) -> Result<ReadOutcome> {
        if buf.is_empty() {
            return Ok(ReadOutcome::Full);
        }

        // Fast header path: bytes 0..100 mirrored in superblock (when page 1 isn't pending).
        let end = offset.saturating_add(buf.len() as u64);
        if end <= 100 {
            if let Some(p1) = self.pending_writes.get(&1) {
                buf.copy_from_slice(&p1[offset as usize..end as usize]);
                return Ok(ReadOutcome::Full);
            }
            if let Some(hdr) = &self.staged_header {
                buf.copy_from_slice(&hdr[offset as usize..end as usize]);
                return Ok(ReadOutcome::Full);
            }
            if let Some(sb) = &self.superblock
                && sb.header_len == 100
                && sb.page_count >= 1
            {
                buf.copy_from_slice(&sb.sqlite_header[offset as usize..end as usize]);
                return Ok(ReadOutcome::Full);
            }
        }

        let (ps, page_count) = self.current_geometry();
        if ps == 0 || page_count == 0 {
            buf.fill(0);
            return Ok(ReadOutcome::ShortRead);
        }
        let ps_u64 = ps as u64;
        let logical_len = page_count.saturating_mul(ps_u64);
        if offset >= logical_len {
            buf.fill(0);
            return Ok(ReadOutcome::ShortRead);
        }

        let first_pgno = 1 + offset / ps_u64;
        let max_readable = (logical_len - offset).min(buf.len() as u64) as usize;
        let last_pgno = 1 + (offset + max_readable as u64 - 1) / ps_u64;

        let mut sofar = 0usize;
        for pgno in first_pgno..=last_pgno {
            let page_ofs = if sofar == 0 {
                (offset % ps_u64) as usize
            } else {
                0
            };
            let want = (max_readable - sofar).min(ps as usize - page_ofs);
            let dst = &mut buf[sofar..sofar + want];
            self.read_page_slice(pgno, page_ofs, dst)?;
            sofar += want;
        }

        if sofar < buf.len() {
            buf[sofar..].fill(0);
            Ok(ReadOutcome::ShortRead)
        } else {
            Ok(ReadOutcome::Full)
        }
    }

    pub fn write(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        if self.read_only {
            return Err(Error::InvalidConfig(
                "cannot write to a read-only sqmicro database".into(),
            ));
        }
        if data.is_empty() {
            return Ok(());
        }

        if !self.has_pending_writes() {
            self.refresh()?;
        }

        if self.staged_page_size == 0 {
            if offset != 0 {
                return Err(Error::Corrupt(format!(
                    "first write to empty database at non-zero offset {offset}"
                )));
            }
            let ps = parse_sqlite_page_size(data)
                .filter(|&ps| data.len() % ps as usize == 0)
                .or_else(|| {
                    let len = u32::try_from(data.len()).ok()?;
                    is_valid_page_size(len).then_some(len)
                })
                .ok_or_else(|| {
                    Error::Corrupt(format!("cannot determine valid SQLite page_size from {} bytes", data.len()))
                })?;
            self.staged_page_size = ps;
        }

        let ps = self.staged_page_size as usize;
        if offset % ps as u64 != 0 || data.len() % ps != 0 {
            return Err(Error::Corrupt(format!(
                "unaligned write: offset={offset}, len={}, page_size={ps}",
                data.len()
            )));
        }

        let start_pgno = 1 + offset / ps as u64;
        for (i, page_slice) in data.chunks_exact(ps).enumerate() {
            let pgno = start_pgno + i as u64;
            if pgno == 1 && page_slice.len() >= 100 {
                let mut hdr = [0u8; 100];
                hdr.copy_from_slice(&page_slice[..100]);
                self.staged_header = Some(hdr);
            }
            if let Some(existing) = self.pending_writes.get_mut(&pgno) {
                existing.copy_from_slice(page_slice);
            } else {
                self.pending_writes.insert(pgno, page_slice.to_vec());
                self.pending_bytes += ps;
            }
            if pgno > self.staged_page_count {
                self.staged_page_count = pgno;
            }
        }

        if self.pending_bytes > PENDING_SPILL_CAP_BYTES {
            self.spill_pending_chunks()?;
        }
        Ok(())
    }

    pub fn flush(&mut self, sync_disk: bool) -> Result<()> {
        if self.read_only {
            return Ok(());
        }
        if !self.has_pending_writes() {
            if sync_disk {
                self.io.sync()?;
            }
            return Ok(());
        }

        self.spill_pending_chunks()?;

        let page_size = self.staged_page_size;
        let page_count = self.staged_page_count;
        let l1_entries = if page_count == 0 {
            0usize
        } else {
            ((page_count - 1) / L2_ENTRIES as u64 + 1) as usize
        };
        let mut new_l1 = self.l1.clone();
        new_l1.resize(l1_entries, 0);

        let mut tail = self.staged_tail_offset;
        let mut live_bytes = self.staged_live_bytes;

        // Write immutable copies of all dirty L2 pages in deterministic order.
        let mut dirty_indices: Vec<u32> = self.dirty_l2.keys().copied().collect();
        dirty_indices.sort_unstable();
        for l2_idx in dirty_indices {
            let page = self.dirty_l2.remove(&l2_idx).unwrap();
            if (l2_idx as usize) >= l1_entries {
                continue;
            }
            let (l2_off, next_tail) = alloc_offset(tail, L2_PAGE_SIZE as u64)?;
            let encoded = page.encode();
            self.io.write_at(l2_off, &encoded)?;
            tail = next_tail;
            if new_l1[l2_idx as usize] == 0 {
                live_bytes = live_bytes.saturating_add(L2_PAGE_SIZE as u64);
            }
            new_l1[l2_idx as usize] = l2_off;
            self.l2_cache.insert(l2_idx, page, L2_PAGE_SIZE);
        }

        // Write immutable L1 root.
        let l1_offset = if l1_entries == 0 {
            0
        } else {
            let raw_l1_bytes = l1_entries * 8;
            let aligned_l1_bytes = align16(raw_l1_bytes as u64);
            let mut l1_buf = vec![0u8; aligned_l1_bytes as usize];
            for (i, &off) in new_l1.iter().enumerate() {
                l1_buf[i * 8..i * 8 + 8].copy_from_slice(&off.to_le_bytes());
            }
            let (off, next_tail) = alloc_offset(tail, aligned_l1_bytes)?;
            self.io.write_at(off, &l1_buf)?;
            tail = next_tail;
            off
        };

        if sync_disk {
            self.io.sync()?;
        }

        let prev_gen = self.superblock.as_ref().map_or(0, |sb| sb.generation);
        let prev_compact = self.superblock.as_ref().map_or(0, |sb| sb.compact_id);
        let (header_len, sqlite_header) = if let Some(hdr) = self.staged_header.take() {
            (100, hdr)
        } else if let Some(sb) = &self.superblock {
            (sb.header_len, sb.sqlite_header)
        } else {
            (0, [0u8; 100])
        };

        let chunk_max_pages = self.chunk_pages_limit(page_size) as u32;
        let sb = Superblock {
            format_version: crate::format::FORMAT_VERSION,
            flags: 0,
            generation: prev_gen + 1,
            page_size,
            chunk_max_pages,
            page_count,
            tail_offset: tail,
            live_bytes: live_bytes.min(tail.saturating_sub(SUPERBLOCK_REGION)),
            l1_offset,
            l1_entries: l1_entries as u32,
            header_len,
            compact_id: prev_compact,
            dict_offset: 0,
            dict_len: 0,
            sqlite_header,
        };

        let encoded_sb = sb.encode();
        self.io.write_at(sb.slot_offset(), &encoded_sb)?;
        if sync_disk {
            self.io.sync()?;
        }

        self.staged_tail_offset = tail;
        self.staged_live_bytes = sb.live_bytes;
        self.l1 = new_l1;
        self.superblock = Some(sb);
        self.has_unflushed_spills = false;
        Ok(())
    }

    pub fn has_pending_writes(&self) -> bool {
        !self.pending_writes.is_empty() || self.has_unflushed_spills
    }

    pub fn truncate(
        &mut self,
        new_size: u64,
        compact_lock: Option<&crate::lock::CompactLock>,
    ) -> Result<()> {
        if self.read_only {
            return Err(Error::InvalidConfig(
                "cannot truncate a read-only sqmicro database".into(),
            ));
        }
        if !self.has_pending_writes() {
            self.refresh()?;
        }
        if self.has_pending_writes() {
            self.flush(false)?;
        }

        let (page_size, old_page_count) = self.current_geometry();
        if page_size == 0 {
            if new_size == 0 {
                return Ok(());
            }
            return Err(Error::Corrupt(format!(
                "cannot truncate empty database to non-zero size {new_size}"
            )));
        }
        let ps_u64 = page_size as u64;
        if new_size % ps_u64 != 0 {
            return Err(Error::Corrupt(format!(
                "unaligned truncate size {new_size} for page_size {page_size}"
            )));
        }
        let new_page_count = new_size / ps_u64;
        if new_page_count >= old_page_count {
            return Ok(());
        }

        let new_l1_entries = if new_page_count == 0 {
            0usize
        } else {
            ((new_page_count - 1) / L2_ENTRIES as u64 + 1) as usize
        };

        // Compute exact live bytes for pages 1..=new_page_count.
        let mut unique_chunks = std::collections::HashSet::<u64>::new();
        let mut live_chunk_bytes = 0u64;
        let mut trimmed_last_l2: Option<(u32, L2Page)> = None;
        for l2_idx in 0..new_l1_entries {
            let mut l2 = self.load_committed_l2(l2_idx as u32)?;
            let is_last = l2_idx + 1 == new_l1_entries;
            let keep_slots = if is_last {
                let rem = (new_page_count % L2_ENTRIES as u64) as usize;
                if rem == 0 { L2_ENTRIES } else { rem }
            } else {
                L2_ENTRIES
            };
            if is_last && keep_slots < L2_ENTRIES {
                for slot in keep_slots..L2_ENTRIES {
                    l2.entries[slot] = DirEntry::EMPTY;
                }
                trimmed_last_l2 = Some((l2_idx as u32, l2.clone()));
            }
            for slot in 0..keep_slots {
                let entry = l2.entries[slot];
                if !entry.is_empty() && unique_chunks.insert(entry.chunk_offset()) {
                    live_chunk_bytes = live_chunk_bytes.saturating_add(entry.read_len() as u64);
                }
            }
        }
        let exact_live_bytes = live_chunk_bytes
            .saturating_add((new_l1_entries as u64).saturating_mul(L2_PAGE_SIZE as u64))
            .saturating_add(align16((new_l1_entries as u64) * 8));

        let physical_data = self.staged_tail_offset.saturating_sub(SUPERBLOCK_REGION);
        let dead_bytes = physical_data.saturating_sub(exact_live_bytes);

        let should_live_compact = (new_page_count == 0 && physical_data > 0)
            || (self.config.compact_threshold > 0
                && exact_live_bytes <= 16 * 1024 * 1024
                && dead_bytes >= 4 * 1024 * 1024
                && dead_bytes.saturating_mul(100)
                    >= physical_data.saturating_mul(self.config.compact_threshold as u64));

        if should_live_compact
            && let Some(lock) = compact_lock
            && lock.try_lock_exclusive()?
        {
            struct UnlockGuard<'a>(&'a crate::lock::CompactLock);
            impl Drop for UnlockGuard<'_> {
                fn drop(&mut self) {
                    let _ = self.0.unlock();
                }
            }
            let _guard = UnlockGuard(lock);
            return self.live_compact_to(new_page_count, page_size);
        }

        // Logical truncate (safe with concurrent readers; no live bytes overwritten).
        let mut new_l1 = self.l1[..new_l1_entries.min(self.l1.len())].to_vec();
        new_l1.resize(new_l1_entries, 0);
        let mut tail = self.staged_tail_offset;

        if let Some((l2_idx, l2_page)) = trimmed_last_l2 {
            let (l2_off, next_tail) = alloc_offset(tail, L2_PAGE_SIZE as u64)?;
            self.io.write_at(l2_off, &l2_page.encode())?;
            tail = next_tail;
            new_l1[l2_idx as usize] = l2_off;
            self.l2_cache.insert(l2_idx, l2_page, L2_PAGE_SIZE);
        }

        let l1_offset = if new_l1_entries == 0 {
            0
        } else {
            let aligned_l1_bytes = align16((new_l1_entries * 8) as u64);
            let mut l1_buf = vec![0u8; aligned_l1_bytes as usize];
            for (i, &off) in new_l1.iter().enumerate() {
                l1_buf[i * 8..i * 8 + 8].copy_from_slice(&off.to_le_bytes());
            }
            let (off, next_tail) = alloc_offset(tail, aligned_l1_bytes)?;
            self.io.write_at(off, &l1_buf)?;
            tail = next_tail;
            off
        };

        let prev_gen = self.superblock.as_ref().map_or(0, |sb| sb.generation);
        let prev_compact = self.superblock.as_ref().map_or(0, |sb| sb.compact_id);
        let (header_len, sqlite_header) = if new_page_count == 0 {
            (0, [0u8; 100])
        } else if let Some(sb) = &self.superblock {
            (sb.header_len, sb.sqlite_header)
        } else {
            (0, [0u8; 100])
        };

        let sb = Superblock {
            format_version: crate::format::FORMAT_VERSION,
            flags: 0,
            generation: prev_gen + 1,
            page_size,
            chunk_max_pages: self.chunk_pages_limit(page_size) as u32,
            page_count: new_page_count,
            tail_offset: tail,
            live_bytes: exact_live_bytes.min(tail.saturating_sub(SUPERBLOCK_REGION)),
            l1_offset,
            l1_entries: new_l1_entries as u32,
            header_len,
            compact_id: prev_compact,
            dict_offset: 0,
            dict_len: 0,
            sqlite_header,
        };
        self.io.write_at(sb.slot_offset(), &sb.encode())?;

        // Evict dropped L2 pages from cache.
        for idx in new_l1_entries..self.l1.len() {
            self.l2_cache.remove(&(idx as u32));
        }
        self.staged_page_count = new_page_count;
        self.staged_tail_offset = tail;
        self.staged_live_bytes = sb.live_bytes;
        self.l1 = new_l1;
        self.superblock = Some(sb);
        Ok(())
    }

    fn live_compact_to(&mut self, new_page_count: u64, page_size: u32) -> Result<()> {
        let ps = page_size as usize;
        let mut live_pages: Vec<(u64, Vec<u8>)> = Vec::with_capacity(new_page_count as usize);
        let mut header_mirror = [0u8; 100];
        let mut header_len = 0u32;

        for pgno in 1..=new_page_count {
            let entry = self.lookup_dir_entry(pgno)?;
            if entry.is_empty() {
                continue;
            }
            let mut buf = vec![0u8; ps];
            self.read_page_slice(pgno, 0, &mut buf)?;
            if pgno == 1 && buf.len() >= 100 {
                header_mirror.copy_from_slice(&buf[..100]);
                header_len = 100;
            }
            live_pages.push((pgno, buf));
        }

        let prev_gen = self.superblock.as_ref().map_or(0, |sb| sb.generation);
        let prev_compact = self.superblock.as_ref().map_or(0, |sb| sb.compact_id);

        // Reset in-memory layout and rewrite compactly from SUPERBLOCK_REGION.
        self.l1.clear();
        self.l2_cache.clear();
        self.chunk_cache.clear();
        self.dirty_l2.clear();
        self.pending_writes.clear();
        self.pending_bytes = 0;
        self.has_unflushed_spills = false;
        self.staged_page_size = page_size;
        self.staged_page_count = new_page_count;
        self.staged_tail_offset = SUPERBLOCK_REGION;
        self.staged_live_bytes = 0;

        for (pgno, data) in live_pages {
            self.pending_bytes += data.len();
            self.pending_writes.insert(pgno, data);
        }
        self.spill_pending_chunks()?;

        let new_l1_entries = if new_page_count == 0 {
            0usize
        } else {
            ((new_page_count - 1) / L2_ENTRIES as u64 + 1) as usize
        };
        let mut new_l1 = vec![0u64; new_l1_entries];
        let mut tail = self.staged_tail_offset;
        let mut live_bytes = self.staged_live_bytes;

        let mut dirty_indices: Vec<u32> = self.dirty_l2.keys().copied().collect();
        dirty_indices.sort_unstable();
        for l2_idx in dirty_indices {
            let page = self.dirty_l2.remove(&l2_idx).unwrap();
            let (l2_off, next_tail) = alloc_offset(tail, L2_PAGE_SIZE as u64)?;
            self.io.write_at(l2_off, &page.encode())?;
            tail = next_tail;
            live_bytes = live_bytes.saturating_add(L2_PAGE_SIZE as u64);
            new_l1[l2_idx as usize] = l2_off;
            self.l2_cache.insert(l2_idx, page, L2_PAGE_SIZE);
        }

        let l1_offset = if new_l1_entries == 0 {
            0
        } else {
            let aligned_l1_bytes = align16((new_l1_entries * 8) as u64);
            let mut l1_buf = vec![0u8; aligned_l1_bytes as usize];
            for (i, &off) in new_l1.iter().enumerate() {
                l1_buf[i * 8..i * 8 + 8].copy_from_slice(&off.to_le_bytes());
            }
            let (off, next_tail) = alloc_offset(tail, aligned_l1_bytes)?;
            self.io.write_at(off, &l1_buf)?;
            tail = next_tail;
            live_bytes = live_bytes.saturating_add(aligned_l1_bytes);
            off
        };

        self.io.sync()?;

        let sb = Superblock {
            format_version: crate::format::FORMAT_VERSION,
            flags: 0,
            generation: prev_gen + 1,
            page_size,
            chunk_max_pages: self.chunk_pages_limit(page_size) as u32,
            page_count: new_page_count,
            tail_offset: tail,
            live_bytes: live_bytes.min(tail.saturating_sub(SUPERBLOCK_REGION)),
            l1_offset,
            l1_entries: new_l1_entries as u32,
            header_len,
            compact_id: prev_compact + 1,
            dict_offset: 0,
            dict_len: 0,
            sqlite_header: header_mirror,
        };
        let encoded_sb = sb.encode();
        // Write both slots so neither superblock slot references truncated offsets.
        self.io.write_at(0, &encoded_sb)?;
        self.io.write_at(SUPERBLOCK_SIZE as u64, &encoded_sb)?;
        self.io.sync()?;
        self.io.truncate(tail)?;
        self.io.sync()?;

        self.staged_tail_offset = tail;
        self.staged_live_bytes = sb.live_bytes;
        self.l1 = new_l1;
        self.superblock = Some(sb);
        self.has_unflushed_spills = false;
        Ok(())
    }

    pub(crate) fn is_page_empty(&mut self, pgno: u64) -> Result<bool> {
        Ok(self.lookup_dir_entry(pgno)?.is_empty())
    }

    pub fn superblock(&self) -> Option<&Superblock> {
        self.superblock.as_ref()
    }

    pub fn into_io(self) -> B {
        self.io
    }

    pub(crate) fn io_mut(&mut self) -> &mut B {
        &mut self.io
    }

    fn current_geometry(&self) -> (u32, u64) {
        if self.has_pending_writes() || self.staged_page_size != 0 {
            (self.staged_page_size, self.staged_page_count)
        } else if let Some(sb) = &self.superblock {
            (sb.page_size, sb.page_count)
        } else {
            (0, 0)
        }
    }

    fn chunk_pages_limit(&self, page_size: u32) -> usize {
        if page_size == 0 {
            return 1;
        }
        let ps = page_size as usize;
        let target = self
            .config
            .chunk_kib
            .saturating_mul(1024)
            .clamp(ps, MAX_CHUNK_BYTES);
        (target / ps).clamp(1, MAX_CHUNK_PAGES)
    }

    fn spill_pending_chunks(&mut self) -> Result<()> {
        if self.pending_writes.is_empty() {
            return Ok(());
        }
        let page_size = self.staged_page_size;
        let max_pages = self.chunk_pages_limit(page_size);
        let pending = std::mem::take(&mut self.pending_writes);
        self.pending_bytes = 0;

        // Build ChunkInput groups: page 1 always isolated & raw; pages >= 2 grouped in contiguous runs.
        let mut inputs: Vec<ChunkInput> = Vec::new();
        let mut current: Vec<(u64, Vec<u8>)> = Vec::new();
        for (pgno, data) in pending {
            if pgno == 1 {
                if !current.is_empty() {
                    inputs.push(ChunkInput {
                        pages: std::mem::take(&mut current),
                        force_raw: false,
                    });
                }
                inputs.push(ChunkInput {
                    pages: vec![(1, data)],
                    force_raw: true,
                });
                continue;
            }
            let contiguous = current
                .last()
                .is_none_or(|(last_pgno, _)| *last_pgno + 1 == pgno);
            if !contiguous || current.len() >= max_pages {
                inputs.push(ChunkInput {
                    pages: std::mem::take(&mut current),
                    force_raw: false,
                });
            }
            current.push((pgno, data));
        }
        if !current.is_empty() {
            inputs.push(ChunkInput {
                pages: current,
                force_raw: false,
            });
        }

        let encoded_chunks =
            compress_chunks_parallel(inputs, self.config.level, self.config.threads)?;

        for chunk in encoded_chunks {
            let page_count = u8::try_from(chunk.pagenos.len())
                .map_err(|_| Error::Corrupt("chunk page_count overflow".into()))?;
            let payload_len = u32::try_from(chunk.payload.len())
                .map_err(|_| Error::Corrupt("chunk payload_len overflow".into()))?;
            let total_unaligned = CHUNK_HEADER_SIZE + chunk.payload.len();
            let aligned_len = align16(total_unaligned as u64);

            let (chunk_off, next_tail) = alloc_offset(self.staged_tail_offset, aligned_len)?;
            let hdr = ChunkHeader {
                codec: chunk.codec,
                page_count,
                dict_id: 0,
                payload_len,
            };

            let mut disk_buf = vec![0u8; aligned_len as usize];
            disk_buf[..CHUNK_HEADER_SIZE].copy_from_slice(&hdr.encode());
            disk_buf[CHUNK_HEADER_SIZE..total_unaligned].copy_from_slice(&chunk.payload);
            self.io.write_at(chunk_off, &disk_buf)?;

            self.staged_tail_offset = next_tail;
            self.staged_live_bytes = self.staged_live_bytes.saturating_add(aligned_len);

            for (slot_idx, pgno) in chunk.pagenos.into_iter().enumerate() {
                let entry = DirEntry::new(chunk_off, total_unaligned, slot_idx as u8)?;
                self.set_dirty_dir_entry(pgno, entry)?;
            }
        }

        self.has_unflushed_spills = true;
        Ok(())
    }

    fn set_dirty_dir_entry(&mut self, pgno: u64, entry: DirEntry) -> Result<()> {
        let idx = pgno - 1;
        let l2_idx = (idx / L2_ENTRIES as u64) as u32;
        let slot = (idx % L2_ENTRIES as u64) as usize;
        if !self.dirty_l2.contains_key(&l2_idx) {
            let base = self.load_committed_l2(l2_idx)?;
            self.dirty_l2.insert(l2_idx, base);
        }
        self.dirty_l2.get_mut(&l2_idx).unwrap().entries[slot] = entry;
        Ok(())
    }

    fn load_committed_l2(&mut self, l2_idx: u32) -> Result<L2Page> {
        if let Some(cached) = self.l2_cache.get(&l2_idx) {
            return Ok(cached.clone());
        }
        let l2_off = self.l1.get(l2_idx as usize).copied().unwrap_or(0);
        if l2_off == 0 {
            return Ok(L2Page::empty());
        }
        let mut buf = [0u8; L2_PAGE_SIZE];
        let n = self.io.read_at(l2_off, &mut buf)?;
        if n != L2_PAGE_SIZE {
            return Err(Error::Corrupt(format!(
                "truncated L2 directory page at offset {l2_off}"
            )));
        }
        let page = L2Page::decode(&buf);
        self.l2_cache.insert(l2_idx, page.clone(), L2_PAGE_SIZE);
        Ok(page)
    }

    fn lookup_dir_entry(&mut self, pgno: u64) -> Result<DirEntry> {
        let idx = pgno - 1;
        let l2_idx = (idx / L2_ENTRIES as u64) as u32;
        let slot = (idx % L2_ENTRIES as u64) as usize;
        if let Some(dirty) = self.dirty_l2.get(&l2_idx) {
            return Ok(dirty.entries[slot]);
        }
        let page = self.load_committed_l2(l2_idx)?;
        Ok(page.entries[slot])
    }

    fn read_page_slice(&mut self, pgno: u64, page_ofs: usize, dst: &mut [u8]) -> Result<()> {
        if let Some(pending) = self.pending_writes.get(&pgno) {
            dst.copy_from_slice(&pending[page_ofs..page_ofs + dst.len()]);
            return Ok(());
        }

        let entry = self.lookup_dir_entry(pgno)?;
        if entry.is_empty() {
            dst.fill(0);
            return Ok(());
        }

        let chunk_off = entry.chunk_offset();
        let ps = self.current_geometry().0 as usize;
        let page_idx = entry.page_idx() as usize;
        let start = page_idx * ps + page_ofs;
        let end = start + dst.len();

        if let Some(chunk_data) = self.chunk_cache.get(&chunk_off) {
            if end > chunk_data.len() {
                return Err(Error::Corrupt(format!(
                    "chunk at {chunk_off} shorter than expected for page {pgno}"
                )));
            }
            dst.copy_from_slice(&chunk_data[start..end]);
            return Ok(());
        }

        let read_len = entry.read_len();
        let mut raw_disk = vec![0u8; read_len];
        let n = self.io.read_at(chunk_off, &mut raw_disk)?;
        if n < CHUNK_HEADER_SIZE {
            return Err(Error::Corrupt(format!(
                "truncated chunk header at offset {chunk_off}"
            )));
        }
        let hdr = ChunkHeader::decode(raw_disk[..CHUNK_HEADER_SIZE].try_into().unwrap())?;
        let payload_end = CHUNK_HEADER_SIZE
            .checked_add(hdr.payload_len as usize)
            .ok_or_else(|| Error::Corrupt("chunk payload_len overflow".into()))?;
        if payload_end > n {
            return Err(Error::Corrupt(format!(
                "truncated chunk payload at offset {chunk_off}: need {payload_end}, got {n}"
            )));
        }

        let expected_raw_len = (hdr.page_count as usize)
            .checked_mul(ps)
            .ok_or_else(|| Error::Corrupt("chunk raw length overflow".into()))?;
        let decompressed = self.decompressor.decompress(
            hdr.codec,
            &raw_disk[CHUNK_HEADER_SIZE..payload_end],
            expected_raw_len,
        )?;
        if end > decompressed.len() {
            return Err(Error::Corrupt(format!(
                "page index {page_idx} out of bounds for chunk with {} pages",
                hdr.page_count
            )));
        }
        dst.copy_from_slice(&decompressed[start..end]);
        let byte_len = decompressed.len();
        self.chunk_cache.insert(chunk_off, decompressed, byte_len);
        Ok(())
    }
}

fn read_superblock<B: BlockIo>(io: &mut B) -> Result<Option<Superblock>> {
    let mut region = [0u8; SUPERBLOCK_REGION as usize];
    let n = io.read_at(0, &mut region)?;
    if n < SUPERBLOCK_SIZE {
        return Ok(None);
    }
    Ok(Superblock::select_latest(&region))
}

fn read_l1<B: BlockIo>(io: &mut B, sb: &Superblock) -> Result<Vec<u64>> {
    let count = sb.l1_entries as usize;
    if count == 0 {
        return Ok(Vec::new());
    }
    let byte_len = count
        .checked_mul(8)
        .ok_or_else(|| Error::Corrupt("l1_entries overflow".into()))?;
    let mut buf = vec![0u8; byte_len];
    let n = io.read_at(sb.l1_offset, &mut buf)?;
    if n != byte_len {
        return Err(Error::Corrupt(format!(
            "truncated L1 root at offset {}: expected {byte_len}, got {n}",
            sb.l1_offset
        )));
    }
    let mut out = Vec::with_capacity(count);
    for chunk in buf.chunks_exact(8) {
        out.push(u64::from_le_bytes(chunk.try_into().unwrap()));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[derive(Clone, Default)]
    struct MemBlockIo(Rc<RefCell<Vec<u8>>>);

    impl MemBlockIo {
        fn shared() -> Self {
            Self::default()
        }

        fn len(&self) -> usize {
            self.0.borrow().len()
        }
    }

    impl BlockIo for MemBlockIo {
        fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize> {
            let data = self.0.borrow();
            let off = offset as usize;
            if off >= data.len() {
                return Ok(0);
            }
            let avail = (data.len() - off).min(buf.len());
            buf[..avail].copy_from_slice(&data[off..off + avail]);
            Ok(avail)
        }

        fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<()> {
            let mut data = self.0.borrow_mut();
            let end = offset as usize + buf.len();
            if end > data.len() {
                data.resize(end, 0);
            }
            data[offset as usize..end].copy_from_slice(buf);
            Ok(())
        }

        fn sync(&mut self) -> Result<()> {
            Ok(())
        }

        fn file_size(&mut self) -> Result<u64> {
            Ok(self.0.borrow().len() as u64)
        }

        fn truncate(&mut self, size: u64) -> Result<()> {
            self.0.borrow_mut().truncate(size as usize);
            Ok(())
        }
    }

    #[test]
    fn empty_store_returns_short_read_and_zero_size() {
        let mut store = PageStore::open(MemBlockIo::default(), Config::default(), false).unwrap();
        assert_eq!(store.logical_size().unwrap(), 0);
        let mut buf = [0xffu8; 100];
        assert!(matches!(
            store.read(0, &mut buf).unwrap(),
            ReadOutcome::ShortRead
        ));
        assert_eq!(buf, [0u8; 100]);
    }

    #[test]
    fn rejects_non_sqmicro_existing_file() {
        let mut mem = MemBlockIo::default();
        let mut plain = vec![0u8; 4096];
        plain[..16].copy_from_slice(b"SQLite format 3\0");
        mem.write_at(0, &plain).unwrap();
        assert!(matches!(
            PageStore::open(mem, Config::default(), true),
            Err(Error::NotASqmicroDb)
        ));
    }

    #[test]
    fn write_flush_and_reopen_roundtrips_grouped_chunks_and_fast_header() {
        let mem = MemBlockIo::shared();
        let mut writer = PageStore::open(mem.clone(), Config::default(), false).unwrap();
        let page_size = 8192usize;
        let mut p1 = vec![0u8; page_size];
        p1[..16].copy_from_slice(b"SQLite format 3\0");
        p1[16..18].copy_from_slice(&(page_size as u16).to_be_bytes());
        p1[24..28].copy_from_slice(&42u32.to_be_bytes());
        writer.write(0, &p1).unwrap();

        for pgno in 2..=21u64 {
            let mut page = vec![(pgno as u8).wrapping_mul(3); page_size];
            page[..8].copy_from_slice(&pgno.to_le_bytes());
            writer.write((pgno - 1) * page_size as u64, &page).unwrap();
        }
        let mut check = vec![0u8; page_size];
        writer.read(10 * page_size as u64, &mut check).unwrap();
        assert_eq!(&check[..8], &11u64.to_le_bytes());

        writer.flush(true).unwrap();
        assert!(mem.len() < 40_000, "physical size was {}", mem.len());

        let mut reader = PageStore::open(mem.clone(), Config::default(), true).unwrap();
        assert_eq!(reader.logical_size().unwrap(), 21 * page_size as u64);

        let mut change_cnt = [0u8; 4];
        assert!(matches!(
            reader.read(24, &mut change_cnt).unwrap(),
            ReadOutcome::Full
        ));
        assert_eq!(u32::from_be_bytes(change_cnt), 42);

        let mut slice = [0u8; 32];
        assert!(matches!(
            reader.read(120, &mut slice).unwrap(),
            ReadOutcome::Full
        ));
        assert_eq!(slice, p1[120..152]);

        for pgno in 2..=21u64 {
            let mut out = vec![0u8; page_size];
            assert!(matches!(
                reader.read((pgno - 1) * page_size as u64, &mut out).unwrap(),
                ReadOutcome::Full
            ));
            assert_eq!(&out[..8], &pgno.to_le_bytes());
        }
    }

    #[test]
    fn sparse_gap_write_zero_fills_unwritten_pages() {
        let mut writer = PageStore::open(MemBlockIo::default(), Config::default(), false).unwrap();
        let mut p1 = vec![0u8; 4096];
        p1[16..18].copy_from_slice(&4096u16.to_be_bytes());
        writer.write(0, &p1).unwrap();
        writer.write(4 * 4096, &vec![7u8; 4096]).unwrap();
        writer.flush(true).unwrap();

        let mut out = vec![0xffu8; 4096];
        assert!(matches!(
            writer.read(2 * 4096, &mut out).unwrap(),
            ReadOutcome::Full
        ));
        assert_eq!(out, vec![0u8; 4096]);
    }

    #[test]
    fn pending_writes_spill_above_16mib_and_publish_on_flush() {
        let mut writer = PageStore::open(MemBlockIo::default(), Config::default(), false).unwrap();
        let page_size = 16384usize;
        let mut p1 = vec![0u8; page_size];
        p1[16..18].copy_from_slice(&(page_size as u16).to_be_bytes());
        writer.write(0, &p1).unwrap();
        for pgno in 2..=1100u64 {
            let mut page = vec![(pgno % 251) as u8; page_size];
            page[..8].copy_from_slice(&pgno.to_le_bytes());
            writer.write((pgno - 1) * page_size as u64, &page).unwrap();
        }
        writer.flush(true).unwrap();
        let mut out = vec![0u8; page_size];
        writer.read(500 * page_size as u64, &mut out).unwrap();
        assert_eq!(&out[..8], &501u64.to_le_bytes());
    }

    #[test]
    fn truncate_triggers_live_compaction_and_reader_invalidates_chunk_cache() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("live_compact.db");
        let writer_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        let mut writer = PageStore::open(FileBlockIo(writer_file), Config::default(), false).unwrap();
        let lock = crate::lock::CompactLock::open(&path).unwrap();

        let page_size = 16384usize;
        let mut p1 = vec![0u8; page_size];
        p1[16..18].copy_from_slice(&(page_size as u16).to_be_bytes());
        writer.write(0, &p1).unwrap();
        let mut x = 0xdead_beef_cafe_babeu64;
        for pgno in 2..=320u64 {
            let buf: Vec<u8> = (0..page_size)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    x as u8
                })
                .collect();
            writer.write((pgno - 1) * page_size as u64, &buf).unwrap();
        }
        writer.flush(true).unwrap();
        let big_size = std::fs::metadata(&path).unwrap().len();
        assert!(big_size > 4 * 1024 * 1024);

        let reader_file = std::fs::OpenOptions::new().read(true).open(&path).unwrap();
        let mut reader = PageStore::open(FileBlockIo(reader_file), Config::default(), true).unwrap();
        let mut before = vec![0u8; page_size];
        reader.read(page_size as u64, &mut before).unwrap();
        let old_compact_id = reader.superblock().unwrap().compact_id;

        let new_p2 = vec![0xABu8; page_size];
        writer.write(page_size as u64, &new_p2).unwrap();
        writer.truncate(2 * page_size as u64, Some(&lock)).unwrap();

        let small_size = std::fs::metadata(&path).unwrap().len();
        assert!(
            small_size < 128 * 1024,
            "expected physical shrink, got {small_size}"
        );

        reader.refresh().unwrap();
        assert!(reader.superblock().unwrap().compact_id > old_compact_id);
        let mut after = vec![0u8; page_size];
        reader.read(page_size as u64, &mut after).unwrap();
        assert_eq!(after, new_p2);
    }

    #[test]
    fn live_compaction_skips_nonblockingly_when_reader_holds_shared_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("skip_compact.db");
        let writer_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        let mut writer = PageStore::open(FileBlockIo(writer_file), Config::default(), false).unwrap();
        let w_lock = crate::lock::CompactLock::open(&path).unwrap();
        let r_lock = crate::lock::CompactLock::open(&path).unwrap();

        let page_size = 16384usize;
        let mut p1 = vec![0u8; page_size];
        p1[16..18].copy_from_slice(&(page_size as u16).to_be_bytes());
        writer.write(0, &p1).unwrap();
        let mut x = 123456789u64;
        for pgno in 2..=320u64 {
            let buf: Vec<u8> = (0..page_size)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    x as u8
                })
                .collect();
            writer.write((pgno - 1) * page_size as u64, &buf).unwrap();
        }
        writer.flush(true).unwrap();
        let before_size = std::fs::metadata(&path).unwrap().len();

        r_lock.lock_shared().unwrap();
        writer.truncate(page_size as u64, Some(&w_lock)).unwrap();
        assert_eq!(writer.logical_size().unwrap(), page_size as u64);
        assert!(std::fs::metadata(&path).unwrap().len() >= before_size);
        r_lock.unlock().unwrap();
    }
}
