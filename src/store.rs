#![allow(dead_code)]

use crate::Config;
use crate::cache::LruCache;
use crate::codec::{ChunkInput, Decompressor, EncodedChunk, compress_chunks_parallel};
use crate::error::{Error, Result};
use crate::format::{
    CHUNK_HEADER_SIZE, ChunkHeader, DirEntry, L2_ENTRIES, L2_PAGE_SIZE, L2Page, MAX_CHUNK_BYTES,
    MAX_CHUNK_PAGES, SUPERBLOCK_REGION, SUPERBLOCK_SIZE, Superblock, align16, alloc_offset,
    is_valid_page_size, parse_sqlite_page_size,
};
use std::collections::{BTreeMap, HashMap};

const L2_CACHE_MAX_BYTES: usize = 1024 * 1024; // 1 MiB (256 L2 pages = 131,072 inner pages)
const PENDING_SPILL_CAP_BYTES: usize = 16 * 1024 * 1024; // 16 MiB

// Readers wait while a live compaction rewrites the file, so these keep that well under a typical
// busy timeout.
const LIVE_COMPACT_MAX_BYTES: u64 = 4 * 1024 * 1024;
const LIVE_COMPACT_MAX_LOGICAL_BYTES: u64 = 16 * 1024 * 1024;

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
    /// `dirty_l2` has changes to publish, for spilled pages or a truncation.
    has_unflushed_spills: bool,
    /// Consecutive superblock writes that failed, and so may or may not have reached the file.
    failed_publishes: u64,
    /// The current superblock may not be on the disk: it was read from the file, or a sync failed
    /// after it was written.
    base_unsynced: bool,
    /// The current superblock may not be on the disk because it was written since the last sync,
    /// or for a reason in `base_unsynced`. A failed sync through another handle can lose it.
    superblock_unsynced: bool,
    /// A truncation is staged that may leave enough dead space to compact.
    may_compact: bool,
    /// A sync failed after pages were spilled, so they may never reach the disk.
    spills_unsure: bool,

    /// The superblock must be checked before the next read.
    stale: bool,
}

impl<B: BlockIo> PageStore<B> {
    pub fn open(mut io: B, config: Config, read_only: bool) -> Result<Self> {
        let decompressor = Decompressor::new()?;
        let chunk_cache_bytes = config.cache_mib.saturating_mul(1024 * 1024);
        let superblock = read_superblock(&mut io, 0)?;
        let l1 = match &superblock {
            Some(sb) => read_l1(&mut io, sb)?,
            None => Vec::new(),
        };

        let mut store = Self {
            io,
            config,
            read_only,
            base_unsynced: superblock.is_some(),
            superblock_unsynced: superblock.is_some(),
            may_compact: false,
            spills_unsure: false,
            superblock,
            l1,
            l2_cache: LruCache::new(L2_CACHE_MAX_BYTES),
            chunk_cache: LruCache::new(chunk_cache_bytes),
            decompressor,
            staged_page_size: 0,
            staged_page_count: 0,
            staged_tail_offset: SUPERBLOCK_REGION,
            staged_live_bytes: 0,
            staged_header: None,
            pending_writes: BTreeMap::new(),
            pending_bytes: 0,
            dirty_l2: HashMap::new(),
            has_unflushed_spills: false,
            failed_publishes: 0,
            stale: false,
        };
        store.reset_staged();
        Ok(store)
    }

    /// Makes the staged geometry match the committed superblock.
    fn reset_staged(&mut self) {
        let sb = self.superblock.as_ref();
        self.staged_page_size = sb.map_or(0, |sb| sb.page_size);
        self.staged_page_count = sb.map_or(0, |sb| sb.page_count);
        self.staged_tail_offset = sb.map_or(SUPERBLOCK_REGION, |sb| sb.tail_offset);
        self.staged_live_bytes = sb.map_or(0, |sb| sb.live_bytes);
        self.failed_publishes = 0;
    }

    /// Drops everything that hasn't been published. Must be called when a flush has failed and the
    /// lock that protected it is about to be released: from then on another connection may publish
    /// its own changes, which these must not overwrite. The WAL or journal still has the pages.
    pub fn discard_staged(&mut self) {
        self.pending_writes.clear();
        self.pending_bytes = 0;
        self.dirty_l2.clear();
        self.staged_header = None;
        self.has_unflushed_spills = false;
        self.spills_unsure = false;
        self.may_compact = false;
        // Spilled chunks may be cached under offsets that the next writer will use.
        self.chunk_cache.clear();
        self.reset_staged();
        self.base_unsynced = true;
        self.superblock_unsynced = true;
        self.stale = true;
    }

    /// Asks for `refresh` to run before the next read.
    pub fn mark_stale(&mut self) {
        self.stale = true;
    }

    pub fn is_stale(&self) -> bool {
        self.stale
    }

    pub fn refresh(&mut self) -> Result<()> {
        if self.has_pending_writes() {
            return Ok(());
        }
        let held_generation = self.superblock.as_ref().map_or(0, |sb| sb.generation);
        let Some(new_sb) = read_superblock(&mut self.io, held_generation)? else {
            if self.superblock.take().is_some() {
                self.l1.clear();
                self.l2_cache.clear();
                self.chunk_cache.clear();
                self.reset_staged();
            }
            self.stale = false;
            return Ok(());
        };

        if self.superblock.as_ref() == Some(&new_sb) {
            self.stale = false;
            return Ok(());
        }

        // Offsets can be reused after a compaction, and also after a superblock was lost by a
        // failed sync and the file went back to an older one.
        let offsets_reused = self.superblock.as_ref().is_some_and(|old| {
            old.compact_id != new_sb.compact_id
                || new_sb.generation <= old.generation
                || new_sb.tail_offset < old.tail_offset
        });
        let new_l1 = read_l1(&mut self.io, &new_sb)?;

        if offsets_reused {
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

        self.l1 = new_l1;
        self.superblock = Some(new_sb);
        self.base_unsynced = true;
        self.superblock_unsynced = true;
        self.reset_staged();
        self.stale = false;
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
                && self.current_geometry().1 >= 1
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

        // The store keeps the page size of its first write until it is emptied, even if SQLite's page
        // size changes later (`PRAGMA page_size` and `VACUUM`).
        if self.staged_page_size == 0 {
            // SQLite writes whole pages, but not always page 1 first, so only a write at offset 0
            // has a header to look at.
            let header_ps = if offset == 0 {
                parse_sqlite_page_size(data)
            } else {
                None
            };
            let ps = header_ps
                .filter(|&ps| data.len() % ps as usize == 0)
                .or_else(|| {
                    let len = u32::try_from(data.len()).ok()?;
                    is_valid_page_size(len).then_some(len)
                })
                .ok_or_else(|| {
                    Error::Corrupt(format!(
                        "cannot determine valid SQLite page_size from {} bytes",
                        data.len()
                    ))
                })?;
            self.staged_page_size = ps;
        }

        let ps = self.staged_page_size as usize;

        // A page smaller than the store's is merged into the store page that contains it.
        let merged;
        let (offset, data) =
            if data.len() < ps && data.len().is_power_of_two() && offset % data.len() as u64 == 0 {
                let at = (offset % ps as u64) as usize;
                let pgno = 1 + offset / ps as u64;
                let mut page = vec![0u8; ps];
                if pgno <= self.staged_page_count {
                    self.read_page_slice(pgno, 0, &mut page)?;
                }
                page[at..at + data.len()].copy_from_slice(data);
                merged = page;
                (offset - at as u64, merged.as_slice())
            } else {
                (offset, data)
            };
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
        let res = self.flush_staged(sync_disk);
        if res.is_err() && self.spills_unsure {
            // Pages spilled before a failed sync may never reach the disk, so they can't be published.
            // Whoever wrote them has to write them again, from the WAL or the journal.
            self.discard_staged();
        }
        res
    }

    fn flush_staged(&mut self, sync_disk: bool) -> Result<()> {
        if self.read_only {
            return Ok(());
        }
        if !self.has_pending_writes() {
            if sync_disk {
                self.sync()?;
            }
            return Ok(());
        }
        if self.spills_unsure {
            return Err(Error::Io(std::io::Error::other(
                "a sync failed since these pages were written",
            )));
        }

        self.spill_pending_chunks()?;

        let page_count = self.staged_page_count;
        // An empty database can start again with any page size.
        let page_size = if page_count == 0 {
            0
        } else {
            self.staged_page_size
        };
        let l1_entries = if page_count == 0 {
            0usize
        } else {
            ((page_count - 1) / L2_ENTRIES as u64 + 1) as usize
        };
        let mut new_l1 = self.l1.clone();
        new_l1.resize(l1_entries, 0);

        let mut tail = self.staged_tail_offset;
        let mut live_bytes = self.staged_live_bytes;

        // Write immutable copies of all dirty L2 pages in deterministic order without mutating
        // `self.dirty_l2` or `self.l2_cache` until disk writes and superblock publication succeed.
        let mut dirty_indices: Vec<u32> = self.dirty_l2.keys().copied().collect();
        dirty_indices.sort_unstable();
        for &l2_idx in &dirty_indices {
            if (l2_idx as usize) >= l1_entries {
                continue;
            }
            let page = &self.dirty_l2[&l2_idx];
            let (l2_off, next_tail) = alloc_offset(tail, L2_PAGE_SIZE as u64)?;
            let encoded = page.encode();
            self.io.write_at(l2_off, &encoded)?;
            tail = next_tail;
            if new_l1[l2_idx as usize] == 0 {
                live_bytes = live_bytes.saturating_add(L2_PAGE_SIZE as u64);
            }
            new_l1[l2_idx as usize] = l2_off;
        }

        let (l1_offset, tail) = self.write_l1(&new_l1, tail)?;

        let prev_compact = self.superblock.as_ref().map_or(0, |sb| sb.compact_id);
        let (header_len, sqlite_header) = if page_count == 0 {
            (0, [0u8; 100])
        } else if let Some(hdr) = self.staged_header {
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
            generation: 0, // set by publish()
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

        self.publish(sb)?;

        // Disk publication succeeded: now commit in-memory state.
        for l2_idx in dirty_indices {
            if let Some(page) = self.dirty_l2.remove(&l2_idx)
                && (l2_idx as usize) < l1_entries
            {
                self.l2_cache.insert(l2_idx, page, L2_PAGE_SIZE);
            }
        }
        // A staged truncation can drop whole L2 pages.
        for idx in l1_entries..self.l1.len() {
            self.l2_cache.remove(&(idx as u32));
        }
        self.staged_header = None;
        self.l1 = new_l1;
        self.has_unflushed_spills = false;
        self.spills_unsure = false;

        // Other connections can see the new superblock, so if this fails the in-memory commit above
        // still stands.
        if sync_disk {
            self.sync()?;
        }
        Ok(())
    }

    /// Writes an immutable L1 root at `tail`. Returns its offset and the new tail.
    fn write_l1(&mut self, l1: &[u64], tail: u64) -> Result<(u64, u64)> {
        if l1.is_empty() {
            return Ok((0, tail));
        }
        let mut buf = vec![0u8; align16((l1.len() * 8) as u64) as usize];
        for (slot, off) in buf.chunks_exact_mut(8).zip(l1) {
            slot.copy_from_slice(&off.to_le_bytes());
        }
        let (off, next_tail) = alloc_offset(tail, buf.len() as u64)?;
        self.io.write_at(off, &buf)?;
        Ok((off, next_tail))
    }

    /// Makes `sb` the current superblock, with the next generation number.
    ///
    /// Everything written so far is made durable first. That covers both the data `sb` points to
    /// and the previous superblock, so a crash always leaves a slot that describes intact data.
    fn publish(&mut self, mut sb: Superblock) -> Result<()> {
        let prev_gen = self.superblock.as_ref().map_or(0, |sb| sb.generation);
        sb.generation = prev_gen + 1 + 2 * self.failed_publishes;
        self.sync()?;
        self.staged_tail_offset = sb.tail_offset;
        if let Err(e) = self.io.write_at(sb.slot_offset(), &sb.encode()) {
            // Other connections may already see this superblock. A retry must leave the space it
            // points to alone, and use a new generation in the same slot.
            self.failed_publishes += 1;
            return Err(e);
        }
        self.superblock = Some(sb);
        self.superblock_unsynced = true;
        self.reset_staged();
        Ok(())
    }

    /// Must run before anything is written past the tail of the current superblock, if that may not
    /// be on the disk. If it ends a compaction that was cut short, its tail is lower than that of the
    /// superblock a crash would fall back to, whose data is then still needed.
    fn sync_base(&mut self) -> Result<()> {
        if self.base_unsynced {
            self.sync()?;
        }
        Ok(())
    }

    /// Syncs the file. After a failed sync, data may never reach the disk unless it is written
    /// again, so the current superblock is written again first if it may not be there. Everything
    /// it points to was synced before it was written, and the bytes are the same, so readers can't
    /// catch it half written.
    fn sync(&mut self) -> Result<()> {
        if self.superblock_unsynced
            && let Some(sb) = &self.superblock
        {
            let encoded = sb.encode();
            let mut slot = [0u8; SUPERBLOCK_SIZE];
            let n = self.io.read_at(sb.slot_offset(), &mut slot)?;
            let replaced =
                Superblock::decode(&slot).is_some_and(|it| it.generation > sb.generation);
            if n == SUPERBLOCK_SIZE && slot == encoded {
                self.io.write_at(sb.slot_offset(), &encoded)?;
            } else if self.base_unsynced || !replaced {
                // Gone from the file without having reached the disk, or replaced by another
                // process. Either way, what is there has to be read again, and nothing that builds
                // on this one can be published.
                self.discard_staged();
                return Err(Error::Io(std::io::Error::other(
                    "the superblock changed while it was in use",
                )));
            }
        }
        let res = self.io.sync();
        let failed = res.is_err();
        self.base_unsynced = failed;
        self.superblock_unsynced = failed;
        if failed && self.has_unflushed_spills {
            self.spills_unsure = true;
        }
        res
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
        // A rollback-mode transaction truncates before its pages are flushed, and so does the
        // rollback of one. Neither may be published yet: the journal may be gone if it fails.
        if self.has_pending_writes() {
            return self.truncate_staged(new_size);
        }
        self.refresh()?;

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
        // Rounded up: a smaller SQLite page size leaves sizes that aren't whole store pages.
        let new_page_count = new_size.div_ceil(ps_u64);
        if new_page_count >= old_page_count {
            return Ok(());
        }

        let new_l1_entries = new_page_count.div_ceil(L2_ENTRIES as u64) as usize;
        let mut trimmed_last_l2: Option<(u32, L2Page)> = None;
        let rem = (new_page_count % L2_ENTRIES as u64) as usize;
        if rem != 0 {
            let l2_idx = new_l1_entries as u32 - 1;
            let mut l2 = self.load_committed_l2(l2_idx)?;
            l2.entries[rem..].fill(DirEntry::EMPTY);
            trimmed_last_l2 = Some((l2_idx, l2));
        }
        let live_bytes = self.live_bytes(new_page_count, trimmed_last_l2.as_ref())?;

        // Logical truncate (safe with concurrent readers; no live bytes overwritten).
        self.sync_base()?;
        let mut new_l1 = self.l1[..new_l1_entries.min(self.l1.len())].to_vec();
        new_l1.resize(new_l1_entries, 0);
        let mut tail = self.staged_tail_offset;

        if let Some((l2_idx, l2_page)) = &trimmed_last_l2 {
            let (l2_off, next_tail) = alloc_offset(tail, L2_PAGE_SIZE as u64)?;
            self.io.write_at(l2_off, &l2_page.encode())?;
            tail = next_tail;
            new_l1[*l2_idx as usize] = l2_off;
        }

        let (l1_offset, tail) = self.write_l1(&new_l1, tail)?;

        let prev_compact = self.superblock.as_ref().map_or(0, |sb| sb.compact_id);
        let (header_len, sqlite_header) = if new_page_count == 0 {
            (0, [0u8; 100])
        } else if let Some(sb) = &self.superblock {
            (sb.header_len, sb.sqlite_header)
        } else {
            (0, [0u8; 100])
        };

        let effective_page_size = if new_page_count == 0 { 0 } else { page_size };
        let sb = Superblock {
            format_version: crate::format::FORMAT_VERSION,
            flags: 0,
            generation: 0, // set by publish()
            page_size: effective_page_size,
            chunk_max_pages: self.chunk_pages_limit(effective_page_size) as u32,
            page_count: new_page_count,
            tail_offset: tail,
            live_bytes: live_bytes.min(tail.saturating_sub(SUPERBLOCK_REGION)),
            l1_offset,
            l1_entries: new_l1_entries as u32,
            header_len,
            compact_id: prev_compact,
            dict_offset: 0,
            dict_len: 0,
            sqlite_header,
        };
        self.publish(sb)?;

        // Evict dropped L2 pages from cache.
        for idx in new_l1_entries..self.l1.len() {
            self.l2_cache.remove(&(idx as u32));
        }
        if let Some((l2_idx, l2_page)) = trimmed_last_l2 {
            self.l2_cache.insert(l2_idx, l2_page, L2_PAGE_SIZE);
        }
        self.l1 = new_l1;

        self.compact_if_worthwhile(compact_lock);
        Ok(())
    }

    /// Truncates what is staged, for the next flush to publish.
    fn truncate_staged(&mut self, new_size: u64) -> Result<()> {
        let ps = self.staged_page_size as u64;
        if ps == 0 {
            return Ok(());
        }
        let new_page_count = new_size.div_ceil(ps);
        if new_page_count >= self.staged_page_count {
            return Ok(());
        }
        let kept_l2 = new_page_count.div_ceil(L2_ENTRIES as u64) as u32;
        let rem = (new_page_count % L2_ENTRIES as u64) as usize;
        // The only step that can fail, so it goes before anything changes.
        let last_l2 = if rem != 0 && !self.dirty_l2.contains_key(&(kept_l2 - 1)) {
            Some(self.load_committed_l2(kept_l2 - 1)?)
        } else {
            None
        };
        let dropped = self.pending_writes.split_off(&(new_page_count + 1));
        self.pending_bytes -= dropped.len() * ps as usize;
        // Should the database grow again before the flush, the pages that were there are gone.
        let staged_l2 = self.staged_page_count.div_ceil(L2_ENTRIES as u64) as u32;
        for idx in kept_l2..staged_l2.max(self.l1.len() as u32) {
            self.dirty_l2.insert(idx, L2Page::empty());
        }
        if rem != 0 {
            let l2 = match last_l2 {
                Some(base) => self.dirty_l2.entry(kept_l2 - 1).or_insert(base),
                None => self.dirty_l2.get_mut(&(kept_l2 - 1)).unwrap(),
            };
            l2.entries[rem..].fill(DirEntry::EMPTY);
        }
        self.staged_page_count = new_page_count;
        if new_page_count == 0 {
            self.staged_header = None;
        }
        self.has_unflushed_spills = true;
        Ok(())
    }

    /// Bytes that pages `1..=page_count` and their directory take up, counting `last_l2` instead of
    /// the committed L2 page with the same index.
    fn live_bytes(&mut self, page_count: u64, last_l2: Option<&(u32, L2Page)>) -> Result<u64> {
        let l1_entries = page_count.div_ceil(L2_ENTRIES as u64);
        let mut chunks = std::collections::HashSet::<u64>::new();
        let mut bytes = 0u64;
        for l2_idx in 0..l1_entries as u32 {
            let l2 = match last_l2 {
                Some((idx, l2)) if *idx == l2_idx => l2.clone(),
                _ => self.load_committed_l2(l2_idx)?,
            };
            for entry in l2.entries {
                if !entry.is_empty() && chunks.insert(entry.chunk_offset()) {
                    bytes = bytes.saturating_add(entry.read_len() as u64);
                }
            }
        }
        Ok(bytes
            .saturating_add(l1_entries.saturating_mul(L2_PAGE_SIZE as u64))
            .saturating_add(align16(l1_entries * 8)))
    }

    /// Truncates for the next flush to publish, then `compact_if_truncated` to compact. For
    /// rollback-journal mode, where a commit or rollback that fails may otherwise have published a
    /// truncation without the pages that go with it.
    pub fn truncate_later(&mut self, new_size: u64) -> Result<()> {
        if self.read_only {
            return Err(Error::InvalidConfig(
                "cannot truncate a read-only sqmicro database".into(),
            ));
        }
        if !self.has_pending_writes() {
            self.refresh()?;
        }
        let pages = self.staged_page_count;
        self.truncate_staged(new_size)?;
        self.may_compact |= self.staged_page_count < pages;
        Ok(())
    }

    /// Compacts if a truncation since the last call allows it, once it has been published.
    pub fn compact_if_truncated(&mut self, lock: Option<&crate::lock::CompactLock>) {
        if !self.has_pending_writes() && std::mem::take(&mut self.may_compact) {
            self.compact_if_worthwhile(lock);
        }
    }

    /// Compacts the database in place if that is cheap and reclaims enough. Best effort: if it
    /// fails, whatever was published last describes intact data.
    fn compact_if_worthwhile(&mut self, lock: Option<&crate::lock::CompactLock>) {
        let Some(lock) = lock else {
            return;
        };
        let worthwhile = self.live_bytes(self.staged_page_count, None).map(|live| {
            let physical = self.staged_tail_offset.saturating_sub(SUPERBLOCK_REGION);
            let dead = physical.saturating_sub(live);
            let logical = self
                .staged_page_count
                .saturating_mul(self.staged_page_size as u64);
            (self.staged_page_count == 0 && physical > 0)
                || (self.config.compact_threshold > 0
                    && live <= LIVE_COMPACT_MAX_BYTES
                    && logical <= LIVE_COMPACT_MAX_LOGICAL_BYTES
                    && dead >= 4 * 1024 * 1024
                    && dead.saturating_mul(100)
                        >= physical.saturating_mul(self.config.compact_threshold as u64))
        });
        if worthwhile.unwrap_or(false) && self.live_compact(lock).is_err() {
            self.discard_staged();
        }
    }

    /// Rewrites the database at the start of the file and cuts off the rest.
    ///
    /// The old data can't be overwritten while a superblock still needs it, so a complete copy is
    /// first published past the end. A crash at any point leaves one intact copy published.
    fn live_compact(&mut self, lock: &crate::lock::CompactLock) -> Result<()> {
        // Read and compress first, so that readers only wait for the rewrite itself.
        let page_size = self.staged_page_size;
        let mut pages = Vec::new();
        for pgno in 1..=self.staged_page_count {
            if !self.is_page_empty(pgno)? {
                let mut buf = vec![0u8; page_size as usize];
                self.read_page_slice(pgno, 0, &mut buf)?;
                pages.push((pgno, buf));
            }
        }
        let chunks = compress_chunks_parallel(
            group_chunks(
                pages.iter().map(|(pgno, data)| (*pgno, data.as_slice())),
                self.chunk_pages_limit(page_size),
            ),
            self.config.level,
            self.config.threads,
        )?;

        let old_tail = self.staged_tail_offset;
        let l1_entries = self.l1.len() as u64;
        let packed_bytes = chunks.iter().map(|c| c.block.len() as u64).sum::<u64>()
            + l1_entries * L2_PAGE_SIZE as u64
            + align16(l1_entries * 8);
        if SUPERBLOCK_REGION + packed_bytes > old_tail || !lock.try_lock_exclusive()? {
            return Ok(());
        }
        struct UnlockGuard<'a>(&'a crate::lock::CompactLock);
        impl Drop for UnlockGuard<'_> {
            fn drop(&mut self) {
                let _ = self.0.unlock();
            }
        }
        let _guard = UnlockGuard(lock);

        let (sb, l1) = self.write_copy(&chunks, old_tail)?;
        self.publish_copy(sb, l1)?;
        self.sync()?; // the copy must be what a crash falls back to before its source goes

        let (mut sb, l1) = self.write_copy(&chunks, SUPERBLOCK_REGION)?;
        sb.compact_id += 1; // offsets are reused, so readers must drop their caches
        self.publish_copy(sb, l1)?;
        self.sync()?;

        let _ = self.io.truncate(self.staged_tail_offset);
        Ok(())
    }

    /// Writes `chunks` and a directory for them from `base` on. Returns the matching superblock,
    /// otherwise like the current one, and L1.
    fn write_copy(&mut self, chunks: &[EncodedChunk], base: u64) -> Result<(Superblock, Vec<u64>)> {
        let mut sb = self
            .superblock
            .clone()
            .ok_or_else(|| Error::Corrupt("no superblock to copy".into()))?;
        let (mut tail, mut live_bytes, entries) = self.write_chunks(chunks, base)?;

        let mut l2_pages = BTreeMap::new();
        for (pgno, entry) in entries {
            let idx = pgno - 1;
            l2_pages
                .entry((idx / L2_ENTRIES as u64) as usize)
                .or_insert_with(L2Page::empty)
                .entries[(idx % L2_ENTRIES as u64) as usize] = entry;
        }
        let mut l1 = vec![0u64; self.l1.len()];
        for (l2_idx, page) in l2_pages {
            let (l2_off, next_tail) = alloc_offset(tail, L2_PAGE_SIZE as u64)?;
            self.io.write_at(l2_off, &page.encode())?;
            l1[l2_idx] = l2_off;
            tail = next_tail;
            live_bytes += L2_PAGE_SIZE as u64;
        }

        (sb.l1_offset, sb.tail_offset) = self.write_l1(&l1, tail)?;
        sb.live_bytes = live_bytes;
        Ok((sb, l1))
    }

    fn publish_copy(&mut self, sb: Superblock, l1: Vec<u64>) -> Result<()> {
        self.publish(sb)?;
        self.l1 = l1;
        self.l2_cache.clear();
        self.chunk_cache.clear();
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

        // On a brand-new 0-byte DB, publish a valid empty superblock before writing chunks at
        // 0x2000 so concurrent readers never observe file_len > 0 with an all-zero superblock.
        if self.superblock.is_none() {
            let init_sb = Superblock::initial();
            self.io.write_at(init_sb.slot_offset(), &init_sb.encode())?;
            self.sync()?;
            self.superblock = Some(init_sb);
        }
        self.sync_base()?;

        let page_size = self.staged_page_size;
        let max_pages = self.chunk_pages_limit(page_size);

        // Nothing staged changes until the disk writes succeed.
        let encoded_chunks = compress_chunks_parallel(
            group_chunks(
                self.pending_writes
                    .iter()
                    .map(|(&pgno, data)| (pgno, data.as_slice())),
                max_pages,
            ),
            self.config.level,
            self.config.threads,
        )?;
        let (next_tail, added_live_bytes, staged_entries) =
            self.write_chunks(&encoded_chunks, self.staged_tail_offset)?;

        // All disk writes succeeded: now apply staged directory updates and clear pending_writes.
        let mut updated_l2 = self.dirty_l2.clone();
        for (pgno, entry) in staged_entries {
            let idx = pgno - 1;
            let l2_idx = (idx / L2_ENTRIES as u64) as u32;
            let slot = (idx % L2_ENTRIES as u64) as usize;
            if !updated_l2.contains_key(&l2_idx) {
                let base = self.load_committed_l2(l2_idx)?;
                updated_l2.insert(l2_idx, base);
            }
            updated_l2.get_mut(&l2_idx).unwrap().entries[slot] = entry;
        }
        self.dirty_l2 = updated_l2;
        self.staged_tail_offset = next_tail;
        self.staged_live_bytes = self.staged_live_bytes.saturating_add(added_live_bytes);
        self.pending_writes.clear();
        self.pending_bytes = 0;
        self.has_unflushed_spills = true;
        Ok(())
    }

    /// Writes `chunks` from `tail` on. Returns the new tail, the bytes used, and where each page went.
    fn write_chunks(
        &mut self,
        chunks: &[EncodedChunk],
        mut tail: u64,
    ) -> Result<(u64, u64, Vec<(u64, DirEntry)>)> {
        let mut bytes = 0u64;
        let mut entries = Vec::new();
        for chunk in chunks {
            let block_len = chunk.block.len() as u64;
            let (chunk_off, next_tail) = alloc_offset(tail, block_len)?;
            self.io.write_at(chunk_off, &chunk.block)?;
            tail = next_tail;
            bytes += block_len;
            for (slot_idx, &pgno) in chunk.pagenos.iter().enumerate() {
                let entry = DirEntry::new(chunk_off, chunk.read_len(), slot_idx as u8)?;
                entries.push((pgno, entry));
            }
        }
        Ok((tail, bytes, entries))
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

        self.chunk_cache
            .insert(chunk_off, decompressed.into_owned(), byte_len);

        Ok(())
    }
}

/// Groups runs of neighbouring pages into chunks. Page 1 gets its own uncompressed chunk.
fn group_chunks<'a>(
    pages: impl Iterator<Item = (u64, &'a [u8])>,
    max_pages: usize,
) -> Vec<ChunkInput<'a>> {
    let mut chunks: Vec<ChunkInput<'a>> = Vec::new();
    for (pgno, data) in pages {
        match chunks.last_mut() {
            Some(chunk)
                if !chunk.force_raw
                    && chunk.pages.len() < max_pages
                    && chunk.pages.last().is_some_and(|(last, _)| last + 1 == pgno) =>
            {
                chunk.pages.push((pgno, data));
            }
            _ => chunks.push(ChunkInput {
                pages: vec![(pgno, data)],
                force_raw: pgno == 1,
            }),
        }
    }
    chunks
}

/// Returns `None` for an empty database: an empty file, or one whose first superblock write was cut
/// short by a crash.
fn read_superblock<B: BlockIo>(io: &mut B, held_generation: u64) -> Result<Option<Superblock>> {
    let mut region = [0u8; SUPERBLOCK_REGION as usize];
    let mut retries = 4;
    loop {
        region.fill(0);
        io.read_at(0, &mut region)?;
        let found = Superblock::select_latest(&region);

        // Another process may be writing a slot right now. Look again before settling for the other
        // one, but do settle: a crash can leave a torn slot for good.
        let suspicious = Superblock::has_torn_slot(&region)
            || found
                .as_ref()
                .is_some_and(|sb| sb.generation < held_generation);
        if suspicious && retries > 0 {
            retries -= 1;
            std::thread::yield_now();
            continue;
        }
        if found.is_some() {
            return Ok(found);
        }
        if Superblock::is_unfinished_initial(&region) {
            if io.file_size()? <= SUPERBLOCK_REGION {
                return Ok(None);
            }
            // Another process may have created the database since the read.
            if retries > 0 {
                retries -= 1;
                continue;
            }
        }
        return Err(match Superblock::unsupported_version(&region) {
            Some(version) => Error::UnsupportedVersion(version),
            None => Error::NotASqmicroDb,
        });
    }
}

fn read_l1<B: BlockIo>(io: &mut B, sb: &Superblock) -> Result<Vec<u64>> {
    let count = sb.l1_entries as usize;
    if count == 0 {
        return Ok(Vec::new());
    }
    let byte_len = count
        .checked_mul(8)
        .ok_or_else(|| Error::Corrupt("l1_entries overflow".into()))?;
    let truncated = || Error::Corrupt(format!("truncated L1 root at offset {}", sb.l1_offset));
    // Checked before allocating, so a bad count can't ask for gigabytes.
    if sb.l1_offset.saturating_add(byte_len as u64) > io.file_size()? {
        return Err(truncated());
    }
    let mut buf = vec![0u8; byte_len];
    if io.read_at(sb.l1_offset, &mut buf)? != byte_len {
        return Err(truncated());
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
    fn rejects_unsupported_format_version_without_modifying_file() {
        let mem = MemBlockIo::shared();
        let mut sb = Superblock::empty();
        sb.generation = 2;
        sb.format_version = 1;
        mem.clone()
            .write_at(sb.slot_offset(), &sb.encode())
            .unwrap();
        let before = mem.0.borrow().clone();
        for read_only in [true, false] {
            assert!(matches!(
                PageStore::open(mem.clone(), Config::default(), read_only),
                Err(Error::UnsupportedVersion(1))
            ));
        }
        assert_eq!(*mem.0.borrow(), before);
    }

    #[test]
    fn corrupt_current_version_superblock_is_not_reported_as_unsupported() {
        let mem = MemBlockIo::shared();
        let mut sb = Superblock::empty();
        sb.generation = 2;
        let mut bytes = sb.encode();
        bytes[40] ^= 0xff;
        mem.clone().write_at(sb.slot_offset(), &bytes).unwrap();
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
                reader
                    .read((pgno - 1) * page_size as u64, &mut out)
                    .unwrap(),
                ReadOutcome::Full
            ));
            assert_eq!(&out[..8], &pgno.to_le_bytes());
        }

        // 16 pages to a chunk, and page 1 alone, uncompressed although it is mostly zeros.
        let entries: Vec<DirEntry> = (1..=21)
            .map(|pgno| reader.lookup_dir_entry(pgno).unwrap())
            .collect();
        let chunk = |e: &DirEntry| e.chunk_offset();
        assert!(entries[1..].iter().all(|e| chunk(e) != chunk(&entries[0])));
        assert!(entries[0].read_len() >= page_size);
        assert!(
            entries[1..17]
                .iter()
                .all(|e| chunk(e) == chunk(&entries[1]))
        );
        assert!(
            entries[17..]
                .iter()
                .all(|e| chunk(e) == chunk(&entries[17]))
        );
        assert_ne!(chunk(&entries[1]), chunk(&entries[17]));

        // The header comes from the superblock, without reading page 1.
        let at = entries[0].chunk_offset() as usize;
        mem.0.borrow_mut()[at..at + entries[0].read_len()].fill(0xff);
        let mut reader = PageStore::open(mem, Config::default(), true).unwrap();
        reader.read(24, &mut change_cnt).unwrap();
        assert_eq!(u32::from_be_bytes(change_cnt), 42);
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
        assert!(writer.has_unflushed_spills);
        assert!(writer.pending_bytes <= PENDING_SPILL_CAP_BYTES);
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
        let mut writer =
            PageStore::open(FileBlockIo(writer_file), Config::default(), false).unwrap();
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
        let mut reader =
            PageStore::open(FileBlockIo(reader_file), Config::default(), true).unwrap();
        let mut before = vec![0u8; page_size];
        reader.read(page_size as u64, &mut before).unwrap();
        let old_compact_id = reader.superblock().unwrap().compact_id;

        let new_p2 = vec![0xABu8; page_size];
        writer.write(page_size as u64, &new_p2).unwrap();
        writer.flush(false).unwrap();
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
        let mut writer =
            PageStore::open(FileBlockIo(writer_file), Config::default(), false).unwrap();
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

    #[test]
    fn reader_can_open_and_refresh_during_initial_16mib_spill() {
        let mem = MemBlockIo::shared();
        let mut writer = PageStore::open(mem.clone(), Config::default(), false).unwrap();
        let mut reader = PageStore::open(mem.clone(), Config::default(), true).unwrap();

        let page_size = 16384usize;
        let mut p1 = vec![0u8; page_size];
        p1[16..18].copy_from_slice(&(page_size as u16).to_be_bytes());
        writer.write(0, &p1).unwrap();
        // >16 MiB before flush -> triggers initial spill on a brand-new 0-byte DB.
        for pgno in 2..=1100u64 {
            let mut page = vec![(pgno % 251) as u8; page_size];
            page[..8].copy_from_slice(&pgno.to_le_bytes());
            writer.write((pgno - 1) * page_size as u64, &page).unwrap();
        }
        assert!(mem.len() > 8192);

        // Concurrent reader refreshing OR freshly opening mid-spill must NOT fail with NotASqmicroDb!
        reader.refresh().unwrap();
        assert_eq!(reader.logical_size().unwrap(), 0);
        let mut fresh_reader = PageStore::open(mem.clone(), Config::default(), true).unwrap();
        assert_eq!(fresh_reader.logical_size().unwrap(), 0);
    }

    #[test]
    fn live_compaction_only_rewrites_small_databases() {
        // 16 KiB pages that remain after the truncation (compressible and not), and that it drops.
        for (compressible, random, dropped, want) in [
            (1000u64, 200u64, 320u64, false), // more than 16 MiB uncompressed
            (800, 200, 320, true),
            (0, 270, 320, false), // more than 4 MiB compressed
            (0, 240, 320, true),
            (0, 200, 240, false), // less than 4 MiB dead
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("a.db");
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
                .unwrap();
            let mut writer = PageStore::open(FileBlockIo(file), Config::default(), false).unwrap();
            let lock = crate::lock::CompactLock::open(&path).unwrap();

            let ps = 16384usize;
            let kept = compressible + random;
            let mut x = 0x1122_3344_5566_7788u64;
            for pgno in 1..=kept + dropped {
                let mut buf = vec![0x11u8; ps];
                if pgno > compressible {
                    for word in buf.chunks_exact_mut(8) {
                        x ^= x << 13;
                        x ^= x >> 7;
                        x ^= x << 17;
                        word.copy_from_slice(&x.to_le_bytes());
                    }
                }
                buf[16..18].copy_from_slice(&(ps as u16).to_be_bytes());
                writer.write((pgno - 1) * ps as u64, &buf).unwrap();
            }
            writer.flush(true).unwrap();
            let before = writer.superblock().unwrap().compact_id;
            writer.truncate(kept * ps as u64, Some(&lock)).unwrap();
            let compacted = writer.superblock().unwrap().compact_id != before;
            assert_eq!(
                compacted, want,
                "{compressible} + {random} pages, {dropped} dropped"
            );
        }
    }

    /// Keeps the operations since the last sync, to build the files a power loss could leave.
    #[derive(Clone, Default)]
    struct CrashIo(Rc<RefCell<CrashState>>);

    #[derive(Default)]
    struct CrashState {
        current: Vec<u8>,
        durable: Vec<u8>,
        unsynced: Vec<Op>,
        /// Files a crash at any point so far could have left.
        crashes: Vec<Vec<u8>>,
        record: bool,
        /// Fail the write at this offset once the file has it (`true`) or before (`false`).
        fail_write: Option<(u64, bool)>,
        fail_sync: bool,
        fail_read: bool,
        /// Fail the operation that comes after this many others, once the file has it (`true`) or
        /// before (`false`). Syncs that fail have no effect.
        fail_op: Option<(u32, bool)>,
    }

    #[derive(Clone)]
    enum Op {
        Write(u64, Vec<u8>),
        Truncate(u64),
    }

    impl Op {
        fn apply(&self, file: &mut Vec<u8>) {
            match self {
                Op::Write(off, data) => {
                    let end = *off as usize + data.len();
                    if file.len() < end {
                        file.resize(end, 0);
                    }
                    file[*off as usize..end].copy_from_slice(data);
                }
                Op::Truncate(len) => file.resize(*len as usize, 0),
            }
        }
    }

    fn injected() -> Error {
        std::io::Error::other("injected fault").into()
    }

    impl CrashState {
        /// Whether the next operation should fail, and if so whether it should still take effect.
        fn next_op_fails(&mut self) -> Option<bool> {
            let (left, takes_effect) = self.fail_op.take()?;
            if left == 0 {
                return Some(takes_effect);
            }
            self.fail_op = Some((left - 1, takes_effect));
            None
        }

        /// What a power loss leaves if every unsynced write but those to the superblocks got to disk.
        fn crash_without_superblocks(&self) -> Vec<u8> {
            let mut file = self.durable.clone();
            for op in &self.unsynced {
                if !matches!(op, Op::Write(off, _) if *off < SUPERBLOCK_REGION) {
                    op.apply(&mut file);
                }
            }
            file
        }

        fn push(&mut self, op: Op) {
            op.apply(&mut self.current);
            self.unsynced.push(op);
            if !self.record {
                return;
            }
            // Any subset of the unsynced operations may have reached the disk. Subsets without the
            // newest one were covered before it: try it alone, with all others, and with each one
            // missing.
            let newest = self.unsynced.len() - 1;
            let mut keeps: Vec<Box<dyn Fn(usize) -> bool>> =
                vec![Box::new(move |k| k == newest), Box::new(|_| true)];
            for i in 0..newest {
                keeps.push(Box::new(move |k| k != i));
            }
            for keep in keeps {
                let mut file = self.durable.clone();
                for (k, op) in self.unsynced.iter().enumerate() {
                    if keep(k) {
                        op.apply(&mut file);
                    }
                }
                self.crashes.push(file);
            }
        }
    }

    impl BlockIo for CrashIo {
        fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize> {
            let s = self.0.borrow();
            assert!(buf.len() <= 1 << 24, "read of {} bytes", buf.len());
            if s.fail_read {
                return Err(injected());
            }
            let off = (offset as usize).min(s.current.len());
            let n = (s.current.len() - off).min(buf.len());
            buf[..n].copy_from_slice(&s.current[off..off + n]);
            Ok(n)
        }
        fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<()> {
            let mut s = self.0.borrow_mut();
            let fail = match s.fail_write.filter(|(off, _)| *off == offset) {
                Some((_, written)) => Some(written),
                None => s.next_op_fails(),
            };
            if fail.is_none_or(|written| written) {
                s.push(Op::Write(offset, buf.to_vec()));
            }
            match fail {
                Some(_) => Err(injected()),
                None => Ok(()),
            }
        }
        fn sync(&mut self) -> Result<()> {
            let mut s = self.0.borrow_mut();
            // A failed sync may lose the writes for good, even if a later one succeeds.
            if s.fail_sync || s.next_op_fails().is_some() {
                s.unsynced.clear();
                return Err(injected());
            }
            let ops = std::mem::take(&mut s.unsynced);
            for op in &ops {
                op.apply(&mut s.durable);
            }
            Ok(())
        }
        fn file_size(&mut self) -> Result<u64> {
            Ok(self.0.borrow().current.len() as u64)
        }
        fn truncate(&mut self, size: u64) -> Result<()> {
            let mut s = self.0.borrow_mut();
            let fail = s.next_op_fails();
            if fail.is_none_or(|done| done) {
                s.push(Op::Truncate(size));
            }
            match fail {
                Some(_) => Err(injected()),
                None => Ok(()),
            }
        }
    }

    impl CrashIo {
        fn from_file(file: Vec<u8>) -> Self {
            let io = Self::default();
            io.0.borrow_mut().durable = file.clone();
            io.0.borrow_mut().current = file;
            io
        }
        /// The next superblock goes to this offset.
        fn next_slot(&self, store: &PageStore<CrashIo>) -> u64 {
            let mut sb = Superblock::empty();
            sb.generation = store.superblock().map_or(0, |sb| sb.generation) + 1;
            sb.slot_offset()
        }
    }

    const PS: usize = 4096;

    /// A page that is recognizable and doesn't compress, so compaction has something to reclaim.
    fn page(tag: u64) -> Vec<u8> {
        let mut x = tag.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut page = vec![0u8; PS];
        for word in page.chunks_exact_mut(8) {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            word.copy_from_slice(&x.to_le_bytes());
        }
        page[16..18].copy_from_slice(&(PS as u16).to_be_bytes());
        page[100..108].copy_from_slice(&tag.to_le_bytes());
        page
    }

    fn put(store: &mut PageStore<CrashIo>, pgno: u64, tag: u64) {
        store.write((pgno - 1) * PS as u64, &page(tag)).unwrap();
    }

    /// Every page, or an error. Reading anything that isn't one of `page()`'s is reported as such.
    fn contents<B: BlockIo>(store: &mut PageStore<B>) -> Result<Vec<u64>> {
        let pages = store.logical_size()? / PS as u64;
        let mut out = Vec::new();
        for pgno in 1..=pages {
            let mut buf = vec![0u8; PS];
            store.read((pgno - 1) * PS as u64, &mut buf)?;
            let tag = u64::from_le_bytes(buf[100..108].try_into().unwrap());
            assert!(buf == page(tag), "page {pgno} holds garbage");
            out.push(tag);
        }
        Ok(out)
    }

    fn reopened(file: &[u8]) -> Vec<u64> {
        let io = CrashIo::from_file(file.to_vec());
        let mut store = PageStore::open(io, Config::default(), true).expect("unopenable");
        contents(&mut store).expect("unreadable")
    }

    /// Runs `change` on a database of `old` pages and checks every crash along the way.
    fn check_crashes(
        old_pages: u64,
        lock: Option<&crate::lock::CompactLock>,
        change: impl Fn(&mut PageStore<CrashIo>, Option<&crate::lock::CompactLock>),
    ) {
        let io = CrashIo::default();
        let mut store = PageStore::open(io.clone(), Config::default(), false).unwrap();
        // Leaves live pages at the start of the file in an order that packing them will change.
        for pgno in (1..=old_pages).filter(|pgno| !(2..=20).contains(pgno)) {
            put(&mut store, pgno, pgno);
        }
        store.flush(true).unwrap();
        for pgno in 2..=20 {
            put(&mut store, pgno, pgno);
        }
        store.flush(true).unwrap();
        let old_state = contents(&mut store).unwrap();

        io.0.borrow_mut().record = true;
        change(&mut store, lock);
        let new_state = contents(&mut store).unwrap();
        let crashes = std::mem::take(&mut io.0.borrow_mut().crashes);
        assert!(!crashes.is_empty());
        for file in &crashes {
            let got = reopened(file);
            assert!(
                got == old_state || got == new_state,
                "crash left {} pages",
                got.len()
            );
        }
        assert_eq!(reopened(&io.0.borrow().current), new_state);
    }

    #[test]
    fn crash_during_any_flush_leaves_the_old_or_new_state() {
        for sync in [true, false] {
            check_crashes(600, None, |store, _| {
                put(store, 5, 77);
                store.flush(sync).unwrap();
            });
        }
        // Several in a row replace both superblock slots, as partial checkpoints do.
        let io = CrashIo::default();
        let mut store = PageStore::open(io.clone(), Config::default(), false).unwrap();
        for pgno in 1..=600 {
            put(&mut store, pgno, pgno);
        }
        store.flush(true).unwrap();
        let mut states = vec![contents(&mut store).unwrap()];
        io.0.borrow_mut().record = true;
        for round in 1..=3 {
            put(&mut store, 5, round * 1000);
            put(&mut store, 600 + round, round * 1000 + 1);
            store.flush(false).unwrap();
            states.push(contents(&mut store).unwrap());
        }
        for file in &io.0.borrow().crashes {
            assert!(states.contains(&reopened(file)));
        }
    }

    #[test]
    fn crash_during_truncate_or_live_compaction_leaves_an_intact_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lock");
        std::fs::write(&path, b"").unwrap();
        let lock = crate::lock::CompactLock::open(&path).unwrap();
        for new_pages in [0u64, 1, 40, 513] {
            for lock in [None, Some(&lock)] {
                check_crashes(1600, lock, |store, lock| {
                    let before = store.superblock().unwrap().compact_id;
                    store.truncate(new_pages * PS as u64, lock).unwrap();
                    let compacted = store.superblock().unwrap().compact_id != before;
                    assert_eq!(compacted, lock.is_some());
                });
            }
        }
    }

    #[test]
    fn io_errors_during_live_compaction_lose_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lock");
        std::fs::write(&path, b"").unwrap();
        let lock = crate::lock::CompactLock::open(&path).unwrap();
        let base = {
            let io = CrashIo::default();
            let mut store = PageStore::open(io.clone(), Config::default(), false).unwrap();
            for round in 1..=2 {
                for pgno in 1..=600u64 {
                    put(&mut store, pgno, round * 10_000 + pgno);
                }
                store.flush(true).unwrap();
            }
            io.0.borrow().current.clone()
        };
        let all: Vec<u64> = (1..=600).map(|pgno| 20_000 + pgno).collect();
        let want = &all[..40];
        for takes_effect in [false, true] {
            let mut op = 0;
            loop {
                let start = || {
                    let io = CrashIo::from_file(base.clone());
                    let mut store = PageStore::open(io.clone(), Config::default(), false).unwrap();
                    contents(&mut store).unwrap();
                    // Another connection, that will write next.
                    let mut reader = PageStore::open(io.clone(), Config::default(), false).unwrap();
                    contents(&mut reader).unwrap();
                    reader.sync_base().unwrap();
                    io.0.borrow_mut().fail_op = Some((op, takes_effect));
                    let _ = store.truncate(40 * PS as u64, Some(&lock));
                    let failed = io.0.borrow_mut().fail_op.take().is_none();
                    (io, store, reader, failed)
                };

                // The same connection carries on, after releasing its locks.
                let (io, mut store, mut reader, failed) = start();
                store.discard_staged();
                store.refresh().unwrap();
                store.truncate(40 * PS as u64, Some(&lock)).unwrap();
                put(&mut store, 41, 555);
                store.flush(true).unwrap();
                let mut more = want.to_vec();
                more.push(555);
                assert_eq!(contents(&mut store).unwrap(), more);
                reader.refresh().unwrap();
                assert_eq!(contents(&mut reader).unwrap(), more);
                assert_eq!(reopened(&io.0.borrow().current), more);

                // Power is lost while it, or another process, is writing more than was reclaimed.
                for next in 0..3 {
                    let (io, mut store, other, _) = start();
                    match next {
                        0 => store.discard_staged(),
                        1 => store = PageStore::open(io.clone(), Config::default(), false).unwrap(),
                        _ => store = other,
                    }
                    for pgno in 1..=1400 {
                        put(&mut store, pgno, 50_000 + pgno);
                    }
                    store.spill_pending_chunks().unwrap();
                    let got = reopened(&io.0.borrow().crash_without_superblocks());
                    assert!(got == all || got == want, "op {op}: {} pages", got.len());
                }

                if !failed {
                    break;
                }
                op += 1;
            }
            assert!(op >= 15, "only {op} operations");
        }
    }

    #[test]
    fn crash_during_creation_leaves_an_openable_file() {
        for fail_first_sync in [false, true] {
            let io = CrashIo::default();
            let mut store = PageStore::open(io.clone(), Config::default(), false).unwrap();
            io.0.borrow_mut().record = true;
            if fail_first_sync {
                put(&mut store, 1, 1);
                io.0.borrow_mut().fail_sync = true;
                assert!(store.flush(true).is_err());
                io.0.borrow_mut().fail_sync = false;
                store.discard_staged();
            }
            for pgno in 1..=40 {
                put(&mut store, pgno, pgno);
            }
            store.flush(true).unwrap();
            let want = contents(&mut store).unwrap();
            for file in &io.0.borrow().crashes {
                let got = reopened(file);
                assert!(got.is_empty() || got == want);
            }
        }
    }

    #[test]
    fn pages_written_before_a_failed_sync_are_never_published() {
        let io = CrashIo::default();
        let mut store = PageStore::open(io.clone(), Config::default(), false).unwrap();
        for pgno in 1..=40 {
            put(&mut store, pgno, pgno);
        }
        store.flush(true).unwrap();
        for pgno in 1..=40 {
            put(&mut store, pgno, 100 + pgno);
        }
        io.0.borrow_mut().fail_sync = true;
        assert!(store.flush(true).is_err());
        io.0.borrow_mut().fail_sync = false;
        // Their chunks may be gone, so they are dropped, to be written again.
        assert!(!store.has_pending_writes());
        let old: Vec<u64> = (1..=40).collect();
        assert_eq!(contents(&mut store).unwrap(), old);
        assert_eq!(reopened(&io.0.borrow().durable), old);

        // Also if they were spilled before a sync that wasn't part of a flush.
        for pgno in 1..=40 {
            put(&mut store, pgno, 100 + pgno);
        }
        store.spill_pending_chunks().unwrap();
        io.0.borrow_mut().fail_sync = true;
        assert!(store.sync().is_err());
        io.0.borrow_mut().fail_sync = false;
        assert!(store.flush(true).is_err());
        assert!(!store.has_pending_writes());
        assert_eq!(contents(&mut store).unwrap(), old);

        for pgno in 1..=40 {
            put(&mut store, pgno, 200 + pgno);
        }
        store.flush(true).unwrap();
        let new: Vec<u64> = (201..=240).collect();
        assert_eq!(contents(&mut store).unwrap(), new);
        assert_eq!(reopened(&io.0.borrow().durable), new);
    }

    #[test]
    fn superblock_is_not_written_again_over_a_newer_one() {
        let io = CrashIo::default();
        let mut a = PageStore::open(io.clone(), Config::default(), false).unwrap();
        put(&mut a, 1, 1);
        a.flush(true).unwrap();
        let mut b = PageStore::open(io.clone(), Config::default(), false).unwrap();
        a.discard_staged();
        // Two, so that the slot of a's superblock holds a newer one.
        for tag in [2, 3] {
            put(&mut b, 1, tag);
            b.flush(true).unwrap();
        }
        assert!(a.flush(true).is_err());
        assert!(a.is_stale());
        a.refresh().unwrap();
        assert_eq!(contents(&mut a).unwrap(), [3]);
        assert_eq!(reopened(&io.0.borrow().current), [3]);
    }

    /// What the file shows of a superblock that a failed sync lost: what the disk has.
    fn evict_superblock(io: &CrashIo, store: &PageStore<CrashIo>) {
        let at = store.superblock().unwrap().slot_offset() as usize;
        let mut s = io.0.borrow_mut();
        let mut slot = vec![0u8; SUPERBLOCK_SIZE];
        if let Some(durable) = s.durable.get(at..at + SUPERBLOCK_SIZE) {
            slot.copy_from_slice(durable);
        }
        s.current[at..at + SUPERBLOCK_SIZE].copy_from_slice(&slot);
    }

    #[test]
    fn lost_superblock_leaves_no_stale_caches() {
        for reader_refreshes in [false, true] {
            let io = CrashIo::default();
            let mut w = PageStore::open(io.clone(), Config::default(), false).unwrap();
            for pgno in 1..=40 {
                put(&mut w, pgno, pgno);
            }
            w.flush(true).unwrap();
            for pgno in 1..=40 {
                put(&mut w, pgno, 100 + pgno);
            }
            w.flush(false).unwrap();
            io.0.borrow_mut().fail_sync = true;
            assert!(w.flush(true).is_err());
            io.0.borrow_mut().fail_sync = false;
            // Both cache the pages of the superblock that is about to be lost.
            let mut r = PageStore::open(io.clone(), Config::default(), true).unwrap();
            let lost: Vec<u64> = (101..=140).collect();
            assert_eq!(contents(&mut r).unwrap(), lost);
            assert_eq!(contents(&mut w).unwrap(), lost);

            evict_superblock(&io, &w);
            if reader_refreshes {
                r.refresh().unwrap();
            }
            // The next checkpoint goes back to the older superblock, and reuses the space.
            w.refresh().unwrap();
            for pgno in 1..=20 {
                put(&mut w, pgno, 200 + pgno);
            }
            w.flush(true).unwrap();
            let truth = reopened(&io.0.borrow().current);
            assert_eq!(truth[..20], (201..=220).collect::<Vec<_>>());
            assert_eq!(contents(&mut w).unwrap(), truth);
            r.mark_stale();
            r.refresh().unwrap();
            assert_eq!(contents(&mut r).unwrap(), truth);
        }
    }

    #[test]
    fn rollback_succeeds_after_its_superblock_was_lost() {
        let io = CrashIo::default();
        let mut w = PageStore::open(io.clone(), Config::default(), false).unwrap();
        for pgno in 1..=40 {
            put(&mut w, pgno, pgno);
        }
        w.flush(true).unwrap();
        let old = contents(&mut w).unwrap();
        // The commit publishes, then its sync fails.
        for pgno in 1..=40 {
            put(&mut w, pgno, 100 + pgno);
        }
        w.flush(false).unwrap();
        io.0.borrow_mut().fail_sync = true;
        assert!(w.flush(true).is_err());
        io.0.borrow_mut().fail_sync = false;
        evict_superblock(&io, &w);
        // The rollback, and in exclusive locking mode nothing unlocks after it fails. The next
        // statement rolls back again.
        let mut failures = 0;
        loop {
            for pgno in 1..=40 {
                put(&mut w, pgno, pgno);
            }
            w.truncate(40 * PS as u64, None).unwrap();
            if w.flush(false).is_ok() && w.flush(true).is_ok() {
                break;
            }
            failures += 1;
            assert!(failures < 2, "the rollback keeps failing");
        }
        assert_eq!(contents(&mut w).unwrap(), old);
        assert_eq!(reopened(&io.0.borrow().durable), old);
    }

    #[test]
    fn superblock_lost_through_another_handle_and_the_cache_is_not_built_on() {
        let io = CrashIo::default();
        let mut a = PageStore::open(io.clone(), Config::default(), false).unwrap();
        for pgno in 1..=40 {
            put(&mut a, pgno, pgno);
        }
        a.flush(true).unwrap();
        let mut b = PageStore::open(io.clone(), Config::default(), false).unwrap();
        for pgno in 1..=40 {
            put(&mut a, pgno, 100 + pgno);
        }
        a.flush(false).unwrap();
        io.0.borrow_mut().fail_sync = true;
        assert!(b.flush(true).is_err());
        io.0.borrow_mut().fail_sync = false;
        // Once pages are staged, a doesn't look at the superblock again before publishing.
        put(&mut a, 41, 41);
        evict_superblock(&io, &a);
        assert!(a.flush(false).is_err());
        assert!(!a.has_pending_writes());
        a.refresh().unwrap();
        assert_eq!(contents(&mut a).unwrap(), reopened(&io.0.borrow().current));
    }

    #[test]
    fn superblock_lost_through_another_handle_is_written_again() {
        let io = CrashIo::default();
        let mut a = PageStore::open(io.clone(), Config::default(), false).unwrap();
        for pgno in 1..=40 {
            put(&mut a, pgno, pgno);
        }
        a.flush(true).unwrap();
        let mut states = vec![contents(&mut a).unwrap()];
        let mut b = PageStore::open(io.clone(), Config::default(), false).unwrap();
        for pgno in 1..=40 {
            put(&mut a, pgno, 100 + pgno);
        }
        a.flush(false).unwrap();
        let published = contents(&mut a).unwrap();
        states.push(published.clone());
        // Loses what a wrote, without a knowing.
        io.0.borrow_mut().fail_sync = true;
        assert!(b.flush(true).is_err());
        io.0.borrow_mut().fail_sync = false;

        io.0.borrow_mut().record = true;
        put(&mut a, 41, 41);
        a.flush(false).unwrap();
        states.push(contents(&mut a).unwrap());
        for file in &io.0.borrow().crashes {
            assert!(states.contains(&reopened(file)));
        }
        // Once a has synced, a crash doesn't go back further than what it had published.
        assert_eq!(reopened(&io.0.borrow().durable), published);
    }

    #[test]
    fn failed_staged_truncate_changes_nothing() {
        let io = CrashIo::default();
        let mut store = PageStore::open(io.clone(), Config::default(), false).unwrap();
        for pgno in 1..=1100 {
            put(&mut store, pgno, pgno);
        }
        store.flush(true).unwrap();
        let old = contents(&mut store).unwrap();
        let mut store = PageStore::open(io.clone(), Config::default(), false).unwrap();
        // All past the cut, and in another L2 page than the one it trims.
        for pgno in 1030..=1100 {
            put(&mut store, pgno, 10_000 + pgno);
        }
        io.0.borrow_mut().fail_read = true;
        assert!(store.truncate(600 * PS as u64, None).is_err());
        io.0.borrow_mut().fail_read = false;
        assert!(store.has_pending_writes());
        assert_eq!(store.logical_size().unwrap(), 1100 * PS as u64);
        store.discard_staged();
        store.flush(true).unwrap();
        assert_eq!(contents(&mut store).unwrap(), old);
        assert_eq!(reopened(&io.0.borrow().current), old);
    }

    #[test]
    fn superblock_lost_by_a_failed_sync_is_written_again() {
        for more_pages in [false, true] {
            let io = CrashIo::default();
            let mut store = PageStore::open(io.clone(), Config::default(), false).unwrap();
            for pgno in 1..=40 {
                put(&mut store, pgno, pgno);
            }
            store.flush(true).unwrap();
            for pgno in 1..=40 {
                put(&mut store, pgno, 100 + pgno);
            }
            store.flush(false).unwrap();
            // Loses the superblock just written.
            io.0.borrow_mut().fail_sync = true;
            assert!(store.flush(true).is_err());
            io.0.borrow_mut().fail_sync = false;

            let mut states = vec![(101..=140).collect::<Vec<u64>>()];
            io.0.borrow_mut().record = true;
            if more_pages {
                put(&mut store, 41, 41);
                states.push(contents(&mut store).unwrap());
            }
            store.flush(true).unwrap();
            assert_eq!(reopened(&io.0.borrow().durable), *states.last().unwrap());
            for file in &io.0.borrow().crashes {
                assert!(states.contains(&reopened(file)));
            }
        }
    }

    #[test]
    fn retry_after_failed_superblock_write_is_seen_by_readers_that_saw_the_first() {
        for written in [false, true] {
            let io = CrashIo::default();
            let mut store = PageStore::open(io.clone(), Config::default(), false).unwrap();
            for pgno in 1..=600 {
                put(&mut store, pgno, pgno);
            }
            store.flush(true).unwrap();
            let mut reader = PageStore::open(io.clone(), Config::default(), true).unwrap();

            let slot = io.next_slot(&store);
            io.0.borrow_mut().fail_write = Some((slot, written));
            put(&mut store, 7, 1007);
            assert!(store.flush(true).is_err());
            // Knows about the superblock, but has read nothing that it points to yet.
            reader.refresh().unwrap();

            // The retry adds something the first attempt didn't have.
            io.0.borrow_mut().fail_write = None;
            put(&mut store, 8, 1008);
            store.flush(true).unwrap();
            let seen = contents(&mut reader).unwrap();
            assert_eq!(seen[6], if written { 1007 } else { 7 });
            assert_eq!(seen[7], 8);
            reader.refresh().unwrap();
            let now = contents(&mut reader).unwrap();
            assert_eq!((now[6], now[7]), (1007, 1008));
        }
    }

    #[test]
    fn sync_failure_after_publishing_keeps_memory_and_disk_in_step() {
        let io = CrashIo::default();
        let mut store = PageStore::open(io.clone(), Config::default(), false).unwrap();
        put(&mut store, 1, 1);
        store.flush(true).unwrap();
        put(&mut store, 1, 2);
        store.flush(false).unwrap();
        io.0.borrow_mut().fail_sync = true;
        assert!(store.flush(true).is_err());
        io.0.borrow_mut().fail_sync = false;
        let published = store.superblock().unwrap().generation;
        put(&mut store, 1, 3);
        store.flush(true).unwrap();
        assert!(store.superblock().unwrap().generation > published);
        assert_eq!(reopened(&io.0.borrow().current), [3]);
    }

    #[test]
    fn failed_truncate_does_not_lose_pages() {
        let io = CrashIo::default();
        let mut store = PageStore::open(io.clone(), Config::default(), false).unwrap();
        for pgno in 1..=600 {
            put(&mut store, pgno, pgno);
        }
        store.flush(true).unwrap();
        let slot = io.next_slot(&store);
        io.0.borrow_mut().fail_write = Some((slot, false));
        assert!(store.truncate(550 * PS as u64, None).is_err());
        io.0.borrow_mut().fail_write = None;
        put(&mut store, 590, 9590);
        store.flush(true).unwrap();
        let mut want: Vec<u64> = (1..=600).collect();
        want[589] = 9590;
        assert_eq!(contents(&mut store).unwrap(), want);
        assert_eq!(reopened(&io.0.borrow().current), want);
    }

    #[test]
    fn discarded_changes_never_overwrite_another_writers() {
        let io = CrashIo::default();
        let config = Config {
            cache_mib: 64,
            ..Config::default()
        };
        let mut a = PageStore::open(io.clone(), config, false).unwrap();
        for pgno in 1..=100 {
            put(&mut a, pgno, pgno);
        }
        a.flush(true).unwrap();
        let mut b = PageStore::open(io.clone(), Config::default(), false).unwrap();

        // Enough to spill, and to be cached under offsets that b will use.
        for pgno in 1..=4200 {
            put(&mut a, pgno, 50_000 + pgno);
        }
        contents(&mut a).unwrap();
        io.0.borrow_mut().fail_sync = true;
        assert!(a.flush(true).is_err());
        io.0.borrow_mut().fail_sync = false;
        a.discard_staged();
        assert!(a.is_stale());

        for pgno in 1..=200 {
            put(&mut b, pgno, 90_000 + pgno);
        }
        b.flush(true).unwrap();
        a.flush(true).unwrap();
        a.refresh().unwrap();
        let want: Vec<u64> = (1..=200).map(|pgno| 90_000 + pgno).collect();
        assert_eq!(contents(&mut a).unwrap(), want);
        assert_eq!(reopened(&io.0.borrow().current), want);
    }

    #[test]
    fn truncating_staged_pages_publishes_nothing_until_the_flush() {
        // Pages left, whether some were spilled, and whether pages come back before the flush.
        for (new_pages, spill, regrow) in [
            (550u64, false, false),
            (550, true, true),
            (512, false, true),
            (400, false, false), // none of the staged pages are left
            (0, false, false),
            (0, true, true),
        ] {
            let io = CrashIo::default();
            let mut store = PageStore::open(io.clone(), Config::default(), false).unwrap();
            for pgno in 1..=600 {
                put(&mut store, pgno, pgno);
            }
            store.flush(true).unwrap();
            let old = contents(&mut store).unwrap();
            for pgno in 500..=700 {
                put(&mut store, pgno, 10_000 + pgno);
            }
            if new_pages == 0 {
                put(&mut store, 1, 1);
            }
            if spill {
                store.spill_pending_chunks().unwrap();
            }
            store.truncate(new_pages * PS as u64, None).unwrap();
            assert_eq!(store.logical_size().unwrap(), new_pages * PS as u64);
            assert_eq!(reopened(&io.0.borrow().current), old);
            let mut header = [1u8; 100];
            let outcome = store.read(0, &mut header).unwrap();
            if new_pages == 0 {
                assert!(matches!(outcome, ReadOutcome::ShortRead) && header == [0; 100]);
            }

            let want: Vec<u64> = (1..=new_pages)
                .map(|p| if p < 500 { p } else { 10_000 + p })
                .collect();
            assert_eq!(contents(&mut store).unwrap(), want);
            if !regrow {
                store.flush(true).unwrap();
                assert_eq!(contents(&mut store).unwrap(), want);
                assert_eq!(reopened(&io.0.borrow().current), want);
                if new_pages == 0 {
                    assert_eq!(store.superblock().unwrap().page_size, 0);
                }
            }

            // Pages that come back are empty.
            put(&mut store, 680, 680);
            store.flush(true).unwrap();
            let mut reader = PageStore::open(io.clone(), Config::default(), true).unwrap();
            let mut buf = vec![0u8; PS];
            for store in [&mut store, &mut reader] {
                for pgno in [new_pages + 1, 600, 679] {
                    store.read((pgno - 1) * PS as u64, &mut buf).unwrap();
                    assert!(buf.iter().all(|&b| b == 0), "page {pgno}");
                }
                for (i, &tag) in want.iter().chain([&680]).enumerate() {
                    let pgno = if i < want.len() { i as u64 + 1 } else { 680 };
                    store.read((pgno - 1) * PS as u64, &mut buf).unwrap();
                    assert!(buf == page(tag), "page {pgno}");
                }
            }
        }

        // Also those in directory pages that only staged pages used.
        let mut store = PageStore::open(CrashIo::default(), Config::default(), false).unwrap();
        for pgno in 1..=700 {
            put(&mut store, pgno, pgno);
            if pgno == 300 {
                store.flush(true).unwrap();
            }
        }
        store.spill_pending_chunks().unwrap();
        store.truncate(100 * PS as u64, None).unwrap();
        put(&mut store, 680, 680);
        store.flush(true).unwrap();
        let mut buf = vec![0u8; PS];
        store.read(599 * PS as u64, &mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 0));
    }

    #[test]
    fn database_can_start_with_any_page() {
        for ps in [512usize, 4096, 65536] {
            let io = CrashIo::default();
            let mut store = PageStore::open(io, Config::default(), false).unwrap();
            store.write(3 * ps as u64, &vec![4u8; ps]).unwrap();
            let mut p1 = vec![1u8; ps];
            p1[16..18].copy_from_slice(&if ps == 65536 { 1u16 } else { ps as u16 }.to_be_bytes());
            store.write(0, &p1).unwrap();
            store.flush(true).unwrap();
            assert_eq!(store.logical_size().unwrap(), 4 * ps as u64);
            let mut buf = vec![0u8; ps];
            store.read(0, &mut buf).unwrap();
            assert_eq!(buf, p1);
            store.read(3 * ps as u64, &mut buf).unwrap();
            assert_eq!(buf, vec![4u8; ps]);
        }
    }

    #[test]
    fn page_size_changes_keep_every_byte() {
        let io = CrashIo::default();
        let mut store = PageStore::open(io, Config::default(), false).unwrap();
        let mut model = vec![0u8; 64 * 16384];
        let mut x = 1u64;
        for (i, &ps) in [16384usize, 512, 65536, 4096, 16384].iter().enumerate() {
            for _ in 0..200 {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let at = (x as usize % (model.len() / ps)) * ps;
                let fill = vec![(x >> 32) as u8 ^ i as u8; ps];
                store.write(at as u64, &fill).unwrap();
                model[at..at + ps].copy_from_slice(&fill);
            }
            store.flush(true).unwrap();
            let len = store.logical_size().unwrap() as usize;
            let mut got = vec![0u8; len];
            store.read(0, &mut got).unwrap();
            assert!(got == model[..len], "after {ps}-byte pages");
        }
        // Sizes that aren't whole store pages are rounded up.
        store.truncate(5 * 16384 + 512, None).unwrap();
        assert_eq!(store.logical_size().unwrap(), 6 * 16384);
    }

    #[test]
    fn superblock_caught_mid_write_is_read_again() {
        /// Damages the newer slot in the first few reads, with `.2`.
        struct TearingIo(CrashIo, u32, fn(&mut [u8]));
        impl BlockIo for TearingIo {
            fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize> {
                let n = self.0.read_at(offset, buf)?;
                if offset == 0 && self.1 > 0 {
                    self.1 -= 1;
                    let region: &[u8] = buf;
                    let newest = Superblock::select_latest(region.try_into().unwrap()).unwrap();
                    (self.2)(&mut buf[newest.slot_offset() as usize..][..4096]);
                }
                Ok(n)
            }
            fn write_at(&mut self, _: u64, _: &[u8]) -> Result<()> {
                unreachable!()
            }
            fn sync(&mut self) -> Result<()> {
                unreachable!()
            }
            fn file_size(&mut self) -> Result<u64> {
                self.0.file_size()
            }
            fn truncate(&mut self, _: u64) -> Result<()> {
                unreachable!()
            }
        }
        let io = CrashIo::default();
        let mut store = PageStore::open(io.clone(), Config::default(), false).unwrap();
        for tag in 1..=2 {
            put(&mut store, 1, tag);
            store.flush(true).unwrap();
        }
        let half_written: fn(&mut [u8]) = |slot| slot[4090] ^= 0xff;
        let mut reader = PageStore::open(
            TearingIo(io.clone(), 2, half_written),
            Config::default(),
            true,
        )
        .unwrap();
        assert_eq!(contents(&mut reader).unwrap(), [2]);
        // A slot that stays torn is what a crash leaves: the other one counts.
        let mut reader = PageStore::open(
            TearingIo(io.clone(), 1000, half_written),
            Config::default(),
            true,
        )
        .unwrap();
        assert_eq!(contents(&mut reader).unwrap(), [1]);
        // It doesn't look torn if the write hasn't reached its start, but the generation goes back.
        let mut reader = PageStore::open(
            TearingIo(io, 0, |slot| slot.fill(0)),
            Config::default(),
            true,
        )
        .unwrap();
        reader.io_mut().1 = 2;
        reader.refresh().unwrap();
        assert_eq!(contents(&mut reader).unwrap(), [2]);
    }

    #[test]
    fn only_an_empty_or_zeroed_header_counts_as_a_new_database() {
        for len in [0usize, 100, 4096, 8192] {
            let mut store =
                PageStore::open(CrashIo::from_file(vec![0; len]), Config::default(), false)
                    .unwrap();
            assert_eq!(store.logical_size().unwrap(), 0);
            put(&mut store, 1, 5);
            store.flush(true).unwrap();
            assert_eq!(contents(&mut store).unwrap(), [5]);
        }
        // A crash can leave any sectors of the first superblock.
        let initial = Superblock::initial().encode();
        for sectors in 0u8..=255 {
            let mut file = vec![0u8; 8192];
            for i in (0..8).filter(|i| sectors & (1 << i) != 0) {
                file[4096 + i * 512..][..512].copy_from_slice(&initial[i * 512..][..512]);
            }
            let mut store =
                PageStore::open(CrashIo::from_file(file), Config::default(), false).unwrap();
            put(&mut store, 1, 5);
            store.flush(true).unwrap();
            assert_eq!(contents(&mut store).unwrap(), [5]);
        }
        // Zeros over a file with content are damage, and other files aren't ours.
        let mut torn = vec![0u8; 8208];
        torn[4096..4608].copy_from_slice(&initial[..512]);
        let mut text = vec![0u8; 8192];
        text[4100] = b'x';
        let mut text_first = vec![0u8; 8192];
        text_first[0] = b'S';
        for file in [vec![0; 8208], torn, text, text_first] {
            assert!(matches!(
                PageStore::open(CrashIo::from_file(file), Config::default(), false),
                Err(Error::NotASqmicroDb)
            ));
        }
    }

    #[test]
    fn database_created_while_it_is_being_opened_is_recognized() {
        /// Lets another process create the database right before the first size query.
        struct RacingIo(CrashIo, bool);
        impl BlockIo for RacingIo {
            fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize> {
                self.0.read_at(offset, buf)
            }
            fn write_at(&mut self, _: u64, _: &[u8]) -> Result<()> {
                unreachable!()
            }
            fn sync(&mut self) -> Result<()> {
                unreachable!()
            }
            fn file_size(&mut self) -> Result<u64> {
                if std::mem::take(&mut self.1) {
                    let mut writer =
                        PageStore::open(self.0.clone(), Config::default(), false).unwrap();
                    put(&mut writer, 1, 5);
                    writer.flush(true).unwrap();
                }
                self.0.file_size()
            }
            fn truncate(&mut self, _: u64) -> Result<()> {
                unreachable!()
            }
        }
        let io = RacingIo(CrashIo::default(), true);
        let mut reader = PageStore::open(io, Config::default(), true).unwrap();
        assert_eq!(contents(&mut reader).unwrap(), [5]);
    }

    #[test]
    fn crafted_superblocks_are_rejected_without_huge_allocations() {
        let mut good = Superblock::empty();
        good.generation = 2;
        good.page_size = 4096;
        good.page_count = 600;
        good.l1_entries = 2;
        good.l1_offset = SUPERBLOCK_REGION;
        good.tail_offset = SUPERBLOCK_REGION + 16;
        assert!(Superblock::decode(&good.encode()).is_some());
        let bad: [fn(&mut Superblock); 9] = [
            |sb| sb.tail_offset = SUPERBLOCK_REGION, // before the end of L1
            |sb| sb.l1_entries = u32::MAX,
            |sb| sb.page_count = 1 << 40,
            |sb| sb.page_count = u64::MAX,
            |sb| sb.page_size = 0,
            |sb| sb.generation = u64::MAX,
            |sb| sb.compact_id = u64::MAX,
            |sb| sb.tail_offset = u64::MAX - 15,
            |sb| sb.l1_offset = u64::MAX,
        ];
        for change in bad {
            let mut sb = good.clone();
            change(&mut sb);
            assert!(Superblock::decode(&sb.encode()).is_none());
        }
        // Consistent, but the file is far too small for its directory.
        let mut sb = good;
        sb.l1_entries = u32::MAX;
        sb.page_count = u32::MAX as u64 * L2_ENTRIES as u64;
        sb.tail_offset = SUPERBLOCK_REGION + (1 << 36);
        let mut file = vec![0u8; 8192];
        file[..4096].copy_from_slice(&sb.encode());
        assert!(matches!(
            PageStore::open(CrashIo::from_file(file), Config::default(), true),
            Err(Error::Corrupt(_))
        ));
    }
}
