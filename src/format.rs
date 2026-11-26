#![allow(dead_code)]

use crate::error::{Error, Result};

pub const MAGIC: [u8; 8] = *b"SQMICRO1";
pub const FORMAT_VERSION: u32 = 2;
pub const SUPERBLOCK_SIZE: usize = 4096;
pub const SUPERBLOCK_REGION: u64 = 8192;
pub const LOCK_HOLE_START: u64 = 0x4000_0000;
pub const LOCK_HOLE_END: u64 = 0x4000_1000;
pub const COMPACT_LOCK_OFFSET: u64 = 0x4000_0300;
pub const L2_ENTRIES: usize = 512;
pub const L2_PAGE_SIZE: usize = 4096;
pub const CHUNK_HEADER_SIZE: usize = 8;
pub const MAX_CHUNK_BYTES: usize = 256 * 1024;
pub const MAX_CHUNK_PAGES: usize = 64;
pub const MAX_FILE_OFFSET: u64 = (1u64 << 38) * 16;
/// Far beyond any real generation or compact_id, and leaves room to increment them.
const MAX_COUNTER: u64 = u64::MAX / 2;

#[inline]
pub fn align16(v: u64) -> u64 {
    (v + 15) & !15
}

pub fn alloc_offset(tail: u64, aligned_len: u64) -> Result<(u64, u64)> {
    if aligned_len % 16 != 0 {
        return Err(Error::InvalidConfig(format!(
            "allocation length {aligned_len} is not 16-byte aligned"
        )));
    }
    let mut start = align16(tail.max(SUPERBLOCK_REGION));
    let mut end = start
        .checked_add(aligned_len)
        .ok_or_else(|| Error::Corrupt("file offset overflow".into()))?;

    // Never overlap Windows' mandatory lock byte range [0x4000_0000, 0x4000_1000).
    if start < LOCK_HOLE_END && end > LOCK_HOLE_START {
        start = LOCK_HOLE_END;
        end = start
            .checked_add(aligned_len)
            .ok_or_else(|| Error::Corrupt("file offset overflow".into()))?;
    }

    if end >= MAX_FILE_OFFSET {
        return Err(Error::Corrupt(format!(
            "file offset {end} exceeds maximum addressable size {MAX_FILE_OFFSET}"
        )));
    }
    Ok((start, end))
}

pub fn is_valid_page_size(ps: u32) -> bool {
    (512..=65536).contains(&ps) && ps.is_power_of_two()
}

pub fn parse_sqlite_page_size(page1: &[u8]) -> Option<u32> {
    if page1.len() < 18 {
        return None;
    }
    let raw = u16::from_be_bytes([page1[16], page1[17]]);
    let ps = if raw == 1 { 65536 } else { raw as u32 };
    is_valid_page_size(ps).then_some(ps)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Superblock {
    pub format_version: u32,
    pub flags: u32,
    pub generation: u64,
    pub page_size: u32,
    pub chunk_max_pages: u32,
    pub page_count: u64,
    pub tail_offset: u64,
    pub live_bytes: u64,
    pub l1_offset: u64,
    pub l1_entries: u32,
    pub header_len: u32,
    pub compact_id: u64,
    pub dict_offset: u64,
    pub dict_len: u32,
    pub sqlite_header: [u8; 100],
}

impl Superblock {
    pub fn empty() -> Self {
        Self {
            format_version: FORMAT_VERSION,
            flags: 0,
            generation: 0,
            page_size: 0,
            chunk_max_pages: 0,
            page_count: 0,
            tail_offset: SUPERBLOCK_REGION,
            live_bytes: 0,
            l1_offset: 0,
            l1_entries: 0,
            header_len: 0,
            compact_id: 0,
            dict_offset: 0,
            dict_len: 0,
            sqlite_header: [0u8; 100],
        }
    }

    /// The first superblock of every file.
    pub fn initial() -> Self {
        Self {
            generation: 1,
            ..Self::empty()
        }
    }

    #[inline]
    pub fn slot_offset(&self) -> u64 {
        if self.generation & 1 == 0 {
            0
        } else {
            SUPERBLOCK_SIZE as u64
        }
    }

    pub fn encode(&self) -> [u8; SUPERBLOCK_SIZE] {
        let mut buf = [0u8; SUPERBLOCK_SIZE];
        buf[0..8].copy_from_slice(&MAGIC);
        buf[8..12].copy_from_slice(&self.format_version.to_le_bytes());
        buf[12..16].copy_from_slice(&self.flags.to_le_bytes());
        buf[16..24].copy_from_slice(&self.generation.to_le_bytes());
        buf[24..28].copy_from_slice(&self.page_size.to_le_bytes());
        buf[28..32].copy_from_slice(&self.chunk_max_pages.to_le_bytes());
        buf[32..40].copy_from_slice(&self.page_count.to_le_bytes());
        buf[40..48].copy_from_slice(&self.tail_offset.to_le_bytes());
        buf[48..56].copy_from_slice(&self.live_bytes.to_le_bytes());
        buf[56..64].copy_from_slice(&self.l1_offset.to_le_bytes());
        buf[64..68].copy_from_slice(&self.l1_entries.to_le_bytes());
        buf[68..72].copy_from_slice(&self.header_len.to_le_bytes());
        buf[72..80].copy_from_slice(&self.compact_id.to_le_bytes());
        buf[80..88].copy_from_slice(&self.dict_offset.to_le_bytes());
        buf[88..92].copy_from_slice(&self.dict_len.to_le_bytes());
        // 92..96 reserved0 = 0
        buf[96..196].copy_from_slice(&self.sqlite_header);
        let crc = crc32fast::hash(&buf[..4092]);
        buf[4092..4096].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    pub fn decode(buf: &[u8; SUPERBLOCK_SIZE]) -> Option<Self> {
        if buf[0..8] != MAGIC {
            return None;
        }
        let format_version = u32::from_le_bytes(buf[8..12].try_into().ok()?);
        if format_version != FORMAT_VERSION {
            return None;
        }
        let stored_crc = u32::from_le_bytes(buf[4092..4096].try_into().ok()?);
        if crc32fast::hash(&buf[..4092]) != stored_crc {
            return None;
        }
        let flags = u32::from_le_bytes(buf[12..16].try_into().ok()?);
        let generation = u64::from_le_bytes(buf[16..24].try_into().ok()?);
        if generation == 0 || generation > MAX_COUNTER {
            return None;
        }
        let page_size = u32::from_le_bytes(buf[24..28].try_into().ok()?);
        if page_size != 0 && !is_valid_page_size(page_size) {
            return None;
        }
        let chunk_max_pages = u32::from_le_bytes(buf[28..32].try_into().ok()?);
        let page_count = u64::from_le_bytes(buf[32..40].try_into().ok()?);
        let tail_offset = u64::from_le_bytes(buf[40..48].try_into().ok()?);
        if !(SUPERBLOCK_REGION..=MAX_FILE_OFFSET).contains(&tail_offset) || tail_offset % 16 != 0 {
            return None;
        }
        let logical_bytes = page_count.checked_mul(page_size as u64)?;
        if logical_bytes > i64::MAX as u64 || (page_count == 0) != (logical_bytes == 0) {
            return None;
        }
        let live_bytes = u64::from_le_bytes(buf[48..56].try_into().ok()?);
        let l1_offset = u64::from_le_bytes(buf[56..64].try_into().ok()?);
        let l1_entries = u32::from_le_bytes(buf[64..68].try_into().ok()?);
        let l1_end = l1_offset.checked_add(l1_entries as u64 * 8)?;
        if l1_entries as u64 != page_count.div_ceil(L2_ENTRIES as u64) || l1_end > tail_offset {
            return None;
        }
        let header_len = u32::from_le_bytes(buf[68..72].try_into().ok()?);
        if header_len != 0 && header_len != 100 {
            return None;
        }
        let compact_id = u64::from_le_bytes(buf[72..80].try_into().ok()?);
        if compact_id > MAX_COUNTER {
            return None;
        }
        let dict_offset = u64::from_le_bytes(buf[80..88].try_into().ok()?);
        let dict_len = u32::from_le_bytes(buf[88..92].try_into().ok()?);
        let mut sqlite_header = [0u8; 100];
        sqlite_header.copy_from_slice(&buf[96..196]);
        Some(Self {
            format_version,
            flags,
            generation,
            page_size,
            chunk_max_pages,
            page_count,
            tail_offset,
            live_bytes,
            l1_offset,
            l1_entries,
            header_len,
            compact_id,
            dict_offset,
            dict_len,
            sqlite_header,
        })
    }

    pub fn select_latest(region: &[u8; SUPERBLOCK_REGION as usize]) -> Option<Self> {
        let a: &[u8; SUPERBLOCK_SIZE] = region[0..SUPERBLOCK_SIZE].try_into().ok()?;
        let b: &[u8; SUPERBLOCK_SIZE] = region[SUPERBLOCK_SIZE..SUPERBLOCK_SIZE * 2]
            .try_into()
            .ok()?;
        match (Self::decode(a), Self::decode(b)) {
            (Some(sa), Some(sb)) => {
                if sb.generation > sa.generation {
                    Some(sb)
                } else {
                    Some(sa)
                }
            }
            (Some(sa), None) => Some(sa),
            (None, Some(sb)) => Some(sb),
            (None, None) => None,
        }
    }

    /// Whether a slot of this version doesn't decode, as when a concurrent write is caught halfway.
    pub fn has_torn_slot(region: &[u8; SUPERBLOCK_REGION as usize]) -> bool {
        region.chunks_exact(SUPERBLOCK_SIZE).any(|slot| {
            slot[0..8] == MAGIC
                && slot[8..12] == FORMAT_VERSION.to_le_bytes()
                && Self::decode(slot.try_into().unwrap()).is_none()
        })
    }

    /// Whether this is what a crash while writing `initial()` can leave: any part of it, or nothing.
    pub fn is_unfinished_initial(region: &[u8; SUPERBLOCK_REGION as usize]) -> bool {
        let initial = Self::initial();
        let (before, slot) = region.split_at(initial.slot_offset() as usize);
        before.iter().all(|&b| b == 0)
            && slot
                .iter()
                .zip(initial.encode())
                .all(|(&b, want)| b == 0 || b == want)
    }

    /// Format version claimed by a sqmicro file this build cannot read. Only meaningful
    /// once `select_latest` found no valid slot, as the version is not checksum-verified.
    pub fn unsupported_version(region: &[u8; SUPERBLOCK_REGION as usize]) -> Option<u32> {
        region
            .chunks_exact(SUPERBLOCK_SIZE)
            .filter(|slot| slot[0..8] == MAGIC)
            .map(|slot| u32::from_le_bytes([slot[8], slot[9], slot[10], slot[11]]))
            .find(|&version| version != FORMAT_VERSION)
    }
}

const OFFSET_BITS: u32 = 38;
const OFFSET_MASK: u64 = (1u64 << OFFSET_BITS) - 1;
const READ_UNITS_SHIFT: u32 = 38;
const READ_UNITS_BITS: u32 = 15;
const READ_UNITS_MASK: u64 = (1u64 << READ_UNITS_BITS) - 1;
const PAGE_IDX_SHIFT: u32 = 53;
const PAGE_IDX_BITS: u32 = 6;
const PAGE_IDX_MASK: u64 = (1u64 << PAGE_IDX_BITS) - 1;
const FLAGS_SHIFT: u32 = 59;
const FLAG_PRESENT: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DirEntry(u64);

impl DirEntry {
    pub const EMPTY: Self = Self(0);

    pub fn new(chunk_offset: u64, total_chunk_bytes: usize, page_idx: u8) -> Result<Self> {
        if chunk_offset % 16 != 0 || chunk_offset >= MAX_FILE_OFFSET {
            return Err(Error::Corrupt(format!(
                "invalid chunk offset {chunk_offset}"
            )));
        }
        let read_units = (total_chunk_bytes as u64).div_ceil(16);
        if read_units == 0 || read_units > READ_UNITS_MASK {
            return Err(Error::Corrupt(format!(
                "chunk size {total_chunk_bytes} exceeds directory entry capacity"
            )));
        }
        if page_idx as usize >= MAX_CHUNK_PAGES {
            return Err(Error::Corrupt(format!(
                "page index {page_idx} exceeds max chunk pages"
            )));
        }
        let raw = (chunk_offset >> 4)
            | (read_units << READ_UNITS_SHIFT)
            | ((page_idx as u64) << PAGE_IDX_SHIFT)
            | (FLAG_PRESENT << FLAGS_SHIFT);
        Ok(Self(raw))
    }

    #[inline]
    pub fn flags(self) -> u8 {
        (self.0 >> FLAGS_SHIFT) as u8
    }

    #[inline]
    pub fn is_empty(self) -> bool {
        self.flags() == 0
    }

    #[inline]
    pub fn chunk_offset(self) -> u64 {
        (self.0 & OFFSET_MASK) << 4
    }

    #[inline]
    pub fn read_len(self) -> usize {
        (((self.0 >> READ_UNITS_SHIFT) & READ_UNITS_MASK) << 4) as usize
    }

    #[inline]
    pub fn page_idx(self) -> u8 {
        ((self.0 >> PAGE_IDX_SHIFT) & PAGE_IDX_MASK) as u8
    }

    #[inline]
    pub fn to_raw(self) -> u64 {
        self.0
    }

    #[inline]
    pub fn from_raw(raw: u64) -> Self {
        Self(raw)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct L2Page {
    pub entries: [DirEntry; L2_ENTRIES],
}

impl L2Page {
    pub fn empty() -> Self {
        Self {
            entries: [DirEntry::EMPTY; L2_ENTRIES],
        }
    }

    pub fn encode(&self) -> [u8; L2_PAGE_SIZE] {
        let mut buf = [0u8; L2_PAGE_SIZE];
        for (i, entry) in self.entries.iter().enumerate() {
            buf[i * 8..i * 8 + 8].copy_from_slice(&entry.to_raw().to_le_bytes());
        }
        buf
    }

    pub fn decode(buf: &[u8; L2_PAGE_SIZE]) -> Self {
        let mut entries = [DirEntry::EMPTY; L2_ENTRIES];
        for (i, slot) in entries.iter_mut().enumerate() {
            let raw = u64::from_le_bytes(buf[i * 8..i * 8 + 8].try_into().unwrap());
            *slot = DirEntry::from_raw(raw);
        }
        Self { entries }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ChunkCodec {
    Raw = 0,
    Zstd = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkHeader {
    pub codec: ChunkCodec,
    pub page_count: u8,
    pub dict_id: u16,
    pub payload_len: u32,
}

impl ChunkHeader {
    pub fn encode(&self) -> [u8; CHUNK_HEADER_SIZE] {
        let mut buf = [0u8; CHUNK_HEADER_SIZE];
        buf[0] = self.codec as u8;
        buf[1] = self.page_count;
        buf[2..4].copy_from_slice(&self.dict_id.to_le_bytes());
        buf[4..8].copy_from_slice(&self.payload_len.to_le_bytes());
        buf
    }

    pub fn decode(buf: &[u8; CHUNK_HEADER_SIZE]) -> Result<Self> {
        let codec = match buf[0] {
            0 => ChunkCodec::Raw,
            1 => ChunkCodec::Zstd,
            other => {
                return Err(Error::Corrupt(format!("unknown chunk codec {other}")));
            }
        };
        let page_count = buf[1];
        if page_count == 0 || page_count as usize > MAX_CHUNK_PAGES {
            return Err(Error::Corrupt(format!(
                "invalid chunk page_count {page_count}"
            )));
        }
        let dict_id = u16::from_le_bytes([buf[2], buf[3]]);
        let payload_len = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
        Ok(Self {
            codec,
            page_count,
            dict_id,
            payload_len,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn superblock_checksum_is_crc32_ieee() {
        // Pins the on-disk algorithm: changing it requires a FORMAT_VERSION bump.
        assert_eq!(crc32fast::hash(b"123456789"), 0xcbf43926);
        let mut sb = Superblock::empty();
        sb.generation = 1;
        let buf = sb.encode();
        assert_eq!(buf[4092..], crc32fast::hash(&buf[..4092]).to_le_bytes());
    }

    #[test]
    fn superblock_roundtrip_and_ab_selection_with_corrupt_slot() {
        let mut sb_a = Superblock::empty();
        sb_a.generation = 1;
        sb_a.page_size = 16384;
        sb_a.page_count = 10;
        sb_a.l1_offset = 0x2000;
        sb_a.l1_entries = 1;
        sb_a.tail_offset = 0x2000 + 4096;
        sb_a.compact_id = 2;
        sb_a.header_len = 100;
        sb_a.sqlite_header[..16].copy_from_slice(b"SQLite format 3\0");

        let mut sb_b = sb_a.clone();
        sb_b.generation = 2;
        sb_b.page_count = 20;

        let mut region = [0u8; 8192];
        region[sb_a.slot_offset() as usize..sb_a.slot_offset() as usize + 4096]
            .copy_from_slice(&sb_a.encode());
        region[sb_b.slot_offset() as usize..sb_b.slot_offset() as usize + 4096]
            .copy_from_slice(&sb_b.encode());
        assert_eq!(Superblock::select_latest(&region).unwrap().generation, 2);

        // Corrupt slot B (generation 2): must fall back cleanly to slot A (generation 1)
        region[sb_b.slot_offset() as usize + 40] ^= 0xff;
        let selected = Superblock::select_latest(&region).unwrap();
        assert_eq!(selected.generation, 1);
        assert_eq!(selected.page_count, 10);
    }

    #[test]
    fn dir_entry_bitpacking_preserves_offset_len_and_page_idx() {
        let offset = 0x1_0000_0000u64; // 4 GiB, 16-byte aligned
        let entry = DirEntry::new(offset, 262_152, 63).unwrap();
        assert!(!entry.is_empty());
        assert_eq!(entry.chunk_offset(), offset);
        assert_eq!(entry.read_len(), 262_160);
        assert_eq!(entry.page_idx(), 63);
        assert_eq!(DirEntry::from_raw(entry.to_raw()), entry);
    }

    #[test]
    fn alloc_offset_skips_windows_1gib_lock_hole() {
        let (off, next) = alloc_offset(0x3fff_f000, 0x1000).unwrap();
        assert_eq!((off, next), (0x3fff_f000, 0x4000_0000));
        let (off, next) = alloc_offset(0x3fff_f800, 0x1000).unwrap();
        assert_eq!((off, next), (0x4000_1000, 0x4000_2000));
        let (off, next) = alloc_offset(0x4000_0000, 0x200).unwrap();
        assert_eq!((off, next), (0x4000_1000, 0x4000_1200));
    }

    #[test]
    fn parse_sqlite_page_size_handles_all_powers_of_two_and_65536() {
        let mut hdr = [0u8; 100];
        hdr[16..18].copy_from_slice(&4096u16.to_be_bytes());
        assert_eq!(parse_sqlite_page_size(&hdr), Some(4096));
        hdr[16..18].copy_from_slice(&1u16.to_be_bytes());
        assert_eq!(parse_sqlite_page_size(&hdr), Some(65536));
        hdr[16..18].copy_from_slice(&3000u16.to_be_bytes());
        assert_eq!(parse_sqlite_page_size(&hdr), None);
    }
}
