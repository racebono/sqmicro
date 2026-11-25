#![allow(dead_code)]

use crate::error::{Error, Result};
use crate::format::{CHUNK_HEADER_SIZE, ChunkCodec, ChunkHeader, align16};
use std::borrow::Cow;
use std::ffi::{CStr, c_void};
use std::ptr::NonNull;

fn zstd_err(code: usize) -> String {
    let ptr = unsafe { zstd_sys::ZSTD_getErrorName(code) };
    if ptr.is_null() {
        format!("zstd error code {code}")
    } else {
        unsafe { CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned()
    }
}

pub struct Compressor {
    cctx: NonNull<zstd_sys::ZSTD_CCtx>,
}

// Safety: ZSTD_CCtx is safe to move across threads when not used concurrently.
unsafe impl Send for Compressor {}

impl Drop for Compressor {
    fn drop(&mut self) {
        unsafe {
            zstd_sys::ZSTD_freeCCtx(self.cctx.as_ptr());
        }
    }
}

impl Compressor {
    pub fn new(level: i32) -> Result<Self> {
        let cctx = NonNull::new(unsafe { zstd_sys::ZSTD_createCCtx() })
            .ok_or_else(|| Error::Zstd("ZSTD_createCCtx returned null".into()))?;
        let this = Self { cctx };
        unsafe {
            let rc = zstd_sys::ZSTD_CCtx_setParameter(
                this.cctx.as_ptr(),
                zstd_sys::ZSTD_cParameter::ZSTD_c_compressionLevel,
                level,
            );
            if zstd_sys::ZSTD_isError(rc) != 0 {
                return Err(Error::Zstd(zstd_err(rc)));
            }
            let rc = zstd_sys::ZSTD_CCtx_setParameter(
                this.cctx.as_ptr(),
                zstd_sys::ZSTD_cParameter::ZSTD_c_checksumFlag,
                1,
            );
            if zstd_sys::ZSTD_isError(rc) != 0 {
                return Err(Error::Zstd(zstd_err(rc)));
            }
        }
        Ok(this)
    }

    /// Compresses a block started with `new_block`, keeping room for the chunk header in the
    /// result. Returns `raw` itself when compression is skipped or doesn't help.
    pub fn compress(&mut self, raw: Vec<u8>, force_raw: bool) -> Result<(ChunkCodec, Vec<u8>)> {
        let src = &raw[CHUNK_HEADER_SIZE..];
        if force_raw || src.is_empty() {
            return Ok((ChunkCodec::Raw, raw));
        }
        let bound = unsafe { zstd_sys::ZSTD_compressBound(src.len()) };

        let mut out = new_block(bound);
        let dst = &mut out.spare_capacity_mut()[..bound];

        let written = unsafe {
            zstd_sys::ZSTD_compress2(
                self.cctx.as_ptr(),
                dst.as_mut_ptr().cast::<c_void>(),
                dst.len(),
                src.as_ptr().cast::<c_void>(),
                src.len(),
            )
        };

        if unsafe { zstd_sys::ZSTD_isError(written) } != 0 {
            return Err(Error::Zstd(zstd_err(written)));
        }
        // SAFETY: zstd initialized the `written` bytes after the header, and `written <= bound`.
        unsafe { out.set_len(out.len() + written) };

        if written >= src.len() {
            return Ok((ChunkCodec::Raw, raw));
        }

        Ok((ChunkCodec::Zstd, out))
    }
}

/// An empty chunk block: a placeholder header, plus capacity for `payload_len` bytes and up to
/// 15 bytes of padding.
#[inline]
fn new_block(payload_len: usize) -> Vec<u8> {
    let mut out = Box::<[u8]>::new_uninit_slice(CHUNK_HEADER_SIZE + payload_len + 15);
    // SAFETY: `out` holds at least `CHUNK_HEADER_SIZE` bytes.
    unsafe { std::ptr::write_bytes(out.as_mut_ptr().cast::<u8>(), 0, CHUNK_HEADER_SIZE) };
    let out = Box::into_raw(out);
    // SAFETY: the allocation has the layout of a `Vec<u8>` with capacity `out.len()`, and its
    // first `CHUNK_HEADER_SIZE` bytes are initialized.
    unsafe { Vec::from_raw_parts(out.cast(), CHUNK_HEADER_SIZE, out.len()) }
}

pub struct Decompressor {
    dctx: NonNull<zstd_sys::ZSTD_DCtx>,
}

// Safety: ZSTD_DCtx is safe to move across threads when not used concurrently.
unsafe impl Send for Decompressor {}

impl Drop for Decompressor {
    fn drop(&mut self) {
        unsafe {
            zstd_sys::ZSTD_freeDCtx(self.dctx.as_ptr());
        }
    }
}

impl Decompressor {
    pub fn new() -> Result<Self> {
        let dctx = NonNull::new(unsafe { zstd_sys::ZSTD_createDCtx() })
            .ok_or_else(|| Error::Zstd("ZSTD_createDCtx returned null".into()))?;
        Ok(Self { dctx })
    }

    pub fn decompress<'p>(
        &mut self,
        codec: ChunkCodec,
        payload: &'p [u8],
        expected_len: usize,
    ) -> Result<Cow<'p, [u8]>> {
        match codec {
            ChunkCodec::Raw => {
                if payload.len() != expected_len {
                    return Err(Error::Corrupt(format!(
                        "raw chunk length {} != expected {expected_len}",
                        payload.len()
                    )));
                }
                Ok(Cow::Borrowed(payload))
            }
            ChunkCodec::Zstd => {
                let mut out = Box::<[u8]>::new_uninit_slice(expected_len);
                let decoded = unsafe {
                    zstd_sys::ZSTD_decompressDCtx(
                        self.dctx.as_ptr(),
                        out.as_mut_ptr().cast::<c_void>(),
                        out.len(),
                        payload.as_ptr().cast::<c_void>(),
                        payload.len(),
                    )
                };

                if unsafe { zstd_sys::ZSTD_isError(decoded) } != 0 {
                    return Err(Error::Corrupt(format!(
                        "zstd decompression failed: {}",
                        zstd_err(decoded)
                    )));
                }

                let out = unsafe {
                    let ptr = Box::into_raw(out);
                    Vec::<u8>::from_raw_parts(ptr.cast(), decoded, ptr.len())
                };

                if out.len() != expected_len {
                    return Err(Error::Corrupt(format!(
                        "zstd decoded {} bytes, expected {expected_len}",
                        out.len()
                    )));
                }

                Ok(Cow::Owned(out))
            }
        }
    }
}

pub struct ChunkInput<'a> {
    pub pages: Vec<(u64, &'a [u8])>,
    pub force_raw: bool,
}

pub struct EncodedChunk {
    pub pagenos: Vec<u64>,
    pub header: ChunkHeader,
    /// Exactly what goes on disk: header, payload, then zero padding to 16 bytes.
    pub block: Vec<u8>,
    pub raw_len: usize,
}

impl EncodedChunk {
    /// Header and payload length, without the padding.
    pub fn read_len(&self) -> usize {
        CHUNK_HEADER_SIZE + self.header.payload_len as usize
    }

    pub fn payload(&self) -> &[u8] {
        &self.block[CHUNK_HEADER_SIZE..self.read_len()]
    }
}

fn encode_one(compressor: &mut Compressor, input: &ChunkInput) -> Result<EncodedChunk> {
    let raw_len: usize = input.pages.iter().map(|(_, data)| data.len()).sum();
    let mut pagenos = Vec::with_capacity(input.pages.len());
    let mut raw = new_block(raw_len);
    for (pgno, data) in &input.pages {
        pagenos.push(*pgno);
        raw.extend_from_slice(data);
    }

    let (codec, mut block) = compressor.compress(raw, input.force_raw)?;
    let header = ChunkHeader {
        codec,
        page_count: u8::try_from(pagenos.len())
            .map_err(|_| Error::Corrupt("chunk page_count overflow".into()))?,
        dict_id: 0,
        payload_len: u32::try_from(block.len() - CHUNK_HEADER_SIZE)
            .map_err(|_| Error::Corrupt("chunk payload_len overflow".into()))?,
    };
    block[..CHUNK_HEADER_SIZE].copy_from_slice(&header.encode());
    block.resize(align16(block.len() as u64) as usize, 0);

    Ok(EncodedChunk {
        pagenos,
        header,
        block,
        raw_len,
    })
}

pub fn compress_chunks_parallel(
    chunks: Vec<ChunkInput>,
    level: i32,
    threads: usize,
) -> Result<Vec<EncodedChunk>> {
    if chunks.is_empty() {
        return Ok(Vec::new());
    }
    if threads <= 1 || chunks.len() < 2 {
        let mut compressor = Compressor::new(level)?;
        let mut out = Vec::with_capacity(chunks.len());
        for chunk in &chunks {
            out.push(encode_one(&mut compressor, chunk)?);
        }
        return Ok(out);
    }

    let worker_count = threads.min(chunks.len());
    let chunk_size = chunks.len().div_ceil(worker_count);
    let results: Vec<Result<Vec<EncodedChunk>>> = std::thread::scope(|scope| {
        let handles: Vec<_> = chunks
            .chunks(chunk_size)
            .map(|slice| {
                scope.spawn(move || {
                    let mut compressor = Compressor::new(level)?;
                    let mut local = Vec::with_capacity(slice.len());
                    for item in slice {
                        local.push(encode_one(&mut compressor, item)?);
                    }
                    Ok(local)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| {
                h.join()
                    .map_err(|_| Error::Zstd("compression worker thread panicked".into()))?
            })
            .collect()
    });

    let mut out = Vec::with_capacity(chunks.len());
    for group in results {
        out.extend(group?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block_of(raw: &[u8]) -> Vec<u8> {
        let mut block = new_block(raw.len());
        block.extend_from_slice(raw);
        block
    }

    #[test]
    fn compressible_chunk_uses_zstd_and_roundtrips() {
        let raw = vec![0x42u8; 16 * 1024];
        let mut c = Compressor::new(3).unwrap();
        let (codec, block) = c.compress(block_of(&raw), false).unwrap();
        assert_eq!(codec, ChunkCodec::Zstd);
        let payload = &block[CHUNK_HEADER_SIZE..];
        assert!(payload.len() < raw.len() / 10);
        let mut d = Decompressor::new().unwrap();
        assert_eq!(d.decompress(codec, payload, raw.len()).unwrap(), raw);
    }

    #[test]
    fn incompressible_or_forced_raw_falls_back_to_raw() {
        let mut x = 0x1234_5678_9abc_def0u64;
        let raw: Vec<u8> = (0..4096)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect();
        let mut c = Compressor::new(3).unwrap();
        let (codec, block) = c.compress(block_of(&raw), false).unwrap();
        assert_eq!(codec, ChunkCodec::Raw);
        assert_eq!(block[CHUNK_HEADER_SIZE..], raw);

        let zeros = vec![0u8; 4096];
        let (codec2, block2) = c.compress(block_of(&zeros), true).unwrap();
        assert_eq!(codec2, ChunkCodec::Raw);
        assert_eq!(block2[CHUNK_HEADER_SIZE..], zeros);
    }

    #[test]
    fn bitflip_in_compressed_payload_fails_checksum_with_corrupt() {
        let raw: Vec<u8> = (0..16384).map(|i| (i % 97) as u8).collect();
        let mut c = Compressor::new(3).unwrap();
        let (codec, mut block) = c.compress(block_of(&raw), false).unwrap();
        assert_eq!(codec, ChunkCodec::Zstd);
        let payload = &mut block[CHUNK_HEADER_SIZE..];
        let mid = payload.len() / 2;
        payload[mid] ^= 0x01;
        let mut d = Decompressor::new().unwrap();
        assert!(matches!(
            d.decompress(codec, payload, raw.len()),
            Err(Error::Corrupt(_))
        ));
    }

    #[test]
    fn parallel_compression_preserves_order_and_contents() {
        let pages: Vec<[Vec<u8>; 2]> = (0..8u8)
            .map(|i| [vec![i; 8192], vec![i + 10; 8192]])
            .collect();
        let inputs: Vec<ChunkInput> = (0u64..)
            .zip(&pages)
            .map(|(i, [a, b])| ChunkInput {
                pages: vec![(i + 1, a.as_slice()), (i + 2, b.as_slice())],
                force_raw: i == 0,
            })
            .collect();
        let encoded = compress_chunks_parallel(inputs, 3, 4).unwrap();
        assert_eq!(encoded.len(), 8);
        assert_eq!(encoded[0].header.codec, ChunkCodec::Raw);
        let mut d = Decompressor::new().unwrap();
        for (i, ch) in encoded.iter().enumerate() {
            assert_eq!(ch.pagenos, vec![i as u64 + 1, i as u64 + 2]);
            assert_eq!(ch.block[..CHUNK_HEADER_SIZE], ch.header.encode());
            assert_eq!(ch.block.len() as u64, align16(ch.read_len() as u64));
            assert!(ch.block[ch.read_len()..].iter().all(|&b| b == 0));
            let decoded = d
                .decompress(ch.header.codec, ch.payload(), ch.raw_len)
                .unwrap();
            assert_eq!(&decoded[..8192], &vec![i as u8; 8192][..]);
            assert_eq!(&decoded[8192..], &vec![(i + 10) as u8; 8192][..]);
        }
    }
}
