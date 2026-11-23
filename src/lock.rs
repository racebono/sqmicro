#![allow(dead_code)]

use crate::error::Result;
use std::fs::{File, OpenOptions};
use std::path::Path;

pub struct CompactLock {
    file: File,
}

impl CompactLock {
    pub fn open(path: &Path) -> Result<Self> {
        let mut opts = OpenOptions::new();
        opts.read(true);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            use windows_sys::Win32::Storage::FileSystem::{FILE_SHARE_READ, FILE_SHARE_WRITE};
            opts.share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE);
        }
        let file = opts.open(path)?;
        Ok(Self { file })
    }

    pub fn lock_shared(&self) -> Result<()> {
        #[cfg(windows)]
        {
            use crate::format::COMPACT_LOCK_OFFSET;
            use std::os::windows::io::AsRawHandle;
            use windows_sys::Win32::Storage::FileSystem::LockFileEx;
            use windows_sys::Win32::System::IO::OVERLAPPED;

            let mut ov: OVERLAPPED = unsafe { std::mem::zeroed() };
            ov.Anonymous.Anonymous.Offset = COMPACT_LOCK_OFFSET as u32;
            ov.Anonymous.Anonymous.OffsetHigh = (COMPACT_LOCK_OFFSET >> 32) as u32;
            let ok = unsafe { LockFileEx(self.file.as_raw_handle(), 0, 0, 1, 0, &mut ov) };
            if ok == 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            Ok(())
        }
        #[cfg(not(windows))]
        {
            self.file.lock_shared()?;
            Ok(())
        }
    }

    pub fn unlock(&self) -> Result<()> {
        #[cfg(windows)]
        {
            use crate::format::COMPACT_LOCK_OFFSET;
            use std::os::windows::io::AsRawHandle;
            use windows_sys::Win32::Storage::FileSystem::UnlockFileEx;
            use windows_sys::Win32::System::IO::OVERLAPPED;

            let mut ov: OVERLAPPED = unsafe { std::mem::zeroed() };
            ov.Anonymous.Anonymous.Offset = COMPACT_LOCK_OFFSET as u32;
            ov.Anonymous.Anonymous.OffsetHigh = (COMPACT_LOCK_OFFSET >> 32) as u32;
            let ok = unsafe { UnlockFileEx(self.file.as_raw_handle(), 0, 1, 0, &mut ov) };
            if ok == 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            Ok(())
        }
        #[cfg(not(windows))]
        {
            self.file.unlock()?;
            Ok(())
        }
    }

    pub fn try_lock_exclusive(&self) -> Result<bool> {
        #[cfg(windows)]
        {
            use crate::format::COMPACT_LOCK_OFFSET;
            use std::os::windows::io::AsRawHandle;
            use windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION;
            use windows_sys::Win32::Storage::FileSystem::{
                LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY, LockFileEx,
            };
            use windows_sys::Win32::System::IO::OVERLAPPED;

            let mut ov: OVERLAPPED = unsafe { std::mem::zeroed() };
            ov.Anonymous.Anonymous.Offset = COMPACT_LOCK_OFFSET as u32;
            ov.Anonymous.Anonymous.OffsetHigh = (COMPACT_LOCK_OFFSET >> 32) as u32;
            let ok = unsafe {
                LockFileEx(
                    self.file.as_raw_handle(),
                    LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
                    0,
                    1,
                    0,
                    &mut ov,
                )
            };
            if ok != 0 {
                return Ok(true);
            }
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(ERROR_LOCK_VIOLATION as i32) {
                return Ok(false);
            }
            Err(err.into())
        }
        #[cfg(not(windows))]
        {
            match self.file.try_lock() {
                Ok(()) => Ok(true),
                Err(std::fs::TryLockError::WouldBlock) => Ok(false),
                Err(std::fs::TryLockError::Error(e)) => Err(e.into()),
            }
        }
    }

    pub fn lock_shared_guard(&self) -> Result<SharedLockGuard<'_>> {
        self.lock_shared()?;
        Ok(SharedLockGuard(self))
    }
}

pub struct SharedLockGuard<'a>(&'a CompactLock);

impl Drop for SharedLockGuard<'_> {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_locks_allow_multiple_readers_and_block_nonblocking_exclusive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("locktest.db");
        std::fs::write(&path, b"").unwrap();

        let r1 = CompactLock::open(&path).unwrap();
        let r2 = CompactLock::open(&path).unwrap();
        let w = CompactLock::open(&path).unwrap();

        r1.lock_shared().unwrap();
        r2.lock_shared().unwrap();
        assert!(!w.try_lock_exclusive().unwrap());

        r1.unlock().unwrap();
        assert!(!w.try_lock_exclusive().unwrap());

        r2.unlock().unwrap();
        assert!(w.try_lock_exclusive().unwrap());
        w.unlock().unwrap();
    }

    #[test]
    fn dropping_compact_lock_allows_file_deletion() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("deleteme.db");
        std::fs::write(&path, b"x").unwrap();
        {
            let lock = CompactLock::open(&path).unwrap();
            lock.lock_shared().unwrap();
            lock.unlock().unwrap();
        }
        std::fs::remove_file(&path).unwrap();
    }
}
