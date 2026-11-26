use crate::Config;
use crate::error::{Error, Result};
use crate::lock::CompactLock;
use crate::store::{BlockIo, PageStore, ReadOutcome};
use libsqlite3_sys as ffi;
use std::collections::HashSet;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Component, Path, Prefix};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Mutex, OnceLock};

pub const VFS_NAME: &str = "sqmicro";

/// Registers the default `"sqmicro"` VFS with SQLite. Safe and idempotent to call multiple times.
pub fn register() -> Result<()> {
    static REGISTERED: OnceLock<std::result::Result<(), (i32, String)>> = OnceLock::new();
    let res = REGISTERED.get_or_init(|| {
        register_with(VFS_NAME, Config::default()).map_err(|e| match e {
            Error::Sqlite { code, message } => (code, message),
            other => (ffi::SQLITE_ERROR, other.to_string()),
        })
    });
    match res {
        Ok(()) => Ok(()),
        Err((code, msg)) => Err(Error::Sqlite {
            code: *code,
            message: msg.clone(),
        }),
    }
}

/// Registers a `sqmicro` VFS under a custom name and default configuration.
///
/// If a VFS with `name` is already registered by `sqmicro`, returns `Ok(())`.
pub fn register_with(name: &str, defaults: Config) -> Result<()> {
    if name.is_empty() || name.contains('\0') {
        return Err(Error::InvalidConfig(format!("invalid VFS name {name:?}")));
    }
    static NAMES: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    let names = NAMES.get_or_init(|| Mutex::new(HashSet::new()));
    let mut guard = names.lock().unwrap_or_else(|e| e.into_inner());
    if guard.contains(name) {
        return Ok(());
    }

    let c_name = CString::new(name)
        .map_err(|_| Error::InvalidConfig(format!("VFS name contains NUL: {name:?}")))?;

    let wrapped = unsafe { ffi::sqlite3_vfs_find(std::ptr::null()) };
    if wrapped.is_null() {
        return Err(Error::Sqlite {
            code: ffi::SQLITE_ERROR,
            message: "no default SQLite VFS found".into(),
        });
    }

    let wrapped_ref = unsafe { &*wrapped };
    let sz_os_file = std::cmp::max(
        std::mem::size_of::<SqmicroFileSlot>() as c_int,
        wrapped_ref.szOsFile,
    );

    let mut vfs = Box::new(SqmicroVfs {
        base: ffi::sqlite3_vfs {
            iVersion: wrapped_ref.iVersion.min(2),
            szOsFile: sz_os_file,
            mxPathname: wrapped_ref.mxPathname,
            pNext: std::ptr::null_mut(),
            zName: c_name.as_ptr(),
            pAppData: std::ptr::null_mut(),
            xOpen: Some(vfs_open),
            xDelete: Some(vfs_delete),
            xAccess: Some(vfs_access),
            xFullPathname: Some(vfs_full_pathname),
            xDlOpen: wrapped_ref.xDlOpen,
            xDlError: wrapped_ref.xDlError,
            xDlSym: wrapped_ref.xDlSym,
            xDlClose: wrapped_ref.xDlClose,
            xRandomness: Some(vfs_randomness),
            xSleep: Some(vfs_sleep),
            xCurrentTime: Some(vfs_current_time),
            xGetLastError: Some(vfs_get_last_error),
            xCurrentTimeInt64: wrapped_ref.xCurrentTimeInt64.map(|_| vfs_current_time_int64 as _),
            xSetSystemCall: None,
            xGetSystemCall: None,
            xNextSystemCall: None,
        },
        wrapped,
        defaults,
        _name: c_name,
    });

    // Ensure zName points into the heap-pinned CString inside Box<SqmicroVfs>.
    vfs.base.zName = vfs._name.as_ptr();
    let vfs_ptr = Box::into_raw(vfs);

    let rc = unsafe { ffi::sqlite3_vfs_register(&mut (*vfs_ptr).base, 0) };
    if rc != ffi::SQLITE_OK {
        unsafe { drop(Box::from_raw(vfs_ptr)) };
        return Err(Error::Sqlite {
            code: rc,
            message: format!("sqlite3_vfs_register failed for {name:?}"),
        });
    }

    guard.insert(name.to_owned());
    Ok(())
}

/// Formats `path` as a percent-encoded SQLite `file:` URI suitable for appending `?vfs=sqmicro`.
pub fn uri(path: &Path) -> Result<String> {
    let invalid = |why: &str| Error::InvalidPath(format!("{why}: {}", path.display()));
    let abs = std::path::absolute(path)?;
    let mut out = String::from("file://");
    for component in abs.components() {
        match component {
            Component::Prefix(prefix) => match prefix.kind() {
                Prefix::Disk(drive) | Prefix::VerbatimDisk(drive) => {
                    out.push('/');
                    out.push(char::from(drive));
                    out.push(':');
                }
                Prefix::UNC(server, share) | Prefix::VerbatimUNC(server, share) => {
                    out.push_str("//");
                    escape_uri_component(
                        &mut out,
                        server.to_str().ok_or_else(|| invalid("not UTF-8"))?,
                    );
                    out.push('/');
                    escape_uri_component(
                        &mut out,
                        share.to_str().ok_or_else(|| invalid("not UTF-8"))?,
                    );
                }
                _ => return Err(invalid("unsupported path prefix")),
            },
            Component::RootDir => out.push('/'),
            Component::Normal(name) => {
                if !out.ends_with('/') {
                    out.push('/');
                }
                escape_uri_component(&mut out, name.to_str().ok_or_else(|| invalid("not UTF-8"))?);
            }
            Component::ParentDir => {
                if !out.ends_with('/') {
                    out.push('/');
                }
                out.push_str("..");
            }
            Component::CurDir => {}
        }
    }
    Ok(out)
}

fn escape_uri_component(out: &mut String, part: &str) {
    for byte in part.bytes() {
        match byte {
            b'%' | b'?' | b'#' | b'/' | b'\\' | b'&' | b'=' | 0..=b' ' | 0x7f.. => {
                use std::fmt::Write;
                let _ = write!(out, "%{byte:02X}");
            }
            _ => out.push(char::from(byte)),
        }
    }
}

#[repr(C)]
struct SqmicroVfs {
    base: ffi::sqlite3_vfs,
    wrapped: *mut ffi::sqlite3_vfs,
    defaults: Config,
    _name: CString,
}

#[repr(C)]
struct SqmicroFileSlot {
    base: ffi::sqlite3_file,
    state: *mut SqmicroFileState,
}

struct OsFileBlockIo {
    // 8-byte aligned heap storage for the wrapped OS sqlite3_file.
    raw: Box<[u64]>,
    closed: bool,
    sync_flags: c_int,
}

impl OsFileBlockIo {
    fn new(sz_os_file: usize) -> Self {
        let words = sz_os_file.div_ceil(std::mem::size_of::<u64>()).max(1);
        Self {
            raw: vec![0u64; words].into_boxed_slice(),
            closed: true,
            sync_flags: ffi::SQLITE_SYNC_NORMAL,
        }
    }

    #[inline]
    fn as_file_ptr(&mut self) -> *mut ffi::sqlite3_file {
        self.raw.as_mut_ptr().cast::<ffi::sqlite3_file>()
    }

    fn close_os(&mut self) -> c_int {
        if self.closed {
            return ffi::SQLITE_OK;
        }
        self.closed = true;
        unsafe {
            let p = self.as_file_ptr();
            let methods = (*p).pMethods;
            if !methods.is_null()
                && let Some(x_close) = (*methods).xClose
            {
                return x_close(p);
            }
        }
        ffi::SQLITE_OK
    }
}

impl Drop for OsFileBlockIo {
    fn drop(&mut self) {
        let _ = self.close_os();
    }
}

impl BlockIo for OsFileBlockIo {
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let amt = c_int::try_from(buf.len())
            .map_err(|_| Error::Corrupt("read length exceeds c_int".into()))?;
        let rc = unsafe {
            let p = self.as_file_ptr();
            let x_read = (*(*p).pMethods)
                .xRead
                .ok_or_else(|| Error::Sqlite {
                    code: ffi::SQLITE_IOERR_READ,
                    message: "wrapped VFS missing xRead".into(),
                })?;
            x_read(p, buf.as_mut_ptr().cast::<c_void>(), amt, offset as i64)
        };
        if rc == ffi::SQLITE_OK {
            Ok(buf.len())
        } else if rc == ffi::SQLITE_IOERR_SHORT_READ {
            let fsize = self.file_size()?;
            let avail = fsize.saturating_sub(offset).min(buf.len() as u64) as usize;
            Ok(avail)
        } else {
            Err(Error::Sqlite {
                code: rc,
                message: format!("underlying xRead failed at offset {offset}"),
            })
        }
    }

    fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let amt = c_int::try_from(buf.len())
            .map_err(|_| Error::Corrupt("write length exceeds c_int".into()))?;
        let rc = unsafe {
            let p = self.as_file_ptr();
            let x_write = (*(*p).pMethods)
                .xWrite
                .ok_or_else(|| Error::Sqlite {
                    code: ffi::SQLITE_IOERR_WRITE,
                    message: "wrapped VFS missing xWrite".into(),
                })?;
            x_write(p, buf.as_ptr().cast::<c_void>(), amt, offset as i64)
        };
        if rc == ffi::SQLITE_OK {
            Ok(())
        } else {
            Err(Error::Sqlite {
                code: rc,
                message: format!("underlying xWrite failed at offset {offset}"),
            })
        }
    }

    fn sync(&mut self) -> Result<()> {
        let rc = unsafe {
            let p = self.as_file_ptr();
            let x_sync = (*(*p).pMethods)
                .xSync
                .ok_or_else(|| Error::Sqlite {
                    code: ffi::SQLITE_IOERR_FSYNC,
                    message: "wrapped VFS missing xSync".into(),
                })?;
            x_sync(p, self.sync_flags)
        };
        if rc == ffi::SQLITE_OK {
            Ok(())
        } else {
            Err(Error::Sqlite {
                code: rc,
                message: "underlying xSync failed".into(),
            })
        }
    }

    fn file_size(&mut self) -> Result<u64> {
        let mut sz: ffi::sqlite3_int64 = 0;
        let rc = unsafe {
            let p = self.as_file_ptr();
            let x_fs = (*(*p).pMethods)
                .xFileSize
                .ok_or_else(|| Error::Sqlite {
                    code: ffi::SQLITE_IOERR_FSTAT,
                    message: "wrapped VFS missing xFileSize".into(),
                })?;
            x_fs(p, &mut sz)
        };
        if rc == ffi::SQLITE_OK {
            Ok(sz.max(0) as u64)
        } else {
            Err(Error::Sqlite {
                code: rc,
                message: "underlying xFileSize failed".into(),
            })
        }
    }

    fn truncate(&mut self, size: u64) -> Result<()> {
        let rc = unsafe {
            let p = self.as_file_ptr();
            let x_trunc = (*(*p).pMethods)
                .xTruncate
                .ok_or_else(|| Error::Sqlite {
                    code: ffi::SQLITE_IOERR_TRUNCATE,
                    message: "wrapped VFS missing xTruncate".into(),
                })?;
            x_trunc(p, size as i64)
        };
        if rc == ffi::SQLITE_OK {
            Ok(())
        } else {
            Err(Error::Sqlite {
                code: rc,
                message: format!("underlying xTruncate({size}) failed"),
            })
        }
    }
}

struct SqmicroFileState {
    store: PageStore<OsFileBlockIo>,
    compact_lock: Option<CompactLock>,
    shared_lock_count: u32,
    /// First region of the WAL index, while mapped.
    wal_index: *mut c_void,
    /// A checkpoint whose flush failed, until SQLite has been told.
    failed_checkpoint: Option<FailedCheckpoint>,
    /// A checkpoint has copied its pages, and its truncate may come next.
    checkpoint_copied: bool,
}

struct FailedCheckpoint {
    /// Result of the flush.
    rc: c_int,
    /// `nBackfill` before the checkpoint.
    backfill: Option<u32>,
}

/// Offset of `nBackfill` in the WAL index (https://sqlite.org/walformat.html).
const WAL_INDEX_BACKFILL_OFFSET: usize = 96;
/// Slots in the WAL index lock array.
const WAL_CKPT_LOCK: c_int = 1;
const WAL_READ_LOCK_0: c_int = 3;

impl SqmicroFileState {
    #[inline]
    fn os_file_ptr(&mut self) -> *mut ffi::sqlite3_file {
        self.store.io_mut().as_file_ptr()
    }

    fn wal_backfill(&self) -> Option<&AtomicU32> {
        if self.wal_index.is_null() {
            return None;
        }
        // SAFETY: SQLite keeps the region mapped until xShmUnmap, which clears `wal_index`. It is
        // at least 32 KiB, aligned, and SQLite also accesses this field atomically.
        unsafe {
            let field = self.wal_index.cast::<u8>().add(WAL_INDEX_BACKFILL_OFFSET);
            Some(AtomicU32::from_ptr(field.cast()))
        }
    }
}

/// Forgets a checkpoint that failed. The WAL still has its pages.
fn end_failed_checkpoint(state: &mut SqmicroFileState) {
    state.failed_checkpoint = None;
    if state.store.has_pending_writes() {
        state.store.discard_staged();
    }
}

fn c_path_to_pathbuf(z_name: *const c_char) -> Result<std::path::PathBuf> {
    let bytes = unsafe { CStr::from_ptr(z_name).to_bytes() };
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        Ok(std::path::PathBuf::from(std::ffi::OsStr::from_bytes(bytes)))
    }
    #[cfg(not(unix))]
    {
        let s = std::str::from_utf8(bytes)
            .map_err(|_| Error::InvalidPath("database path is not valid UTF-8".into()))?;
        Ok(std::path::PathBuf::from(s))
    }
}

fn err_to_sqlite(err: Error, default_ioerr: c_int) -> c_int {
    match err {
        Error::Sqlite { code, .. } => code,
        Error::Corrupt(_) => ffi::SQLITE_CORRUPT,
        Error::NotASqmicroDb | Error::UnsupportedVersion(_) => ffi::SQLITE_NOTADB,
        Error::InvalidConfig(_) | Error::InvalidPath(_) => ffi::SQLITE_CANTOPEN,
        Error::Io(_) | Error::Zstd(_) => default_ioerr,
    }
}

fn parse_uri_config(z_name: *const c_char, flags: c_int, defaults: Config) -> Config {
    if (flags & ffi::SQLITE_OPEN_URI) == 0 {
        return defaults;
    }
    let mut cfg = defaults;
    unsafe {
        let level = ffi::sqlite3_uri_int64(z_name, c"level".as_ptr(), cfg.level as i64);
        if let Ok(v) = i32::try_from(level) {
            cfg.level = v;
        }
        let chunk_kib =
            ffi::sqlite3_uri_int64(z_name, c"chunk_kib".as_ptr(), cfg.chunk_kib as i64);
        if let Ok(v) = usize::try_from(chunk_kib)
            && v > 0
        {
            cfg.chunk_kib = v;
        }
        let cache_mib =
            ffi::sqlite3_uri_int64(z_name, c"cache_mib".as_ptr(), cfg.cache_mib as i64);
        if let Ok(v) = usize::try_from(cache_mib) {
            cfg.cache_mib = v;
        }
        let threads = ffi::sqlite3_uri_int64(z_name, c"threads".as_ptr(), cfg.threads as i64);
        if let Ok(v) = usize::try_from(threads)
            && v >= 1
        {
            cfg.threads = v;
        }
        let compact_threshold = ffi::sqlite3_uri_int64(
            z_name,
            c"compact_threshold".as_ptr(),
            cfg.compact_threshold as i64,
        );
        if (0..=100).contains(&compact_threshold) {
            cfg.compact_threshold = compact_threshold as u8;
        }
    }
    cfg
}

unsafe fn state_mut<'a>(p_file: *mut ffi::sqlite3_file) -> &'a mut SqmicroFileState {
    let slot = p_file.cast::<SqmicroFileSlot>();
    unsafe { &mut *(*slot).state }
}

static SQMICRO_IO_METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 2,
    xClose: Some(io_close),
    xRead: Some(io_read),
    xWrite: Some(io_write),
    xTruncate: Some(io_truncate),
    xSync: Some(io_sync),
    xFileSize: Some(io_file_size),
    xLock: Some(io_lock),
    xUnlock: Some(io_unlock),
    xCheckReservedLock: Some(io_check_reserved_lock),
    xFileControl: Some(io_file_control),
    xSectorSize: Some(io_sector_size),
    xDeviceCharacteristics: Some(io_device_characteristics),
    xShmMap: Some(io_shm_map),
    xShmLock: Some(io_shm_lock),
    xShmBarrier: Some(io_shm_barrier),
    xShmUnmap: Some(io_shm_unmap),
    xFetch: None,
    xUnfetch: None,
};

unsafe extern "C" fn vfs_open(
    p_vfs: *mut ffi::sqlite3_vfs,
    z_name: *const c_char,
    p_file: *mut ffi::sqlite3_file,
    flags: c_int,
    p_out_flags: *mut c_int,
) -> c_int {
    let res = catch_unwind(AssertUnwindSafe(|| unsafe {
        let vfs = &mut *p_vfs.cast::<SqmicroVfs>();
        let wrapped = vfs.wrapped;
        let is_main_db = (flags & ffi::SQLITE_OPEN_MAIN_DB) != 0
            && !z_name.is_null()
            && *z_name != 0;

        if !is_main_db {
            return ((*wrapped).xOpen.unwrap())(wrapped, z_name, p_file, flags, p_out_flags);
        }

        let cfg = parse_uri_config(z_name, flags, vfs.defaults);
        let read_only = (flags & ffi::SQLITE_OPEN_READONLY) != 0;

        let mut os_io = OsFileBlockIo::new((*wrapped).szOsFile as usize);
        let rc = ((*wrapped).xOpen.unwrap())(
            wrapped,
            z_name,
            os_io.as_file_ptr(),
            flags,
            p_out_flags,
        );
        if rc != ffi::SQLITE_OK {
            (*p_file).pMethods = std::ptr::null();
            return rc;
        }
        os_io.closed = false;

        // The lock needs a second handle to the file. On Unix, closing that would drop the locks
        // SQLite holds on the file through every connection in this process, so there is no lock
        // there, and without it no live compaction.
        let mut compact_lock = None;
        if cfg!(windows) {
            match c_path_to_pathbuf(z_name).and_then(|path| CompactLock::open(&path)) {
                Ok(lock) => compact_lock = Some(lock),
                Err(e) => {
                    (*p_file).pMethods = std::ptr::null();
                    return err_to_sqlite(e, ffi::SQLITE_CANTOPEN);
                }
            }
        }

        // Hold shared CompactLock while reading initial superblock + L1 root so a concurrent
        // live_compact cannot overwrite or truncate offsets beneath PageStore::open.
        let open_res = {
            let _guard = match compact_lock.as_ref().map(|l| l.lock_shared_guard()).transpose() {
                Ok(g) => g,
                Err(e) => {
                    (*p_file).pMethods = std::ptr::null();
                    return err_to_sqlite(e, ffi::SQLITE_CANTOPEN);
                }
            };
            PageStore::open(os_io, cfg, read_only)
        };
        let store = match open_res {
            Ok(store) => store,
            Err(e) => {
                drop(compact_lock);
                (*p_file).pMethods = std::ptr::null();
                return err_to_sqlite(e, ffi::SQLITE_CANTOPEN);
            }
        };

        let state = Box::new(SqmicroFileState {
            store,
            compact_lock,
            shared_lock_count: 0,
            wal_index: std::ptr::null_mut(),
            failed_checkpoint: None,
            checkpoint_copied: false,
        });

        let slot = p_file.cast::<SqmicroFileSlot>();
        (*slot).base.pMethods = &SQMICRO_IO_METHODS;
        (*slot).state = Box::into_raw(state);
        ffi::SQLITE_OK
    }));
    res.unwrap_or(ffi::SQLITE_INTERNAL)
}

unsafe extern "C" fn vfs_delete(
    p_vfs: *mut ffi::sqlite3_vfs,
    z_name: *const c_char,
    sync_dir: c_int,
) -> c_int {
    let res = catch_unwind(AssertUnwindSafe(|| unsafe {
        let wrapped = (*p_vfs.cast::<SqmicroVfs>()).wrapped;
        ((*wrapped).xDelete.unwrap())(wrapped, z_name, sync_dir)
    }));
    res.unwrap_or(ffi::SQLITE_INTERNAL)
}

unsafe extern "C" fn vfs_access(
    p_vfs: *mut ffi::sqlite3_vfs,
    z_name: *const c_char,
    flags: c_int,
    p_res_out: *mut c_int,
) -> c_int {
    let res = catch_unwind(AssertUnwindSafe(|| unsafe {
        let wrapped = (*p_vfs.cast::<SqmicroVfs>()).wrapped;
        ((*wrapped).xAccess.unwrap())(wrapped, z_name, flags, p_res_out)
    }));
    res.unwrap_or(ffi::SQLITE_INTERNAL)
}

unsafe extern "C" fn vfs_full_pathname(
    p_vfs: *mut ffi::sqlite3_vfs,
    z_name: *const c_char,
    n_out: c_int,
    z_out: *mut c_char,
) -> c_int {
    let res = catch_unwind(AssertUnwindSafe(|| unsafe {
        let wrapped = (*p_vfs.cast::<SqmicroVfs>()).wrapped;
        ((*wrapped).xFullPathname.unwrap())(wrapped, z_name, n_out, z_out)
    }));
    res.unwrap_or(ffi::SQLITE_INTERNAL)
}

unsafe extern "C" fn vfs_randomness(
    p_vfs: *mut ffi::sqlite3_vfs,
    n_byte: c_int,
    z_out: *mut c_char,
) -> c_int {
    let res = catch_unwind(AssertUnwindSafe(|| unsafe {
        let wrapped = (*p_vfs.cast::<SqmicroVfs>()).wrapped;
        ((*wrapped).xRandomness.unwrap())(wrapped, n_byte, z_out)
    }));
    res.unwrap_or(0)
}

unsafe extern "C" fn vfs_sleep(p_vfs: *mut ffi::sqlite3_vfs, microseconds: c_int) -> c_int {
    let res = catch_unwind(AssertUnwindSafe(|| unsafe {
        let wrapped = (*p_vfs.cast::<SqmicroVfs>()).wrapped;
        ((*wrapped).xSleep.unwrap())(wrapped, microseconds)
    }));
    res.unwrap_or(0)
}

unsafe extern "C" fn vfs_current_time(p_vfs: *mut ffi::sqlite3_vfs, p_out: *mut f64) -> c_int {
    let res = catch_unwind(AssertUnwindSafe(|| unsafe {
        let wrapped = (*p_vfs.cast::<SqmicroVfs>()).wrapped;
        ((*wrapped).xCurrentTime.unwrap())(wrapped, p_out)
    }));
    res.unwrap_or(ffi::SQLITE_INTERNAL)
}

unsafe extern "C" fn vfs_current_time_int64(
    p_vfs: *mut ffi::sqlite3_vfs,
    p_out: *mut ffi::sqlite3_int64,
) -> c_int {
    let res = catch_unwind(AssertUnwindSafe(|| unsafe {
        let wrapped = (*p_vfs.cast::<SqmicroVfs>()).wrapped;
        if let Some(f) = (*wrapped).xCurrentTimeInt64 {
            f(wrapped, p_out)
        } else {
            ffi::SQLITE_ERROR
        }
    }));
    res.unwrap_or(ffi::SQLITE_INTERNAL)
}

unsafe extern "C" fn vfs_get_last_error(
    p_vfs: *mut ffi::sqlite3_vfs,
    n_buf: c_int,
    z_buf: *mut c_char,
) -> c_int {
    let res = catch_unwind(AssertUnwindSafe(|| unsafe {
        let wrapped = (*p_vfs.cast::<SqmicroVfs>()).wrapped;
        if let Some(f) = (*wrapped).xGetLastError {
            f(wrapped, n_buf, z_buf)
        } else {
            0
        }
    }));
    res.unwrap_or(0)
}

unsafe extern "C" fn io_close(p_file: *mut ffi::sqlite3_file) -> c_int {
    let res = catch_unwind(AssertUnwindSafe(|| unsafe {
        let slot = p_file.cast::<SqmicroFileSlot>();
        if (*slot).state.is_null() {
            return ffi::SQLITE_OK;
        }
        let mut state = Box::from_raw((*slot).state);
        (*slot).state = std::ptr::null_mut();

        // Anything still staged failed to flush while a lock protected it, and nothing does now.
        if state.shared_lock_count > 0
            && let Some(lock) = &state.compact_lock
        {
            let _ = lock.unlock();
            state.shared_lock_count = 0;
        }
        drop(state.compact_lock.take());

        state.store.into_io().close_os()
    }));
    res.unwrap_or(ffi::SQLITE_INTERNAL)
}

unsafe extern "C" fn io_read(
    p_file: *mut ffi::sqlite3_file,
    z_buf: *mut c_void,
    i_amt: c_int,
    i_ofst: ffi::sqlite3_int64,
) -> c_int {
    let res = catch_unwind(AssertUnwindSafe(|| unsafe {
        if i_amt < 0 || i_ofst < 0 {
            return ffi::SQLITE_IOERR_READ;
        }
        if i_amt == 0 {
            return ffi::SQLITE_OK;
        }
        if z_buf.is_null() {
            return ffi::SQLITE_IOERR_READ;
        }
        let buf = std::slice::from_raw_parts_mut(z_buf.cast::<u8>(), i_amt as usize);
        let state = state_mut(p_file);
        // Guard pre-SHM and rollback-mode reads against concurrent live compaction.
        let _guard = if state.shared_lock_count == 0 {
            match state.compact_lock.as_ref().map(|l| l.lock_shared_guard()) {
                Some(Ok(g)) => Some(g),
                Some(Err(e)) => return err_to_sqlite(e, ffi::SQLITE_IOERR_READ),
                None => None,
            }
        } else {
            None
        };
        // A WAL reader must not look at the superblock before SQLite has decided which frames to
        // read from the WAL, and the first read is the earliest point after that. Without a WAL
        // read lock, page 1 is read before SQLite knows whether there is a WAL, so a compaction may
        // have moved it since the last check.
        if (state.store.is_stale() || (state.shared_lock_count == 0 && i_ofst < 512))
            && let Err(e) = state.store.refresh()
        {
            return err_to_sqlite(e, ffi::SQLITE_IOERR_READ);
        }
        match state.store.read(i_ofst as u64, buf) {
            Ok(ReadOutcome::Full) => ffi::SQLITE_OK,
            Ok(ReadOutcome::ShortRead) => ffi::SQLITE_IOERR_SHORT_READ,
            Err(e) => err_to_sqlite(e, ffi::SQLITE_IOERR_READ),
        }
    }));
    res.unwrap_or(ffi::SQLITE_INTERNAL)
}

unsafe extern "C" fn io_write(
    p_file: *mut ffi::sqlite3_file,
    z_buf: *const c_void,
    i_amt: c_int,
    i_ofst: ffi::sqlite3_int64,
) -> c_int {
    let res = catch_unwind(AssertUnwindSafe(|| unsafe {
        if i_amt < 0 || i_ofst < 0 {
            return ffi::SQLITE_IOERR_WRITE;
        }
        if i_amt == 0 {
            return ffi::SQLITE_OK;
        }
        if z_buf.is_null() {
            return ffi::SQLITE_IOERR_WRITE;
        }
        let data = std::slice::from_raw_parts(z_buf.cast::<u8>(), i_amt as usize);
        let state = state_mut(p_file);
        match state.store.write(i_ofst as u64, data) {
            Ok(()) => ffi::SQLITE_OK,
            Err(e) => err_to_sqlite(e, ffi::SQLITE_IOERR_WRITE),
        }
    }));
    res.unwrap_or(ffi::SQLITE_INTERNAL)
}

unsafe extern "C" fn io_truncate(
    p_file: *mut ffi::sqlite3_file,
    size: ffi::sqlite3_int64,
) -> c_int {
    let res = catch_unwind(AssertUnwindSafe(|| unsafe {
        if size < 0 {
            return ffi::SQLITE_IOERR_TRUNCATE;
        }
        let state = state_mut(p_file);
        if let Some(failed) = state.failed_checkpoint.take() {
            return failed.rc;
        }
        // A checkpoint's truncate is published at once: the WAL still has its pages. Any other is
        // part of a rollback-journal commit or rollback, and only published with it, at the next
        // FCNTL_SYNC or FCNTL_COMMIT_PHASETWO.
        let res = if std::mem::take(&mut state.checkpoint_copied) {
            state.store.truncate(size as u64, state.compact_lock.as_ref())
        } else {
            state.store.truncate_later(size as u64)
        };
        match res {
            Ok(()) => ffi::SQLITE_OK,
            Err(e) => err_to_sqlite(e, ffi::SQLITE_IOERR_TRUNCATE),
        }
    }));
    res.unwrap_or(ffi::SQLITE_INTERNAL)
}

unsafe extern "C" fn io_sync(p_file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    let res = catch_unwind(AssertUnwindSafe(|| unsafe {
        let state = state_mut(p_file);
        state.store.io_mut().sync_flags = flags;
        match state.store.flush(true) {
            Ok(()) => ffi::SQLITE_OK,
            Err(e) => err_to_sqlite(e, ffi::SQLITE_IOERR_FSYNC),
        }
    }));
    res.unwrap_or(ffi::SQLITE_INTERNAL)
}

unsafe extern "C" fn io_file_size(
    p_file: *mut ffi::sqlite3_file,
    p_size: *mut ffi::sqlite3_int64,
) -> c_int {
    let res = catch_unwind(AssertUnwindSafe(|| unsafe {
        let state = state_mut(p_file);
        if !state.store.has_pending_writes() {
            let _guard = if state.shared_lock_count == 0 {
                match state.compact_lock.as_ref().map(|l| l.lock_shared_guard()) {
                    Some(Ok(g)) => Some(g),
                    Some(Err(e)) => return err_to_sqlite(e, ffi::SQLITE_IOERR_FSTAT),
                    None => None,
                }
            } else {
                None
            };
            if let Err(e) = state.store.refresh() {
                return err_to_sqlite(e, ffi::SQLITE_IOERR_FSTAT);
            }
        }
        match state.store.logical_size() {
            Ok(sz) => {
                *p_size = sz as ffi::sqlite3_int64;
                ffi::SQLITE_OK
            }
            Err(e) => err_to_sqlite(e, ffi::SQLITE_IOERR_FSTAT),
        }
    }));
    res.unwrap_or(ffi::SQLITE_INTERNAL)
}

unsafe extern "C" fn io_lock(p_file: *mut ffi::sqlite3_file, e_lock: c_int) -> c_int {
    let res = catch_unwind(AssertUnwindSafe(|| unsafe {
        let state = state_mut(p_file);
        let os_p = state.os_file_ptr();
        let rc = ((*(*os_p).pMethods).xLock.unwrap())(os_p, e_lock);
        // A WAL connection in exclusive locking mode takes no more read marks.
        if rc == ffi::SQLITE_OK && e_lock == ffi::SQLITE_LOCK_EXCLUSIVE {
            state.store.mark_stale();
        }
        // Rollback-journal transactions and recoveries take these, and checkpoints don't.
        if rc == ffi::SQLITE_OK && e_lock >= ffi::SQLITE_LOCK_RESERVED {
            state.checkpoint_copied = false;
        }
        if rc == ffi::SQLITE_OK
            && e_lock == ffi::SQLITE_LOCK_SHARED
            && !state.store.has_pending_writes()
        {
            let _guard = if state.shared_lock_count == 0 {
                match state.compact_lock.as_ref().map(|l| l.lock_shared_guard()) {
                    Some(Ok(g)) => Some(g),
                    Some(Err(e)) => return err_to_sqlite(e, ffi::SQLITE_IOERR_READ),
                    None => None,
                }
            } else {
                None
            };
            if let Err(e) = state.store.refresh() {
                return err_to_sqlite(e, ffi::SQLITE_IOERR_READ);
            }
        }
        rc
    }));
    res.unwrap_or(ffi::SQLITE_INTERNAL)
}

unsafe extern "C" fn io_unlock(p_file: *mut ffi::sqlite3_file, e_lock: c_int) -> c_int {
    let res = catch_unwind(AssertUnwindSafe(|| unsafe {
        let state = state_mut(p_file);
        // Commits flush in FCNTL_SYNC, so these are left by a failed transaction or checkpoint.
        if state.store.has_pending_writes() {
            state.store.discard_staged();
        }
        let os_p = state.os_file_ptr();
        ((*(*os_p).pMethods).xUnlock.unwrap())(os_p, e_lock)
    }));
    res.unwrap_or(ffi::SQLITE_INTERNAL)
}

unsafe extern "C" fn io_check_reserved_lock(
    p_file: *mut ffi::sqlite3_file,
    p_res_out: *mut c_int,
) -> c_int {
    let res = catch_unwind(AssertUnwindSafe(|| unsafe {
        let state = state_mut(p_file);
        let os_p = state.os_file_ptr();
        ((*(*os_p).pMethods).xCheckReservedLock.unwrap())(os_p, p_res_out)
    }));
    res.unwrap_or(ffi::SQLITE_INTERNAL)
}

unsafe extern "C" fn io_file_control(
    p_file: *mut ffi::sqlite3_file,
    op: c_int,
    p_arg: *mut c_void,
) -> c_int {
    let res = catch_unwind(AssertUnwindSafe(|| unsafe {
        let state = state_mut(p_file);
        // Commits always get here before the journal is finalized, unlike xSync, which
        // `synchronous=OFF` skips.
        // COMMIT_PHASETWO follows the truncate at the end of a rollback-journal commit.
        if op == ffi::SQLITE_FCNTL_SYNC || op == ffi::SQLITE_FCNTL_COMMIT_PHASETWO {
            if let Err(e) = state.store.flush(false) {
                return err_to_sqlite(e, ffi::SQLITE_IOERR_WRITE);
            }
            state.store.compact_if_truncated(state.compact_lock.as_ref());
        }
        match op {
            ffi::SQLITE_FCNTL_SIZE_HINT | ffi::SQLITE_FCNTL_CHUNK_SIZE => ffi::SQLITE_OK,
            ffi::SQLITE_FCNTL_MMAP_SIZE => {
                if !p_arg.is_null() {
                    *p_arg.cast::<ffi::sqlite3_int64>() = 0;
                }
                ffi::SQLITE_OK
            }
            ffi::SQLITE_FCNTL_CKPT_START => {
                // The last checkpoint never released its lock if the locking mode is exclusive.
                end_failed_checkpoint(state);
                ffi::SQLITE_OK
            }
            ffi::SQLITE_FCNTL_CKPT_DONE => {
                state.checkpoint_copied = true;
                if let Err(e) = state.store.flush(false) {
                    // SQLite ignores this result. If the checkpoint is complete, it truncates next,
                    // which then reports the failure instead of retrying: after a failed sync, the
                    // data may never reach the disk unless it is written again. If the checkpoint is
                    // partial, SQLite counts the frames as backfilled, which has to be undone (see
                    // io_shm_lock).
                    let rc = err_to_sqlite(e, ffi::SQLITE_IOERR_WRITE);
                    state.failed_checkpoint = Some(FailedCheckpoint {
                        rc,
                        backfill: state.wal_backfill().map(|b| b.load(Ordering::Acquire)),
                    });
                    return rc;
                }
                ffi::SQLITE_OK
            }
            ffi::SQLITE_FCNTL_LOCKSTATE
            | ffi::SQLITE_FCNTL_LAST_ERRNO
            | ffi::SQLITE_FCNTL_HAS_MOVED
            | ffi::SQLITE_FCNTL_PERSIST_WAL
            | ffi::SQLITE_FCNTL_POWERSAFE_OVERWRITE
            | ffi::SQLITE_FCNTL_WIN32_AV_RETRY
            | ffi::SQLITE_FCNTL_BUSYHANDLER
            | ffi::SQLITE_FCNTL_SYNC
            | ffi::SQLITE_FCNTL_COMMIT_PHASETWO => {
                let os_p = state.os_file_ptr();
                if let Some(fc) = (*(*os_p).pMethods).xFileControl {
                    fc(os_p, op, p_arg)
                } else {
                    ffi::SQLITE_NOTFOUND
                }
            }
            _ => ffi::SQLITE_NOTFOUND,
        }
    }));
    res.unwrap_or(ffi::SQLITE_INTERNAL)
}

unsafe extern "C" fn io_sector_size(_p_file: *mut ffi::sqlite3_file) -> c_int {
    4096
}

unsafe extern "C" fn io_device_characteristics(p_file: *mut ffi::sqlite3_file) -> c_int {
    let res = catch_unwind(AssertUnwindSafe(|| unsafe {
        let os_p = state_mut(p_file).os_file_ptr();
        // The only bit that still holds for a compressed file. `psow=0` in the URI turns it off.
        (*(*os_p).pMethods)
            .xDeviceCharacteristics
            .map_or(0, |f| f(os_p) & ffi::SQLITE_IOCAP_POWERSAFE_OVERWRITE)
    }));
    res.unwrap_or(0)
}

unsafe extern "C" fn io_shm_map(
    p_file: *mut ffi::sqlite3_file,
    i_pg: c_int,
    pgsz: c_int,
    b_extend: c_int,
    pp: *mut *mut c_void,
) -> c_int {
    let res = catch_unwind(AssertUnwindSafe(|| unsafe {
        let state = state_mut(p_file);
        let os_p = state.os_file_ptr();
        if let Some(f) = (*(*os_p).pMethods).xShmMap {
            let rc = f(os_p, i_pg, pgsz, b_extend, pp);
            if rc == ffi::SQLITE_OK && i_pg == 0 && !pp.is_null() {
                state.wal_index = *pp;
            }
            rc
        } else {
            ffi::SQLITE_IOERR_SHMMAP
        }
    }));
    res.unwrap_or(ffi::SQLITE_INTERNAL)
}

unsafe extern "C" fn io_shm_lock(
    p_file: *mut ffi::sqlite3_file,
    offset: c_int,
    n: c_int,
    flags: c_int,
) -> c_int {
    let res = catch_unwind(AssertUnwindSafe(|| unsafe {
        let state = state_mut(p_file);
        let is_read_mark = offset >= 3 && (flags & ffi::SQLITE_SHM_SHARED) != 0;
        let is_lock = (flags & ffi::SQLITE_SHM_LOCK) != 0;
        let is_unlock = (flags & ffi::SQLITE_SHM_UNLOCK) != 0;

        if is_read_mark && is_lock {
            let need_acquire = state.shared_lock_count == 0;
            if need_acquire
                && let Some(lock) = &state.compact_lock
                && let Err(e) = lock.lock_shared()
            {
                return err_to_sqlite(e, ffi::SQLITE_IOERR_SHMLOCK);
            }
            let os_p = state.os_file_ptr();
            let rc = if let Some(f) = (*(*os_p).pMethods).xShmLock {
                f(os_p, offset, n, flags)
            } else {
                ffi::SQLITE_IOERR_SHMLOCK
            };
            if rc != ffi::SQLITE_OK {
                if need_acquire
                    && let Some(lock) = &state.compact_lock
                {
                    let _ = lock.unlock();
                }
                return rc;
            }
            state.shared_lock_count = state.shared_lock_count.saturating_add(1);
            state.store.mark_stale();
            return ffi::SQLITE_OK;
        }

        if is_unlock && (flags & ffi::SQLITE_SHM_EXCLUSIVE) != 0 {
            let releases = |slot: c_int| (offset..offset + n).contains(&slot);
            // SQLite has just counted frames as backfilled that a failed flush left unwritten.
            if releases(WAL_READ_LOCK_0)
                && let Some(failed) = &state.failed_checkpoint
                && let Some(backfill) = failed.backfill
                && let Some(field) = state.wal_backfill()
            {
                field.store(backfill, Ordering::Release);
            }
            if releases(WAL_CKPT_LOCK) {
                end_failed_checkpoint(state);
            }
        }

        let os_p = state.os_file_ptr();
        let rc = if let Some(f) = (*(*os_p).pMethods).xShmLock {
            f(os_p, offset, n, flags)
        } else {
            ffi::SQLITE_IOERR_SHMLOCK
        };

        if rc == ffi::SQLITE_OK && is_read_mark && is_unlock && state.shared_lock_count > 0 {
            state.shared_lock_count -= 1;
            if state.shared_lock_count == 0
                && let Some(lock) = &state.compact_lock
            {
                let _ = lock.unlock();
            }
        }
        rc
    }));
    res.unwrap_or(ffi::SQLITE_INTERNAL)
}

unsafe extern "C" fn io_shm_barrier(p_file: *mut ffi::sqlite3_file) {
    let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
        let state = state_mut(p_file);
        let os_p = state.os_file_ptr();
        if let Some(f) = (*(*os_p).pMethods).xShmBarrier {
            f(os_p);
        }
    }));
}

unsafe extern "C" fn io_shm_unmap(p_file: *mut ffi::sqlite3_file, delete_flag: c_int) -> c_int {
    let res = catch_unwind(AssertUnwindSafe(|| unsafe {
        let state = state_mut(p_file);
        state.wal_index = std::ptr::null_mut();
        let os_p = state.os_file_ptr();
        if let Some(f) = (*(*os_p).pMethods).xShmUnmap {
            f(os_p, delete_flag)
        } else {
            ffi::SQLITE_OK
        }
    }));
    res.unwrap_or(ffi::SQLITE_INTERNAL)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_length_read_and_write_with_null_ptr_return_ok() {
        unsafe {
            assert_eq!(io_read(std::ptr::null_mut(), std::ptr::null_mut(), 0, 0), ffi::SQLITE_OK);
            assert_eq!(io_write(std::ptr::null_mut(), std::ptr::null(), 0, 0), ffi::SQLITE_OK);
        }
    }
}
