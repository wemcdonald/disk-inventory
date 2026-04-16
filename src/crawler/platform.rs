//! Platform-specific filesystem metadata collection.
//!
//! Centralizes per-entry metadata extraction behind a single `get_metadata()` call,
//! with platform-optimized implementations:
//!
//! - **macOS**: Uses `std::os::unix::fs::MetadataExt` (and in the future,
//!   `getattrlistbulk()` for bulk directory reads).
//! - **Linux**: Uses `std::os::unix::fs::MetadataExt` (and in the future,
//!   `io_uring` + `IORING_OP_STATX` for batched stat).
//! - **Other**: Generic fallback with minimal metadata.

use crate::models::FileType;
use std::path::Path;

/// Metadata collected from a filesystem entry.
#[derive(Debug, Clone)]
pub struct EntryMeta {
    pub file_type: FileType,
    pub size_bytes: u64,
    pub inode: u64,
    pub device_id: u64,
    pub hardlink_count: u64,
    pub blocks: u64,
    pub mtime: i64,
    pub atime: i64,
    pub ctime: i64,
    pub birth_time: Option<i64>,
    pub uid: u32,
    pub gid: u32,
    pub mode: u32,
}

/// Collect metadata for a single path using the best available platform API.
pub fn get_metadata(path: &Path) -> std::io::Result<EntryMeta> {
    platform_impl::get_metadata(path)
}

/// Bulk metadata collection for a directory's contents.
///
/// On macOS, this will eventually use `getattrlistbulk()` to read directory
/// entries with metadata in fewer syscalls (5-10x faster than readdir+stat on APFS).
///
/// On Linux, this will eventually use `io_uring` with `IORING_OP_STATX` to batch
/// stat calls (2-5x faster on NVMe).
///
/// Returns `None` when no bulk optimization is available, signalling the caller
/// should fall back to per-file `get_metadata()`.
#[cfg(target_os = "macos")]
#[allow(unused_assignments)] // offset is incremented by read macros on the last field
pub fn bulk_readdir(dir_path: &Path) -> Option<Vec<(String, EntryMeta)>> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    // Apple attribute constants
    const ATTR_BIT_MAP_COUNT: u16 = 5;

    // commonattr
    const ATTR_CMN_RETURNED_ATTRS: u32 = 0x8000_0000;
    const ATTR_CMN_NAME: u32 = 0x0000_0001;
    const ATTR_CMN_DEVID: u32 = 0x0000_0002;
    const ATTR_CMN_OBJTYPE: u32 = 0x0000_0008;
    const ATTR_CMN_CRTIME: u32 = 0x0000_0200;
    const ATTR_CMN_MODTIME: u32 = 0x0000_0400;
    const ATTR_CMN_CHGTIME: u32 = 0x0000_0800;
    const ATTR_CMN_ACCTIME: u32 = 0x0000_1000;
    const ATTR_CMN_OWNERID: u32 = 0x0000_8000;
    const ATTR_CMN_GRPID: u32 = 0x0001_0000;
    const ATTR_CMN_ACCESSMASK: u32 = 0x0002_0000;
    const ATTR_CMN_FILEID: u32 = 0x0200_0000;

    // fileattr
    const ATTR_FILE_LINKCOUNT: u32 = 0x0000_0001;
    const ATTR_FILE_TOTALSIZE: u32 = 0x0000_0002;
    const ATTR_FILE_ALLOCSIZE: u32 = 0x0000_0004;

    // fsobj_type_t values
    const VREG: u32 = 1;
    const VDIR: u32 = 2;
    const VLNK: u32 = 5;

    // Options
    const FSOPT_PACK_INVAL_ATTRS: u32 = 0x0000_0008;

    /// attrlist struct matching Apple's layout.
    #[repr(C)]
    #[derive(Default)]
    struct AttrList {
        bitmapcount: u16,
        reserved: u16,
        commonattr: u32,
        volattr: u32,
        dirattr: u32,
        fileattr: u32,
        forkattr: u32,
    }

    extern "C" {
        fn getattrlistbulk(
            dirfd: libc::c_int,
            alist: *const AttrList,
            attr_buf: *mut libc::c_void,
            attr_buf_size: libc::size_t,
            options: u64,
        ) -> libc::c_int;
    }

    // Convert path to CString
    let c_path = CString::new(dir_path.as_os_str().as_bytes()).ok()?;

    // Open the directory
    let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDONLY) };
    if fd < 0 {
        return None;
    }

    // Set up attrlist requesting all the attributes we need
    let attr_list = AttrList {
        bitmapcount: ATTR_BIT_MAP_COUNT,
        reserved: 0,
        commonattr: ATTR_CMN_RETURNED_ATTRS
            | ATTR_CMN_NAME
            | ATTR_CMN_DEVID
            | ATTR_CMN_OBJTYPE
            | ATTR_CMN_CRTIME
            | ATTR_CMN_MODTIME
            | ATTR_CMN_CHGTIME
            | ATTR_CMN_ACCTIME
            | ATTR_CMN_OWNERID
            | ATTR_CMN_GRPID
            | ATTR_CMN_ACCESSMASK
            | ATTR_CMN_FILEID,
        volattr: 0,
        dirattr: 0,
        fileattr: ATTR_FILE_LINKCOUNT | ATTR_FILE_TOTALSIZE | ATTR_FILE_ALLOCSIZE,
        forkattr: 0,
    };

    const BUF_SIZE: usize = 256 * 1024; // 256 KB
    let mut buffer = vec![0u8; BUF_SIZE];
    let mut results: Vec<(String, EntryMeta)> = Vec::new();

    loop {
        let count = unsafe {
            getattrlistbulk(
                fd,
                &attr_list as *const AttrList,
                buffer.as_mut_ptr() as *mut libc::c_void,
                BUF_SIZE,
                FSOPT_PACK_INVAL_ATTRS as u64,
            )
        };

        if count < 0 {
            // Syscall error — close fd and fall back
            unsafe { libc::close(fd) };
            return None;
        }
        if count == 0 {
            // No more entries
            break;
        }

        // Parse entries from the buffer
        let mut offset: usize = 0;
        for _ in 0..count {
            if offset + 4 > BUF_SIZE {
                unsafe { libc::close(fd) };
                return None;
            }

            let entry_start = offset;
            let entry_len =
                u32::from_ne_bytes(buffer[offset..offset + 4].try_into().ok()?) as usize;

            if entry_len < 4 || entry_start + entry_len > BUF_SIZE {
                unsafe { libc::close(fd) };
                return None;
            }

            offset += 4;

            // attribute_set_t: 5 x u32 (returned attribute bitmaps)
            if offset + 20 > entry_start + entry_len {
                unsafe { libc::close(fd) };
                return None;
            }
            let returned_commonattr =
                u32::from_ne_bytes(buffer[offset..offset + 4].try_into().ok()?);
            // skip volattr (offset+4), dirattr (offset+8), read fileattr (offset+12)
            let returned_fileattr =
                u32::from_ne_bytes(buffer[offset + 12..offset + 16].try_into().ok()?);
            offset += 20;

            // Helper to read bytes safely within this entry
            let entry_end = entry_start + entry_len;
            macro_rules! read_u32 {
                () => {{
                    if offset + 4 > entry_end {
                        unsafe { libc::close(fd) };
                        return None;
                    }
                    let val = u32::from_ne_bytes(buffer[offset..offset + 4].try_into().ok()?);
                    offset += 4;
                    val
                }};
            }
            macro_rules! read_i32 {
                () => {{
                    if offset + 4 > entry_end {
                        unsafe { libc::close(fd) };
                        return None;
                    }
                    let val = i32::from_ne_bytes(buffer[offset..offset + 4].try_into().ok()?);
                    offset += 4;
                    val
                }};
            }
            macro_rules! read_u64 {
                () => {{
                    if offset + 8 > entry_end {
                        unsafe { libc::close(fd) };
                        return None;
                    }
                    let val = u64::from_ne_bytes(buffer[offset..offset + 8].try_into().ok()?);
                    offset += 8;
                    val
                }};
            }
            macro_rules! read_i64 {
                () => {{
                    if offset + 8 > entry_end {
                        unsafe { libc::close(fd) };
                        return None;
                    }
                    let val = i64::from_ne_bytes(buffer[offset..offset + 8].try_into().ok()?);
                    offset += 8;
                    val
                }};
            }

            // ATTR_CMN_NAME: attrreference_t (i32 offset, u32 length)
            let name = if returned_commonattr & ATTR_CMN_NAME != 0 {
                let attr_ref_pos = offset;
                let name_offset = read_i32!();
                let name_len = read_u32!();
                let name_start = (attr_ref_pos as isize + name_offset as isize) as usize;
                // name_len includes null terminator
                let actual_len = if name_len > 0 {
                    name_len as usize - 1
                } else {
                    0
                };
                if name_start + actual_len > entry_end {
                    // Name extends past entry — skip this entry
                    offset = entry_start + entry_len;
                    continue;
                }
                String::from_utf8_lossy(&buffer[name_start..name_start + actual_len]).to_string()
            } else {
                offset = entry_start + entry_len;
                continue;
            };

            // Skip "." and ".."
            if name == "." || name == ".." {
                offset = entry_start + entry_len;
                continue;
            }

            // ATTR_CMN_DEVID: dev_t (i32)
            let device_id = if returned_commonattr & ATTR_CMN_DEVID != 0 {
                read_i32!() as u64
            } else {
                0
            };

            // ATTR_CMN_OBJTYPE: fsobj_type_t (u32)
            let obj_type = if returned_commonattr & ATTR_CMN_OBJTYPE != 0 {
                read_u32!()
            } else {
                0
            };

            // ATTR_CMN_CRTIME: timespec (i64 tv_sec, i64 tv_nsec)
            let birth_time = if returned_commonattr & ATTR_CMN_CRTIME != 0 {
                let sec = read_i64!();
                let _nsec = read_i64!();
                Some(sec)
            } else {
                None
            };

            // ATTR_CMN_MODTIME: timespec
            let mtime = if returned_commonattr & ATTR_CMN_MODTIME != 0 {
                let sec = read_i64!();
                let _nsec = read_i64!();
                sec
            } else {
                0
            };

            // ATTR_CMN_CHGTIME: timespec
            let ctime = if returned_commonattr & ATTR_CMN_CHGTIME != 0 {
                let sec = read_i64!();
                let _nsec = read_i64!();
                sec
            } else {
                0
            };

            // ATTR_CMN_ACCTIME: timespec
            let atime = if returned_commonattr & ATTR_CMN_ACCTIME != 0 {
                let sec = read_i64!();
                let _nsec = read_i64!();
                sec
            } else {
                0
            };

            // ATTR_CMN_OWNERID: uid_t (u32)
            let uid = if returned_commonattr & ATTR_CMN_OWNERID != 0 {
                read_u32!()
            } else {
                0
            };

            // ATTR_CMN_GRPID: gid_t (u32)
            let gid = if returned_commonattr & ATTR_CMN_GRPID != 0 {
                read_u32!()
            } else {
                0
            };

            // ATTR_CMN_ACCESSMASK: u32
            let mode = if returned_commonattr & ATTR_CMN_ACCESSMASK != 0 {
                read_u32!()
            } else {
                0
            };

            // ATTR_CMN_FILEID: u64
            let inode = if returned_commonattr & ATTR_CMN_FILEID != 0 {
                read_u64!()
            } else {
                0
            };

            // File attributes — only present when returned_attrs says so
            let (hardlink_count, total_size, alloc_size) = if returned_fileattr != 0 {
                let linkcount = if returned_fileattr & ATTR_FILE_LINKCOUNT != 0 {
                    read_u32!() as u64
                } else {
                    1
                };
                let tsize = if returned_fileattr & ATTR_FILE_TOTALSIZE != 0 {
                    read_i64!() as u64
                } else {
                    0
                };
                let asize = if returned_fileattr & ATTR_FILE_ALLOCSIZE != 0 {
                    read_i64!() as u64
                } else {
                    0
                };
                (linkcount, tsize, asize)
            } else {
                (1, 0, 0)
            };

            let file_type = match obj_type {
                VREG => FileType::File,
                VDIR => FileType::Directory,
                VLNK => FileType::Symlink,
                _ => FileType::Other,
            };

            let size_bytes = if file_type == FileType::File {
                total_size
            } else {
                0
            };

            // Compute blocks from alloc_size (512-byte blocks)
            let blocks = alloc_size / 512;

            results.push((
                name,
                EntryMeta {
                    file_type,
                    size_bytes,
                    inode,
                    device_id,
                    hardlink_count,
                    blocks,
                    mtime,
                    atime,
                    ctime,
                    birth_time,
                    uid,
                    gid,
                    mode,
                },
            ));

            // Advance to next entry (handles alignment)
            offset = entry_start + entry_len;
        }
    }

    unsafe { libc::close(fd) };
    Some(results)
}

#[cfg(target_os = "linux")]
pub fn bulk_readdir(_dir_path: &Path) -> Option<Vec<(String, EntryMeta)>> {
    // TODO: Implement io_uring + IORING_OP_STATX batched stat.
    //
    // The implementation would:
    // 1. Read directory entries with getdents64
    // 2. Submit batched IORING_OP_STATX requests via io_uring
    // 3. Harvest completions and build (name, EntryMeta) pairs
    //
    // For v1, return None to fall back to per-file stat via get_metadata().
    None
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn bulk_readdir(_dir_path: &Path) -> Option<Vec<(String, EntryMeta)>> {
    None
}

// ---------------------------------------------------------------------------
// macOS implementation
// ---------------------------------------------------------------------------

#[cfg(target_os = "macos")]
mod platform_impl {
    use super::*;

    pub fn get_metadata(path: &Path) -> std::io::Result<EntryMeta> {
        // TODO: Use getattrlist() for single-file metadata (faster than stat on APFS).
        // For v1, use symlink_metadata + MetadataExt which is already well-tested.
        let meta = std::fs::symlink_metadata(path)?;
        use std::os::unix::fs::MetadataExt;

        let file_type = classify(&meta);
        let size_bytes = if file_type == FileType::File {
            meta.len()
        } else {
            0
        };

        Ok(EntryMeta {
            file_type,
            size_bytes,
            inode: meta.ino(),
            device_id: meta.dev(),
            hardlink_count: meta.nlink(),
            blocks: meta.blocks(),
            mtime: meta.mtime(),
            atime: meta.atime(),
            ctime: meta.ctime(),
            // macOS exposes birth time (creation time) but MetadataExt::ctime()
            // returns the *change* time (inode change), not creation time.
            // std::fs::Metadata::created() gives the real birth time on macOS.
            birth_time: meta
                .created()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64),
            uid: meta.uid(),
            gid: meta.gid(),
            mode: meta.mode(),
        })
    }

    fn classify(meta: &std::fs::Metadata) -> FileType {
        if meta.is_symlink() {
            FileType::Symlink
        } else if meta.is_dir() {
            FileType::Directory
        } else if meta.is_file() {
            FileType::File
        } else {
            FileType::Other
        }
    }
}

// ---------------------------------------------------------------------------
// Linux implementation
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
mod platform_impl {
    use super::*;

    pub fn get_metadata(path: &Path) -> std::io::Result<EntryMeta> {
        // On Linux, glibc's stat uses statx() under the hood on modern kernels.
        // For v1, use symlink_metadata + MetadataExt.
        // TODO: Direct statx() syscall requesting only needed fields.
        let meta = std::fs::symlink_metadata(path)?;
        use std::os::unix::fs::MetadataExt;

        let file_type = classify(&meta);
        let size_bytes = if file_type == FileType::File {
            meta.len()
        } else {
            0
        };

        Ok(EntryMeta {
            file_type,
            size_bytes,
            inode: meta.ino(),
            device_id: meta.dev(),
            hardlink_count: meta.nlink(),
            blocks: meta.blocks(),
            mtime: meta.mtime(),
            atime: meta.atime(),
            ctime: meta.ctime(),
            // Linux doesn't always expose birth time through the Rust std API.
            // statx() can return it on ext4/btrfs, but std::fs doesn't expose it
            // on all distros. Leave as None for v1.
            birth_time: None,
            uid: meta.uid(),
            gid: meta.gid(),
            mode: meta.mode(),
        })
    }

    fn classify(meta: &std::fs::Metadata) -> FileType {
        if meta.is_symlink() {
            FileType::Symlink
        } else if meta.is_dir() {
            FileType::Directory
        } else if meta.is_file() {
            FileType::File
        } else {
            FileType::Other
        }
    }
}

// ---------------------------------------------------------------------------
// Generic fallback
// ---------------------------------------------------------------------------

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
mod platform_impl {
    use super::*;

    pub fn get_metadata(path: &Path) -> std::io::Result<EntryMeta> {
        let meta = std::fs::symlink_metadata(path)?;

        let file_type = if meta.is_symlink() {
            FileType::Symlink
        } else if meta.is_dir() {
            FileType::Directory
        } else if meta.is_file() {
            FileType::File
        } else {
            FileType::Other
        };

        let size_bytes = if file_type == FileType::File {
            meta.len()
        } else {
            0
        };

        Ok(EntryMeta {
            file_type,
            size_bytes,
            inode: 0,
            device_id: 0,
            hardlink_count: 1,
            blocks: 0,
            mtime: meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
            atime: 0,
            ctime: 0,
            birth_time: None,
            uid: 0,
            gid: 0,
            mode: 0,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_metadata_file() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        std::fs::write(&file_path, "hello world").unwrap();

        let meta = get_metadata(&file_path).unwrap();
        assert_eq!(meta.file_type, FileType::File);
        assert_eq!(meta.size_bytes, 11);
        assert!(meta.mtime > 0);
        #[cfg(unix)]
        assert!(meta.inode > 0);
    }

    #[test]
    fn test_get_metadata_directory() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("subdir");
        std::fs::create_dir(&sub).unwrap();

        let meta = get_metadata(&sub).unwrap();
        assert_eq!(meta.file_type, FileType::Directory);
        assert_eq!(meta.size_bytes, 0);
    }

    #[test]
    fn test_get_metadata_nonexistent() {
        let result = get_metadata(Path::new("/nonexistent/path/that/does/not/exist"));
        assert!(result.is_err());
    }

    #[cfg(unix)]
    #[test]
    fn test_get_metadata_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target.txt");
        std::fs::write(&target, "target content").unwrap();

        let link = dir.path().join("link.txt");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let meta = get_metadata(&link).unwrap();
        assert_eq!(meta.file_type, FileType::Symlink);
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn test_bulk_readdir_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        assert!(bulk_readdir(dir.path()).is_none());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn test_bulk_readdir_reads_directory() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("file1.txt"), "hello").unwrap();
        std::fs::write(dir.path().join("file2.rs"), "fn main(){}").unwrap();
        std::fs::create_dir(dir.path().join("subdir")).unwrap();

        let result = bulk_readdir(dir.path());
        // Should return Some with entries
        if let Some(entries) = result {
            assert!(entries.len() >= 3, "got {} entries", entries.len());

            let names: Vec<&str> = entries.iter().map(|(n, _)| n.as_str()).collect();
            assert!(names.contains(&"file1.txt"));
            assert!(names.contains(&"file2.rs"));
            assert!(names.contains(&"subdir"));

            // Check file metadata
            for (name, meta) in &entries {
                match name.as_str() {
                    "file1.txt" => {
                        assert_eq!(meta.file_type, FileType::File);
                        assert_eq!(meta.size_bytes, 5);
                        assert!(meta.inode > 0);
                        assert!(meta.mtime > 0);
                    }
                    "file2.rs" => {
                        assert_eq!(meta.file_type, FileType::File);
                        assert_eq!(meta.size_bytes, 11);
                    }
                    "subdir" => {
                        assert_eq!(meta.file_type, FileType::Directory);
                        assert_eq!(meta.size_bytes, 0);
                    }
                    _ => {}
                }
            }
        } else {
            panic!("bulk_readdir should return Some on macOS");
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn test_bulk_readdir_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let result = bulk_readdir(dir.path());
        // Empty dir: could return Some(empty vec) or None — both acceptable
        if let Some(entries) = result {
            // .DS_Store might appear, but there should be no unexpected entries
            assert!(entries.len() <= 1, "empty dir should have 0-1 entries, got {}", entries.len());
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn test_bulk_readdir_nonexistent() {
        let result = bulk_readdir(Path::new("/nonexistent/dir/that/does/not/exist"));
        assert!(result.is_none(), "nonexistent dir should return None");
    }
}
