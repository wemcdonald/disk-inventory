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
pub fn bulk_readdir(_dir_path: &Path) -> Option<Vec<(String, EntryMeta)>> {
    // TODO: Implement getattrlistbulk() FFI.
    //
    // The implementation would:
    // 1. Open the directory with open()
    // 2. Call getattrlistbulk() with ATTR_CMN_NAME | ATTR_CMN_OBJTYPE |
    //    ATTR_CMN_MODTIME | ATTR_CMN_CRTIME | ATTR_FILE_TOTALSIZE | etc.
    // 3. Parse the returned variable-length buffer into (name, EntryMeta) pairs
    // 4. Return all entries in a single batch
    //
    // For v1, return None to fall back to per-file stat via get_metadata().
    None
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

    #[test]
    fn test_bulk_readdir_returns_none() {
        // For v1, bulk_readdir always returns None (stub).
        let dir = tempfile::tempdir().unwrap();
        assert!(bulk_readdir(dir.path()).is_none());
    }
}
