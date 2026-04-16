use crate::config::Config;
use crate::crawler::platform;
use crate::models::*;
use anyhow::Result;
use std::path::Path;

/// Check if an I/O error is harmless and expected during filesystem crawling.
/// These get demoted to debug level instead of warn.
fn is_harmless_io_error(e: &dyn std::fmt::Display) -> bool {
    let msg = e.to_string();
    // ENOENT: file vanished between readdir and stat (Spotlight, tmp files)
    msg.contains("No such file or directory")
    // ETIMEDOUT: network mount not responding
    || msg.contains("Operation timed out")
    // ESTALE: NFS handle gone
    || msg.contains("Stale NFS file handle")
    // EACCES on system-protected paths
    || msg.contains("Operation not permitted")
    || msg.contains("Permission denied")
}

/// Walk a directory tree and collect FileEntry records.
///
/// Uses jwalk for parallel directory traversal and delegates per-entry
/// metadata collection to [`platform::get_metadata`], which selects the
/// best syscall strategy for the current OS.
pub fn walk_directory(
    root: &Path,
    scan_id: i64,
    config: &Config,
    progress_callback: Option<&dyn Fn(u64, u64, u64, &str)>,
) -> Result<(Vec<FileEntry>, u64)> {
    let mut entries = Vec::new();
    let mut last_progress = std::time::Instant::now();
    let mut file_count: u64 = 0;
    let mut dir_count: u64 = 0;
    let mut byte_count: u64 = 0;
    let mut permission_errors: u64 = 0;

    // Get root device ID for cross-filesystem detection
    let root_device_id = if !config.scanner.cross_filesystems {
        platform::get_metadata(root).ok().map(|m| m.device_id)
    } else {
        None
    };

    let walker = jwalk::WalkDir::new(root)
        .skip_hidden(false)
        .sort(false)
        .max_depth(config.scanner.max_depth as usize)
        .follow_links(config.scanner.follow_symlinks);

    for result in walker {
        let dir_entry = match result {
            Ok(entry) => entry,
            Err(e) => {
                if is_harmless_io_error(&e) {
                    let msg = e.to_string();
                    if msg.contains("Permission denied") || msg.contains("Operation not permitted") {
                        permission_errors += 1;
                    }
                    tracing::debug!("walk error (skipped): {}", e);
                } else {
                    tracing::warn!("walk error: {}", e);
                }
                continue;
            }
        };

        let name = dir_entry.file_name().to_string_lossy().to_string();

        // Check exclusion
        if config.is_excluded(&name) {
            continue;
        }

        let path = dir_entry.path();
        let path_str = path.to_string_lossy().to_string();

        // Collect metadata via platform-optimized implementation
        let meta = match platform::get_metadata(&path) {
            Ok(m) => m,
            Err(e) => {
                if is_harmless_io_error(&e) {
                    let msg = e.to_string();
                    if msg.contains("Permission denied") || msg.contains("Operation not permitted") {
                        permission_errors += 1;
                    }
                    tracing::debug!("metadata error (skipped): {}: {}", path_str, e);
                } else {
                    tracing::warn!("metadata error for {}: {}", path_str, e);
                }
                continue;
            }
        };

        // Cross-filesystem check: skip entries on different devices
        if let Some(root_dev) = root_device_id {
            if meta.device_id != root_dev {
                tracing::debug!("skipping cross-device entry: {}", path_str);
                continue;
            }
        }

        let extension = if meta.file_type == FileType::File {
            extract_extension(&name)
        } else {
            None
        };
        let parent = parent_path(&path_str);
        let depth = path_depth(&path_str);
        let components = path_component_count(&path_str);

        // Symlink target
        let symlink_target = if meta.file_type == FileType::Symlink {
            std::fs::read_link(&path)
                .ok()
                .map(|t| t.to_string_lossy().to_string())
        } else {
            None
        };

        // Track progress
        if meta.file_type == FileType::File {
            file_count += 1;
            // Use disk_size (blocks*512) for accurate reporting
            byte_count += if meta.blocks > 0 { meta.blocks * 512 } else { meta.size_bytes };
        } else if meta.file_type == FileType::Directory {
            dir_count += 1;
        }

        if let Some(cb) = &progress_callback {
            if file_count % 1000 == 0
                || last_progress.elapsed() > std::time::Duration::from_secs(2)
            {
                cb(file_count, dir_count, byte_count, &path_str);
                last_progress = std::time::Instant::now();
            }
        }

        entries.push(FileEntry {
            id: None,
            path: path_str,
            parent_path: parent,
            name,
            extension,
            file_type: meta.file_type,
            inode: meta.inode,
            device_id: meta.device_id,
            hardlink_count: meta.hardlink_count,
            symlink_target,
            size_bytes: meta.size_bytes,
            blocks: meta.blocks,
            mtime: meta.mtime,
            ctime: meta.ctime,
            atime: meta.atime,
            birth_time: meta.birth_time,
            uid: meta.uid,
            gid: meta.gid,
            mode: meta.mode,
            scan_id,
            first_seen_scan: scan_id,
            is_deleted: false,
            depth,
            path_components: components,
        });
    }

    Ok((entries, permission_errors))
}

/// Walk a directory tree incrementally — only descend into directories
/// whose mtime is newer than `since_timestamp`.
///
/// Returns (entries, dirs_scanned, dirs_skipped).
pub fn walk_directory_incremental(
    root: &Path,
    scan_id: i64,
    config: &Config,
    since_timestamp: i64,
    progress_callback: Option<&dyn Fn(u64, u64, u64, &str)>,
) -> Result<(Vec<FileEntry>, u64, u64, u64)> {
    let mut entries = Vec::new();
    let mut dirs_scanned: u64 = 0;
    let mut dirs_skipped: u64 = 0;
    let mut file_count: u64 = 0;
    let mut dir_count: u64 = 0;
    let mut byte_count: u64 = 0;
    let mut permission_errors: u64 = 0;
    let last_progress = std::cell::RefCell::new(std::time::Instant::now());

    walk_recursive(
        root,
        scan_id,
        config,
        since_timestamp,
        &mut entries,
        &mut dirs_scanned,
        &mut dirs_skipped,
        0,
        &progress_callback,
        &mut file_count,
        &mut dir_count,
        &mut byte_count,
        &last_progress,
        &mut permission_errors,
    )?;

    Ok((entries, dirs_scanned, dirs_skipped, permission_errors))
}

fn build_file_entry(
    path: &Path,
    scan_id: i64,
) -> Option<FileEntry> {
    let path_str = path.to_string_lossy().to_string();
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| path_str.clone());

    let meta = match platform::get_metadata(path) {
        Ok(m) => m,
        Err(e) => {
            if is_harmless_io_error(&e) {
                tracing::debug!("metadata error (skipped): {}: {}", path_str, e);
            } else {
                tracing::warn!("metadata error for {}: {}", path_str, e);
            }
            return None;
        }
    };

    let extension = if meta.file_type == FileType::File {
        extract_extension(&name)
    } else {
        None
    };
    let parent = parent_path(&path_str);
    let depth = path_depth(&path_str);
    let components = path_component_count(&path_str);

    let symlink_target = if meta.file_type == FileType::Symlink {
        std::fs::read_link(path)
            .ok()
            .map(|t| t.to_string_lossy().to_string())
    } else {
        None
    };

    Some(FileEntry {
        id: None,
        path: path_str,
        parent_path: parent,
        name,
        extension,
        file_type: meta.file_type,
        inode: meta.inode,
        device_id: meta.device_id,
        hardlink_count: meta.hardlink_count,
        symlink_target,
        size_bytes: meta.size_bytes,
        blocks: meta.blocks,
        mtime: meta.mtime,
        ctime: meta.ctime,
        atime: meta.atime,
        birth_time: meta.birth_time,
        uid: meta.uid,
        gid: meta.gid,
        mode: meta.mode,
        scan_id,
        first_seen_scan: scan_id,
        is_deleted: false,
        depth,
        path_components: components,
    })
}

fn walk_recursive(
    dir: &Path,
    scan_id: i64,
    config: &Config,
    since_timestamp: i64,
    entries: &mut Vec<FileEntry>,
    dirs_scanned: &mut u64,
    dirs_skipped: &mut u64,
    depth: u32,
    progress_callback: &Option<&dyn Fn(u64, u64, u64, &str)>,
    file_count: &mut u64,
    dir_count: &mut u64,
    byte_count: &mut u64,
    last_progress: &std::cell::RefCell<std::time::Instant>,
    permission_errors: &mut u64,
) -> Result<()> {
    if depth > config.scanner.max_depth {
        return Ok(());
    }

    // Check directory mtime via platform metadata
    let dir_meta = match platform::get_metadata(dir) {
        Ok(m) => m,
        Err(e) => {
            if is_harmless_io_error(&e) {
                let msg = e.to_string();
                if msg.contains("Permission denied") || msg.contains("Operation not permitted") {
                    *permission_errors += 1;
                }
                tracing::debug!("cannot stat directory (skipped): {}: {}", dir.display(), e);
            } else {
                tracing::warn!("Cannot stat directory {}: {}", dir.display(), e);
            }
            return Ok(());
        }
    };

    let dir_str = dir.to_string_lossy().to_string();
    let dir_name = dir
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| dir_str.clone());

    // Check exclusion
    if config.is_excluded(&dir_name) {
        return Ok(());
    }

    // Always record the directory entry itself
    if let Some(dir_entry) = build_file_entry(dir, scan_id) {
        *dir_count += 1;
        entries.push(dir_entry);
    }

    // Report progress
    if let Some(cb) = progress_callback {
        if *file_count % 1000 == 0
            || last_progress.borrow().elapsed() > std::time::Duration::from_secs(2)
        {
            cb(*file_count, *dir_count, *byte_count, &dir_str);
            *last_progress.borrow_mut() = std::time::Instant::now();
        }
    }

    let dir_mtime = dir_meta.mtime;

    if dir_mtime <= since_timestamp {
        // Directory hasn't changed — skip scanning its contents
        // But still recurse into subdirectories to check THEIR mtimes
        *dirs_skipped += 1;

        if let Ok(read_dir) = std::fs::read_dir(dir) {
            for entry in read_dir.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if !config.is_excluded(&name) {
                        walk_recursive(
                            &path,
                            scan_id,
                            config,
                            since_timestamp,
                            entries,
                            dirs_scanned,
                            dirs_skipped,
                            depth + 1,
                            progress_callback,
                            file_count,
                            dir_count,
                            byte_count,
                            last_progress,
                            permission_errors,
                        )?;
                    }
                }
                // Skip files in unchanged directories — they're already in the DB
            }
        }
    } else {
        // Directory has changed — scan all contents
        *dirs_scanned += 1;

        if let Ok(read_dir) = std::fs::read_dir(dir) {
            for entry_result in read_dir {
                let entry = match entry_result {
                    Ok(e) => e,
                    Err(e) => {
                        if is_harmless_io_error(&e) {
                            let msg = e.to_string();
                            if msg.contains("Permission denied") || msg.contains("Operation not permitted") {
                                *permission_errors += 1;
                            }
                            tracing::debug!("readdir error (skipped): {}: {}", dir.display(), e);
                        } else {
                            tracing::warn!("readdir error in {}: {}", dir.display(), e);
                        }
                        continue;
                    }
                };

                let path = entry.path();
                let name = entry.file_name().to_string_lossy().to_string();

                if config.is_excluded(&name) {
                    continue;
                }

                if path.is_dir() {
                    // Subdirectories record themselves at the top of walk_recursive
                    walk_recursive(
                        &path,
                        scan_id,
                        config,
                        since_timestamp,
                        entries,
                        dirs_scanned,
                        dirs_skipped,
                        depth + 1,
                        progress_callback,
                        file_count,
                        dir_count,
                        byte_count,
                        last_progress,
                        permission_errors,
                    )?;
                } else {
                    // Build FileEntry for non-directory entries
                    if let Some(file_entry) = build_file_entry(&path, scan_id) {
                        if file_entry.file_type == FileType::File {
                            *file_count += 1;
                            *byte_count += file_entry.size_bytes;
                        }
                        entries.push(file_entry);
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_tree() -> TempDir {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("big.bin"), vec![0u8; 1024]).unwrap();
        std::fs::write(dir.path().join("small.txt"), "hello").unwrap();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("sub").join("medium.log"), vec![0u8; 512]).unwrap();
        std::fs::write(dir.path().join("sub").join("tiny.rs"), "fn main(){}").unwrap();
        std::fs::create_dir(dir.path().join("empty_dir")).unwrap();
        dir
    }

    #[test]
    fn test_walk_directory() {
        let dir = create_test_tree();
        let config = Config::default();
        let (entries, _permission_errors) = walk_directory(dir.path(), 1, &config, None).unwrap();

        // Should find: root dir, big.bin, small.txt, sub/, medium.log, tiny.rs, empty_dir/
        // That's 7 entries (root + 2 files + 2 dirs + 2 files in sub)
        assert_eq!(entries.len(), 7, "expected 7 entries, got {}: {:?}",
            entries.len(), entries.iter().map(|e| &e.name).collect::<Vec<_>>());

        // Check files exist
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"big.bin"));
        assert!(names.contains(&"small.txt"));
        assert!(names.contains(&"sub"));
        assert!(names.contains(&"medium.log"));
        assert!(names.contains(&"tiny.rs"));
        assert!(names.contains(&"empty_dir"));

        // Verify sizes
        let big = entries.iter().find(|e| e.name == "big.bin").unwrap();
        assert_eq!(big.size_bytes, 1024);
        assert_eq!(big.file_type, FileType::File);
        assert_eq!(big.extension, Some("bin".to_string()));

        let small = entries.iter().find(|e| e.name == "small.txt").unwrap();
        assert_eq!(small.size_bytes, 5);
        assert_eq!(small.file_type, FileType::File);

        // Verify directories have size 0
        let sub = entries.iter().find(|e| e.name == "sub").unwrap();
        assert_eq!(sub.size_bytes, 0);
        assert_eq!(sub.file_type, FileType::Directory);

        let empty = entries.iter().find(|e| e.name == "empty_dir").unwrap();
        assert_eq!(empty.size_bytes, 0);
        assert_eq!(empty.file_type, FileType::Directory);
    }

    #[test]
    fn test_walk_excludes_patterns() {
        let dir = create_test_tree();
        // Create a .DS_Store file (which is in the default exclude list)
        std::fs::write(dir.path().join(".DS_Store"), "excluded").unwrap();

        let config = Config::default();
        let (entries, _permission_errors) = walk_directory(dir.path(), 1, &config, None).unwrap();

        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        assert!(!names.contains(&".DS_Store"), ".DS_Store should be excluded");
    }

    #[test]
    fn test_crawl_handles_permission_errors() {
        // Create a simple directory and walk it — just verify no panic
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("ok.txt"), "fine").unwrap();

        let config = Config::default();
        let result = walk_directory(dir.path(), 1, &config, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_walk_directory_incremental_all_changed() {
        // With since_timestamp=0, everything should be scanned (all dirs are newer)
        let dir = create_test_tree();
        let config = Config::default();

        let (entries, dirs_scanned, _dirs_skipped, _perm_errs) =
            walk_directory_incremental(dir.path(), 1, &config, 0, None).unwrap();

        // Should find the same entries as a full walk
        assert_eq!(
            entries.len(),
            7,
            "expected 7 entries (same as full walk), got {}: {:?}",
            entries.len(),
            entries.iter().map(|e| &e.name).collect::<Vec<_>>()
        );
        assert!(dirs_scanned >= 1, "at least root should be scanned");

        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"big.bin"));
        assert!(names.contains(&"small.txt"));
        assert!(names.contains(&"sub"));
        assert!(names.contains(&"medium.log"));
    }

    #[test]
    fn test_walk_directory_incremental_skips_old_dirs() {
        let dir = create_test_tree();
        let config = Config::default();

        // Use a timestamp far in the future so all directories are "old"
        let future_ts = chrono::Utc::now().timestamp() + 10_000;
        let (entries, dirs_scanned, dirs_skipped, _perm_errs) =
            walk_directory_incremental(dir.path(), 1, &config, future_ts, None).unwrap();

        // Should only find directory entries (since we always record directory entries)
        // but no file entries (since all dirs are "unchanged")
        let file_entries: Vec<_> = entries
            .iter()
            .filter(|e| e.file_type == FileType::File)
            .collect();
        assert_eq!(
            file_entries.len(),
            0,
            "no files should be found when all dirs are skipped"
        );

        // All directories should be skipped
        assert_eq!(dirs_scanned, 0, "no dirs should be fully scanned");
        assert!(dirs_skipped >= 1, "at least some dirs should be skipped");
    }

    #[test]
    fn test_walk_incremental_excludes_patterns() {
        let dir = create_test_tree();
        std::fs::create_dir(dir.path().join(".DS_Store_dir")).unwrap();
        std::fs::write(dir.path().join(".DS_Store"), "excluded").unwrap();

        let config = Config::default();
        let (entries, _, _, _) =
            walk_directory_incremental(dir.path(), 1, &config, 0, None).unwrap();

        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        assert!(
            !names.contains(&".DS_Store"),
            ".DS_Store should be excluded in incremental walk"
        );
    }
}
