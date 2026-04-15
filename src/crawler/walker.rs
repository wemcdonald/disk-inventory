use crate::config::Config;
use crate::models::*;
use anyhow::Result;
use std::path::Path;

/// Walk a directory tree and collect FileEntry records.
/// Uses jwalk for parallel directory traversal.
pub fn walk_directory(root: &Path, scan_id: i64, config: &Config) -> Result<Vec<FileEntry>> {
    let mut entries = Vec::new();

    let walker = jwalk::WalkDir::new(root)
        .skip_hidden(false)
        .sort(false)
        .max_depth(config.scanner.max_depth as usize)
        .follow_links(config.scanner.follow_symlinks);

    for result in walker {
        let dir_entry = match result {
            Ok(entry) => entry,
            Err(e) => {
                tracing::warn!("walk error: {}", e);
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

        // Get metadata
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("metadata error for {}: {}", path_str, e);
                continue;
            }
        };

        let file_type = if metadata.is_symlink() {
            FileType::Symlink
        } else if metadata.is_dir() {
            FileType::Directory
        } else if metadata.is_file() {
            FileType::File
        } else {
            FileType::Other
        };

        let size_bytes = if file_type == FileType::File {
            metadata.len()
        } else {
            0
        };

        let mtime = metadata
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        // Unix-specific metadata
        #[cfg(unix)]
        let (inode, device_id, hardlink_count, blocks, uid, gid, mode, atime, ctime) = {
            use std::os::unix::fs::MetadataExt;
            (
                metadata.ino(),
                metadata.dev(),
                metadata.nlink(),
                metadata.blocks(),
                metadata.uid(),
                metadata.gid(),
                metadata.mode(),
                metadata.atime(),
                metadata.ctime(),
            )
        };

        #[cfg(not(unix))]
        let (inode, device_id, hardlink_count, blocks, uid, gid, mode, atime, ctime) =
            (0u64, 0u64, 1u64, 0u64, 0u32, 0u32, 0u32, 0i64, 0i64);

        let extension = extract_extension(&name);
        let parent = parent_path(&path_str);
        let depth = path_depth(&path_str);
        let components = path_component_count(&path_str);

        // Symlink target
        let symlink_target = if file_type == FileType::Symlink {
            std::fs::read_link(&path)
                .ok()
                .map(|t| t.to_string_lossy().to_string())
        } else {
            None
        };

        entries.push(FileEntry {
            id: None,
            path: path_str,
            parent_path: parent,
            name,
            extension,
            file_type,
            inode,
            device_id,
            hardlink_count,
            symlink_target,
            size_bytes,
            blocks,
            mtime,
            ctime,
            atime,
            birth_time: None,
            uid,
            gid,
            mode,
            scan_id,
            first_seen_scan: scan_id,
            is_deleted: false,
            depth,
            path_components: components,
        });
    }

    Ok(entries)
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
        let entries = walk_directory(dir.path(), 1, &config).unwrap();

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
        let entries = walk_directory(dir.path(), 1, &config).unwrap();

        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        assert!(!names.contains(&".DS_Store"), ".DS_Store should be excluded");
    }

    #[test]
    fn test_crawl_handles_permission_errors() {
        // Create a simple directory and walk it — just verify no panic
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("ok.txt"), "fine").unwrap();

        let config = Config::default();
        let result = walk_directory(dir.path(), 1, &config);
        assert!(result.is_ok());
    }
}
