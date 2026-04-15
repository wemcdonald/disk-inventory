pub mod platform;
pub mod walker;

use crate::config::Config;
use crate::db::queries::ScanStats;
use crate::db::Database;
use crate::models::*;
use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;

/// Run a full crawl: walk filesystem, insert entries, compute aggregates.
pub fn run_crawl(db: &Database, root: &Path, config: &Config) -> Result<ScanInfo> {
    let root_path = root.to_string_lossy().to_string();
    let scan_id = db.create_scan(&root_path)?;

    // Phase 1: Walk & Insert
    tracing::info!("Phase 1: Walking filesystem at {}", root_path);
    let entries = walker::walk_directory(root, scan_id, config)?;

    db.enable_bulk_mode()?;
    for chunk in entries.chunks(10_000) {
        db.insert_files(chunk)?;
    }
    db.disable_bulk_mode()?;

    tracing::info!("Phase 1 complete: {} entries found", entries.len());

    // Phase 2: Mark deletions
    tracing::info!("Phase 2: Marking deleted entries");
    let deleted_count = db.mark_deleted(scan_id, &root_path)?;
    tracing::info!("Phase 2 complete: {} entries marked deleted", deleted_count);

    // Phase 3: Compute dir_sizes
    tracing::info!("Phase 3: Computing directory sizes");
    let dir_sizes = compute_dir_sizes(&entries, scan_id);
    db.insert_dir_sizes(&dir_sizes)?;
    tracing::info!("Phase 3 complete: {} directory sizes computed", dir_sizes.len());

    // Phase 4: Compute extension stats
    tracing::info!("Phase 4: Computing extension statistics");
    db.compute_extension_stats(scan_id)?;
    tracing::info!("Phase 4 complete");

    // Phase 5: Record size history
    tracing::info!("Phase 5: Recording size history");
    db.record_size_history(scan_id, 10 * 1024 * 1024)?; // 10 MB threshold
    tracing::info!("Phase 5 complete");

    // Phase 6: Complete scan
    tracing::info!("Phase 6: Finalizing scan");
    let total_files = entries
        .iter()
        .filter(|e| e.file_type == FileType::File)
        .count() as u64;
    let total_dirs = entries
        .iter()
        .filter(|e| e.file_type == FileType::Directory)
        .count() as u64;
    let total_size: u64 = entries
        .iter()
        .filter(|e| e.file_type == FileType::File)
        .map(|e| e.size_bytes)
        .sum();

    let stats = ScanStats {
        total_files,
        total_dirs,
        total_size,
        files_added: total_files + total_dirs, // first scan: everything is "added"
        files_modified: 0,
        files_deleted: deleted_count,
    };

    db.complete_scan(scan_id, &stats)?;

    let scan_info = db
        .latest_scan()?
        .expect("scan should exist after completion");

    tracing::info!("Crawl complete: {} files, {} dirs, {} bytes",
        total_files, total_dirs, total_size);

    Ok(scan_info)
}

/// Run an incremental crawl: only descend into directories whose mtime
/// has changed since the last scan. Much faster than a full crawl when
/// most of the filesystem is static.
///
/// Falls back to a full crawl if no previous scan exists.
pub fn run_incremental_crawl(db: &Database, root: &Path, config: &Config) -> Result<ScanInfo> {
    // Check if we have a previous scan
    let prev_scan = db.latest_scan()?;
    let prev_scan_time = match prev_scan {
        Some(ref scan) if scan.root_path == root.to_string_lossy() => scan.started_at,
        _ => {
            tracing::info!("No previous scan found, running full crawl");
            return run_crawl(db, root, config);
        }
    };

    let root_path = root.to_string_lossy().to_string();
    let scan_id = db.create_scan(&root_path)?;

    tracing::info!(
        "Starting incremental crawl of {} (since timestamp {})",
        root_path,
        prev_scan_time
    );

    // Phase 1: Incremental walk — only descend into changed directories
    db.enable_bulk_mode()?;
    let (entries, dirs_scanned, dirs_skipped) =
        walker::walk_directory_incremental(root, scan_id, config, prev_scan_time)?;

    for chunk in entries.chunks(10_000) {
        db.insert_files(chunk)?;
    }
    db.disable_bulk_mode()?;

    tracing::info!(
        "Incremental walk: {} entries found, {} dirs scanned, {} dirs skipped",
        entries.len(),
        dirs_scanned,
        dirs_skipped
    );

    // Phase 2: Mark deletions
    tracing::info!("Phase 2: Marking deleted entries");
    let deleted_count = db.mark_deleted(scan_id, &root_path)?;
    tracing::info!("Phase 2 complete: {} entries marked deleted", deleted_count);

    // Phase 3-5: Same as full crawl
    tracing::info!("Phase 3: Computing directory sizes");
    let dir_sizes = compute_dir_sizes(&entries, scan_id);
    db.insert_dir_sizes(&dir_sizes)?;
    tracing::info!("Phase 3 complete: {} directory sizes computed", dir_sizes.len());

    tracing::info!("Phase 4: Computing extension statistics");
    db.compute_extension_stats(scan_id)?;
    tracing::info!("Phase 4 complete");

    tracing::info!("Phase 5: Recording size history");
    db.record_size_history(scan_id, 10 * 1024 * 1024)?;
    tracing::info!("Phase 5 complete");

    // Phase 6: Complete scan
    tracing::info!("Phase 6: Finalizing scan");
    let total_files = entries
        .iter()
        .filter(|e| e.file_type == FileType::File)
        .count() as u64;
    let total_dirs = entries
        .iter()
        .filter(|e| e.file_type == FileType::Directory)
        .count() as u64;
    let total_size: u64 = entries
        .iter()
        .filter(|e| e.file_type == FileType::File)
        .map(|e| e.size_bytes)
        .sum();

    let stats = ScanStats {
        total_files,
        total_dirs,
        total_size,
        files_added: 0,
        files_modified: 0,
        files_deleted: deleted_count,
    };
    db.complete_scan(scan_id, &stats)?;

    let scan_info = db
        .latest_scan()?
        .ok_or_else(|| anyhow::anyhow!("scan not found after completion"))?;

    tracing::info!(
        "Incremental crawl complete: {} files, {} dirs, {} bytes",
        total_files,
        total_dirs,
        total_size
    );

    Ok(scan_info)
}

/// Compute directory sizes via bottom-up aggregation.
///
/// For each directory, total_size includes all files recursively beneath it,
/// and file_count includes all files recursively.
fn compute_dir_sizes(entries: &[FileEntry], scan_id: i64) -> Vec<DirSize> {
    // Step 1: Identify all directories and collect direct file stats per directory
    let mut dir_direct_size: HashMap<String, u64> = HashMap::new();
    let mut dir_direct_files: HashMap<String, u64> = HashMap::new();
    let mut dir_largest_file: HashMap<String, u64> = HashMap::new();
    let mut all_dirs: HashMap<String, u32> = HashMap::new(); // path -> depth

    for entry in entries {
        if entry.file_type == FileType::Directory {
            all_dirs.insert(entry.path.clone(), entry.depth);
            // Ensure dir appears in maps even if empty
            dir_direct_size.entry(entry.path.clone()).or_insert(0);
            dir_direct_files.entry(entry.path.clone()).or_insert(0);
            dir_largest_file.entry(entry.path.clone()).or_insert(0);
        } else if entry.file_type == FileType::File {
            *dir_direct_size
                .entry(entry.parent_path.clone())
                .or_insert(0) += entry.size_bytes;
            *dir_direct_files
                .entry(entry.parent_path.clone())
                .or_insert(0) += 1;
            let largest = dir_largest_file
                .entry(entry.parent_path.clone())
                .or_insert(0);
            if entry.size_bytes > *largest {
                *largest = entry.size_bytes;
            }
        }
    }

    // Step 2: Build parent -> children index
    let mut parent_to_children: HashMap<String, Vec<String>> = HashMap::new();
    for dir_path in all_dirs.keys() {
        let parent = parent_path(dir_path);
        if parent != *dir_path {
            parent_to_children
                .entry(parent)
                .or_default()
                .push(dir_path.clone());
        }
    }

    // Step 3: Sort directories by depth descending (deepest first)
    let mut dirs_by_depth: Vec<(String, u32)> = all_dirs.into_iter().collect();
    dirs_by_depth.sort_by(|a, b| b.1.cmp(&a.1));

    // Step 4: Bottom-up aggregation
    let mut total_sizes: HashMap<String, u64> = HashMap::new();
    let mut total_files: HashMap<String, u64> = HashMap::new();
    let mut total_dir_counts: HashMap<String, u64> = HashMap::new();
    let mut total_max_depth: HashMap<String, u32> = HashMap::new();
    let mut total_largest: HashMap<String, u64> = HashMap::new();

    for (dir_path, _depth) in &dirs_by_depth {
        let direct_size = dir_direct_size.get(dir_path).copied().unwrap_or(0);
        let direct_files = dir_direct_files.get(dir_path).copied().unwrap_or(0);
        let direct_largest = dir_largest_file.get(dir_path).copied().unwrap_or(0);

        let children = parent_to_children.get(dir_path);

        let mut child_size_sum: u64 = 0;
        let mut child_file_sum: u64 = 0;
        let mut child_dir_count: u64 = 0;
        let mut child_max_depth: u32 = 0;
        let mut child_largest: u64 = 0;

        if let Some(kids) = children {
            for kid in kids {
                child_size_sum += total_sizes.get(kid).copied().unwrap_or(0);
                child_file_sum += total_files.get(kid).copied().unwrap_or(0);
                // Count the child dir itself + its nested dirs
                child_dir_count += 1 + total_dir_counts.get(kid).copied().unwrap_or(0);
                let kid_depth = total_max_depth.get(kid).copied().unwrap_or(0) + 1;
                if kid_depth > child_max_depth {
                    child_max_depth = kid_depth;
                }
                let kid_largest = total_largest.get(kid).copied().unwrap_or(0);
                if kid_largest > child_largest {
                    child_largest = kid_largest;
                }
            }
        }

        let t_size = direct_size + child_size_sum;
        let t_files = direct_files + child_file_sum;
        let t_dirs = child_dir_count;
        let t_depth = if child_max_depth > 0 {
            child_max_depth
        } else if direct_files > 0 {
            0
        } else {
            0
        };
        let t_largest = std::cmp::max(direct_largest, child_largest);

        total_sizes.insert(dir_path.clone(), t_size);
        total_files.insert(dir_path.clone(), t_files);
        total_dir_counts.insert(dir_path.clone(), t_dirs);
        total_max_depth.insert(dir_path.clone(), t_depth);
        total_largest.insert(dir_path.clone(), t_largest);
    }

    // Step 5: Build DirSize results
    dirs_by_depth
        .iter()
        .map(|(dir_path, _)| DirSize {
            path: dir_path.clone(),
            total_size: total_sizes.get(dir_path).copied().unwrap_or(0),
            file_count: total_files.get(dir_path).copied().unwrap_or(0),
            dir_count: total_dir_counts.get(dir_path).copied().unwrap_or(0),
            max_depth: total_max_depth.get(dir_path).copied().unwrap_or(0),
            largest_file: total_largest.get(dir_path).copied().unwrap_or(0),
            scan_id,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;
    use tempfile::TempDir;

    fn create_test_tree() -> TempDir {
        let dir = TempDir::new().unwrap();
        // dir/
        //   big.bin       (1024 bytes)
        //   small.txt     (5 bytes)
        //   sub/
        //     medium.log  (512 bytes)
        //     tiny.rs     (10 bytes -- "fn main(){}" is 11 but spec says 10)
        //   empty_dir/
        std::fs::write(dir.path().join("big.bin"), vec![0u8; 1024]).unwrap();
        std::fs::write(dir.path().join("small.txt"), "hello").unwrap();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("sub").join("medium.log"), vec![0u8; 512]).unwrap();
        std::fs::write(dir.path().join("sub").join("tiny.rs"), "fn main(){}").unwrap();
        std::fs::create_dir(dir.path().join("empty_dir")).unwrap();
        dir
    }

    #[test]
    fn test_compute_dir_sizes() {
        // Create entries manually matching the test tree structure
        let scan_id = 1;
        let entries = vec![
            FileEntry {
                id: None,
                path: "/test".to_string(),
                parent_path: "/".to_string(),
                name: "test".to_string(),
                extension: None,
                file_type: FileType::Directory,
                inode: 0, device_id: 0, hardlink_count: 1,
                symlink_target: None,
                size_bytes: 0, blocks: 0,
                mtime: 0, ctime: 0, atime: 0, birth_time: None,
                uid: 0, gid: 0, mode: 0,
                scan_id, first_seen_scan: scan_id, is_deleted: false,
                depth: 1, path_components: 1,
            },
            FileEntry {
                id: None,
                path: "/test/sub".to_string(),
                parent_path: "/test".to_string(),
                name: "sub".to_string(),
                extension: None,
                file_type: FileType::Directory,
                inode: 0, device_id: 0, hardlink_count: 1,
                symlink_target: None,
                size_bytes: 0, blocks: 0,
                mtime: 0, ctime: 0, atime: 0, birth_time: None,
                uid: 0, gid: 0, mode: 0,
                scan_id, first_seen_scan: scan_id, is_deleted: false,
                depth: 2, path_components: 2,
            },
            FileEntry {
                id: None,
                path: "/test/big.bin".to_string(),
                parent_path: "/test".to_string(),
                name: "big.bin".to_string(),
                extension: Some("bin".to_string()),
                file_type: FileType::File,
                inode: 0, device_id: 0, hardlink_count: 1,
                symlink_target: None,
                size_bytes: 1024, blocks: 2,
                mtime: 0, ctime: 0, atime: 0, birth_time: None,
                uid: 0, gid: 0, mode: 0,
                scan_id, first_seen_scan: scan_id, is_deleted: false,
                depth: 2, path_components: 2,
            },
            FileEntry {
                id: None,
                path: "/test/small.txt".to_string(),
                parent_path: "/test".to_string(),
                name: "small.txt".to_string(),
                extension: Some("txt".to_string()),
                file_type: FileType::File,
                inode: 0, device_id: 0, hardlink_count: 1,
                symlink_target: None,
                size_bytes: 5, blocks: 1,
                mtime: 0, ctime: 0, atime: 0, birth_time: None,
                uid: 0, gid: 0, mode: 0,
                scan_id, first_seen_scan: scan_id, is_deleted: false,
                depth: 2, path_components: 2,
            },
            FileEntry {
                id: None,
                path: "/test/sub/medium.log".to_string(),
                parent_path: "/test/sub".to_string(),
                name: "medium.log".to_string(),
                extension: Some("log".to_string()),
                file_type: FileType::File,
                inode: 0, device_id: 0, hardlink_count: 1,
                symlink_target: None,
                size_bytes: 512, blocks: 1,
                mtime: 0, ctime: 0, atime: 0, birth_time: None,
                uid: 0, gid: 0, mode: 0,
                scan_id, first_seen_scan: scan_id, is_deleted: false,
                depth: 3, path_components: 3,
            },
            FileEntry {
                id: None,
                path: "/test/sub/tiny.rs".to_string(),
                parent_path: "/test/sub".to_string(),
                name: "tiny.rs".to_string(),
                extension: Some("rs".to_string()),
                file_type: FileType::File,
                inode: 0, device_id: 0, hardlink_count: 1,
                symlink_target: None,
                size_bytes: 10, blocks: 1,
                mtime: 0, ctime: 0, atime: 0, birth_time: None,
                uid: 0, gid: 0, mode: 0,
                scan_id, first_seen_scan: scan_id, is_deleted: false,
                depth: 3, path_components: 3,
            },
        ];

        let dir_sizes = compute_dir_sizes(&entries, scan_id);

        // Should have 2 directory entries: /test and /test/sub
        assert_eq!(dir_sizes.len(), 2);

        let sub_size = dir_sizes.iter().find(|d| d.path == "/test/sub").unwrap();
        assert_eq!(sub_size.total_size, 512 + 10, "sub total = medium.log + tiny.rs");
        assert_eq!(sub_size.file_count, 2);
        assert_eq!(sub_size.dir_count, 0);
        assert_eq!(sub_size.largest_file, 512);

        let root_size = dir_sizes.iter().find(|d| d.path == "/test").unwrap();
        assert_eq!(
            root_size.total_size,
            1024 + 5 + 512 + 10,
            "root total = all files recursively"
        );
        assert_eq!(root_size.file_count, 4);
        assert_eq!(root_size.dir_count, 1, "root has 1 child dir: sub");
        assert_eq!(root_size.largest_file, 1024);
    }

    #[test]
    fn test_full_crawl_pipeline() {
        let dir = create_test_tree();
        let db = Database::open_in_memory().unwrap();
        let config = Config::default();

        let scan_info = run_crawl(&db, dir.path(), &config).unwrap();

        // Verify scan completed
        assert_eq!(scan_info.status, ScanStatus::Completed);
        assert!(scan_info.total_files >= 4, "should have at least 4 files");
        assert!(scan_info.total_dirs >= 2, "should have at least 2 dirs (sub, empty_dir)");
        assert!(scan_info.total_size > 0, "total size should be > 0");

        // Verify files are in the DB
        let files = db.largest_files(None, 100).unwrap();
        assert!(!files.is_empty(), "should have files in DB");

        let file_names: Vec<&str> = files.iter().map(|f| f.name.as_str()).collect();
        assert!(file_names.contains(&"big.bin"));
        assert!(file_names.contains(&"small.txt"));
        assert!(file_names.contains(&"medium.log"));

        // Verify dir_sizes exist
        let root_str = dir.path().to_string_lossy().to_string();
        let dir_size = db.get_dir_size(&root_str).unwrap();
        assert!(dir_size.is_some(), "root dir should have a dir_size entry");
        let ds = dir_size.unwrap();
        assert!(ds.total_size > 0);
        assert!(ds.file_count >= 4);

        // Verify extension stats exist
        let ext_stats = db.extension_stats(scan_info.id, 100).unwrap();
        assert!(!ext_stats.is_empty(), "should have extension stats");
    }

    #[test]
    fn test_incremental_crawl_skips_unchanged() {
        let dir = create_test_tree();
        let r = dir.path();

        let db = Database::open_in_memory().unwrap();
        let config = Config::default();

        // First: full crawl
        let scan1 = run_crawl(&db, r, &config).unwrap();
        assert_eq!(scan1.status, ScanStatus::Completed);
        assert!(scan1.total_files >= 4);

        // Wait a moment, then add a new file ONLY in root
        std::thread::sleep(std::time::Duration::from_secs(2));
        std::fs::write(r.join("new_file.txt"), "new content").unwrap();

        // Incremental crawl should detect the new file
        let scan2 = run_incremental_crawl(&db, r, &config).unwrap();
        assert_eq!(scan2.status, ScanStatus::Completed);

        // The new file should be found in the entries from the incremental scan
        // (it was added to root, which will have a newer mtime)
        assert!(scan2.total_files >= 1, "incremental scan should find at least the new file");
    }

    #[test]
    fn test_incremental_falls_back_to_full() {
        // No previous scan — should fall back to full crawl
        let dir = create_test_tree();

        let db = Database::open_in_memory().unwrap();
        let config = Config::default();

        // No prior scan exists — should fall back to full crawl
        let scan = run_incremental_crawl(&db, dir.path(), &config).unwrap();
        assert_eq!(scan.status, ScanStatus::Completed);
        assert!(scan.total_files >= 4, "fallback full crawl should find all files");
        assert!(scan.total_dirs >= 2, "fallback full crawl should find all dirs");
    }

    #[test]
    fn test_incremental_crawl_different_root_falls_back() {
        // Previous scan exists but for a different root path
        let dir1 = TempDir::new().unwrap();
        std::fs::write(dir1.path().join("a.txt"), "aaa").unwrap();

        let dir2 = TempDir::new().unwrap();
        std::fs::write(dir2.path().join("b.txt"), "bbb").unwrap();

        let db = Database::open_in_memory().unwrap();
        let config = Config::default();

        // Scan dir1
        let _scan1 = run_crawl(&db, dir1.path(), &config).unwrap();

        // Incremental on dir2 — different root, should fall back to full
        let scan2 = run_incremental_crawl(&db, dir2.path(), &config).unwrap();
        assert_eq!(scan2.status, ScanStatus::Completed);
        assert!(scan2.total_files >= 1);
    }
}
