use crate::db::Database;
use crate::models::*;
use anyhow::Result;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use tracing::warn;

/// Find duplicate files using a tiered approach:
/// 1. Group by size (from DB, free)
/// 2. Partial hash: first 4KB with xxhash64
/// 3. Full hash: complete file xxhash64
pub fn find_duplicates(
    db: &Database,
    path: Option<&str>,
    min_size: u64,
    extensions: Option<&[String]>,
    limit: u32,
) -> Result<Vec<DuplicateGroup>> {
    // Tier 1: group by size from DB
    let size_groups = db.files_with_duplicate_sizes(path, min_size, extensions)?;

    let mut results: Vec<DuplicateGroup> = Vec::new();

    for (_size, entries) in size_groups {
        // Tier 2: partial hash (first 4KB)
        let mut partial_groups: HashMap<u64, Vec<&FileEntry>> = HashMap::new();
        for entry in &entries {
            match hash_partial(&entry.path) {
                Ok(h) => {
                    partial_groups.entry(h).or_default().push(entry);
                }
                Err(e) => {
                    warn!("skipping file {} for partial hash: {}", entry.path, e);
                }
            }
        }

        // Only keep groups with 2+ files
        for (_partial_hash, candidates) in partial_groups {
            if candidates.len() < 2 {
                continue;
            }

            // Tier 3: full hash
            let mut full_groups: HashMap<u64, Vec<&FileEntry>> = HashMap::new();
            for entry in &candidates {
                match hash_full(&entry.path) {
                    Ok(h) => {
                        full_groups.entry(h).or_default().push(entry);
                    }
                    Err(e) => {
                        warn!("skipping file {} for full hash: {}", entry.path, e);
                    }
                }
            }

            // Build DuplicateGroup for groups with 2+ confirmed duplicates
            for (full_hash, group) in full_groups {
                if group.len() < 2 {
                    continue;
                }

                let size_bytes = group[0].size_bytes;
                let wasted_bytes = size_bytes * (group.len() as u64 - 1);

                let files: Vec<DuplicateFile> = group
                    .iter()
                    .map(|e| DuplicateFile {
                        path: e.path.clone(),
                        modified: e.mtime,
                        accessed: e.atime,
                    })
                    .collect();

                results.push(DuplicateGroup {
                    hash: format!("{:016x}", full_hash),
                    size_bytes,
                    wasted_bytes,
                    files,
                });
            }
        }
    }

    // Sort by wasted_bytes descending
    results.sort_by(|a, b| b.wasted_bytes.cmp(&a.wasted_bytes));

    // Truncate to limit
    results.truncate(limit as usize);

    Ok(results)
}

/// Hash the first 4KB of a file using xxhash64.
fn hash_partial(path: &str) -> Result<u64> {
    let mut file = File::open(path)?;
    let mut buf = [0u8; 4096];
    let n = file.read(&mut buf)?;
    Ok(xxhash_rust::xxh3::xxh3_64(&buf[..n]))
}

/// Hash the complete file contents using xxhash64.
fn hash_full(path: &str) -> Result<u64> {
    let data = std::fs::read(path)?;
    Ok(xxhash_rust::xxh3::xxh3_64(&data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;
    use crate::models::FileType;
    use tempfile::TempDir;

    /// Create a test FileEntry with reasonable defaults.
    fn make_entry(
        path: &str,
        parent: &str,
        name: &str,
        ext: Option<&str>,
        size: u64,
        scan_id: i64,
    ) -> FileEntry {
        FileEntry {
            id: None,
            path: path.to_string(),
            parent_path: parent.to_string(),
            name: name.to_string(),
            extension: ext.map(|s| s.to_string()),
            file_type: FileType::File,
            inode: 0,
            device_id: 0,
            hardlink_count: 1,
            symlink_target: None,
            size_bytes: size,
            blocks: (size + 511) / 512,
            mtime: 1700000000,
            ctime: 1700000000,
            atime: 1700000000,
            birth_time: Some(1700000000),
            uid: 501,
            gid: 20,
            mode: 0o644,
            scan_id,
            first_seen_scan: scan_id,
            is_deleted: false,
            depth: 2,
            path_components: 2,
        }
    }

    fn setup_duplicate_test() -> (Database, TempDir) {
        let dir = TempDir::new().unwrap();
        let db = Database::open_in_memory().unwrap();
        let scan_id = db.create_scan(dir.path().to_str().unwrap()).unwrap();

        // Create real files with known content
        let content_a = b"This is duplicate content A, repeated for testing purposes.";
        let content_b = b"This is different content B, also for testing.";

        let file1 = dir.path().join("file1.txt");
        let file2 = dir.path().join("file2.txt");
        let file3 = dir.path().join("file3.txt");
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        let file4 = dir.path().join("sub/file4.txt");

        std::fs::write(&file1, content_a).unwrap();
        std::fs::write(&file2, content_a).unwrap(); // duplicate of file1
        std::fs::write(&file3, content_b).unwrap(); // different
        std::fs::write(&file4, content_a).unwrap(); // another duplicate

        let size_a = content_a.len() as u64;
        let size_b = content_b.len() as u64;

        let dir_str = dir.path().to_str().unwrap();
        let entries = vec![
            make_entry(
                file1.to_str().unwrap(),
                dir_str,
                "file1.txt",
                Some("txt"),
                size_a,
                scan_id,
            ),
            make_entry(
                file2.to_str().unwrap(),
                dir_str,
                "file2.txt",
                Some("txt"),
                size_a,
                scan_id,
            ),
            make_entry(
                file3.to_str().unwrap(),
                dir_str,
                "file3.txt",
                Some("txt"),
                size_b,
                scan_id,
            ),
            make_entry(
                file4.to_str().unwrap(),
                &format!("{}/sub", dir_str),
                "file4.txt",
                Some("txt"),
                size_a,
                scan_id,
            ),
        ];

        db.insert_files(&entries).unwrap();

        (db, dir)
    }

    #[test]
    fn test_find_duplicates_basic() {
        let (db, _dir) = setup_duplicate_test();

        let groups = find_duplicates(&db, None, 0, None, 100).unwrap();

        // file1, file2, file4 should be detected as duplicates (same content_a)
        // file3 has different content so should not appear
        assert_eq!(groups.len(), 1, "should find exactly one duplicate group");

        let group = &groups[0];
        assert_eq!(group.files.len(), 3, "group should have 3 files");

        let content_a = b"This is duplicate content A, repeated for testing purposes.";
        let size_a = content_a.len() as u64;
        assert_eq!(group.size_bytes, size_a);
        // wasted = size * (3 - 1) = size * 2
        assert_eq!(group.wasted_bytes, size_a * 2);
    }

    #[test]
    fn test_find_duplicates_min_size_filter() {
        let (db, _dir) = setup_duplicate_test();

        // Set min_size above the file sizes (content_a is ~59 bytes)
        let groups = find_duplicates(&db, None, 10_000, None, 100).unwrap();
        assert!(groups.is_empty(), "should find no duplicates above min_size");
    }

    #[test]
    fn test_find_duplicates_no_duplicates() {
        let dir = TempDir::new().unwrap();
        let db = Database::open_in_memory().unwrap();
        let scan_id = db.create_scan(dir.path().to_str().unwrap()).unwrap();

        // All unique files with different sizes
        let file1 = dir.path().join("unique1.txt");
        let file2 = dir.path().join("unique2.txt");
        let file3 = dir.path().join("unique3.txt");

        std::fs::write(&file1, b"content one").unwrap();
        std::fs::write(&file2, b"content two!!").unwrap();
        std::fs::write(&file3, b"content three!!!!").unwrap();

        let dir_str = dir.path().to_str().unwrap();
        let entries = vec![
            make_entry(
                file1.to_str().unwrap(),
                dir_str,
                "unique1.txt",
                Some("txt"),
                11,
                scan_id,
            ),
            make_entry(
                file2.to_str().unwrap(),
                dir_str,
                "unique2.txt",
                Some("txt"),
                13,
                scan_id,
            ),
            make_entry(
                file3.to_str().unwrap(),
                dir_str,
                "unique3.txt",
                Some("txt"),
                17,
                scan_id,
            ),
        ];

        db.insert_files(&entries).unwrap();

        let groups = find_duplicates(&db, None, 0, None, 100).unwrap();
        assert!(groups.is_empty(), "all unique files should yield no duplicates");
    }

    #[test]
    fn test_hash_partial() {
        let dir = TempDir::new().unwrap();

        let file1 = dir.path().join("same1.bin");
        let file2 = dir.path().join("same2.bin");
        let file3 = dir.path().join("diff.bin");

        let content = b"identical content for hashing test";
        std::fs::write(&file1, content).unwrap();
        std::fs::write(&file2, content).unwrap();
        std::fs::write(&file3, b"different content entirely").unwrap();

        let h1 = hash_partial(file1.to_str().unwrap()).unwrap();
        let h2 = hash_partial(file2.to_str().unwrap()).unwrap();
        let h3 = hash_partial(file3.to_str().unwrap()).unwrap();

        assert_eq!(h1, h2, "identical files should have same partial hash");
        assert_ne!(h1, h3, "different files should have different partial hash");
    }

    #[test]
    fn test_hash_full() {
        let dir = TempDir::new().unwrap();

        let file1 = dir.path().join("full1.bin");
        let file2 = dir.path().join("full2.bin");
        let file3 = dir.path().join("fulldiff.bin");

        let content = b"identical content for full hash test";
        std::fs::write(&file1, content).unwrap();
        std::fs::write(&file2, content).unwrap();
        std::fs::write(&file3, b"totally different file data").unwrap();

        let h1 = hash_full(file1.to_str().unwrap()).unwrap();
        let h2 = hash_full(file2.to_str().unwrap()).unwrap();
        let h3 = hash_full(file3.to_str().unwrap()).unwrap();

        assert_eq!(h1, h2, "identical files should have same full hash");
        assert_ne!(h1, h3, "different files should have different full hash");
    }
}
