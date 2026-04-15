use anyhow::{Context, Result};
use rusqlite::params;

use crate::models::{
    DirSize, ExtensionStat, FileEntry, FileType, ScanInfo, ScanStatus,
};

use super::Database;

// ---------------------------------------------------------------------------
// Helper structs
// ---------------------------------------------------------------------------

/// Statistics collected during a scan, used to finalize a scan record.
pub struct ScanStats {
    pub total_files: u64,
    pub total_dirs: u64,
    pub total_size: u64,
    pub files_added: u64,
    pub files_modified: u64,
    pub files_deleted: u64,
}

/// Criteria for searching files with flexible filters.
pub struct SearchCriteria {
    pub path: Option<String>,
    pub name_pattern: Option<String>,
    pub min_size: Option<u64>,
    pub max_size: Option<u64>,
    pub extensions: Option<Vec<String>>,
    pub modified_after: Option<i64>,
    pub modified_before: Option<i64>,
    pub accessed_after: Option<i64>,
    pub accessed_before: Option<i64>,
    pub limit: u32,
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Build the upper bound for a path prefix range scan.
/// Replaces the trailing '/' with '0' (ASCII 48, one after '/' which is 47).
fn path_upper_bound(path: &str) -> String {
    let mut bound = path.to_string();
    if !bound.ends_with('/') {
        bound.push('/');
    }
    bound.pop();
    bound.push('0');
    bound
}

/// Map a rusqlite Row to a FileEntry. Expects columns in schema order.
fn file_entry_from_row(row: &rusqlite::Row) -> rusqlite::Result<FileEntry> {
    Ok(FileEntry {
        id: row.get(0)?,
        path: row.get(1)?,
        parent_path: row.get(2)?,
        name: row.get(3)?,
        extension: row.get(4)?,
        file_type: FileType::from_u8(row.get::<_, u8>(5)?),
        inode: row.get(6)?,
        device_id: row.get(7)?,
        hardlink_count: row.get(8)?,
        symlink_target: row.get(9)?,
        size_bytes: row.get(10)?,
        blocks: row.get(11)?,
        mtime: row.get(12)?,
        ctime: row.get(13)?,
        atime: row.get(14)?,
        birth_time: row.get(15)?,
        uid: row.get(16)?,
        gid: row.get(17)?,
        mode: row.get(18)?,
        scan_id: row.get(19)?,
        first_seen_scan: row.get(20)?,
        is_deleted: row.get::<_, i32>(21)? != 0,
        depth: row.get(22)?,
        path_components: row.get(23)?,
    })
}

/// The SELECT column list matching file_entry_from_row expectations.
const FILE_COLUMNS: &str = "id, path, parent_path, name, extension, file_type, \
    inode, device_id, hardlink_count, symlink_target, size_bytes, blocks, \
    mtime, ctime, atime, birth_time, uid, gid, mode, scan_id, first_seen_scan, \
    is_deleted, depth, path_components";

// ---------------------------------------------------------------------------
// Database impl
// ---------------------------------------------------------------------------

impl Database {
    // -----------------------------------------------------------------------
    // Scan management
    // -----------------------------------------------------------------------

    /// Create a new scan record, return its ID.
    pub fn create_scan(&self, root_path: &str) -> Result<i64> {
        let conn = self.conn();
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT INTO scans (root_path, started_at, status) VALUES (?1, ?2, 'running')",
            params![root_path, now],
        )
        .context("failed to create scan")?;
        Ok(conn.last_insert_rowid())
    }

    /// Mark a scan as completed with stats.
    pub fn complete_scan(&self, scan_id: i64, stats: &ScanStats) -> Result<()> {
        let conn = self.conn();
        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "UPDATE scans SET
                completed_at = ?1,
                total_files = ?2,
                total_dirs = ?3,
                total_size = ?4,
                files_added = ?5,
                files_modified = ?6,
                files_deleted = ?7,
                status = 'completed'
            WHERE id = ?8",
            params![
                now,
                stats.total_files,
                stats.total_dirs,
                stats.total_size,
                stats.files_added,
                stats.files_modified,
                stats.files_deleted,
                scan_id,
            ],
        )
        .context("failed to complete scan")?;
        Ok(())
    }

    /// Get the most recent completed scan.
    pub fn latest_scan(&self) -> Result<Option<ScanInfo>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT id, root_path, started_at, completed_at, total_files, total_dirs,
                    total_size, files_added, files_modified, files_deleted, status
             FROM scans
             WHERE status = 'completed'
             ORDER BY completed_at DESC
             LIMIT 1",
        )?;

        let mut rows = stmt.query_map([], |row| {
            let status_str: String = row.get(10)?;
            Ok(ScanInfo {
                id: row.get(0)?,
                root_path: row.get(1)?,
                started_at: row.get(2)?,
                completed_at: row.get(3)?,
                total_files: row.get(4)?,
                total_dirs: row.get(5)?,
                total_size: row.get(6)?,
                files_added: row.get(7)?,
                files_modified: row.get(8)?,
                files_deleted: row.get(9)?,
                status: ScanStatus::from_str(&status_str).unwrap_or(ScanStatus::Running),
            })
        })?;

        match rows.next() {
            Some(Ok(info)) => Ok(Some(info)),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }

    // -----------------------------------------------------------------------
    // File entry operations
    // -----------------------------------------------------------------------

    /// Insert or update a batch of file entries in a single transaction.
    pub fn insert_files(&self, entries: &[FileEntry]) -> Result<()> {
        let conn = self.conn();
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR REPLACE INTO files (
                    path, parent_path, name, extension, file_type,
                    inode, device_id, hardlink_count, symlink_target,
                    size_bytes, blocks, mtime, ctime, atime, birth_time,
                    uid, gid, mode, scan_id, first_seen_scan,
                    is_deleted, depth, path_components
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5,
                    ?6, ?7, ?8, ?9,
                    ?10, ?11, ?12, ?13, ?14, ?15,
                    ?16, ?17, ?18, ?19, ?20,
                    ?21, ?22, ?23
                )",
            )?;
            for entry in entries {
                stmt.execute(params![
                    entry.path,
                    entry.parent_path,
                    entry.name,
                    entry.extension,
                    entry.file_type as u8,
                    entry.inode,
                    entry.device_id,
                    entry.hardlink_count,
                    entry.symlink_target,
                    entry.size_bytes,
                    entry.blocks,
                    entry.mtime,
                    entry.ctime,
                    entry.atime,
                    entry.birth_time,
                    entry.uid,
                    entry.gid,
                    entry.mode,
                    entry.scan_id,
                    entry.first_seen_scan,
                    entry.is_deleted as i32,
                    entry.depth,
                    entry.path_components,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Mark entries from earlier scans as deleted within a path prefix.
    /// Returns the count of rows marked deleted.
    pub fn mark_deleted(&self, current_scan_id: i64, root_path: &str) -> Result<u64> {
        let conn = self.conn();
        let upper = path_upper_bound(root_path);
        let count = conn.execute(
            "UPDATE files SET is_deleted = 1
             WHERE scan_id < ?1
               AND is_deleted = 0
               AND path >= ?2 AND path < ?3",
            params![current_scan_id, root_path, upper],
        )?;
        Ok(count as u64)
    }

    // -----------------------------------------------------------------------
    // Dir sizes
    // -----------------------------------------------------------------------

    /// Insert or replace pre-computed directory sizes.
    pub fn insert_dir_sizes(&self, sizes: &[DirSize]) -> Result<()> {
        let conn = self.conn();
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR REPLACE INTO dir_sizes (
                    path, total_size, file_count, dir_count, max_depth, largest_file, scan_id
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?;
            for ds in sizes {
                stmt.execute(params![
                    ds.path,
                    ds.total_size,
                    ds.file_count,
                    ds.dir_count,
                    ds.max_depth,
                    ds.largest_file,
                    ds.scan_id,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Get the recursive size of a specific directory. O(1) lookup.
    pub fn get_dir_size(&self, path: &str) -> Result<Option<DirSize>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT path, total_size, file_count, dir_count, max_depth, largest_file, scan_id
             FROM dir_sizes WHERE path = ?1",
        )?;

        let mut rows = stmt.query_map(params![path], |row| {
            Ok(DirSize {
                path: row.get(0)?,
                total_size: row.get(1)?,
                file_count: row.get(2)?,
                dir_count: row.get(3)?,
                max_depth: row.get(4)?,
                largest_file: row.get(5)?,
                scan_id: row.get(6)?,
            })
        })?;

        match rows.next() {
            Some(Ok(ds)) => Ok(Some(ds)),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }

    /// Get top N largest directories by total_size.
    pub fn largest_dirs(&self, path: Option<&str>, limit: u32) -> Result<Vec<DirSize>> {
        let conn = self.conn();

        let (sql, bound_params): (String, Vec<Box<dyn rusqlite::types::ToSql>>) = match path {
            Some(p) => {
                let upper = path_upper_bound(p);
                (
                    "SELECT path, total_size, file_count, dir_count, max_depth, largest_file, scan_id
                     FROM dir_sizes
                     WHERE path >= ?1 AND path < ?2
                     ORDER BY total_size DESC
                     LIMIT ?3"
                        .to_string(),
                    vec![Box::new(p.to_string()), Box::new(upper), Box::new(limit)],
                )
            }
            None => (
                "SELECT path, total_size, file_count, dir_count, max_depth, largest_file, scan_id
                 FROM dir_sizes
                 ORDER BY total_size DESC
                 LIMIT ?1"
                    .to_string(),
                vec![Box::new(limit)],
            ),
        };

        let mut stmt = conn.prepare(&sql)?;
        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            bound_params.iter().map(|b| b.as_ref()).collect();
        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            Ok(DirSize {
                path: row.get(0)?,
                total_size: row.get(1)?,
                file_count: row.get(2)?,
                dir_count: row.get(3)?,
                max_depth: row.get(4)?,
                largest_file: row.get(5)?,
                scan_id: row.get(6)?,
            })
        })?;

        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
    }

    // -----------------------------------------------------------------------
    // Query functions
    // -----------------------------------------------------------------------

    /// Top N largest files, optionally within a path prefix.
    pub fn largest_files(&self, path: Option<&str>, limit: u32) -> Result<Vec<FileEntry>> {
        let conn = self.conn();

        let (sql, bound_params): (String, Vec<Box<dyn rusqlite::types::ToSql>>) = match path {
            Some(p) => {
                let prefix = if p.ends_with('/') {
                    p.to_string()
                } else {
                    format!("{}/", p)
                };
                let upper = path_upper_bound(&prefix);
                (
                    format!(
                        "SELECT {} FROM files
                         WHERE file_type = 0 AND is_deleted = 0
                           AND path >= ?1 AND path < ?2
                         ORDER BY size_bytes DESC
                         LIMIT ?3",
                        FILE_COLUMNS
                    ),
                    vec![Box::new(prefix), Box::new(upper), Box::new(limit)],
                )
            }
            None => (
                format!(
                    "SELECT {} FROM files
                     WHERE file_type = 0 AND is_deleted = 0
                     ORDER BY size_bytes DESC
                     LIMIT ?1",
                    FILE_COLUMNS
                ),
                vec![Box::new(limit)],
            ),
        };

        let mut stmt = conn.prepare(&sql)?;
        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            bound_params.iter().map(|b| b.as_ref()).collect();
        let rows = stmt.query_map(params_refs.as_slice(), file_entry_from_row)?;

        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
    }

    /// List immediate children of a directory, sorted by size desc.
    pub fn list_children(&self, parent: &str, limit: u32) -> Result<Vec<FileEntry>> {
        let conn = self.conn();
        let sql = format!(
            "SELECT {} FROM files
             WHERE parent_path = ?1 AND is_deleted = 0
             ORDER BY size_bytes DESC
             LIMIT ?2",
            FILE_COLUMNS
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![parent, limit], file_entry_from_row)?;

        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
    }

    /// Search files by name using FTS5.
    pub fn search_files_fts(&self, query: &str, limit: u32) -> Result<Vec<FileEntry>> {
        let conn = self.conn();
        let sql = format!(
            "SELECT {} FROM files
             WHERE id IN (
                 SELECT rowid FROM files_fts WHERE files_fts MATCH ?1
             ) AND is_deleted = 0
             ORDER BY size_bytes DESC
             LIMIT ?2",
            FILE_COLUMNS
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![query, limit], file_entry_from_row)?;

        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
    }

    /// Compute and store extension statistics for a scan.
    pub fn compute_extension_stats(&self, scan_id: i64) -> Result<()> {
        let conn = self.conn();
        conn.execute(
            "INSERT OR REPLACE INTO extension_stats (extension, scan_id, file_count, total_size, avg_size, largest_size)
             SELECT extension, ?1, COUNT(*), SUM(size_bytes), AVG(size_bytes), MAX(size_bytes)
             FROM files
             WHERE file_type = 0 AND is_deleted = 0 AND extension IS NOT NULL AND scan_id = ?1
             GROUP BY extension",
            params![scan_id],
        )?;
        Ok(())
    }

    /// Get extension statistics for a scan.
    pub fn extension_stats(&self, scan_id: i64, limit: u32) -> Result<Vec<ExtensionStat>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT extension, file_count, total_size, avg_size, largest_size
             FROM extension_stats
             WHERE scan_id = ?1
             ORDER BY total_size DESC
             LIMIT ?2",
        )?;

        let rows = stmt.query_map(params![scan_id, limit], |row| {
            Ok(ExtensionStat {
                extension: row.get(0)?,
                file_count: row.get(1)?,
                total_size: row.get(2)?,
                avg_size: row.get(3)?,
                largest_size: row.get(4)?,
            })
        })?;

        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
    }

    /// Find files matching various criteria.
    pub fn find_files(&self, criteria: &SearchCriteria) -> Result<Vec<FileEntry>> {
        let conn = self.conn();

        let mut conditions = vec!["file_type = 0".to_string(), "is_deleted = 0".to_string()];
        let mut bound_params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        let mut param_idx = 1u32;

        if let Some(ref p) = criteria.path {
            let prefix = if p.ends_with('/') {
                p.to_string()
            } else {
                format!("{}/", p)
            };
            let upper = path_upper_bound(&prefix);
            conditions.push(format!("path >= ?{}", param_idx));
            bound_params.push(Box::new(prefix));
            param_idx += 1;
            conditions.push(format!("path < ?{}", param_idx));
            bound_params.push(Box::new(upper));
            param_idx += 1;
        }

        if let Some(ref pattern) = criteria.name_pattern {
            conditions.push(format!("name LIKE ?{}", param_idx));
            bound_params.push(Box::new(pattern.clone()));
            param_idx += 1;
        }

        if let Some(min) = criteria.min_size {
            conditions.push(format!("size_bytes >= ?{}", param_idx));
            bound_params.push(Box::new(min));
            param_idx += 1;
        }

        if let Some(max) = criteria.max_size {
            conditions.push(format!("size_bytes <= ?{}", param_idx));
            bound_params.push(Box::new(max));
            param_idx += 1;
        }

        if let Some(ref exts) = criteria.extensions {
            if !exts.is_empty() {
                let placeholders: Vec<String> = exts
                    .iter()
                    .enumerate()
                    .map(|(i, _)| format!("?{}", param_idx + i as u32))
                    .collect();
                conditions.push(format!("extension IN ({})", placeholders.join(", ")));
                for ext in exts {
                    bound_params.push(Box::new(ext.clone()));
                }
                param_idx += exts.len() as u32;
            }
        }

        if let Some(after) = criteria.modified_after {
            conditions.push(format!("mtime >= ?{}", param_idx));
            bound_params.push(Box::new(after));
            param_idx += 1;
        }

        if let Some(before) = criteria.modified_before {
            conditions.push(format!("mtime <= ?{}", param_idx));
            bound_params.push(Box::new(before));
            param_idx += 1;
        }

        if let Some(after) = criteria.accessed_after {
            conditions.push(format!("atime >= ?{}", param_idx));
            bound_params.push(Box::new(after));
            param_idx += 1;
        }

        if let Some(before) = criteria.accessed_before {
            conditions.push(format!("atime <= ?{}", param_idx));
            bound_params.push(Box::new(before));
            param_idx += 1;
        }

        // limit
        let limit_placeholder = format!("?{}", param_idx);
        bound_params.push(Box::new(criteria.limit));

        let where_clause = conditions.join(" AND ");
        let sql = format!(
            "SELECT {} FROM files WHERE {} ORDER BY size_bytes DESC LIMIT {}",
            FILE_COLUMNS, where_clause, limit_placeholder
        );

        let mut stmt = conn.prepare(&sql)?;
        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            bound_params.iter().map(|b| b.as_ref()).collect();
        let rows = stmt.query_map(params_refs.as_slice(), file_entry_from_row)?;

        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::FileType;

    /// Create a test FileEntry with reasonable defaults.
    fn make_entry(
        path: &str,
        parent: &str,
        name: &str,
        ext: Option<&str>,
        file_type: FileType,
        size: u64,
        scan_id: i64,
        depth: u32,
        components: u32,
    ) -> FileEntry {
        FileEntry {
            id: None,
            path: path.to_string(),
            parent_path: parent.to_string(),
            name: name.to_string(),
            extension: ext.map(|s| s.to_string()),
            file_type,
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
            depth,
            path_components: components,
        }
    }

    /// Seed an in-memory DB with a known file tree for testing.
    ///
    /// ```text
    /// /test/                 (dir)
    ///   /test/big.mp4        (1,000,000,000 bytes, ext=mp4)
    ///   /test/small.txt      (1,024 bytes, ext=txt)
    ///   /test/sub/           (dir)
    ///     /test/sub/med.log  (50,000,000 bytes, ext=log)
    ///     /test/sub/tiny.rs  (500 bytes, ext=rs)
    /// ```
    fn test_db_with_data() -> (Database, i64) {
        let db = Database::open_in_memory().expect("open_in_memory");
        let scan_id = db.create_scan("/test").expect("create_scan");

        let entries = vec![
            make_entry("/test", "/", "test", None, FileType::Directory, 0, scan_id, 1, 1),
            make_entry(
                "/test/big.mp4",
                "/test",
                "big.mp4",
                Some("mp4"),
                FileType::File,
                1_000_000_000,
                scan_id,
                2,
                2,
            ),
            make_entry(
                "/test/small.txt",
                "/test",
                "small.txt",
                Some("txt"),
                FileType::File,
                1_024,
                scan_id,
                2,
                2,
            ),
            make_entry(
                "/test/sub",
                "/test",
                "sub",
                None,
                FileType::Directory,
                0,
                scan_id,
                2,
                2,
            ),
            make_entry(
                "/test/sub/med.log",
                "/test/sub",
                "med.log",
                Some("log"),
                FileType::File,
                50_000_000,
                scan_id,
                3,
                3,
            ),
            make_entry(
                "/test/sub/tiny.rs",
                "/test/sub",
                "tiny.rs",
                Some("rs"),
                FileType::File,
                500,
                scan_id,
                3,
                3,
            ),
        ];

        db.insert_files(&entries).expect("insert_files");
        (db, scan_id)
    }

    #[test]
    fn test_create_and_complete_scan() {
        let db = Database::open_in_memory().expect("open_in_memory");
        let scan_id = db.create_scan("/home").expect("create_scan");
        assert!(scan_id > 0);

        // Before completion, latest_scan should return None (no completed scans)
        let latest = db.latest_scan().expect("latest_scan");
        assert!(latest.is_none());

        let stats = ScanStats {
            total_files: 100,
            total_dirs: 10,
            total_size: 999_999,
            files_added: 90,
            files_modified: 5,
            files_deleted: 3,
        };
        db.complete_scan(scan_id, &stats).expect("complete_scan");

        let latest = db.latest_scan().expect("latest_scan").expect("should have a scan");
        assert_eq!(latest.id, scan_id);
        assert_eq!(latest.root_path, "/home");
        assert_eq!(latest.total_files, 100);
        assert_eq!(latest.total_dirs, 10);
        assert_eq!(latest.total_size, 999_999);
        assert_eq!(latest.status, ScanStatus::Completed);
    }

    #[test]
    fn test_insert_and_query_files() {
        let (db, _scan_id) = test_db_with_data();

        // Count all files (including dirs)
        let conn = db.conn();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM files", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 6, "should have 6 entries total");
    }

    #[test]
    fn test_largest_files() {
        let (db, _scan_id) = test_db_with_data();

        let files = db.largest_files(None, 10).expect("largest_files");
        assert_eq!(files.len(), 4, "should have 4 files (not dirs)");
        assert_eq!(files[0].name, "big.mp4");
        assert_eq!(files[0].size_bytes, 1_000_000_000);
        assert_eq!(files[1].name, "med.log");
        assert_eq!(files[1].size_bytes, 50_000_000);
    }

    #[test]
    fn test_largest_files_with_path_filter() {
        let (db, _scan_id) = test_db_with_data();

        let files = db.largest_files(Some("/test/sub"), 10).expect("largest_files");
        assert_eq!(files.len(), 2, "should have 2 files under /test/sub/");
        assert_eq!(files[0].name, "med.log");
        assert_eq!(files[1].name, "tiny.rs");
    }

    #[test]
    fn test_dir_sizes() {
        let (db, scan_id) = test_db_with_data();

        let sizes = vec![
            DirSize {
                path: "/test".to_string(),
                total_size: 1_050_001_524,
                file_count: 4,
                dir_count: 1,
                max_depth: 3,
                largest_file: 1_000_000_000,
                scan_id,
            },
            DirSize {
                path: "/test/sub".to_string(),
                total_size: 50_000_500,
                file_count: 2,
                dir_count: 0,
                max_depth: 1,
                largest_file: 50_000_000,
                scan_id,
            },
        ];

        db.insert_dir_sizes(&sizes).expect("insert_dir_sizes");

        let ds = db
            .get_dir_size("/test")
            .expect("get_dir_size")
            .expect("should find /test");
        assert_eq!(ds.total_size, 1_050_001_524);
        assert_eq!(ds.file_count, 4);

        let ds_sub = db
            .get_dir_size("/test/sub")
            .expect("get_dir_size")
            .expect("should find /test/sub");
        assert_eq!(ds_sub.total_size, 50_000_500);

        let missing = db.get_dir_size("/nonexistent").expect("get_dir_size");
        assert!(missing.is_none());
    }

    #[test]
    fn test_largest_dirs() {
        let (db, scan_id) = test_db_with_data();

        let sizes = vec![
            DirSize {
                path: "/test".to_string(),
                total_size: 1_050_001_524,
                file_count: 4,
                dir_count: 1,
                max_depth: 3,
                largest_file: 1_000_000_000,
                scan_id,
            },
            DirSize {
                path: "/test/sub".to_string(),
                total_size: 50_000_500,
                file_count: 2,
                dir_count: 0,
                max_depth: 1,
                largest_file: 50_000_000,
                scan_id,
            },
        ];

        db.insert_dir_sizes(&sizes).expect("insert_dir_sizes");

        let dirs = db.largest_dirs(None, 10).expect("largest_dirs");
        assert_eq!(dirs.len(), 2);
        assert_eq!(dirs[0].path, "/test");
        assert_eq!(dirs[1].path, "/test/sub");
    }

    #[test]
    fn test_mark_deleted() {
        let (db, scan_id) = test_db_with_data();

        // Create a second scan
        let scan2_id = db.create_scan("/test").expect("create_scan");
        assert!(scan2_id > scan_id);

        // Mark files from scan 1 as deleted under /test/
        let deleted = db.mark_deleted(scan2_id, "/test/").expect("mark_deleted");
        // Should mark 5 entries: big.mp4, small.txt, sub, sub/med.log, sub/tiny.rs
        // The /test dir itself has path="/test" which is outside the range >= "/test/"
        assert_eq!(deleted, 5);

        // Verify they are marked deleted
        let files = db.largest_files(None, 100).expect("largest_files");
        assert_eq!(files.len(), 0, "all files should be marked deleted");
    }

    #[test]
    fn test_search_fts() {
        let (db, _scan_id) = test_db_with_data();

        // Search for "big"
        let results = db.search_files_fts("big", 10).expect("search_files_fts");
        assert!(!results.is_empty(), "should find big.mp4");
        assert!(results.iter().any(|f| f.name == "big.mp4"));

        // Search for "mp4" -- FTS matches on name tokens
        let results = db.search_files_fts("mp4", 10).expect("search_files_fts");
        assert!(!results.is_empty(), "should find files matching mp4");
    }

    #[test]
    fn test_list_children() {
        let (db, _scan_id) = test_db_with_data();

        let children = db.list_children("/test", 10).expect("list_children");
        assert_eq!(children.len(), 3, "should have 3 children: big.mp4, small.txt, sub");

        // Verify they're sorted by size desc
        assert!(children[0].size_bytes >= children[1].size_bytes);
        assert!(children[1].size_bytes >= children[2].size_bytes);

        // Verify names
        let names: Vec<&str> = children.iter().map(|f| f.name.as_str()).collect();
        assert!(names.contains(&"big.mp4"));
        assert!(names.contains(&"small.txt"));
        assert!(names.contains(&"sub"));
    }

    #[test]
    fn test_extension_stats() {
        let (db, scan_id) = test_db_with_data();

        db.compute_extension_stats(scan_id)
            .expect("compute_extension_stats");

        let stats = db.extension_stats(scan_id, 10).expect("extension_stats");
        assert!(!stats.is_empty());

        // mp4 should have the biggest total
        assert_eq!(stats[0].extension, "mp4");
        assert_eq!(stats[0].total_size, 1_000_000_000);
        assert_eq!(stats[0].file_count, 1);
    }

    #[test]
    fn test_find_files_by_size() {
        let (db, _scan_id) = test_db_with_data();

        let criteria = SearchCriteria {
            path: None,
            name_pattern: None,
            min_size: Some(1_000_000),
            max_size: None,
            extensions: None,
            modified_after: None,
            modified_before: None,
            accessed_after: None,
            accessed_before: None,
            limit: 100,
        };

        let files = db.find_files(&criteria).expect("find_files");
        assert_eq!(files.len(), 2, "should find big.mp4 and med.log");
        assert_eq!(files[0].name, "big.mp4");
        assert_eq!(files[1].name, "med.log");
    }

    #[test]
    fn test_find_files_by_extension() {
        let (db, _scan_id) = test_db_with_data();

        let criteria = SearchCriteria {
            path: None,
            name_pattern: None,
            min_size: None,
            max_size: None,
            extensions: Some(vec!["rs".to_string(), "log".to_string()]),
            modified_after: None,
            modified_before: None,
            accessed_after: None,
            accessed_before: None,
            limit: 100,
        };

        let files = db.find_files(&criteria).expect("find_files");
        assert_eq!(files.len(), 2, "should find med.log and tiny.rs");
        // Sorted by size desc: med.log first, then tiny.rs
        assert_eq!(files[0].name, "med.log");
        assert_eq!(files[1].name, "tiny.rs");
    }
}
