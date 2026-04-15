use anyhow::{Context, Result};
use rusqlite::params;

use crate::models::{
    DirSize, ExtensionStat, FileEntry, FileType, ScanInfo, ScanProgress, ScanStatus,
};
use rusqlite::OptionalExtension;

use super::Database;

// ---------------------------------------------------------------------------
// Helper structs
// ---------------------------------------------------------------------------

/// Statistics collected during a scan, used to finalize a scan record.
#[derive(Default)]
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

/// A single entry in the trend analysis results.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TrendEntry {
    pub path: String,
    pub current_size: u64,
    pub current_size_human: String,
    pub previous_size: u64,
    pub growth_bytes: i64,
    pub growth_human: String,
    pub growth_percent: f64,
    pub file_count_change: i64,
}

/// Statistics returned by history compaction.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CompactionStats {
    pub entries_before: u64,
    pub entries_after: u64,
    pub weekly_compacted: u64,
    pub monthly_compacted: u64,
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
             ORDER BY id DESC
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

    /// Update scan progress (called periodically during crawl).
    pub fn update_scan_progress(&self, scan_id: i64, progress: &ScanProgress) -> Result<()> {
        let json = serde_json::to_string(progress)?;
        let conn = self.conn();
        conn.execute(
            "UPDATE scans SET progress = ?1 WHERE id = ?2",
            params![json, scan_id],
        )?;
        Ok(())
    }

    /// Mark any scans stuck in 'running' state as 'failed'.
    /// Called on startup to clean up after crashes or Ctrl-C.
    pub fn cleanup_stale_scans(&self) -> Result<u64> {
        let conn = self.conn();
        let count = conn.execute(
            "UPDATE scans SET status = 'failed' WHERE status = 'running'",
            [],
        )?;
        Ok(count as u64)
    }

    /// Find any currently running scan.
    pub fn active_scan(&self) -> Result<Option<(ScanInfo, Option<ScanProgress>)>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT id, root_path, started_at, completed_at, total_files, total_dirs,
                    total_size, files_added, files_modified, files_deleted, status, progress
             FROM scans WHERE status = 'running' ORDER BY id DESC LIMIT 1",
        )?;
        let result = stmt
            .query_row([], |row| {
                let status_str: String = row.get(10)?;
                let progress_json: Option<String> = row.get(11)?;
                let scan = ScanInfo {
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
                };
                let progress = progress_json.and_then(|j| serde_json::from_str(&j).ok());
                Ok((scan, progress))
            })
            .optional()?;
        Ok(result)
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
        // Sanitize: strip glob chars and FTS5 special syntax, quote tokens
        let sanitized: String = query
            .chars()
            .filter(|c| !matches!(c, '*' | '?' | '{' | '}' | '[' | ']'))
            .collect();
        let sanitized = sanitized.trim();
        if sanitized.is_empty() {
            return Ok(Vec::new());
        }
        let fts_query: String = sanitized
            .split_whitespace()
            .map(|token| format!("\"{}\"", token.replace('"', "")))
            .collect::<Vec<_>>()
            .join(" ");

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
        let rows = stmt.query_map(params![fts_query, limit], file_entry_from_row)?;

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
             SELECT extension, ?1, COUNT(*), SUM(size_bytes), CAST(AVG(size_bytes) AS INTEGER), MAX(size_bytes)
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

    // -----------------------------------------------------------------------
    // Size history & trends
    // -----------------------------------------------------------------------

    /// Record size history entries for directories above a size threshold.
    /// Compares current dir_sizes against the previous scan's history to compute deltas.
    pub fn record_size_history(&self, scan_id: i64, min_size: u64) -> Result<()> {
        let conn = self.conn();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Get the previous scan ID from size_history
        let prev_scan_id: Option<i64> = conn
            .query_row(
                "SELECT MAX(scan_id) FROM size_history WHERE scan_id < ?1",
                params![scan_id],
                |row| row.get(0),
            )
            .ok()
            .flatten();

        // For each dir_size above threshold, insert a history entry with delta
        conn.execute(
            "INSERT INTO size_history (path, scan_id, recorded_at, total_size, file_count, delta_size, delta_files)
             SELECT ds.path, ?1, ?2, ds.total_size, ds.file_count,
                    ds.total_size - COALESCE(prev.total_size, 0),
                    CAST(ds.file_count AS INTEGER) - CAST(COALESCE(prev.file_count, 0) AS INTEGER)
             FROM dir_sizes ds
             LEFT JOIN size_history prev ON prev.path = ds.path AND prev.scan_id = ?3
             WHERE ds.total_size >= ?4",
            params![scan_id, now, prev_scan_id.unwrap_or(0), min_size as i64],
        )?;

        Ok(())
    }

    /// Query trend data: directories with the biggest size changes.
    pub fn query_trends(
        &self,
        path: Option<&str>,
        since: i64, // unix timestamp
        limit: u32,
        sort_by: &str, // "absolute_growth", "growth_rate", "current_size"
    ) -> Result<Vec<TrendEntry>> {
        let conn = self.conn();

        // Build the query: for each path that has history entries since the cutoff,
        // get the earliest and latest snapshot in the window.
        let path_filter = match path {
            Some(p) => {
                let prefix = if p.ends_with('/') {
                    p.to_string()
                } else {
                    format!("{}/", p)
                };
                format!(
                    "AND (h.path = '{}' OR h.path LIKE '{}%')",
                    p.replace('\'', "''"),
                    prefix.replace('\'', "''")
                )
            }
            None => String::new(),
        };

        let order_clause = match sort_by {
            "growth_rate" => "ORDER BY CASE WHEN earliest_size = 0 THEN 0 ELSE ABS(CAST(latest_size - earliest_size AS REAL) / earliest_size) END DESC",
            "current_size" => "ORDER BY latest_size DESC",
            _ => "ORDER BY ABS(latest_size - earliest_size) DESC", // "absolute_growth"
        };

        let sql = format!(
            "WITH ranked AS (
                SELECT
                    h.path,
                    h.total_size,
                    h.file_count,
                    h.recorded_at,
                    ROW_NUMBER() OVER (PARTITION BY h.path ORDER BY h.recorded_at ASC, h.scan_id ASC) AS rn_asc,
                    ROW_NUMBER() OVER (PARTITION BY h.path ORDER BY h.recorded_at DESC, h.scan_id DESC) AS rn_desc
                FROM size_history h
                WHERE h.recorded_at >= ?1
                {path_filter}
            ),
            summary AS (
                SELECT
                    earliest.path,
                    latest.total_size AS latest_size,
                    earliest.total_size AS earliest_size,
                    CAST(latest.file_count AS INTEGER) - CAST(earliest.file_count AS INTEGER) AS file_count_change
                FROM ranked earliest
                JOIN ranked latest ON earliest.path = latest.path AND latest.rn_desc = 1
                WHERE earliest.rn_asc = 1
            )
            SELECT path, latest_size, earliest_size, file_count_change
            FROM summary
            {order_clause}
            LIMIT ?2"
        );

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![since, limit], |row| {
            let path: String = row.get(0)?;
            let current_size: u64 = row.get(1)?;
            let previous_size: u64 = row.get(2)?;
            let file_count_change: i64 = row.get(3)?;
            let growth_bytes = current_size as i64 - previous_size as i64;
            let growth_percent = if previous_size == 0 {
                if current_size > 0 {
                    100.0
                } else {
                    0.0
                }
            } else {
                growth_bytes as f64 / previous_size as f64 * 100.0
            };

            let growth_human = if growth_bytes >= 0 {
                format!("+{}", crate::models::format_size(growth_bytes as u64))
            } else {
                format!("-{}", crate::models::format_size((-growth_bytes) as u64))
            };

            Ok(TrendEntry {
                path,
                current_size,
                current_size_human: crate::models::format_size(current_size),
                previous_size,
                growth_bytes,
                growth_human,
                growth_percent,
                file_count_change,
            })
        })?;

        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }
        Ok(result)
    }

    // -----------------------------------------------------------------------
    // Duplicate detection
    // -----------------------------------------------------------------------

    /// Find files that share the same size (potential duplicates).
    /// Returns a list of (size, files) tuples for sizes with more than one file.
    pub fn files_with_duplicate_sizes(
        &self,
        path: Option<&str>,
        min_size: u64,
        extensions: Option<&[String]>,
    ) -> Result<Vec<(u64, Vec<FileEntry>)>> {
        let conn = self.conn();

        // Build optional conditions for the size-grouping query
        let mut conditions = vec![
            "file_type = 0".to_string(),
            "is_deleted = 0".to_string(),
        ];
        let mut bound_params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        let mut param_idx = 1u32;

        conditions.push(format!("size_bytes >= ?{}", param_idx));
        bound_params.push(Box::new(min_size));
        param_idx += 1;

        if let Some(p) = path {
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

        if let Some(exts) = extensions {
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
                #[allow(unused_assignments)]
                { param_idx += exts.len() as u32; }
            }
        }

        let where_clause = conditions.join(" AND ");

        // First query: find sizes with duplicates
        let size_sql = format!(
            "SELECT size_bytes FROM files \
             WHERE {} \
             GROUP BY size_bytes HAVING COUNT(*) > 1 \
             ORDER BY size_bytes DESC",
            where_clause
        );

        let mut size_stmt = conn.prepare(&size_sql)?;
        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            bound_params.iter().map(|b| b.as_ref()).collect();
        let size_rows = size_stmt.query_map(params_refs.as_slice(), |row| {
            row.get::<_, u64>(0)
        })?;

        let mut dup_sizes: Vec<u64> = Vec::new();
        for r in size_rows {
            dup_sizes.push(r?);
        }

        // Second: for each duplicate size, fetch the matching files
        let mut result: Vec<(u64, Vec<FileEntry>)> = Vec::new();

        for size in dup_sizes {
            let mut file_conditions = vec![
                "file_type = 0".to_string(),
                "is_deleted = 0".to_string(),
            ];
            let mut file_params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
            let mut fp_idx = 1u32;

            file_conditions.push(format!("size_bytes = ?{}", fp_idx));
            file_params.push(Box::new(size));
            fp_idx += 1;

            if let Some(p) = path {
                let prefix = if p.ends_with('/') {
                    p.to_string()
                } else {
                    format!("{}/", p)
                };
                let upper = path_upper_bound(&prefix);
                file_conditions.push(format!("path >= ?{}", fp_idx));
                file_params.push(Box::new(prefix));
                fp_idx += 1;
                file_conditions.push(format!("path < ?{}", fp_idx));
                file_params.push(Box::new(upper));
                fp_idx += 1;
            }

            if let Some(exts) = extensions {
                if !exts.is_empty() {
                    let placeholders: Vec<String> = exts
                        .iter()
                        .enumerate()
                        .map(|(i, _)| format!("?{}", fp_idx + i as u32))
                        .collect();
                    file_conditions.push(format!(
                        "extension IN ({})",
                        placeholders.join(", ")
                    ));
                    for ext in exts {
                        file_params.push(Box::new(ext.clone()));
                    }
                }
            }

            let file_where = file_conditions.join(" AND ");
            let file_sql = format!(
                "SELECT {} FROM files WHERE {}",
                FILE_COLUMNS, file_where
            );

            let mut file_stmt = conn.prepare(&file_sql)?;
            let fp_refs: Vec<&dyn rusqlite::types::ToSql> =
                file_params.iter().map(|b| b.as_ref()).collect();
            let file_rows =
                file_stmt.query_map(fp_refs.as_slice(), file_entry_from_row)?;

            let mut entries: Vec<FileEntry> = Vec::new();
            for r in file_rows {
                entries.push(r?);
            }

            if entries.len() > 1 {
                result.push((size, entries));
            }
        }

        Ok(result)
    }

    // -----------------------------------------------------------------------
    // Query functions
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // History compaction
    // -----------------------------------------------------------------------

    /// Compact old size_history entries:
    /// - Keep daily entries for the last `retention_days` days
    /// - Roll up to weekly for `retention_days` .. 6 months old
    /// - Roll up to monthly for 6+ months old
    ///
    /// Compaction works by:
    /// 1. For weekly: group entries by (path, week_number), keep the latest per group
    /// 2. For monthly: group entries by (path, year_month), keep the latest per group
    /// 3. Delete the non-kept entries
    pub fn compact_history(&self, retention_days: u32) -> Result<CompactionStats> {
        let conn = self.conn();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let daily_cutoff = now - (retention_days as i64 * 86400);
        let weekly_cutoff = now - (180 * 86400); // 6 months

        // Count before
        let total_before: i64 =
            conn.query_row("SELECT COUNT(*) FROM size_history", [], |r| r.get(0))?;

        // Step 1: Weekly compaction (retention_days - 6 months old)
        // For each (path, week), keep only the entry with the latest recorded_at.
        // Delete entries that are NOT the latest per (path, week) in this range.
        let weekly_deleted = conn.execute(
            "DELETE FROM size_history
             WHERE recorded_at < ?1 AND recorded_at >= ?2
             AND rowid NOT IN (
                 SELECT rowid FROM (
                     SELECT rowid, ROW_NUMBER() OVER (
                         PARTITION BY path, (recorded_at / 604800)
                         ORDER BY recorded_at DESC
                     ) as rn
                     FROM size_history
                     WHERE recorded_at < ?1 AND recorded_at >= ?2
                 ) WHERE rn = 1
             )",
            params![daily_cutoff, weekly_cutoff],
        )?;

        // Step 2: Monthly compaction (6+ months old)
        // For each (path, year-month), keep only the latest entry.
        let monthly_deleted = conn.execute(
            "DELETE FROM size_history
             WHERE recorded_at < ?1
             AND rowid NOT IN (
                 SELECT rowid FROM (
                     SELECT rowid, ROW_NUMBER() OVER (
                         PARTITION BY path, (recorded_at / 2592000)
                         ORDER BY recorded_at DESC
                     ) as rn
                     FROM size_history
                     WHERE recorded_at < ?1
                 ) WHERE rn = 1
             )",
            params![weekly_cutoff],
        )?;

        let total_after: i64 =
            conn.query_row("SELECT COUNT(*) FROM size_history", [], |r| r.get(0))?;

        Ok(CompactionStats {
            entries_before: total_before as u64,
            entries_after: total_after as u64,
            weekly_compacted: weekly_deleted as u64,
            monthly_compacted: monthly_deleted as u64,
        })
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

    #[test]
    fn test_record_size_history() {
        let (db, scan_id) = test_db_with_data();

        // Insert dir_sizes for scan 1
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

        // Record history for scan 1 with a low threshold so both dirs are included
        db.record_size_history(scan_id, 1_000_000)
            .expect("record_size_history scan 1");

        // Verify history entries for scan 1 (first scan, so delta = total_size since no prev)
        {
            let conn = db.conn();
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM size_history WHERE scan_id = ?1",
                    params![scan_id],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 2, "should have 2 history entries for scan 1");
        }

        // Create scan 2 with different sizes
        let scan2_id = db.create_scan("/test").expect("create_scan 2");

        let sizes2 = vec![
            DirSize {
                path: "/test".to_string(),
                total_size: 1_200_000_000, // grew
                file_count: 5,
                dir_count: 1,
                max_depth: 3,
                largest_file: 1_000_000_000,
                scan_id: scan2_id,
            },
            DirSize {
                path: "/test/sub".to_string(),
                total_size: 60_000_000, // grew
                file_count: 3,
                dir_count: 0,
                max_depth: 1,
                largest_file: 50_000_000,
                scan_id: scan2_id,
            },
        ];
        db.insert_dir_sizes(&sizes2).expect("insert_dir_sizes scan 2");

        // Record history for scan 2
        db.record_size_history(scan2_id, 1_000_000)
            .expect("record_size_history scan 2");

        // Verify scan 2 history has correct deltas
        {
            let conn = db.conn();
            let delta_size: i64 = conn
                .query_row(
                    "SELECT delta_size FROM size_history WHERE scan_id = ?1 AND path = '/test'",
                    params![scan2_id],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(
                delta_size,
                1_200_000_000 - 1_050_001_524,
                "delta for /test should be growth amount"
            );

            let delta_files: i64 = conn
                .query_row(
                    "SELECT delta_files FROM size_history WHERE scan_id = ?1 AND path = '/test'",
                    params![scan2_id],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(delta_files, 1, "file count grew by 1");
        }
    }

    #[test]
    fn test_query_trends() {
        let (db, scan_id) = test_db_with_data();

        // Insert dir_sizes for scan 1
        let sizes = vec![
            DirSize {
                path: "/test".to_string(),
                total_size: 1_000_000_000,
                file_count: 4,
                dir_count: 1,
                max_depth: 3,
                largest_file: 1_000_000_000,
                scan_id,
            },
            DirSize {
                path: "/test/sub".to_string(),
                total_size: 50_000_000,
                file_count: 2,
                dir_count: 0,
                max_depth: 1,
                largest_file: 50_000_000,
                scan_id,
            },
        ];
        db.insert_dir_sizes(&sizes).expect("insert_dir_sizes");
        db.record_size_history(scan_id, 1_000_000)
            .expect("record_size_history scan 1");

        // Create scan 2 with growth
        let scan2_id = db.create_scan("/test").expect("create_scan 2");

        let sizes2 = vec![
            DirSize {
                path: "/test".to_string(),
                total_size: 1_500_000_000, // +500MB
                file_count: 6,
                dir_count: 1,
                max_depth: 3,
                largest_file: 1_000_000_000,
                scan_id: scan2_id,
            },
            DirSize {
                path: "/test/sub".to_string(),
                total_size: 100_000_000, // +50MB (doubled)
                file_count: 4,
                dir_count: 0,
                max_depth: 1,
                largest_file: 50_000_000,
                scan_id: scan2_id,
            },
        ];
        db.insert_dir_sizes(&sizes2).expect("insert_dir_sizes scan 2");
        db.record_size_history(scan2_id, 1_000_000)
            .expect("record_size_history scan 2");

        // Query trends sorted by absolute growth (since epoch, so all entries included)
        let trends = db
            .query_trends(None, 0, 10, "absolute_growth")
            .expect("query_trends");

        assert_eq!(trends.len(), 2, "should have 2 trend entries");

        // /test grew by 500MB, /test/sub grew by 50MB -> /test first
        assert_eq!(trends[0].path, "/test");
        assert_eq!(trends[0].current_size, 1_500_000_000);
        assert_eq!(trends[0].previous_size, 1_000_000_000);
        assert_eq!(trends[0].growth_bytes, 500_000_000);
        assert_eq!(trends[0].file_count_change, 2);

        assert_eq!(trends[1].path, "/test/sub");
        assert_eq!(trends[1].current_size, 100_000_000);
        assert_eq!(trends[1].previous_size, 50_000_000);
        assert_eq!(trends[1].growth_bytes, 50_000_000);

        // Query by growth_rate: /test/sub doubled (100%) vs /test grew 50%
        let trends_rate = db
            .query_trends(None, 0, 10, "growth_rate")
            .expect("query_trends growth_rate");

        assert_eq!(
            trends_rate[0].path, "/test/sub",
            "/test/sub has higher growth rate"
        );
        assert!((trends_rate[0].growth_percent - 100.0).abs() < 0.1);
    }

    #[test]
    fn test_query_trends_empty() {
        let db = Database::open_in_memory().expect("open_in_memory");

        // No history data -- should return empty
        let trends = db
            .query_trends(None, 0, 10, "absolute_growth")
            .expect("query_trends empty");

        assert!(
            trends.is_empty(),
            "should return empty trends when no history data"
        );
    }

    #[test]
    fn test_compact_history_weekly() {
        let db = Database::open_in_memory().unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let conn = db.conn();

        // Insert daily entries for a path spanning 35-55 days ago
        // (should be compacted to weekly since it's between 30-180 days old)
        for day in 35..55 {
            let ts = now - (day * 86400);
            conn.execute(
                "INSERT INTO size_history (path, scan_id, recorded_at, total_size, file_count, delta_size, delta_files) VALUES (?1, ?2, ?3, ?4, ?5, 0, 0)",
                params!["/test/dir", day, ts, 1000 + day * 10, 5],
            )
            .unwrap();
        }

        // Insert recent entries (should NOT be compacted)
        for day in 0..5 {
            let ts = now - (day * 86400);
            conn.execute(
                "INSERT INTO size_history (path, scan_id, recorded_at, total_size, file_count, delta_size, delta_files) VALUES (?1, ?2, ?3, ?4, ?5, 0, 0)",
                params!["/test/dir", 100 + day, ts, 2000, 5],
            )
            .unwrap();
        }
        drop(conn);

        let total_before: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM size_history", [], |r| r.get(0))
            .unwrap();
        assert_eq!(total_before, 25); // 20 old + 5 recent

        let stats = db.compact_history(30).unwrap();

        // Recent entries (5) should be untouched
        // Old entries (20 daily over ~3 weeks) should be compacted to ~3 weekly entries
        assert!(
            stats.entries_after < stats.entries_before,
            "compaction should reduce entries"
        );
        assert!(
            stats.weekly_compacted > 0,
            "should have weekly compactions"
        );

        // Recent entries should still be there
        let recent_count: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM size_history WHERE recorded_at > ?1",
                params![now - 10 * 86400],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(recent_count, 5, "recent entries should be preserved");
    }

    #[test]
    fn test_compact_history_empty_table() {
        let db = Database::open_in_memory().unwrap();
        let stats = db.compact_history(30).unwrap();
        assert_eq!(stats.entries_before, 0);
        assert_eq!(stats.entries_after, 0);
    }

    #[test]
    fn test_update_and_read_scan_progress() {
        let db = Database::open_in_memory().unwrap();
        let scan_id = db.create_scan("/test").unwrap();

        let progress = ScanProgress {
            phase: "walking".to_string(),
            phase_number: 1,
            total_phases: 7,
            files_so_far: 12345,
            dirs_so_far: 89,
            bytes_so_far: 456789,
            bytes_so_far_human: "446.1 KiB".to_string(),
            current_dir: "/test/some/path".to_string(),
            elapsed_secs: 10,
        };

        db.update_scan_progress(scan_id, &progress).unwrap();

        let result = db.active_scan().unwrap();
        assert!(result.is_some(), "should find the running scan");

        let (scan, progress_opt) = result.unwrap();
        assert_eq!(scan.id, scan_id);
        assert_eq!(scan.root_path, "/test");
        assert_eq!(scan.status, ScanStatus::Running);

        let p = progress_opt.expect("progress should be present");
        assert_eq!(p.phase, "walking");
        assert_eq!(p.phase_number, 1);
        assert_eq!(p.total_phases, 7);
        assert_eq!(p.files_so_far, 12345);
        assert_eq!(p.dirs_so_far, 89);
        assert_eq!(p.bytes_so_far, 456789);
        assert_eq!(p.current_dir, "/test/some/path");
        assert_eq!(p.elapsed_secs, 10);
    }

    #[test]
    fn test_active_scan_none_when_completed() {
        let db = Database::open_in_memory().unwrap();
        let scan_id = db.create_scan("/test").unwrap();

        // Write some progress
        let progress = ScanProgress {
            phase: "walking".to_string(),
            phase_number: 1,
            total_phases: 7,
            files_so_far: 100,
            ..Default::default()
        };
        db.update_scan_progress(scan_id, &progress).unwrap();

        // Now complete the scan
        let stats = ScanStats {
            total_files: 100,
            total_dirs: 10,
            total_size: 5000,
            files_added: 100,
            files_modified: 0,
            files_deleted: 0,
        };
        db.complete_scan(scan_id, &stats).unwrap();

        // active_scan should return None because the scan is completed
        let result = db.active_scan().unwrap();
        assert!(result.is_none(), "completed scans should not show as active");
    }

    #[test]
    fn test_scan_status_full_during_scan() {
        let db = Database::open_in_memory().unwrap();

        // First, create and complete a scan
        let scan_id_1 = db.create_scan("/test").unwrap();
        let stats = ScanStats {
            total_files: 50,
            total_dirs: 5,
            total_size: 2500,
            files_added: 50,
            files_modified: 0,
            files_deleted: 0,
        };
        db.complete_scan(scan_id_1, &stats).unwrap();

        // Now start a second scan (still running)
        let scan_id_2 = db.create_scan("/test").unwrap();
        let progress = ScanProgress {
            phase: "walking".to_string(),
            phase_number: 1,
            total_phases: 7,
            files_so_far: 200,
            dirs_so_far: 20,
            bytes_so_far: 999999,
            bytes_so_far_human: "976.6 KiB".to_string(),
            current_dir: "/test/deep/dir".to_string(),
            elapsed_secs: 5,
        };
        db.update_scan_progress(scan_id_2, &progress).unwrap();

        // Use the query layer function
        let result = crate::query::query_scan_status_full(&db).unwrap();

        // Should have both an active scan and a completed scan
        assert!(result.active_scan.is_some(), "should have active scan progress");
        let active = result.active_scan.unwrap();
        assert_eq!(active.phase, "walking");
        assert_eq!(active.files_so_far, 200);

        assert!(result.last_completed_scan.is_some(), "should have last completed scan");
        let completed = result.last_completed_scan.unwrap();
        assert_eq!(completed.id, scan_id_1);
        assert_eq!(completed.total_files, 50);
    }
}
