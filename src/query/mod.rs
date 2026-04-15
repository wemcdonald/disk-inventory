use serde::Serialize;

use crate::db::queries::SearchCriteria;
use crate::db::Database;
use crate::models::{format_size, FileType, ScanInfo, ScanProgress};
use anyhow::{Context, Result};

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct OverviewResult {
    pub path: String,
    pub total_size: u64,
    pub total_size_human: String,
    pub file_count: u64,
    pub dir_count: u64,
    pub children: Vec<ChildSummary>,
    pub last_scan_time: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct ChildSummary {
    pub path: String,
    pub name: String,
    pub total_size: u64,
    pub total_size_human: String,
    pub file_count: u64,
    pub percentage: f64,
    pub is_dir: bool,
}

#[derive(Debug, Serialize)]
pub struct LargeItemResult {
    pub items: Vec<LargeItem>,
    pub last_scan_time: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct LargeItem {
    pub path: String,
    pub size_bytes: u64,
    pub size_human: String,
    pub item_type: String, // "file" or "directory"
    pub modified: Option<i64>,
    pub accessed: Option<i64>,
    pub extension: Option<String>,
    pub file_count: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct TypeBreakdownResult {
    pub types: Vec<TypeStat>,
    pub last_scan_time: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct TypeStat {
    pub extension: String,
    pub file_count: u64,
    pub total_size: u64,
    pub total_size_human: String,
    pub avg_size: u64,
    pub percentage: f64,
}

#[derive(Debug, Serialize)]
pub struct SearchResult {
    pub files: Vec<SearchHit>,
    pub total_matches: usize,
    pub last_scan_time: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct SearchHit {
    pub path: String,
    pub name: String,
    pub size_bytes: u64,
    pub size_human: String,
    pub extension: Option<String>,
    pub modified: Option<i64>,
    pub is_dir: bool,
}

// ---------------------------------------------------------------------------
// Query functions
// ---------------------------------------------------------------------------

/// Resolve the target path: if `path` is None, use the root_path from the
/// latest completed scan. Returns `(resolved_path, last_scan_time)`.
fn resolve_path(db: &Database, path: Option<&str>) -> Result<(String, Option<i64>)> {
    let scan = db.latest_scan()?;
    match (path, &scan) {
        (Some(p), _) => Ok((p.to_string(), scan.as_ref().and_then(|s| s.completed_at))),
        (None, Some(s)) => Ok((s.root_path.clone(), s.completed_at)),
        (None, None) => anyhow::bail!("no path specified and no completed scan found"),
    }
}

/// High-level overview of disk usage at a path.
/// Gets dir_size for the target, lists children with sizes and percentages.
pub fn query_overview(db: &Database, path: Option<&str>, _depth: u32) -> Result<OverviewResult> {
    let (target_path, last_scan_time) = resolve_path(db, path)?;

    // Get dir_size for target path
    let dir_size = db
        .get_dir_size(&target_path)?
        .with_context(|| format!("no size data for path: {}", target_path))?;

    let total_size = dir_size.total_size;
    let file_count = dir_size.file_count;
    let dir_count = dir_size.dir_count;

    // List children
    let children_entries = db.list_children(&target_path, 1000)?;

    let mut children = Vec::with_capacity(children_entries.len());
    for entry in &children_entries {
        let is_dir = entry.file_type == FileType::Directory;
        let (child_size, child_file_count) = if is_dir {
            match db.get_dir_size(&entry.path)? {
                Some(ds) => (ds.total_size, ds.file_count),
                None => (entry.size_bytes, 0),
            }
        } else {
            (entry.size_bytes, 1)
        };

        let percentage = if total_size > 0 {
            child_size as f64 / total_size as f64 * 100.0
        } else {
            0.0
        };

        children.push(ChildSummary {
            path: entry.path.clone(),
            name: entry.name.clone(),
            total_size: child_size,
            total_size_human: format_size(child_size),
            file_count: child_file_count,
            percentage,
            is_dir,
        });
    }

    // Sort children by total_size descending (dirs may have been re-valued)
    children.sort_by(|a, b| b.total_size.cmp(&a.total_size));

    Ok(OverviewResult {
        path: target_path,
        total_size,
        total_size_human: format_size(total_size),
        file_count,
        dir_count,
        children,
        last_scan_time,
    })
}

/// Find largest items (files, directories, or both).
pub fn query_large_items(
    db: &Database,
    path: Option<&str>,
    item_type: &str, // "files", "directories", "both"
    min_size: u64,
    limit: u32,
    extensions: Option<&[String]>,
    older_than_days: Option<u32>,
) -> Result<LargeItemResult> {
    let (target_path, last_scan_time) = resolve_path(db, path)?;
    let path_filter = Some(target_path.as_str());

    let mut items: Vec<LargeItem> = Vec::new();

    // Collect files
    if item_type == "files" || item_type == "both" {
        // Use find_files with SearchCriteria for extension/age filtering
        let modified_before = older_than_days.map(|days| {
            chrono::Utc::now().timestamp() - (days as i64 * 86400)
        });

        let criteria = SearchCriteria {
            path: Some(target_path.clone()),
            name_pattern: None,
            min_size: if min_size > 0 { Some(min_size) } else { None },
            max_size: None,
            extensions: extensions.map(|e| e.to_vec()),
            modified_after: None,
            modified_before,
            accessed_after: None,
            accessed_before: None,
            limit,
        };

        let files = if extensions.is_some() || older_than_days.is_some() || min_size > 0 {
            db.find_files(&criteria)?
        } else {
            db.largest_files(path_filter, limit)?
        };

        for f in files {
            items.push(LargeItem {
                path: f.path,
                size_bytes: f.size_bytes,
                size_human: format_size(f.size_bytes),
                item_type: "file".to_string(),
                modified: Some(f.mtime),
                accessed: Some(f.atime),
                extension: f.extension,
                file_count: None,
            });
        }
    }

    // Collect directories
    if item_type == "directories" || item_type == "both" {
        let dirs = db.largest_dirs(path_filter, limit)?;
        for d in dirs {
            if d.total_size >= min_size {
                items.push(LargeItem {
                    path: d.path,
                    size_bytes: d.total_size,
                    size_human: format_size(d.total_size),
                    item_type: "directory".to_string(),
                    modified: None,
                    accessed: None,
                    extension: None,
                    file_count: Some(d.file_count),
                });
            }
        }
    }

    // Sort by size descending and truncate to limit
    items.sort_by(|a, b| b.size_bytes.cmp(&a.size_bytes));
    items.truncate(limit as usize);

    Ok(LargeItemResult {
        items,
        last_scan_time,
    })
}

/// Breakdown by file extension.
pub fn query_usage_by_type(
    db: &Database,
    _path: Option<&str>,
    limit: u32,
) -> Result<TypeBreakdownResult> {
    let scan = db
        .latest_scan()?
        .context("no completed scan found")?;

    let last_scan_time = scan.completed_at;
    let stats = db.extension_stats(scan.id, limit)?;

    // Compute total size for percentage calculation
    let grand_total: u64 = stats.iter().map(|s| s.total_size).sum();

    let types = stats
        .into_iter()
        .map(|s| {
            let percentage = if grand_total > 0 {
                s.total_size as f64 / grand_total as f64 * 100.0
            } else {
                0.0
            };
            TypeStat {
                extension: s.extension,
                file_count: s.file_count,
                total_size: s.total_size,
                total_size_human: format_size(s.total_size),
                avg_size: s.avg_size,
                percentage,
            }
        })
        .collect();

    Ok(TypeBreakdownResult {
        types,
        last_scan_time,
    })
}

/// Flexible file search.
pub fn query_search(
    db: &Database,
    name_pattern: Option<&str>,
    path: Option<&str>,
    min_size: Option<u64>,
    max_size: Option<u64>,
    limit: u32,
) -> Result<SearchResult> {
    let scan = db.latest_scan()?;
    let last_scan_time = scan.as_ref().and_then(|s| s.completed_at);

    let entries = if let Some(pattern) = name_pattern {
        // Use FTS search when a name pattern is provided
        db.search_files_fts(pattern, limit)?
    } else {
        // Use find_files with SearchCriteria
        let criteria = SearchCriteria {
            path: path.map(|s| s.to_string()),
            name_pattern: None,
            min_size,
            max_size,
            extensions: None,
            modified_after: None,
            modified_before: None,
            accessed_after: None,
            accessed_before: None,
            limit,
        };
        db.find_files(&criteria)?
    };

    let total_matches = entries.len();
    let files = entries
        .into_iter()
        .map(|e| SearchHit {
            path: e.path,
            name: e.name,
            size_bytes: e.size_bytes,
            size_human: format_size(e.size_bytes),
            extension: e.extension,
            modified: Some(e.mtime),
            is_dir: e.file_type == FileType::Directory,
        })
        .collect();

    Ok(SearchResult {
        files,
        total_matches,
        last_scan_time,
    })
}

/// Get current scan status.
pub fn query_scan_status(db: &Database) -> Result<Option<ScanInfo>> {
    db.latest_scan()
}

#[derive(Debug, Serialize)]
pub struct ScanStatusResult {
    pub active_scan: Option<ScanProgress>,
    pub last_completed_scan: Option<ScanInfo>,
}

/// Enhanced scan status that includes active scan progress.
pub fn query_scan_status_full(db: &Database) -> Result<ScanStatusResult> {
    let active = db.active_scan()?;
    let completed = db.latest_scan()?;
    Ok(ScanStatusResult {
        active_scan: active.and_then(|(_, progress)| progress),
        last_completed_scan: completed,
    })
}

// ---------------------------------------------------------------------------
// Trends
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct TrendsResult {
    pub trends: Vec<TrendItem>,
    pub period: String,
    pub last_scan_time: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct TrendItem {
    pub path: String,
    pub current_size: u64,
    pub current_size_human: String,
    pub previous_size: u64,
    pub growth_bytes: i64,
    pub growth_human: String,
    pub growth_percent: f64,
    pub file_count_change: i64,
}

/// Query size trends over a given period.
pub fn query_trends(
    db: &Database,
    path: Option<&str>,
    period: &str,  // "day", "week", "month", "quarter", "year"
    sort_by: &str, // "absolute_growth", "growth_rate", "current_size"
    limit: u32,
) -> Result<TrendsResult> {
    let scan = db.latest_scan()?;
    let last_scan_time = scan.as_ref().and_then(|s| s.completed_at);

    // Convert period to seconds
    let period_secs: i64 = match period {
        "day" => 86400,
        "week" => 7 * 86400,
        "month" => 30 * 86400,
        "quarter" => 90 * 86400,
        "year" => 365 * 86400,
        _ => 7 * 86400, // default to week
    };
    let since = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        - period_secs;

    let entries = db.query_trends(path, since, limit, sort_by)?;

    let trends = entries
        .into_iter()
        .map(|e| TrendItem {
            path: e.path,
            current_size: e.current_size,
            current_size_human: e.current_size_human,
            previous_size: e.previous_size,
            growth_bytes: e.growth_bytes,
            growth_human: e.growth_human,
            growth_percent: e.growth_percent,
            file_count_change: e.file_count_change,
        })
        .collect();

    Ok(TrendsResult {
        trends,
        period: period.to_string(),
        last_scan_time,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::queries::ScanStats;
    use crate::models::{DirSize, FileEntry, FileType};

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

    /// Seed an in-memory DB with a known file tree, complete the scan, and
    /// insert dir_sizes + extension_stats.
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

        let dir_sizes = vec![
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
        db.insert_dir_sizes(&dir_sizes).expect("insert_dir_sizes");

        db.compute_extension_stats(scan_id)
            .expect("compute_extension_stats");

        let stats = ScanStats {
            total_files: 4,
            total_dirs: 1,
            total_size: 1_050_001_524,
            files_added: 4,
            files_modified: 0,
            files_deleted: 0,
        };
        db.complete_scan(scan_id, &stats).expect("complete_scan");

        (db, scan_id)
    }

    #[test]
    fn test_query_overview() {
        let (db, _scan_id) = test_db_with_data();

        let result = query_overview(&db, Some("/test"), 1).expect("query_overview");

        assert_eq!(result.path, "/test");
        assert_eq!(result.total_size, 1_050_001_524);
        assert_eq!(result.file_count, 4);
        assert_eq!(result.dir_count, 1);
        assert!(!result.total_size_human.is_empty());
        assert!(result.last_scan_time.is_some());

        // Should have 3 children: big.mp4, small.txt, sub
        assert_eq!(result.children.len(), 3);

        // Children sorted by size desc, so first should be big.mp4 or sub
        // big.mp4 = 1,000,000,000  sub = 50,000,500
        assert_eq!(result.children[0].name, "big.mp4");
        assert_eq!(result.children[0].total_size, 1_000_000_000);
        assert!(!result.children[0].is_dir);

        // sub directory should have its dir_size total
        let sub_child = result.children.iter().find(|c| c.name == "sub").unwrap();
        assert_eq!(sub_child.total_size, 50_000_500);
        assert!(sub_child.is_dir);
        assert_eq!(sub_child.file_count, 2);

        // Check percentages sum roughly
        let pct_sum: f64 = result.children.iter().map(|c| c.percentage).sum();
        assert!(pct_sum > 99.0, "percentages should sum to ~100%, got {}", pct_sum);
    }

    #[test]
    fn test_query_overview_default_path() {
        let (db, _scan_id) = test_db_with_data();

        // With path=None, should use root_path from latest scan ("/test")
        let result = query_overview(&db, None, 1).expect("query_overview default path");
        assert_eq!(result.path, "/test");
    }

    #[test]
    fn test_query_large_items_files() {
        let (db, _scan_id) = test_db_with_data();

        let result =
            query_large_items(&db, Some("/test"), "files", 0, 10, None, None)
                .expect("query_large_items files");

        assert!(result.last_scan_time.is_some());
        assert!(!result.items.is_empty());

        // All items should be files
        for item in &result.items {
            assert_eq!(item.item_type, "file");
        }

        // Should be sorted by size descending
        for w in result.items.windows(2) {
            assert!(w[0].size_bytes >= w[1].size_bytes);
        }

        // First should be big.mp4
        assert!(result.items[0].path.ends_with("big.mp4"));
        assert_eq!(result.items[0].size_bytes, 1_000_000_000);
    }

    #[test]
    fn test_query_large_items_files_with_min_size() {
        let (db, _scan_id) = test_db_with_data();

        let result =
            query_large_items(&db, Some("/test"), "files", 1_000_000, 10, None, None)
                .expect("query_large_items with min_size");

        // Only big.mp4 (1B) and med.log (50M) are >= 1MB
        assert_eq!(result.items.len(), 2);
        assert!(result.items[0].size_bytes >= 1_000_000);
        assert!(result.items[1].size_bytes >= 1_000_000);
    }

    #[test]
    fn test_query_large_items_dirs() {
        let (db, _scan_id) = test_db_with_data();

        let result =
            query_large_items(&db, Some("/test"), "directories", 0, 10, None, None)
                .expect("query_large_items dirs");

        assert!(!result.items.is_empty());

        // All items should be directories
        for item in &result.items {
            assert_eq!(item.item_type, "directory");
            assert!(item.file_count.is_some());
        }

        // Should include /test and /test/sub
        let paths: Vec<&str> = result.items.iter().map(|i| i.path.as_str()).collect();
        assert!(paths.contains(&"/test"));
        assert!(paths.contains(&"/test/sub"));
    }

    #[test]
    fn test_query_large_items_both() {
        let (db, _scan_id) = test_db_with_data();

        let result =
            query_large_items(&db, Some("/test"), "both", 0, 10, None, None)
                .expect("query_large_items both");

        // Should have both files and directories
        let has_files = result.items.iter().any(|i| i.item_type == "file");
        let has_dirs = result.items.iter().any(|i| i.item_type == "directory");
        assert!(has_files, "should include files");
        assert!(has_dirs, "should include directories");

        // Sorted by size desc
        for w in result.items.windows(2) {
            assert!(w[0].size_bytes >= w[1].size_bytes);
        }
    }

    #[test]
    fn test_query_usage_by_type() {
        let (db, _scan_id) = test_db_with_data();

        let result = query_usage_by_type(&db, None, 10).expect("query_usage_by_type");

        assert!(result.last_scan_time.is_some());
        assert!(!result.types.is_empty());

        // mp4 should be first (largest total)
        assert_eq!(result.types[0].extension, "mp4");
        assert_eq!(result.types[0].total_size, 1_000_000_000);
        assert_eq!(result.types[0].file_count, 1);
        assert!(!result.types[0].total_size_human.is_empty());

        // Percentages should sum to ~100%
        let pct_sum: f64 = result.types.iter().map(|t| t.percentage).sum();
        assert!(
            (pct_sum - 100.0).abs() < 0.1,
            "percentages should sum to ~100%, got {}",
            pct_sum
        );
    }

    #[test]
    fn test_query_search() {
        let (db, _scan_id) = test_db_with_data();

        // FTS search for "big"
        let result = query_search(&db, Some("big"), None, None, None, 10)
            .expect("query_search fts");

        assert!(result.total_matches > 0);
        assert!(result.files.iter().any(|f| f.name == "big.mp4"));

        // Verify fields are populated
        let hit = &result.files[0];
        assert!(!hit.size_human.is_empty());
        assert!(hit.modified.is_some());
    }

    #[test]
    fn test_query_search_by_size() {
        let (db, _scan_id) = test_db_with_data();

        // Search without name_pattern, using size filter
        let result = query_search(&db, None, None, Some(1_000_000), None, 10)
            .expect("query_search by size");

        // Should find big.mp4 and med.log
        assert_eq!(result.total_matches, 2);
        for hit in &result.files {
            assert!(hit.size_bytes >= 1_000_000);
        }
    }

    #[test]
    fn test_query_scan_status() {
        let (db, _scan_id) = test_db_with_data();

        let scan = query_scan_status(&db).expect("query_scan_status");
        assert!(scan.is_some());

        let info = scan.unwrap();
        assert_eq!(info.root_path, "/test");
        assert_eq!(info.total_files, 4);
        assert_eq!(info.total_size, 1_050_001_524);
        assert_eq!(info.status, crate::models::ScanStatus::Completed);
    }

    #[test]
    fn test_query_scan_status_empty() {
        let db = Database::open_in_memory().expect("open_in_memory");
        let scan = query_scan_status(&db).expect("query_scan_status");
        assert!(scan.is_none());
    }

    #[test]
    fn test_query_trends_empty() {
        let db = Database::open_in_memory().expect("open_in_memory");

        // No history data -- should return empty trends
        let result = query_trends(&db, None, "week", "absolute_growth", 10)
            .expect("query_trends empty");

        assert!(result.trends.is_empty(), "should return empty trends when no data");
        assert_eq!(result.period, "week");
        assert!(result.last_scan_time.is_none());
    }
}
