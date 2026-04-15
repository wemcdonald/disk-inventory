pub mod rules;

use std::collections::HashMap;

use crate::db::Database;
use crate::models::*;
use anyhow::Result;
use rusqlite::params;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct WasteCategorySummary {
    pub category: String,
    pub description: String,
    pub total_size: u64,
    pub total_size_human: String,
    pub item_count: u64,
    pub safety: SafetyRating,
    pub cleanup_command: Option<String>,
    pub items: Vec<WasteItem>,
}

/// Detect waste items in the indexed filesystem.
///
/// - `path`: optional path prefix to restrict the search
/// - `categories`: list of category names to include; empty or containing "all" means all
/// - `min_size`: minimum total bytes for a category to be included in results
/// - `disabled`: list of category names to exclude
pub fn detect_waste(
    db: &Database,
    path: Option<&str>,
    categories: &[String],
    min_size: u64,
    disabled: &[String],
) -> Result<Vec<WasteCategorySummary>> {
    let all_rules = rules::built_in_rules();

    // Determine whether to use all categories
    let use_all = categories.is_empty()
        || categories.iter().any(|c| c.eq_ignore_ascii_case("all"));

    // Filter rules by requested categories and skip disabled ones
    let active_rules: Vec<_> = all_rules
        .into_iter()
        .filter(|r| {
            let cat = r.category;
            let included = use_all || categories.iter().any(|c| c == cat);
            let not_disabled = !disabled.iter().any(|d| d == cat);
            included && not_disabled
        })
        .collect();

    // Collect items grouped by category
    // Key: category name -> (rule metadata, items)
    let mut category_map: HashMap<
        String,
        (SafetyRating, Option<String>, String, Vec<WasteItem>),
    > = HashMap::new();

    let conn = db.conn();

    for rule in &active_rules {
        let mut seen_paths: HashMap<String, bool> = HashMap::new();

        // --- Match by dir_names ---
        for dir_name in rule.dir_names {
            let sql = if path.is_some() {
                "SELECT path, size_bytes FROM files \
                 WHERE file_type = 1 AND is_deleted = 0 AND name = ?1 \
                 AND path >= ?2 AND path < ?3"
                    .to_string()
            } else {
                "SELECT path, size_bytes FROM files \
                 WHERE file_type = 1 AND is_deleted = 0 AND name = ?1"
                    .to_string()
            };

            let mut stmt = conn.prepare(&sql)?;

            let rows: Vec<(String, u64)> = match path {
                Some(p) => {
                    let upper = path_upper_bound(p);
                    let mapped = stmt.query_map(params![dir_name, p, upper], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
                    })?;
                    mapped.filter_map(|r| r.ok()).collect()
                }
                None => {
                    let mapped = stmt.query_map(params![dir_name], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
                    })?;
                    mapped.filter_map(|r| r.ok()).collect()
                }
            };

            for (dir_path, dir_entry_size) in rows {
                if seen_paths.contains_key(&dir_path) {
                    continue;
                }
                seen_paths.insert(dir_path.clone(), true);

                // Look up recursive dir size; fall back to the entry's own size
                let size = lookup_dir_size(&conn, &dir_path).unwrap_or(dir_entry_size);

                let item = WasteItem {
                    path: dir_path,
                    category: rule.category.to_string(),
                    size_bytes: size,
                    safety: rule.safety,
                    cleanup_command: rule.cleanup.map(|s| s.to_string()),
                    description: rule.description.to_string(),
                };

                let entry = category_map
                    .entry(rule.category.to_string())
                    .or_insert_with(|| {
                        (
                            rule.safety,
                            rule.cleanup.map(|s| s.to_string()),
                            rule.description.to_string(),
                            Vec::new(),
                        )
                    });
                entry.3.push(item);
            }
        }

        // --- Match by file_extensions ---
        for ext in rule.file_extensions {
            let sql = if path.is_some() {
                "SELECT path, size_bytes FROM files \
                 WHERE file_type = 0 AND is_deleted = 0 AND extension = ?1 \
                 AND path >= ?2 AND path < ?3"
                    .to_string()
            } else {
                "SELECT path, size_bytes FROM files \
                 WHERE file_type = 0 AND is_deleted = 0 AND extension = ?1"
                    .to_string()
            };

            let mut stmt = conn.prepare(&sql)?;

            let rows: Vec<(String, u64)> = match path {
                Some(p) => {
                    let upper = path_upper_bound(p);
                    let mapped = stmt.query_map(params![ext, p, upper], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
                    })?;
                    mapped.filter_map(|r| r.ok()).collect()
                }
                None => {
                    let mapped = stmt.query_map(params![ext], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
                    })?;
                    mapped.filter_map(|r| r.ok()).collect()
                }
            };

            for (file_path, size) in rows {
                if seen_paths.contains_key(&file_path) {
                    continue;
                }
                seen_paths.insert(file_path.clone(), true);

                let item = WasteItem {
                    path: file_path,
                    category: rule.category.to_string(),
                    size_bytes: size,
                    safety: rule.safety,
                    cleanup_command: rule.cleanup.map(|s| s.to_string()),
                    description: rule.description.to_string(),
                };

                let entry = category_map
                    .entry(rule.category.to_string())
                    .or_insert_with(|| {
                        (
                            rule.safety,
                            rule.cleanup.map(|s| s.to_string()),
                            rule.description.to_string(),
                            Vec::new(),
                        )
                    });
                entry.3.push(item);
            }
        }

        // --- Match by path_contains ---
        for substring in rule.path_contains {
            // Use LIKE with the substring wrapped in %...%
            let like_pattern = format!("%{}%", substring);

            let sql = match path {
                Some(_) => {
                    "SELECT path, size_bytes, file_type FROM files \
                     WHERE is_deleted = 0 AND path LIKE ?1 \
                     AND path >= ?2 AND path < ?3"
                        .to_string()
                }
                None => {
                    "SELECT path, size_bytes, file_type FROM files \
                     WHERE is_deleted = 0 AND path LIKE ?1"
                        .to_string()
                }
            };

            let mut stmt = conn.prepare(&sql)?;

            let rows: Vec<(String, u64, u8)> = match path {
                Some(p) => {
                    let upper = path_upper_bound(p);
                    let mapped =
                        stmt.query_map(params![like_pattern, p, upper], |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, u64>(1)?,
                                row.get::<_, u8>(2)?,
                            ))
                        })?;
                    mapped.filter_map(|r| r.ok()).collect()
                }
                None => {
                    let mapped = stmt.query_map(params![like_pattern], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, u64>(1)?,
                            row.get::<_, u8>(2)?,
                        ))
                    })?;
                    mapped.filter_map(|r| r.ok()).collect()
                }
            };

            for (file_path, entry_size, file_type) in rows {
                if seen_paths.contains_key(&file_path) {
                    continue;
                }
                seen_paths.insert(file_path.clone(), true);

                let size = if file_type == FileType::Directory as u8 {
                    lookup_dir_size(&conn, &file_path).unwrap_or(entry_size)
                } else {
                    entry_size
                };

                let item = WasteItem {
                    path: file_path,
                    category: rule.category.to_string(),
                    size_bytes: size,
                    safety: rule.safety,
                    cleanup_command: rule.cleanup.map(|s| s.to_string()),
                    description: rule.description.to_string(),
                };

                let entry = category_map
                    .entry(rule.category.to_string())
                    .or_insert_with(|| {
                        (
                            rule.safety,
                            rule.cleanup.map(|s| s.to_string()),
                            rule.description.to_string(),
                            Vec::new(),
                        )
                    });
                entry.3.push(item);
            }
        }
    }

    // Build summaries from the grouped data
    let mut summaries: Vec<WasteCategorySummary> = category_map
        .into_iter()
        .map(|(category, (safety, cleanup_command, description, items))| {
            let total_size: u64 = items.iter().map(|i| i.size_bytes).sum();
            let item_count = items.len() as u64;
            WasteCategorySummary {
                category,
                description,
                total_size,
                total_size_human: format_size(total_size),
                item_count,
                safety,
                cleanup_command,
                items,
            }
        })
        .filter(|s| s.total_size >= min_size)
        .collect();

    // Sort by total_size descending
    summaries.sort_by(|a, b| b.total_size.cmp(&a.total_size));

    Ok(summaries)
}

/// Look up the recursive total size of a directory from the dir_sizes table.
fn lookup_dir_size(
    conn: &rusqlite::Connection,
    path: &str,
) -> Option<u64> {
    conn.query_row(
        "SELECT total_size FROM dir_sizes WHERE path = ?1",
        params![path],
        |row| row.get::<_, u64>(0),
    )
    .ok()
}

/// Build the upper bound for a path prefix range scan.
fn path_upper_bound(path: &str) -> String {
    let mut bound = path.to_string();
    if !bound.ends_with('/') {
        bound.push('/');
    }
    bound.pop();
    bound.push('0'); // ASCII 48, one after '/' which is 47
    bound
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::queries::ScanStats;
    use crate::db::Database;

    /// Helper to create a FileEntry with reasonable defaults.
    fn make_entry(
        path: &str,
        name: &str,
        ext: Option<&str>,
        file_type: FileType,
        size: u64,
        scan_id: i64,
    ) -> FileEntry {
        let parent = crate::models::parent_path(path);
        let depth = crate::models::path_depth(path);
        let components = crate::models::path_component_count(path);
        FileEntry {
            id: None,
            path: path.to_string(),
            parent_path: parent,
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

    fn seed_waste_data(db: &Database) -> i64 {
        let scan_id = db.create_scan("/test").unwrap();

        let entries = vec![
            // node_modules directory and a file inside it
            make_entry(
                "/test/project/node_modules",
                "node_modules",
                None,
                FileType::Directory,
                0,
                scan_id,
            ),
            make_entry(
                "/test/project/node_modules/dep/index.js",
                "index.js",
                Some("js"),
                FileType::File,
                50_000,
                scan_id,
            ),
            // Rust target directory and a file inside it
            make_entry(
                "/test/project/target",
                "target",
                None,
                FileType::Directory,
                0,
                scan_id,
            ),
            make_entry(
                "/test/project/target/debug/binary",
                "binary",
                None,
                FileType::File,
                100_000,
                scan_id,
            ),
            // A log file
            make_entry(
                "/test/logs/app.log",
                "app.log",
                Some("log"),
                FileType::File,
                200_000,
                scan_id,
            ),
            // .git directory
            make_entry(
                "/test/project/.git",
                ".git",
                None,
                FileType::Directory,
                0,
                scan_id,
            ),
            // A .pyc file (Python cache matched by extension)
            make_entry(
                "/test/project/module.pyc",
                "module.pyc",
                Some("pyc"),
                FileType::File,
                5_000,
                scan_id,
            ),
        ];

        db.insert_files(&entries).unwrap();

        db.insert_dir_sizes(&[
            DirSize {
                path: "/test/project/node_modules".into(),
                total_size: 50_000,
                file_count: 1,
                dir_count: 0,
                max_depth: 2,
                largest_file: 50_000,
                scan_id,
            },
            DirSize {
                path: "/test/project/target".into(),
                total_size: 100_000,
                file_count: 1,
                dir_count: 0,
                max_depth: 2,
                largest_file: 100_000,
                scan_id,
            },
            DirSize {
                path: "/test/project/.git".into(),
                total_size: 30_000,
                file_count: 5,
                dir_count: 0,
                max_depth: 1,
                largest_file: 10_000,
                scan_id,
            },
        ])
        .unwrap();

        let stats = ScanStats {
            total_files: 4,
            total_dirs: 4,
            total_size: 385_000,
            ..Default::default()
        };
        db.complete_scan(scan_id, &stats).unwrap();

        scan_id
    }

    #[test]
    fn test_detect_all_waste() {
        let db = Database::open_in_memory().unwrap();
        seed_waste_data(&db);

        let results = detect_waste(&db, None, &[], 0, &[]).unwrap();

        let cats: Vec<&str> = results.iter().map(|s| s.category.as_str()).collect();
        assert!(cats.contains(&"node_modules"), "should find node_modules, got: {:?}", cats);
        assert!(cats.contains(&"build_artifacts"), "should find build_artifacts, got: {:?}", cats);
        assert!(cats.contains(&"log_files"), "should find log_files, got: {:?}", cats);
        assert!(cats.contains(&"git_data"), "should find git_data, got: {:?}", cats);
    }

    #[test]
    fn test_detect_specific_category() {
        let db = Database::open_in_memory().unwrap();
        seed_waste_data(&db);

        let results = detect_waste(
            &db,
            None,
            &["node_modules".to_string()],
            0,
            &[],
        )
        .unwrap();

        assert_eq!(results.len(), 1, "should return exactly one category");
        assert_eq!(results[0].category, "node_modules");
        assert_eq!(results[0].total_size, 50_000);
    }

    #[test]
    fn test_detect_respects_min_size() {
        let db = Database::open_in_memory().unwrap();
        seed_waste_data(&db);

        // Set min_size to 60_000 -- should filter out node_modules (50_000),
        // git_data (30_000), and build_artifacts Python .pyc (5_000)
        let results = detect_waste(&db, None, &[], 60_000, &[]).unwrap();

        for summary in &results {
            assert!(
                summary.total_size >= 60_000,
                "category {} has total_size {} which is below min_size 60_000",
                summary.category,
                summary.total_size
            );
        }

        // node_modules at 50_000 should be filtered out
        let cats: Vec<&str> = results.iter().map(|s| s.category.as_str()).collect();
        assert!(
            !cats.contains(&"node_modules"),
            "node_modules (50_000) should be filtered out by min_size 60_000"
        );
    }

    #[test]
    fn test_detect_disabled_categories() {
        let db = Database::open_in_memory().unwrap();
        seed_waste_data(&db);

        let results = detect_waste(
            &db,
            None,
            &[],
            0,
            &["git_data".to_string()],
        )
        .unwrap();

        let cats: Vec<&str> = results.iter().map(|s| s.category.as_str()).collect();
        assert!(
            !cats.contains(&"git_data"),
            "git_data should be disabled, got: {:?}",
            cats
        );
        // Other categories should still be present
        assert!(cats.contains(&"node_modules"));
    }

    #[test]
    fn test_safety_ratings() {
        let db = Database::open_in_memory().unwrap();
        seed_waste_data(&db);

        let results = detect_waste(&db, None, &[], 0, &[]).unwrap();

        for summary in &results {
            match summary.category.as_str() {
                "node_modules" => {
                    assert_eq!(summary.safety, SafetyRating::Safe);
                }
                "log_files" => {
                    assert_eq!(summary.safety, SafetyRating::Review);
                }
                "git_data" => {
                    assert_eq!(summary.safety, SafetyRating::Caution);
                }
                _ => {}
            }
        }
    }

    #[test]
    fn test_results_sorted_by_size_descending() {
        let db = Database::open_in_memory().unwrap();
        seed_waste_data(&db);

        let results = detect_waste(&db, None, &[], 0, &[]).unwrap();

        for window in results.windows(2) {
            assert!(
                window[0].total_size >= window[1].total_size,
                "results should be sorted by total_size descending: {} >= {}",
                window[0].total_size,
                window[1].total_size
            );
        }
    }

    #[test]
    fn test_dir_matches_use_recursive_size() {
        let db = Database::open_in_memory().unwrap();
        seed_waste_data(&db);

        let results = detect_waste(
            &db,
            None,
            &["node_modules".to_string()],
            0,
            &[],
        )
        .unwrap();

        assert_eq!(results.len(), 1);
        // The node_modules directory entry has size 0, but dir_sizes says 50_000
        assert_eq!(results[0].total_size, 50_000);
        assert_eq!(results[0].items[0].size_bytes, 50_000);
    }

    #[test]
    fn test_detect_with_path_filter() {
        let db = Database::open_in_memory().unwrap();
        seed_waste_data(&db);

        // Only look under /test/logs -- should only find the log file
        let results = detect_waste(&db, Some("/test/logs"), &[], 0, &[]).unwrap();

        let cats: Vec<&str> = results.iter().map(|s| s.category.as_str()).collect();
        assert!(cats.contains(&"log_files"));
        // Should not find node_modules, build_artifacts, or git_data
        assert!(!cats.contains(&"node_modules"));
        assert!(!cats.contains(&"git_data"));
    }

    #[test]
    fn test_empty_db_returns_empty() {
        let db = Database::open_in_memory().unwrap();
        let results = detect_waste(&db, None, &[], 0, &[]).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_all_keyword_in_categories() {
        let db = Database::open_in_memory().unwrap();
        seed_waste_data(&db);

        let results = detect_waste(
            &db,
            None,
            &["all".to_string()],
            0,
            &[],
        )
        .unwrap();

        // Should behave the same as empty categories (all rules active)
        let cats: Vec<&str> = results.iter().map(|s| s.category.as_str()).collect();
        assert!(cats.contains(&"node_modules"));
        assert!(cats.contains(&"build_artifacts"));
        assert!(cats.contains(&"log_files"));
        assert!(cats.contains(&"git_data"));
    }

    #[test]
    fn test_cleanup_commands_present() {
        let db = Database::open_in_memory().unwrap();
        seed_waste_data(&db);

        let results = detect_waste(&db, None, &[], 0, &[]).unwrap();

        for summary in &results {
            match summary.category.as_str() {
                "node_modules" => {
                    assert!(summary.cleanup_command.is_some());
                }
                "log_files" => {
                    assert!(summary.cleanup_command.is_none());
                }
                "git_data" => {
                    assert!(summary.cleanup_command.is_none());
                }
                _ => {}
            }
        }
    }

    #[test]
    fn test_built_in_rules_count() {
        let rules = rules::built_in_rules();
        assert_eq!(rules.len(), 19, "should have 19 built-in waste rules");
    }
}
