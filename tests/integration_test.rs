//! End-to-end integration test: create small tempdir tree, crawl, query, verify.
//! NO dangerous commands. Small directory only.

use disk_inventory::config::Config;
use disk_inventory::crawler;
use disk_inventory::db::Database;
use disk_inventory::duplicates;
use disk_inventory::models::*;
use disk_inventory::query;
use disk_inventory::waste;
use std::fs;
use tempfile::TempDir;

/// Create a realistic small filesystem tree for testing.
fn create_test_tree() -> TempDir {
    let dir = TempDir::new().unwrap();
    let root = dir.path();

    // Project with node_modules (waste: safe)
    fs::create_dir_all(root.join("project/node_modules/dep")).unwrap();
    fs::create_dir_all(root.join("project/src")).unwrap();
    fs::write(root.join("project/src/main.rs"), "fn main() {}").unwrap();
    fs::write(root.join("project/package.json"), r#"{"name":"test"}"#).unwrap();
    fs::write(
        root.join("project/node_modules/dep/index.js"),
        vec![0u8; 51200],
    )
    .unwrap();
    fs::write(
        root.join("project/node_modules/dep/bundle.js"),
        vec![0u8; 102400],
    )
    .unwrap();

    // Rust target directory (waste: safe)
    fs::create_dir_all(root.join("project/target/debug")).unwrap();
    fs::write(
        root.join("project/target/debug/binary"),
        vec![0u8; 204800],
    )
    .unwrap();

    // Log files (waste: review)
    fs::create_dir_all(root.join("logs")).unwrap();
    fs::write(root.join("logs/app.log"), vec![0u8; 81920]).unwrap();

    // Photos
    fs::create_dir_all(root.join("photos")).unwrap();
    fs::write(root.join("photos/vacation.jpg"), vec![0u8; 512000]).unwrap();
    fs::write(root.join("photos/portrait.png"), vec![0u8; 307200]).unwrap();

    // Documents
    fs::create_dir_all(root.join("documents")).unwrap();
    fs::write(root.join("documents/report.pdf"), vec![0u8; 153600]).unwrap();

    // Duplicate files (identical content)
    let dup_content = b"This is duplicate content for testing. Padding to make it non-trivial in size for the dedup engine to find.";
    fs::write(root.join("documents/notes.txt"), dup_content).unwrap();
    fs::write(root.join("project/notes_copy.txt"), dup_content).unwrap();
    fs::write(root.join("logs/old_notes.txt"), dup_content).unwrap();

    dir
}

#[test]
fn test_crawl_and_overview() {
    let dir = create_test_tree();
    let db = Database::open_in_memory().unwrap();
    let config = Config::default();

    // Crawl the test tree
    let scan = crawler::run_crawl(&db, dir.path(), &config).unwrap();

    assert_eq!(scan.status, ScanStatus::Completed);
    assert!(scan.total_files >= 10, "should find at least 10 files, got {}", scan.total_files);
    assert!(scan.total_dirs >= 5, "should find at least 5 dirs, got {}", scan.total_dirs);
    assert!(scan.total_size > 1_000_000, "total should be >1MB, got {}", scan.total_size);

    // Query overview
    let overview = query::query_overview(&db, Some(&dir.path().to_string_lossy()), 1).unwrap();
    assert!(overview.total_size > 0);
    assert!(!overview.children.is_empty(), "should have children");

    // Verify children include known directories
    let child_names: Vec<&str> = overview.children.iter().map(|c| c.name.as_str()).collect();
    assert!(child_names.contains(&"project"), "should contain 'project', got: {:?}", child_names);
    assert!(child_names.contains(&"photos"), "should contain 'photos', got: {:?}", child_names);
}

#[test]
fn test_largest_files() {
    let dir = create_test_tree();
    let db = Database::open_in_memory().unwrap();
    let config = Config::default();
    crawler::run_crawl(&db, dir.path(), &config).unwrap();

    let result = query::query_large_items(
        &db,
        Some(&dir.path().to_string_lossy()),
        "files",
        0, // no minimum
        10,
        None,
        None,
    )
    .unwrap();

    assert!(!result.items.is_empty());
    // Largest file should be vacation.jpg (512KB)
    assert_eq!(result.items[0].size_bytes, 512000);
    assert!(result.items[0].path.contains("vacation.jpg"));

    // Verify items are sorted descending
    for i in 1..result.items.len() {
        assert!(
            result.items[i - 1].size_bytes >= result.items[i].size_bytes,
            "items should be sorted by size desc"
        );
    }
}

#[test]
fn test_extension_breakdown() {
    let dir = create_test_tree();
    let db = Database::open_in_memory().unwrap();
    let config = Config::default();
    crawler::run_crawl(&db, dir.path(), &config).unwrap();

    let result = query::query_usage_by_type(&db, Some(&dir.path().to_string_lossy()), 25).unwrap();

    assert!(!result.types.is_empty());

    // Find known extensions
    let ext_names: Vec<&str> = result.types.iter().map(|t| t.extension.as_str()).collect();
    assert!(ext_names.contains(&"jpg"), "should have jpg, got: {:?}", ext_names);
    assert!(ext_names.contains(&"js"), "should have js, got: {:?}", ext_names);
}

#[test]
fn test_file_search() {
    let dir = create_test_tree();
    let db = Database::open_in_memory().unwrap();
    let config = Config::default();
    crawler::run_crawl(&db, dir.path(), &config).unwrap();

    // Search for log files
    let result = query::query_search(&db, Some("log"), None, None, None, 50).unwrap();
    assert!(
        result.files.iter().any(|f| f.path.contains("app.log")),
        "should find app.log"
    );
}

#[test]
fn test_waste_detection() {
    let dir = create_test_tree();
    let db = Database::open_in_memory().unwrap();
    let config = Config::default();
    crawler::run_crawl(&db, dir.path(), &config).unwrap();

    // Detect all waste
    let results = waste::detect_waste(
        &db,
        Some(&dir.path().to_string_lossy()),
        &["all".to_string()],
        0, // no minimum size
        &[],
    )
    .unwrap();

    // Should find node_modules and target as waste
    let categories: Vec<&str> = results.iter().map(|r| r.category.as_str()).collect();
    assert!(
        categories.contains(&"node_modules"),
        "should detect node_modules, got: {:?}",
        categories
    );
    assert!(
        categories.contains(&"build_artifacts"),
        "should detect build_artifacts (target/), got: {:?}",
        categories
    );

    // Verify safety ratings
    let nm = results.iter().find(|r| r.category == "node_modules").unwrap();
    assert_eq!(nm.safety, SafetyRating::Safe);
    assert!(nm.total_size > 0);
}

#[test]
fn test_duplicate_detection() {
    let dir = create_test_tree();
    let db = Database::open_in_memory().unwrap();
    let config = Config::default();
    crawler::run_crawl(&db, dir.path(), &config).unwrap();

    // Find duplicates with low min_size to catch our small test files
    let results = duplicates::find_duplicates(
        &db,
        Some(&dir.path().to_string_lossy()),
        1, // 1 byte minimum
        None,
        20,
    )
    .unwrap();

    // We have 3 copies of the same notes.txt content, plus
    // several zero-filled files of the same size
    // At minimum, the notes.txt files should be detected
    assert!(
        !results.is_empty(),
        "should find at least one duplicate group"
    );

    // Verify structure
    for group in &results {
        assert!(group.files.len() >= 2, "each group should have at least 2 files");
        assert!(group.wasted_bytes > 0);
    }
}

#[test]
fn test_dir_sizes_precomputed() {
    let dir = create_test_tree();
    let db = Database::open_in_memory().unwrap();
    let config = Config::default();
    crawler::run_crawl(&db, dir.path(), &config).unwrap();

    // dir_sizes should be pre-computed and O(1) to look up
    let root_size = db
        .get_dir_size(&dir.path().to_string_lossy())
        .unwrap();
    assert!(root_size.is_some(), "root dir should have a size entry");
    let root = root_size.unwrap();
    assert!(root.total_size > 1_000_000);
    assert!(root.file_count >= 10);

    // Photos subdir
    let photos_path = dir.path().join("photos").to_string_lossy().to_string();
    let photos_size = db.get_dir_size(&photos_path).unwrap();
    assert!(photos_size.is_some());
    let photos = photos_size.unwrap();
    assert_eq!(photos.total_size, 512000 + 307200); // vacation.jpg + portrait.png
    assert_eq!(photos.file_count, 2);
}

#[test]
fn test_json_output_is_valid() {
    let dir = create_test_tree();
    let db = Database::open_in_memory().unwrap();
    let config = Config::default();
    crawler::run_crawl(&db, dir.path(), &config).unwrap();

    // Verify all query results serialize to valid JSON
    let overview = query::query_overview(&db, Some(&dir.path().to_string_lossy()), 1).unwrap();
    let json = serde_json::to_string(&overview).unwrap();
    let _: serde_json::Value = serde_json::from_str(&json).unwrap();

    let large = query::query_large_items(
        &db,
        Some(&dir.path().to_string_lossy()),
        "both",
        0,
        10,
        None,
        None,
    )
    .unwrap();
    let json = serde_json::to_string(&large).unwrap();
    let _: serde_json::Value = serde_json::from_str(&json).unwrap();

    let types = query::query_usage_by_type(&db, Some(&dir.path().to_string_lossy()), 25).unwrap();
    let json = serde_json::to_string(&types).unwrap();
    let _: serde_json::Value = serde_json::from_str(&json).unwrap();
}

#[test]
fn test_scan_status() {
    let dir = create_test_tree();
    let db = Database::open_in_memory().unwrap();
    let config = Config::default();
    crawler::run_crawl(&db, dir.path(), &config).unwrap();

    let status = query::query_scan_status(&db).unwrap();
    assert!(status.is_some());
    let scan = status.unwrap();
    assert_eq!(scan.status, ScanStatus::Completed);
    assert!(scan.completed_at.is_some());
}
