//! Functional verification of core size logic against known files.
//! Crawls a temp directory with exact known file sizes and verifies
//! every computed value matches expectations.

use disk_inventory::config::Config;
use disk_inventory::crawler;
use disk_inventory::db::Database;
use disk_inventory::query;
use std::fs;
use tempfile::TempDir;

#[test]
fn verify_size_aggregation_is_exact() {
    // Create files with EXACT known sizes
    let dir = TempDir::new().unwrap();
    let r = dir.path();

    fs::create_dir_all(r.join("a/b")).unwrap();
    fs::create_dir_all(r.join("c")).unwrap();

    // big.bin: exactly 100,000 bytes
    fs::write(r.join("a/big.bin"), vec![0x42u8; 100_000]).unwrap();
    // medium.txt: exactly 25,000 bytes
    fs::write(r.join("a/b/medium.txt"), vec![0x4Du8; 25_000]).unwrap();
    // small.dat: exactly 5,000 bytes
    fs::write(r.join("c/small.dat"), vec![0x53u8; 5_000]).unwrap();
    // tiny.txt: exactly 17 bytes
    fs::write(r.join("tiny.txt"), "exactly17bytesXX\n").unwrap();

    // Sanity check the files are the sizes we think
    assert_eq!(fs::metadata(r.join("a/big.bin")).unwrap().len(), 100_000);
    assert_eq!(fs::metadata(r.join("a/b/medium.txt")).unwrap().len(), 25_000);
    assert_eq!(fs::metadata(r.join("c/small.dat")).unwrap().len(), 5_000);
    let tiny_size = fs::metadata(r.join("tiny.txt")).unwrap().len();

    let total_expected: u64 = 100_000 + 25_000 + 5_000 + tiny_size;

    // Crawl
    let db = Database::open_in_memory().unwrap();
    let config = Config::default();
    let scan = crawler::run_crawl(&db, r, &config).unwrap();

    // --- Scan totals ---
    assert_eq!(scan.total_files, 4, "scan should report 4 files");
    assert_eq!(scan.total_size, total_expected, "scan total_size wrong: expected {total_expected}, got {}", scan.total_size);

    // --- dir_sizes: leaf directory /a/b ---
    let rstr = r.to_string_lossy();
    let ab = db.get_dir_size(&format!("{}/a/b", rstr)).unwrap().expect("/a/b missing from dir_sizes");
    assert_eq!(ab.total_size, 25_000, "/a/b total_size");
    assert_eq!(ab.file_count, 1, "/a/b file_count");
    assert_eq!(ab.dir_count, 0, "/a/b dir_count");

    // --- dir_sizes: /a (contains big.bin + subdir /a/b) ---
    let a = db.get_dir_size(&format!("{}/a", rstr)).unwrap().expect("/a missing from dir_sizes");
    assert_eq!(a.total_size, 125_000, "/a total_size = big.bin(100k) + /a/b(25k)");
    assert_eq!(a.file_count, 2, "/a file_count = big.bin + medium.txt");
    assert_eq!(a.dir_count, 1, "/a dir_count = /a/b");

    // --- dir_sizes: /c ---
    let c = db.get_dir_size(&format!("{}/c", rstr)).unwrap().expect("/c missing from dir_sizes");
    assert_eq!(c.total_size, 5_000, "/c total_size");
    assert_eq!(c.file_count, 1, "/c file_count");
    assert_eq!(c.dir_count, 0, "/c dir_count");

    // --- dir_sizes: root ---
    let root = db.get_dir_size(&rstr).unwrap().expect("root missing from dir_sizes");
    assert_eq!(root.total_size, total_expected, "root total_size = sum of all files");
    assert_eq!(root.file_count, 4, "root file_count");
    assert_eq!(root.dir_count, 3, "root dir_count = a, a/b, c");

    // --- Verify sum: root == /a + /c + tiny.txt ---
    assert_eq!(root.total_size, a.total_size + c.total_size + tiny_size,
        "root should equal sum of children: a({}) + c({}) + tiny({})", a.total_size, c.total_size, tiny_size);

    // --- Query layer: overview ---
    let overview = query::query_overview(&db, Some(&rstr), 1).unwrap();
    assert_eq!(overview.total_size, total_expected, "overview total");
    assert_eq!(overview.file_count, 4, "overview file_count");

    // Children percentages should sum to ~100%
    let pct_sum: f64 = overview.children.iter().map(|c| c.percentage).sum();
    assert!((pct_sum - 100.0).abs() < 1.0, "child percentages should sum to ~100%, got {pct_sum}");

    // --- Query layer: largest files in correct order ---
    let large = query::query_large_items(&db, Some(&rstr), "files", 0, 10, None, None).unwrap();
    assert_eq!(large.items.len(), 4, "should find all 4 files");
    assert_eq!(large.items[0].size_bytes, 100_000, "1st = big.bin");
    assert_eq!(large.items[1].size_bytes, 25_000, "2nd = medium.txt");
    assert_eq!(large.items[2].size_bytes, 5_000, "3rd = small.dat");
    assert_eq!(large.items[3].size_bytes, tiny_size, "4th = tiny.txt");

    // --- Query layer: largest directories ---
    let large_dirs = query::query_large_items(&db, Some(&rstr), "directories", 0, 10, None, None).unwrap();
    // The root dir should be biggest
    assert!(!large_dirs.items.is_empty());
    assert_eq!(large_dirs.items[0].size_bytes, total_expected, "biggest dir = root");

    // --- Extension stats ---
    let types = query::query_usage_by_type(&db, Some(&rstr), 25).unwrap();
    let bin_stat = types.types.iter().find(|t| t.extension == "bin");
    assert!(bin_stat.is_some(), "should have .bin extension");
    assert_eq!(bin_stat.unwrap().total_size, 100_000);
    assert_eq!(bin_stat.unwrap().file_count, 1);
}

#[test]
fn verify_multiple_scans_track_deletions() {
    let dir = TempDir::new().unwrap();
    let r = dir.path();

    fs::write(r.join("keep.txt"), vec![0u8; 1000]).unwrap();
    fs::write(r.join("delete_me.txt"), vec![0u8; 2000]).unwrap();

    let db = Database::open_in_memory().unwrap();
    let config = Config::default();

    // First scan
    let scan1 = crawler::run_crawl(&db, r, &config).unwrap();
    assert_eq!(scan1.total_files, 2);
    assert_eq!(scan1.total_size, 3000);

    // Delete one file and rescan
    fs::remove_file(r.join("delete_me.txt")).unwrap();
    let scan2 = crawler::run_crawl(&db, r, &config).unwrap();
    assert_eq!(scan2.total_files, 1, "second scan should see 1 file");
    assert_eq!(scan2.total_size, 1000, "second scan size = keep.txt only");

    // dir_sizes should reflect the new state
    let rstr = r.to_string_lossy();
    let ds = db.get_dir_size(&rstr).unwrap().unwrap();
    assert_eq!(ds.total_size, 1000, "dir_size should reflect deletion");
    assert_eq!(ds.file_count, 1);
}

#[test]
fn verify_empty_directories_have_zero_size() {
    let dir = TempDir::new().unwrap();
    let r = dir.path();

    fs::create_dir_all(r.join("empty1/empty2")).unwrap();
    fs::write(r.join("file.txt"), "hello").unwrap();

    let db = Database::open_in_memory().unwrap();
    let config = Config::default();
    crawler::run_crawl(&db, r, &config).unwrap();

    let rstr = r.to_string_lossy();
    let empty2 = db.get_dir_size(&format!("{}/empty1/empty2", rstr)).unwrap().unwrap();
    assert_eq!(empty2.total_size, 0, "empty dir should have 0 size");
    assert_eq!(empty2.file_count, 0);

    let empty1 = db.get_dir_size(&format!("{}/empty1", rstr)).unwrap().unwrap();
    assert_eq!(empty1.total_size, 0, "parent of empty dir also 0");
}
