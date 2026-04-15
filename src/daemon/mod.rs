use crate::config::Config;
use crate::crawler;
use crate::db::Database;
use crate::models::format_size;
use anyhow::Result;

/// Run a single crawl of all configured watch paths and exit.
pub fn run_once(config: &Config) -> Result<()> {
    let db = Database::open(config.db_path())?;

    for watch_path in config.resolved_watch_paths() {
        if !watch_path.exists() {
            tracing::warn!("Watch path does not exist, skipping: {}", watch_path.display());
            continue;
        }
        tracing::info!("Crawling {}", watch_path.display());
        let scan = crawler::run_crawl(&db, &watch_path, config)?;
        tracing::info!(
            "Crawl complete: {} files, {} dirs, {}",
            scan.total_files, scan.total_dirs, format_size(scan.total_size),
        );
    }

    Ok(())
}

/// Print daemon/scan status.
pub fn show_status(config: &Config) -> Result<()> {
    let db_path = config.db_path();
    if !db_path.exists() {
        println!("No database found at {}", db_path.display());
        println!("Run `disk-inventory daemon run` to create the index.");
        return Ok(());
    }
    let db = Database::open(&db_path)?;
    match db.latest_scan()? {
        Some(scan) => {
            println!("Database: {}", db_path.display());
            println!("Last scan: {} ({})",
                chrono::DateTime::from_timestamp(scan.started_at, 0)
                    .map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
                    .unwrap_or_else(|| "unknown".to_string()),
                scan.status.as_str());
            println!("Root: {}", scan.root_path);
            println!("Files: {}", scan.total_files);
            println!("Dirs: {}", scan.total_dirs);
            println!("Total size: {}", format_size(scan.total_size));
        }
        None => {
            println!("Database exists but no scans have been run yet.");
            println!("Run `disk-inventory daemon run` to scan.");
        }
    }
    Ok(())
}
