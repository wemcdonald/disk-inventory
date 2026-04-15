pub mod service;

use crate::config::Config;
use crate::crawler;
use crate::db::Database;
use crate::models::format_size;
use anyhow::{Context, Result};
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;

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

/// Run a single incremental crawl (preferred for subsequent scans).
pub fn run_incremental(config: &Config) -> Result<()> {
    let db = Database::open(config.db_path())?;

    for watch_path in config.resolved_watch_paths() {
        if !watch_path.exists() {
            tracing::warn!(
                "Watch path does not exist, skipping: {}",
                watch_path.display()
            );
            continue;
        }
        tracing::info!("Incremental crawl of {}", watch_path.display());
        let scan = crawler::run_incremental_crawl(&db, &watch_path, config)?;
        tracing::info!(
            "Incremental crawl complete: {} files, {} dirs, {}",
            scan.total_files,
            scan.total_dirs,
            format_size(scan.total_size),
        );
    }

    Ok(())
}

/// Run the daemon: initial crawl, then periodic rescans + IPC.
pub async fn run_daemon(config: Config) -> Result<()> {
    let db = Database::open(config.db_path())?;

    // Initial full crawl
    for watch_path in config.resolved_watch_paths() {
        if !watch_path.exists() {
            tracing::warn!("Watch path does not exist: {}", watch_path.display());
            continue;
        }
        tracing::info!("Initial crawl of {}", watch_path.display());
        match crawler::run_crawl(&db, &watch_path, &config) {
            Ok(scan) => tracing::info!(
                "Initial crawl complete: {} files, {}",
                scan.total_files,
                format_size(scan.total_size)
            ),
            Err(e) => tracing::error!("Initial crawl failed: {}", e),
        }
    }

    // Set up IPC socket
    let socket_path = crate::config::config_dir().join("daemon.sock");
    // Remove stale socket if it exists
    let _ = std::fs::remove_file(&socket_path);
    let listener = UnixListener::bind(&socket_path)?;
    tracing::info!("IPC listening on {}", socket_path.display());

    // Set up the periodic scan interval
    let scan_interval = std::time::Duration::from_secs(config.daemon.scan_interval_secs);
    let mut scan_timer = tokio::time::interval(scan_interval);
    scan_timer.tick().await; // consume the first immediate tick

    // Main event loop
    loop {
        tokio::select! {
            // Periodic rescan
            _ = scan_timer.tick() => {
                tracing::info!("Periodic incremental scan starting");
                for watch_path in config.resolved_watch_paths() {
                    if !watch_path.exists() { continue; }
                    match crawler::run_incremental_crawl(&db, &watch_path, &config) {
                        Ok(scan) => tracing::info!(
                            "Incremental scan complete: {} files, {}",
                            scan.total_files,
                            format_size(scan.total_size)
                        ),
                        Err(e) => tracing::error!("Incremental scan failed: {}", e),
                    }
                }
            }

            // IPC connection
            result = listener.accept() => {
                match result {
                    Ok((stream, _)) => {
                        let db = db.clone();
                        let config = config.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_ipc_connection(stream, &db, &config).await {
                                tracing::error!("IPC error: {}", e);
                            }
                        });
                    }
                    Err(e) => tracing::error!("IPC accept error: {}", e),
                }
            }

            // Shutdown signal
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("Shutting down daemon");
                let _ = std::fs::remove_file(&socket_path);
                break;
            }
        }
    }

    Ok(())
}

async fn handle_ipc_connection(
    stream: tokio::net::UnixStream,
    db: &Database,
    config: &Config,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let cmd = line.trim();

    let response = match cmd {
        "status" => match db.latest_scan()? {
            Some(scan) => serde_json::to_string(&scan)?,
            None => r#"{"status":"no_scans"}"#.to_string(),
        },
        c if c.starts_with("rescan") => {
            let path = c
                .strip_prefix("rescan")
                .map(|s| s.trim())
                .filter(|s| !s.is_empty());
            let paths: Vec<PathBuf> = if let Some(p) = path {
                vec![PathBuf::from(p)]
            } else {
                config.resolved_watch_paths()
            };

            for watch_path in &paths {
                if watch_path.exists() {
                    match crawler::run_incremental_crawl(db, watch_path, config) {
                        Ok(scan) => {
                            tracing::info!("Rescan complete: {} files", scan.total_files)
                        }
                        Err(e) => tracing::error!("Rescan failed: {}", e),
                    }
                }
            }
            r#"{"status":"rescan_complete"}"#.to_string()
        }
        _ => r#"{"error":"unknown command. Use: status, rescan [path]"}"#.to_string(),
    };

    writer.write_all(response.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    Ok(())
}

/// Send a command to the running daemon via its IPC socket.
pub fn send_ipc_command(command: &str) -> Result<String> {
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;

    let socket_path = crate::config::config_dir().join("daemon.sock");
    let mut stream = UnixStream::connect(&socket_path)
        .context("Cannot connect to daemon. Is it running?")?;

    stream.write_all(command.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.flush()?;

    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response)
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
