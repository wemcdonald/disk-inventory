pub mod service;

use crate::config::Config;
use crate::crawler;
use crate::db::Database;
use crate::models::format_size;
use anyhow::{Context, Result};
use std::path::PathBuf;
use std::sync::Arc;
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

    // Prevent multiple daemon instances
    use std::fs::OpenOptions;
    let lock_path = crate::config::config_dir().join("daemon.lock");
    let lock_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&lock_path)
        .context("failed to open daemon lock file")?;

    use std::os::unix::io::AsRawFd;
    let lock_result = unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if lock_result != 0 {
        anyhow::bail!("Another daemon instance is already running (lock file: {})", lock_path.display());
    }
    // Write PID for diagnostics
    use std::io::Write;
    let mut lf = &lock_file;
    let _ = write!(lf, "{}", std::process::id());

    // Keep lock_file alive for the duration of the daemon (do not drop it)
    let _lock_file = lock_file;

    // Scan lock: prevents concurrent crawls from periodic timer, IPC rescan,
    // and watcher from stomping on each other.
    let scanning = Arc::new(std::sync::atomic::AtomicBool::new(false));

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

    // Optional filesystem watcher
    let watcher = if config.daemon.enable_watcher {
        match crate::watcher::DebouncedWatcher::new(
            &config.resolved_watch_paths(),
            config.daemon.watcher_debounce_secs,
        ) {
            Ok(w) => {
                tracing::info!("Filesystem watcher enabled ({}s debounce)",
                    config.daemon.watcher_debounce_secs);
                Some(tokio::sync::Mutex::new(w))
            }
            Err(e) => {
                tracing::warn!("Failed to start filesystem watcher: {}", e);
                None
            }
        }
    } else {
        tracing::info!("Filesystem watcher disabled");
        None
    };

    // Watcher poll timer — check for events every 2s
    let mut watcher_timer = tokio::time::interval(std::time::Duration::from_secs(2));
    watcher_timer.tick().await;

    // Dir sizes recompute timer — 30s after watcher updates
    let mut dir_sizes_timer = tokio::time::interval(std::time::Duration::from_secs(30));
    dir_sizes_timer.tick().await;
    let mut dirs_dirty = false;

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
                if scanning.compare_exchange(false, true, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_ok() {
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
                    scanning.store(false, std::sync::atomic::Ordering::SeqCst);
                } else {
                    tracing::info!("Periodic scan skipped: scan already in progress");
                }
            }

            // IPC connection
            result = listener.accept() => {
                match result {
                    Ok((stream, _)) => {
                        let db = db.clone();
                        let config = config.clone();
                        let scanning = Arc::clone(&scanning);
                        tokio::spawn(async move {
                            if let Err(e) = handle_ipc_connection(stream, &db, &config, &scanning).await {
                                tracing::error!("IPC error: {}", e);
                            }
                        });
                    }
                    Err(e) => tracing::error!("IPC accept error: {}", e),
                }
            }

            // Filesystem watcher poll
            _ = watcher_timer.tick() => {
                if !scanning.load(std::sync::atomic::Ordering::SeqCst) {
                    if let Some(ref watcher_mutex) = watcher {
                        let changed = {
                            let mut w = watcher_mutex.lock().await;
                            w.poll()
                        };
                        if let Some(dirs) = changed {
                            if !dirs.is_empty() {
                                tracing::debug!("Watcher: {} directories changed", dirs.len());
                                let dir_vec: Vec<_> = dirs.into_iter().collect();
                                match crawler::rescan_directories(&db, &dir_vec, &config) {
                                    Ok(n) => {
                                        if n > 0 {
                                            tracing::info!("Watcher rescan: {} entries updated", n);
                                            dirs_dirty = true;
                                        }
                                    }
                                    Err(e) => tracing::error!("Watcher rescan error: {}", e),
                                }
                            }
                        }
                    }
                }
            }

            // Periodic dir_sizes recompute after watcher updates
            _ = dir_sizes_timer.tick() => {
                if dirs_dirty && !scanning.load(std::sync::atomic::Ordering::SeqCst) {
                    tracing::info!("Recomputing dir_sizes after watcher updates");
                    if let Err(e) = db.recompute_dir_sizes() {
                        tracing::error!("dir_sizes recompute error: {}", e);
                    }
                    dirs_dirty = false;
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
    scanning: &Arc<std::sync::atomic::AtomicBool>,
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
            if scanning.compare_exchange(false, true, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_err() {
                r#"{"status":"scan_already_running"}"#.to_string()
            } else {
                let path = c
                    .strip_prefix("rescan")
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty());
                let paths: Vec<PathBuf> = if let Some(p) = path {
                    vec![PathBuf::from(p)]
                } else {
                    config.resolved_watch_paths()
                };

                // Spawn the crawl in the background so the IPC client gets
                // an immediate response instead of blocking for the full scan.
                let db = db.clone();
                let config = config.clone();
                let scanning = Arc::clone(scanning);
                tokio::spawn(async move {
                    for watch_path in &paths {
                        if watch_path.exists() {
                            match crawler::run_incremental_crawl(&db, watch_path, &config) {
                                Ok(scan) => {
                                    tracing::info!("Rescan complete: {} files", scan.total_files)
                                }
                                Err(e) => tracing::error!("Rescan failed: {}", e),
                            }
                        }
                    }
                    scanning.store(false, std::sync::atomic::Ordering::SeqCst);
                });
                r#"{"status":"rescan_started"}"#.to_string()
            }
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
pub fn show_status(config: &Config, format: &crate::cli::OutputFormat) -> Result<()> {
    let db_path = config.db_path();
    if !db_path.exists() {
        match format {
            crate::cli::OutputFormat::Json => {
                println!(r#"{{"status":"no_database"}}"#);
            }
            _ => {
                println!("No database found at {}", db_path.display());
                println!("Run `disk-inventory daemon run` to create the index.");
            }
        }
        return Ok(());
    }
    let db = Database::open(&db_path)?;

    match format {
        crate::cli::OutputFormat::Json => {
            let status = crate::query::query_scan_status_full(&db)?;
            println!("{}", serde_json::to_string_pretty(&status)?);
            return Ok(());
        }
        _ => {}
    }

    // Check for active scan
    if let Some((scan, progress)) = db.active_scan()? {
        println!("Daemon: scanning");
        if let Some(p) = progress {
            println!(
                "Current scan: {} (Phase {}/{})",
                p.phase, p.phase_number, p.total_phases
            );
            println!(
                "  Progress: {} files, {} dirs, {}",
                p.files_so_far, p.dirs_so_far, p.bytes_so_far_human
            );
            if !p.current_dir.is_empty() {
                println!("  Scanning: {}", p.current_dir);
            }
            println!("  Elapsed: {}s", p.elapsed_secs);
        }
        println!("  Root: {}", scan.root_path);
        println!();
    }

    // Show last completed scan
    match db.latest_scan()? {
        Some(scan) => {
            println!("Last completed scan:");
            println!(
                "  Date: {}",
                chrono::DateTime::from_timestamp(scan.started_at, 0)
                    .map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
                    .unwrap_or_else(|| "unknown".into())
            );
            println!("  Root: {}", scan.root_path);
            println!("  Files: {}", scan.total_files);
            println!("  Dirs: {}", scan.total_dirs);
            println!("  Total size: {}", format_size(scan.total_size));
        }
        None => {
            if db.active_scan()?.is_none() {
                println!("No scans have been run yet.");
                println!("Run `disk-inventory daemon run` to scan.");
            }
        }
    }
    Ok(())
}

/// Continuously refresh status every 1 second (like `watch` or `top`).
pub fn show_status_watch(config: &Config) -> Result<()> {
    let format = crate::cli::OutputFormat::Table;
    loop {
        // Clear screen and move cursor to top-left
        print!("\x1B[2J\x1B[1;1H");
        println!("disk-inventory daemon status (refreshing every 1s, Ctrl-C to stop)\n");
        show_status(config, &format)?;
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

/// Block until the current scan completes, showing progress while waiting.
pub fn wait_for_scan(config: &Config) -> Result<()> {
    let db_path = config.db_path();
    if !db_path.exists() {
        anyhow::bail!("No database found. Is the daemon running?");
    }

    println!("Waiting for scan to complete...\n");

    loop {
        let db = Database::open(&db_path)?;
        match db.active_scan()? {
            Some((_scan, Some(progress))) => {
                // Overwrite current line with progress
                print!("\r\x1B[K");
                let dir_display = if progress.current_dir.len() > 50 {
                    format!("...{}", &progress.current_dir[progress.current_dir.len()-47..])
                } else {
                    progress.current_dir.clone()
                };
                print!(
                    "Phase {}/{} ({}): {} files, {} dirs, {} — {}",
                    progress.phase_number,
                    progress.total_phases,
                    progress.phase,
                    progress.files_so_far,
                    progress.dirs_so_far,
                    progress.bytes_so_far_human,
                    dir_display,
                );
                use std::io::Write;
                std::io::stdout().flush()?;
            }
            Some((_, None)) => {
                print!("\r\x1B[KScan running (no progress data yet)...");
                use std::io::Write;
                std::io::stdout().flush()?;
            }
            None => {
                println!("\r\x1B[KScan complete!");
                // Show final stats
                if let Some(scan) = db.latest_scan()? {
                    println!(
                        "  {} files, {} dirs, {}",
                        scan.total_files,
                        scan.total_dirs,
                        format_size(scan.total_size),
                    );
                }
                return Ok(());
            }
        }
        drop(db);
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
}

/// Show daemon log output.
pub fn show_log(lines: usize, follow: bool) -> Result<()> {
    let log_path = crate::config::config_dir().join("daemon.log");
    if !log_path.exists() {
        anyhow::bail!(
            "No daemon log found at {}. Is the daemon installed as a service?",
            log_path.display()
        );
    }

    if follow {
        // Use tail -f
        let status = std::process::Command::new("tail")
            .args(["-f", "-n", &lines.to_string()])
            .arg(&log_path)
            .status()?;
        if !status.success() {
            anyhow::bail!("tail command failed");
        }
    } else {
        // Read last N lines
        let content = std::fs::read_to_string(&log_path)?;
        let all_lines: Vec<&str> = content.lines().collect();
        let start = all_lines.len().saturating_sub(lines);
        for line in &all_lines[start..] {
            println!("{}", line);
        }
    }

    Ok(())
}
