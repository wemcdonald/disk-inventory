use clap::{Parser, Subcommand};
use disk_inventory::cli::{self, OutputFormat};

#[derive(Parser)]
#[command(name = "disk-inventory", version, about = "Fast disk usage analysis")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Show disk usage overview
    Usage {
        /// Root path to analyze
        path: Option<String>,
        /// Depth of directory tree to display
        #[arg(short, long, default_value = "1")]
        depth: u32,
        /// Output format
        #[arg(short = 'f', long, value_enum, default_value = "table")]
        format: OutputFormat,
    },
    /// Show largest files and directories
    Top {
        /// Root path to analyze
        path: Option<String>,
        /// Show only files
        #[arg(long)]
        files: bool,
        /// Show only directories
        #[arg(long)]
        dirs: bool,
        /// Filter by file extensions (comma-separated)
        #[arg(short, long)]
        ext: Option<String>,
        /// Only show items older than N days
        #[arg(long)]
        older: Option<u32>,
        /// Maximum number of results
        #[arg(short, long, default_value = "20")]
        limit: u32,
        /// Output format
        #[arg(short = 'f', long, value_enum, default_value = "table")]
        format: OutputFormat,
    },
    /// Find reclaimable disk space
    Waste {
        path: Option<String>,
        /// Category to scan (e.g., node_modules, build_artifacts)
        #[arg(short, long)]
        category: Option<String>,
        /// Minimum total size per category in bytes
        #[arg(long)]
        min_size: Option<u64>,
        #[arg(short, long, default_value = "20")]
        limit: u32,
        #[arg(short = 'f', long, value_enum, default_value = "table")]
        format: OutputFormat,
    },
    /// Search files by name pattern
    Search {
        /// Glob pattern to search for
        pattern: String,
        /// Root path to search within
        path: Option<String>,
        /// Maximum number of results
        #[arg(short, long, default_value = "50")]
        limit: u32,
        /// Output format
        #[arg(short = 'f', long, value_enum, default_value = "table")]
        format: OutputFormat,
    },
    /// Show disk usage by file type
    Types {
        /// Root path to analyze
        path: Option<String>,
        /// Maximum number of types to show
        #[arg(short, long, default_value = "25")]
        limit: u32,
        /// Output format
        #[arg(short = 'f', long, value_enum, default_value = "table")]
        format: OutputFormat,
    },
    /// Show disk usage trends over time
    Trends {
        path: Option<String>,
        /// Time period: day, week, month, quarter, year
        #[arg(short, long, default_value = "week")]
        period: String,
        #[arg(short, long, default_value = "20")]
        limit: u32,
        #[arg(short = 'f', long, value_enum, default_value = "table")]
        format: OutputFormat,
    },
    /// Find duplicate files
    Duplicates {
        path: Option<String>,
        /// Minimum file size in bytes to check
        #[arg(long, default_value = "1048576")]
        min_size: u64,
        #[arg(short, long, default_value = "20")]
        limit: u32,
        #[arg(short = 'f', long, value_enum, default_value = "table")]
        format: OutputFormat,
    },
    /// Manage the background daemon
    Daemon {
        #[command(subcommand)]
        action: DaemonAction,
    },
    /// Start MCP server (stdio transport)
    Mcp,
}

#[derive(Subcommand)]
enum DaemonAction {
    /// Run daemon in foreground
    Run {
        /// Run a single crawl and exit (don't start periodic daemon)
        #[arg(long)]
        once: bool,
    },
    /// Install as system service
    Install,
    /// Remove system service
    Uninstall,
    /// Show daemon status
    Status,
    /// Trigger an immediate rescan via IPC
    Rescan {
        /// Optional path to rescan (defaults to all watch paths)
        path: Option<String>,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Usage {
            path,
            depth,
            format,
        } => cli::run_usage(path, depth, &format),
        Commands::Top {
            path,
            files,
            dirs,
            ext,
            older,
            limit,
            format,
        } => cli::run_top(path, files, dirs, ext, older, limit, &format),
        Commands::Waste { path, category, min_size, limit: _, format } =>
            cli::run_waste(path, category, min_size, &format),
        Commands::Search {
            pattern,
            path,
            limit,
            format,
        } => cli::run_search(pattern, path, limit, &format),
        Commands::Types {
            path,
            limit,
            format,
        } => cli::run_types(path, limit, &format),
        Commands::Trends { path, period, limit, format } =>
            cli::run_trends(path, &period, limit, &format),
        Commands::Duplicates { path, min_size, limit, format } =>
            cli::run_duplicates(path, min_size, limit, &format),
        Commands::Daemon { action } => match action {
            DaemonAction::Run { once } => {
                tracing_subscriber::fmt()
                    .with_env_filter(
                        tracing_subscriber::EnvFilter::try_from_default_env()
                            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
                    )
                    .with_writer(std::io::stderr)
                    .init();
                let config = disk_inventory::config::Config::load()?;
                if once {
                    disk_inventory::daemon::run_once(&config)?;
                } else {
                    let rt = tokio::runtime::Runtime::new()?;
                    rt.block_on(disk_inventory::daemon::run_daemon(config))?;
                }
                Ok(())
            }
            DaemonAction::Install => {
                disk_inventory::daemon::service::install()?;
                Ok(())
            }
            DaemonAction::Uninstall => {
                disk_inventory::daemon::service::uninstall()?;
                Ok(())
            }
            DaemonAction::Status => {
                let config = disk_inventory::config::Config::load()?;
                disk_inventory::daemon::show_status(&config)?;
                Ok(())
            }
            DaemonAction::Rescan { path } => {
                let cmd = match &path {
                    Some(p) => format!("rescan {}", p),
                    None => "rescan".to_string(),
                };
                match disk_inventory::daemon::send_ipc_command(&cmd) {
                    Ok(response) => {
                        println!("{}", response.trim());
                        Ok(())
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        std::process::exit(1);
                    }
                }
            }
        },
        Commands::Mcp => {
            // Initialize tracing to stderr (stdout is for MCP protocol)
            tracing_subscriber::fmt()
                .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                .with_writer(std::io::stderr)
                .init();
            let config = disk_inventory::config::Config::load()?;
            let db = disk_inventory::db::Database::open(config.db_path())?;
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(disk_inventory::mcp::run_mcp_server(db))?;
            Ok(())
        }
    }
}
