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
    Waste {},
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
    Trends {},
    /// Find duplicate files
    Duplicates {},
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
    Run,
    /// Install as system service
    Install,
    /// Remove system service
    Uninstall,
    /// Show daemon status
    Status,
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
        Commands::Waste {} => todo!("waste"),
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
        Commands::Trends {} => todo!("trends"),
        Commands::Duplicates {} => todo!("duplicates"),
        Commands::Daemon { action } => match action {
            DaemonAction::Run => {
                tracing_subscriber::fmt()
                    .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                    .with_writer(std::io::stderr)
                    .init();
                let config = disk_inventory::config::Config::load()?;
                disk_inventory::daemon::run_once(&config)?;
                Ok(())
            }
            DaemonAction::Install => todo!("daemon install"),
            DaemonAction::Uninstall => todo!("daemon uninstall"),
            DaemonAction::Status => {
                let config = disk_inventory::config::Config::load()?;
                disk_inventory::daemon::show_status(&config)?;
                Ok(())
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
