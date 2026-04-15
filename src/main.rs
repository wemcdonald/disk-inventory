use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "disk-inventory", version, about = "Fast disk usage analysis")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Show disk usage overview
    Usage,
    /// Show largest files and directories
    Top,
    /// Find reclaimable disk space
    Waste,
    /// Search files by name pattern
    Search {
        /// Glob pattern to search for
        pattern: String,
    },
    /// Show disk usage by file type
    Types,
    /// Show disk usage trends over time
    Trends,
    /// Find duplicate files
    Duplicates,
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
        Commands::Usage => todo!("usage"),
        Commands::Top => todo!("top"),
        Commands::Waste => todo!("waste"),
        Commands::Search { pattern: _ } => todo!("search"),
        Commands::Types => todo!("types"),
        Commands::Trends => todo!("trends"),
        Commands::Duplicates => todo!("duplicates"),
        Commands::Daemon { action } => match action {
            DaemonAction::Run => todo!("daemon run"),
            DaemonAction::Install => todo!("daemon install"),
            DaemonAction::Uninstall => todo!("daemon uninstall"),
            DaemonAction::Status => todo!("daemon status"),
        },
        Commands::Mcp => todo!("mcp"),
    }
}
