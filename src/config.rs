use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Returns the disk-inventory config directory: ~/.disk-inventory/
pub fn config_dir() -> PathBuf {
    let home = dirs::home_dir().expect("could not determine home directory");
    home.join(".disk-inventory")
}

/// Returns the default config file path: ~/.disk-inventory/config.toml
pub fn config_path() -> PathBuf {
    config_dir().join("config.toml")
}

/// Expand a leading `~` to the user's home directory.
pub fn expand_tilde(path: &str) -> PathBuf {
    if path == "~" {
        dirs::home_dir().expect("could not determine home directory")
    } else if let Some(rest) = path.strip_prefix("~/") {
        dirs::home_dir()
            .expect("could not determine home directory")
            .join(rest)
    } else {
        PathBuf::from(path)
    }
}

// ---------------------------------------------------------------------------
// Config structs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub daemon: DaemonConfig,
    pub scanner: ScannerConfig,
    pub database: DatabaseConfig,
    pub waste: WasteConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            daemon: DaemonConfig::default(),
            scanner: ScannerConfig::default(),
            database: DatabaseConfig::default(),
            waste: WasteConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DaemonConfig {
    pub scan_interval_secs: u64,
    pub snapshot_interval_secs: u64,
    pub watch_paths: Vec<String>,
    pub enable_watcher: bool,
    pub watcher_debounce_secs: u64,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            scan_interval_secs: 21600,
            snapshot_interval_secs: 86400,
            watch_paths: vec!["~".to_string()],
            enable_watcher: true,
            watcher_debounce_secs: 5,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ScannerConfig {
    pub exclude_patterns: Vec<String>,
    pub max_depth: u32,
    pub follow_symlinks: bool,
    pub cross_filesystems: bool,
}

impl Default for ScannerConfig {
    fn default() -> Self {
        Self {
            exclude_patterns: vec![
                ".Spotlight-V100".to_string(),
                ".fseventsd".to_string(),
                ".DocumentRevisions-V100".to_string(),
                ".Trashes".to_string(),
                ".vol".to_string(),
                ".DS_Store".to_string(),
                "Thumbs.db".to_string(),
                ".disk-inventory".to_string(),
                ".TimeMachine".to_string(),
            ],
            max_depth: 128,
            follow_symlinks: false,
            cross_filesystems: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DatabaseConfig {
    pub path: String,
    pub history_retention_days: u32,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            path: "~/.disk-inventory/index.db".to_string(),
            history_retention_days: 90,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WasteConfig {
    pub disabled_categories: Vec<String>,
    pub custom_rules: Vec<CustomWasteRule>,
}

impl Default for WasteConfig {
    fn default() -> Self {
        Self {
            disabled_categories: Vec::new(),
            custom_rules: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomWasteRule {
    pub name: String,
    pub pattern: String,
    pub category: String,
    pub safety: String,
    pub cleanup: Option<String>,
}

// ---------------------------------------------------------------------------
// Config methods
// ---------------------------------------------------------------------------

impl Config {
    /// Load config from the default path (~/.disk-inventory/config.toml).
    /// Falls back to defaults if the file does not exist.
    pub fn load() -> Result<Self> {
        let path = config_path();
        if path.exists() {
            Self::load_from(&path)
        } else {
            Ok(Self::default())
        }
    }

    /// Load config from a specific file path.
    pub fn load_from(path: &std::path::Path) -> Result<Self> {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config file: {}", path.display()))?;
        let config: Config = toml::from_str(&contents)
            .with_context(|| format!("failed to parse config file: {}", path.display()))?;
        Ok(config)
    }

    /// Resolve the database path with tilde expansion.
    pub fn db_path(&self) -> PathBuf {
        expand_tilde(&self.database.path)
    }

    /// Resolve watch paths with tilde expansion.
    pub fn resolved_watch_paths(&self) -> Vec<PathBuf> {
        self.daemon
            .watch_paths
            .iter()
            .map(|p| expand_tilde(p))
            .collect()
    }

    /// Check if a filename matches any exclude pattern.
    /// Supports simple glob patterns with leading and/or trailing `*` wildcards.
    pub fn is_excluded(&self, name: &str) -> bool {
        self.scanner.exclude_patterns.iter().any(|pattern| {
            match (pattern.starts_with('*'), pattern.ends_with('*')) {
                // *foo* — contains match (strip both wildcards)
                (true, true) => {
                    let inner = &pattern[1..pattern.len() - 1];
                    name.contains(inner)
                }
                // *foo — suffix match
                (true, false) => {
                    let suffix = &pattern[1..];
                    name.ends_with(suffix)
                }
                // foo* — prefix match
                (false, true) => {
                    let prefix = &pattern[..pattern.len() - 1];
                    name.starts_with(prefix)
                }
                // exact match
                (false, false) => name == pattern,
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_default_config() {
        let config = Config::default();

        // Daemon defaults
        assert_eq!(config.daemon.scan_interval_secs, 21600);
        assert_eq!(config.daemon.snapshot_interval_secs, 86400);
        assert_eq!(config.daemon.watch_paths, vec!["~".to_string()]);

        // Scanner defaults
        assert_eq!(config.scanner.max_depth, 128);
        assert!(!config.scanner.follow_symlinks);
        assert!(!config.scanner.cross_filesystems);
        assert!(config.scanner.exclude_patterns.contains(&".DS_Store".to_string()));
        assert!(config.scanner.exclude_patterns.contains(&".Spotlight-V100".to_string()));
        assert!(config.scanner.exclude_patterns.contains(&".TimeMachine".to_string()));
        assert_eq!(config.scanner.exclude_patterns.len(), 9);

        // Database defaults
        assert_eq!(config.database.path, "~/.disk-inventory/index.db");
        assert_eq!(config.database.history_retention_days, 90);

        // Waste defaults
        assert!(config.waste.disabled_categories.is_empty());
        assert!(config.waste.custom_rules.is_empty());
    }

    #[test]
    fn test_load_missing_file() {
        let config = Config::load_from(std::path::Path::new("/nonexistent/path/config.toml"));
        // Should fail to read a missing file; but Config::load() falls back to defaults
        assert!(config.is_err());

        // The load() method with default path may or may not find a file,
        // but we can test by checking that a missing file path returns defaults
        // via the load() codepath logic. Let's test via a temp dir with no file.
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("config.toml");
        // load_from would error, but we confirm load() handles missing gracefully
        // by testing load_from on a non-existent file returns Err
        assert!(Config::load_from(&missing).is_err());

        // Verify that the load() function itself returns defaults (it checks existence first)
        let config = Config::load().unwrap();
        // Should at minimum have the default values
        assert_eq!(config.daemon.scan_interval_secs, 21600);
    }

    #[test]
    fn test_load_partial_toml() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("config.toml");

        {
            let mut f = std::fs::File::create(&path).unwrap();
            writeln!(
                f,
                r#"
[daemon]
scan_interval_secs = 3600

[scanner]
max_depth = 64
"#
            )
            .unwrap();
        }

        let config = Config::load_from(&path).unwrap();

        // Overridden values
        assert_eq!(config.daemon.scan_interval_secs, 3600);
        assert_eq!(config.scanner.max_depth, 64);

        // Defaults should still apply for unspecified fields
        assert_eq!(config.daemon.snapshot_interval_secs, 86400);
        assert_eq!(config.daemon.watch_paths, vec!["~".to_string()]);
        assert!(!config.scanner.follow_symlinks);
        assert_eq!(config.database.path, "~/.disk-inventory/index.db");
        assert_eq!(config.database.history_retention_days, 90);
        assert!(config.waste.disabled_categories.is_empty());
    }

    #[test]
    fn test_expand_tilde() {
        let home = dirs::home_dir().unwrap();

        // Bare tilde
        assert_eq!(expand_tilde("~"), home);

        // Tilde with path
        assert_eq!(expand_tilde("~/Documents"), home.join("Documents"));
        assert_eq!(
            expand_tilde("~/a/b/c"),
            home.join("a").join("b").join("c")
        );

        // No tilde — returned as-is
        assert_eq!(expand_tilde("/usr/local"), PathBuf::from("/usr/local"));
        assert_eq!(expand_tilde("relative/path"), PathBuf::from("relative/path"));
    }

    #[test]
    fn test_is_excluded() {
        let config = Config::default();

        // Exact matches from default patterns
        assert!(config.is_excluded(".DS_Store"));
        assert!(config.is_excluded("Thumbs.db"));
        assert!(config.is_excluded(".Spotlight-V100"));
        assert!(config.is_excluded(".disk-inventory"));
        assert!(config.is_excluded(".TimeMachine"));

        // Should NOT match things not in the list
        assert!(!config.is_excluded("normal_file.txt"));
        assert!(!config.is_excluded("Documents"));

        // Test wildcard patterns via a custom config
        let mut custom = Config::default();
        custom.scanner.exclude_patterns = vec![
            "*.log".to_string(),
            "build_*".to_string(),
            "*temp*".to_string(),
            "exact".to_string(),
        ];

        // Suffix wildcard: *.log
        assert!(custom.is_excluded("app.log"));
        assert!(custom.is_excluded("debug.log"));
        assert!(!custom.is_excluded("log.txt"));

        // Prefix wildcard: build_*
        assert!(custom.is_excluded("build_output"));
        assert!(custom.is_excluded("build_"));
        assert!(!custom.is_excluded("my_build_output"));

        // Contains wildcard: *temp*
        assert!(custom.is_excluded("temp"));
        assert!(custom.is_excluded("my_temp_file"));
        assert!(custom.is_excluded("temporary"));

        // Exact match
        assert!(custom.is_excluded("exact"));
        assert!(!custom.is_excluded("not_exact"));
    }
}
