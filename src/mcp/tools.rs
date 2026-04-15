use rmcp::schemars::{self, JsonSchema};
use serde::Deserialize;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DiskOverviewParams {
    #[schemars(description = "Root path to analyze (default: home directory)")]
    pub path: Option<String>,
    #[schemars(description = "Directory depth to summarize (default: 1, max: 3)")]
    pub depth: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct FindLargeItemsParams {
    #[schemars(description = "Directory to search within")]
    pub path: Option<String>,
    #[schemars(description = "Find 'files', 'directories', or 'both' (default: both)")]
    pub item_type: Option<String>,
    #[schemars(description = "Minimum size in bytes (default: 104857600 = 100MB)")]
    pub min_size_bytes: Option<u64>,
    #[schemars(description = "Maximum number of results (default: 20, max: 100)")]
    pub limit: Option<u32>,
    #[schemars(description = "Filter by file extensions, e.g. ['mp4', 'zip']")]
    pub file_extensions: Option<Vec<String>>,
    #[schemars(description = "Only include items not accessed in this many days")]
    pub older_than_days: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DiskUsageByTypeParams {
    #[schemars(description = "Directory to analyze")]
    pub path: Option<String>,
    #[schemars(description = "Number of top file types to return (default: 25)")]
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SearchFilesParams {
    #[schemars(description = "Directory to search within")]
    pub path: Option<String>,
    #[schemars(description = "Pattern to search for in file names")]
    pub name_pattern: String,
    #[schemars(description = "Minimum file size in bytes")]
    pub min_size_bytes: Option<u64>,
    #[schemars(description = "Maximum file size in bytes")]
    pub max_size_bytes: Option<u64>,
    #[schemars(description = "Maximum results (default: 50)")]
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ScanStatusParams {
    #[schemars(description = "Check 'status' or trigger 'rescan' (default: status)")]
    pub action: Option<String>,
    #[schemars(description = "Path to check or rescan")]
    pub path: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct FindWasteParams {
    #[schemars(description = "Directory to search within")]
    pub path: Option<String>,
    #[schemars(description = "Categories to scan (default: all)")]
    pub categories: Option<Vec<String>>,
    #[schemars(description = "Minimum total size per category in bytes (default: 50MB)")]
    pub min_size_bytes: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct FindDuplicatesParams {
    #[schemars(description = "Directory to search within")]
    pub path: Option<String>,
    #[schemars(description = "Minimum file size in bytes (default: 1MB)")]
    pub min_size_bytes: Option<u64>,
    #[schemars(description = "Maximum number of duplicate groups (default: 20)")]
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DiskTrendsParams {
    #[schemars(description = "Directory to analyze")]
    pub path: Option<String>,
    #[schemars(description = "Time period: day, week, month (default: week)")]
    pub period: Option<String>,
    #[schemars(description = "Number of results (default: 20)")]
    pub limit: Option<u32>,
}
