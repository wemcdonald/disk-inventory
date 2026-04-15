use bytesize::ByteSize;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// FileType
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum FileType {
    File = 0,
    Directory = 1,
    Symlink = 2,
    Other = 3,
}

impl FileType {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => FileType::File,
            1 => FileType::Directory,
            2 => FileType::Symlink,
            _ => FileType::Other,
        }
    }
}

// ---------------------------------------------------------------------------
// FileEntry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    pub id: Option<i64>,
    pub path: String,
    pub parent_path: String,
    pub name: String,
    pub extension: Option<String>,
    pub file_type: FileType,
    pub inode: u64,
    pub device_id: u64,
    pub hardlink_count: u64,
    pub symlink_target: Option<String>,
    pub size_bytes: u64,
    pub blocks: u64,
    pub mtime: i64,
    pub ctime: i64,
    pub atime: i64,
    pub birth_time: Option<i64>,
    pub uid: u32,
    pub gid: u32,
    pub mode: u32,
    pub scan_id: i64,
    pub first_seen_scan: i64,
    pub is_deleted: bool,
    pub depth: u32,
    pub path_components: u32,
}

// ---------------------------------------------------------------------------
// DirSize
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirSize {
    pub path: String,
    pub total_size: u64,
    pub file_count: u64,
    pub dir_count: u64,
    pub max_depth: u32,
    pub largest_file: u64,
    pub scan_id: i64,
}

// ---------------------------------------------------------------------------
// ScanStatus / ScanInfo
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScanStatus {
    Running,
    Completed,
    Failed,
}

impl ScanStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ScanStatus::Running => "running",
            ScanStatus::Completed => "completed",
            ScanStatus::Failed => "failed",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "running" => Some(ScanStatus::Running),
            "completed" => Some(ScanStatus::Completed),
            "failed" => Some(ScanStatus::Failed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanInfo {
    pub id: i64,
    pub root_path: String,
    pub started_at: i64,
    pub completed_at: Option<i64>,
    pub total_files: u64,
    pub total_dirs: u64,
    pub total_size: u64,
    pub files_added: u64,
    pub files_modified: u64,
    pub files_deleted: u64,
    pub status: ScanStatus,
}

// ---------------------------------------------------------------------------
// SizeHistoryEntry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SizeHistoryEntry {
    pub path: String,
    pub scan_id: i64,
    pub recorded_at: i64,
    pub total_size: u64,
    pub file_count: u64,
    pub delta_size: i64,
    pub delta_files: i64,
}

// ---------------------------------------------------------------------------
// ExtensionStat
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtensionStat {
    pub extension: String,
    pub file_count: u64,
    pub total_size: u64,
    pub avg_size: u64,
    pub largest_size: u64,
}

// ---------------------------------------------------------------------------
// SafetyRating / WasteItem
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SafetyRating {
    Safe,
    Review,
    Caution,
}

impl SafetyRating {
    pub fn as_str(&self) -> &'static str {
        match self {
            SafetyRating::Safe => "safe",
            SafetyRating::Review => "review",
            SafetyRating::Caution => "caution",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasteItem {
    pub path: String,
    pub category: String,
    pub size_bytes: u64,
    pub safety: SafetyRating,
    pub cleanup_command: Option<String>,
    pub description: String,
}

// ---------------------------------------------------------------------------
// DuplicateFile / DuplicateGroup
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuplicateFile {
    pub path: String,
    pub modified: i64,
    pub accessed: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuplicateGroup {
    pub hash: String,
    pub size_bytes: u64,
    pub wasted_bytes: u64,
    pub files: Vec<DuplicateFile>,
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Human-readable size string (e.g. "1.5 MiB").
pub fn format_size(bytes: u64) -> String {
    ByteSize(bytes).to_string()
}

/// Extract lowercase extension without the dot.
/// Returns `None` for dotfiles (e.g. ".hidden"), names with no extension,
/// and names ending with a trailing dot.
pub fn extract_extension(name: &str) -> Option<String> {
    // No dot at all → no extension
    let dot_pos = name.rfind('.')?;

    // Dotfile with no further extension (e.g. ".hidden") or leading dot only
    if dot_pos == 0 {
        return None;
    }

    let ext = &name[dot_pos + 1..];

    // Trailing dot (e.g. "file.") → no meaningful extension
    if ext.is_empty() {
        return None;
    }

    Some(ext.to_lowercase())
}

/// Extract parent directory path. Returns "/" for top-level paths.
pub fn parent_path(path: &str) -> String {
    match path.rfind('/') {
        Some(0) => "/".to_string(),
        Some(pos) => path[..pos].to_string(),
        None => "/".to_string(),
    }
}

/// Count depth based on slashes: "/" → 0, "/Users" → 1, "/Users/will" → 2.
pub fn path_depth(path: &str) -> u32 {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return 0;
    }
    // Count slashes, but the leading "/" doesn't add depth for the root itself
    let slash_count = trimmed.chars().filter(|&c| c == '/').count() as u32;
    if trimmed.starts_with('/') {
        // "/Users" has 1 slash → depth 1; "/" trimmed to "" handled above
        slash_count
    } else {
        slash_count
    }
}

/// Number of non-empty path components.
/// "/" → 0, "/Users" → 1, "/Users/will" → 2.
pub fn path_component_count(path: &str) -> u32 {
    path.split('/')
        .filter(|s| !s.is_empty())
        .count() as u32
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_type_roundtrip() {
        assert_eq!(FileType::from_u8(0), FileType::File);
        assert_eq!(FileType::from_u8(1), FileType::Directory);
        assert_eq!(FileType::from_u8(2), FileType::Symlink);
        assert_eq!(FileType::from_u8(3), FileType::Other);
        // Unknown values map to Other
        assert_eq!(FileType::from_u8(99), FileType::Other);
    }

    #[test]
    fn test_extract_extension() {
        // Normal extensions
        assert_eq!(extract_extension("photo.jpg"), Some("jpg".to_string()));
        assert_eq!(extract_extension("archive.tar.gz"), Some("gz".to_string()));

        // Dotfiles → None
        assert_eq!(extract_extension(".hidden"), None);

        // No extension → None
        assert_eq!(extract_extension("Makefile"), None);

        // Trailing dot → None
        assert_eq!(extract_extension("file."), None);

        // Uppercase → lowercased
        assert_eq!(extract_extension("UPPER.JPG"), Some("jpg".to_string()));
    }

    #[test]
    fn test_parent_path() {
        assert_eq!(parent_path("/Users/will/file.txt"), "/Users/will");
        assert_eq!(parent_path("/Users"), "/");
        assert_eq!(parent_path("/"), "/");
    }

    #[test]
    fn test_path_depth() {
        assert_eq!(path_depth("/"), 0);
        assert_eq!(path_depth("/Users"), 1);
        assert_eq!(path_depth("/Users/will"), 2);
        assert_eq!(path_depth("/Users/will/code"), 3);
    }

    #[test]
    fn test_path_component_count() {
        assert_eq!(path_component_count("/"), 0);
        assert_eq!(path_component_count("/Users"), 1);
        assert_eq!(path_component_count("/Users/will"), 2);
        assert_eq!(path_component_count("/Users/will/code"), 3);
    }

    #[test]
    fn test_format_size() {
        // Just verify it doesn't panic and returns something reasonable
        let s = format_size(0);
        assert!(!s.is_empty());
        let s = format_size(1024);
        assert!(!s.is_empty());
        let s = format_size(1_073_741_824);
        assert!(!s.is_empty());
    }

    #[test]
    fn test_scan_status_roundtrip() {
        for status in &[ScanStatus::Running, ScanStatus::Completed, ScanStatus::Failed] {
            let s = status.as_str();
            let back = ScanStatus::from_str(s).expect("should parse back");
            assert_eq!(*status, back);
        }
        // Unknown string returns None
        assert_eq!(ScanStatus::from_str("unknown"), None);
    }
}
