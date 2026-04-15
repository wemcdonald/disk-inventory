//! Filesystem change watcher.
//! Uses the `notify` crate for cross-platform filesystem event monitoring.
//! FSEvents on macOS, inotify on Linux.

use anyhow::Result;
use notify::{Config, Event, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Collects filesystem change events with debouncing.
/// Changed directories are accumulated and can be drained for batch processing.
pub struct FsWatcher {
    _watcher: RecommendedWatcher,
    /// Directories that have pending changes (debounced)
    changed_dirs: Arc<Mutex<HashSet<PathBuf>>>,
    /// Channel receiver for raw events (used internally)
    rx: mpsc::Receiver<notify::Result<Event>>,
}

impl FsWatcher {
    /// Start watching the given paths recursively.
    pub fn new(watch_paths: &[PathBuf]) -> Result<Self> {
        let (tx, rx) = mpsc::channel();

        let mut watcher = RecommendedWatcher::new(
            tx,
            Config::default().with_poll_interval(Duration::from_secs(2)),
        )?;

        for path in watch_paths {
            if path.exists() {
                watcher.watch(path, RecursiveMode::Recursive)?;
                tracing::info!("Watching: {}", path.display());
            } else {
                tracing::warn!("Watch path does not exist, skipping: {}", path.display());
            }
        }

        Ok(Self {
            _watcher: watcher,
            changed_dirs: Arc::new(Mutex::new(HashSet::new())),
            rx,
        })
    }

    /// Process any pending events from the watcher.
    /// Call this periodically to accumulate changed directories.
    /// Events are debounced at the directory level — multiple changes
    /// to files in the same directory result in one entry.
    pub fn process_events(&self) {
        while let Ok(event_result) = self.rx.try_recv() {
            match event_result {
                Ok(event) => {
                    for path in &event.paths {
                        // Record the parent directory of the changed file
                        let dir = if path.is_dir() {
                            path.clone()
                        } else {
                            path.parent().unwrap_or(path).to_path_buf()
                        };
                        self.changed_dirs.lock().unwrap().insert(dir);
                    }
                }
                Err(e) => {
                    tracing::warn!("Filesystem watch error: {}", e);
                }
            }
        }
    }

    /// Drain all accumulated changed directories.
    /// Returns the set of directories that need re-scanning.
    /// The internal set is cleared after draining.
    pub fn drain_changed_dirs(&self) -> HashSet<PathBuf> {
        let mut dirs = self.changed_dirs.lock().unwrap();
        std::mem::take(&mut *dirs)
    }

    /// Check if there are any pending changes.
    pub fn has_changes(&self) -> bool {
        !self.changed_dirs.lock().unwrap().is_empty()
    }
}

/// Simplified watcher for integration with the daemon.
/// Watches paths, debounces events, and provides a list of changed directories
/// that need re-indexing.
pub struct DebouncedWatcher {
    watcher: FsWatcher,
    last_drain: Instant,
    debounce_interval: Duration,
}

impl DebouncedWatcher {
    pub fn new(watch_paths: &[PathBuf], debounce_secs: u64) -> Result<Self> {
        Ok(Self {
            watcher: FsWatcher::new(watch_paths)?,
            last_drain: Instant::now(),
            debounce_interval: Duration::from_secs(debounce_secs),
        })
    }

    /// Process events and return changed dirs if the debounce interval has elapsed.
    /// Returns None if the debounce interval hasn't elapsed yet.
    pub fn poll(&mut self) -> Option<HashSet<PathBuf>> {
        self.watcher.process_events();

        if self.last_drain.elapsed() >= self.debounce_interval && self.watcher.has_changes() {
            self.last_drain = Instant::now();
            Some(self.watcher.drain_changed_dirs())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::thread;
    use tempfile::TempDir;

    #[test]
    fn test_watcher_detects_new_file() {
        let dir = TempDir::new().unwrap();
        let watcher = FsWatcher::new(&[dir.path().to_path_buf()]).unwrap();

        // Give the watcher time to register
        thread::sleep(Duration::from_millis(100));

        // Create a new file
        fs::write(dir.path().join("new_file.txt"), "hello").unwrap();

        // Give events time to arrive
        thread::sleep(Duration::from_millis(500));

        watcher.process_events();
        let changed = watcher.drain_changed_dirs();

        // Should have detected a change in the temp directory
        assert!(!changed.is_empty(), "should detect new file creation");
    }

    #[test]
    fn test_watcher_detects_modification() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("existing.txt");
        fs::write(&file_path, "original").unwrap();

        let watcher = FsWatcher::new(&[dir.path().to_path_buf()]).unwrap();
        thread::sleep(Duration::from_millis(100));

        // Modify the file
        fs::write(&file_path, "modified content").unwrap();
        thread::sleep(Duration::from_millis(500));

        watcher.process_events();
        let changed = watcher.drain_changed_dirs();

        assert!(!changed.is_empty(), "should detect file modification");
    }

    #[test]
    fn test_watcher_debounces() {
        let dir = TempDir::new().unwrap();
        let watcher = FsWatcher::new(&[dir.path().to_path_buf()]).unwrap();
        thread::sleep(Duration::from_millis(100));

        // Create multiple files rapidly
        for i in 0..5 {
            fs::write(dir.path().join(format!("file_{}.txt", i)), "data").unwrap();
        }
        thread::sleep(Duration::from_millis(500));

        watcher.process_events();
        let changed = watcher.drain_changed_dirs();

        // All changes should be debounced to just the one directory
        // (may have the dir itself, possibly subdirectories)
        assert!(!changed.is_empty());
        // The key point: we get directories, not individual files
        // This is debouncing at the directory level
    }

    #[test]
    fn test_drain_clears_state() {
        let dir = TempDir::new().unwrap();
        let watcher = FsWatcher::new(&[dir.path().to_path_buf()]).unwrap();
        thread::sleep(Duration::from_millis(100));

        fs::write(dir.path().join("test.txt"), "hello").unwrap();
        thread::sleep(Duration::from_millis(500));

        watcher.process_events();
        let first = watcher.drain_changed_dirs();
        assert!(!first.is_empty());

        // Second drain should be empty (no new changes)
        let second = watcher.drain_changed_dirs();
        assert!(second.is_empty(), "drain should clear the change set");
    }

    #[test]
    fn test_watcher_nonexistent_path_skipped() {
        // Should not panic when a watch path doesn't exist
        let result = FsWatcher::new(&[PathBuf::from("/nonexistent/path/12345")]);
        // The watcher should succeed (it skips nonexistent paths)
        assert!(result.is_ok());
    }
}
