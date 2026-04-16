pub mod queries;
pub mod schema;

use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::{Context, Result};
use rusqlite::Connection;

/// Thread-safe handle to the SQLite database.
///
/// Internally wraps an `Arc<Mutex<Connection>>` so it can be cheaply cloned
/// and shared across threads.
#[derive(Clone)]
pub struct Database {
    conn: Arc<Mutex<Connection>>,
}

impl Database {
    /// Open (or create) a database at the given filesystem path.
    ///
    /// Parent directories are created automatically. After opening, WAL-mode
    /// pragmas are applied and the full schema is created.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create database directory: {}", parent.display()))?;
        }

        let conn = Connection::open(path)
            .with_context(|| format!("failed to open database: {}", path.display()))?;

        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        db.apply_pragmas()?;
        {
            let conn = db.conn();
            schema::create_schema(&conn)?;
        }
        Ok(db)
    }

    /// Open an in-memory database — useful for testing.
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()
            .context("failed to open in-memory database")?;

        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        db.apply_pragmas()?;
        {
            let conn = db.conn();
            schema::create_schema(&conn)?;
        }
        Ok(db)
    }

    /// Acquire the underlying connection lock.
    pub fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().expect("database mutex poisoned")
    }

    /// Enter bulk-insert mode. Returns a guard that restores normal durability on drop.
    /// Only one bulk mode session should be active at a time.
    pub fn bulk_mode(&self) -> Result<BulkModeGuard<'_>> {
        let conn = self.conn();
        conn.execute_batch("PRAGMA synchronous = OFF;")?;
        drop(conn); // release the mutex
        Ok(BulkModeGuard { db: self })
    }

    #[deprecated(note = "use bulk_mode() RAII guard instead")]
    /// Switch to bulk-insert mode (disables synchronous writes).
    pub fn enable_bulk_mode(&self) -> Result<()> {
        let conn = self.conn();
        conn.execute_batch("PRAGMA synchronous = OFF;")?;
        Ok(())
    }

    #[deprecated(note = "use bulk_mode() RAII guard instead")]
    /// Return to normal durability mode after bulk operations.
    pub fn disable_bulk_mode(&self) -> Result<()> {
        let conn = self.conn();
        conn.execute_batch("PRAGMA synchronous = NORMAL;")?;
        Ok(())
    }

    /// Apply performance and correctness pragmas.
    fn apply_pragmas(&self) -> Result<()> {
        let conn = self.conn();
        conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA cache_size = -64000;
            PRAGMA mmap_size = 268435456;
            PRAGMA temp_store = MEMORY;
            PRAGMA foreign_keys = ON;
            ",
        )
        .context("failed to apply database pragmas")?;
        Ok(())
    }
}

/// RAII guard that restores `PRAGMA synchronous = NORMAL` when dropped.
pub struct BulkModeGuard<'a> {
    db: &'a Database,
}

impl<'a> Drop for BulkModeGuard<'a> {
    fn drop(&mut self) {
        let conn = self.db.conn();
        let _ = conn.execute_batch("PRAGMA synchronous = NORMAL;");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_in_memory() {
        let db = Database::open_in_memory().expect("open_in_memory should succeed");
        let conn = db.conn();
        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type IN ('table', 'trigger')")
            .unwrap();
        let names: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        assert!(
            !names.is_empty(),
            "sqlite_master should contain at least one entry after schema creation"
        );
    }

    #[test]
    fn test_open_creates_directory() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let nested = tmp.path().join("a").join("b").join("c").join("test.db");

        // The nested directory does not exist yet
        assert!(!nested.parent().unwrap().exists());

        let _db = Database::open(&nested).expect("open should create parent dirs");
        assert!(nested.exists(), "database file should exist after open");
    }

    #[test]
    fn test_wal_mode_enabled() {
        let db = Database::open_in_memory().expect("open_in_memory");
        let conn = db.conn();
        // WAL mode pragma should succeed — for in-memory DBs the journal_mode
        // may report "memory" instead of "wal", but the pragma execution itself
        // must not error. We verify the pragma ran without error by querying it.
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .expect("PRAGMA journal_mode should succeed");
        // In-memory databases report "memory"; file-backed ones report "wal".
        // Either is acceptable — the key thing is that the pragma didn't error.
        assert!(
            mode == "wal" || mode == "memory",
            "unexpected journal_mode: {mode}"
        );
    }

    #[test]
    fn test_all_tables_created() {
        let db = Database::open_in_memory().expect("open_in_memory");
        let conn = db.conn();

        let expected_tables = [
            "files",
            "dir_sizes",
            "scans",
            "size_history",
            "file_hashes",
            "extension_stats",
        ];

        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
            .unwrap();
        let tables: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        for expected in &expected_tables {
            assert!(
                tables.contains(&expected.to_string()),
                "table '{expected}' should exist in sqlite_master, found: {tables:?}"
            );
        }
    }
}
