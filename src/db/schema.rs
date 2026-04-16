use rusqlite::Connection;

/// Create all tables, indexes, FTS virtual tables, and triggers.
/// All statements use IF NOT EXISTS so this is safe to call repeatedly.
pub fn create_schema(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch(
        "
        -- Core file metadata
        CREATE TABLE IF NOT EXISTS files (
            id              INTEGER PRIMARY KEY,
            path            TEXT NOT NULL UNIQUE,
            parent_path     TEXT NOT NULL,
            name            TEXT NOT NULL,
            extension       TEXT,
            file_type       INTEGER NOT NULL,
            inode           INTEGER,
            device_id       INTEGER,
            hardlink_count  INTEGER DEFAULT 1,
            symlink_target  TEXT,
            size_bytes      INTEGER NOT NULL DEFAULT 0,
            blocks          INTEGER,
            mtime           INTEGER,
            ctime           INTEGER,
            atime           INTEGER,
            birth_time      INTEGER,
            uid             INTEGER,
            gid             INTEGER,
            mode            INTEGER,
            scan_id         INTEGER NOT NULL,
            first_seen_scan INTEGER NOT NULL,
            is_deleted      INTEGER DEFAULT 0,
            depth           INTEGER NOT NULL,
            path_components INTEGER NOT NULL
        );

        -- Aggregated directory sizes
        CREATE TABLE IF NOT EXISTS dir_sizes (
            path            TEXT PRIMARY KEY,
            total_size      INTEGER NOT NULL,
            file_count      INTEGER NOT NULL,
            dir_count       INTEGER NOT NULL,
            max_depth       INTEGER NOT NULL,
            largest_file    INTEGER,
            scan_id         INTEGER NOT NULL
        );

        -- Scan tracking
        CREATE TABLE IF NOT EXISTS scans (
            id              INTEGER PRIMARY KEY,
            root_path       TEXT NOT NULL,
            started_at      INTEGER NOT NULL,
            completed_at    INTEGER,
            total_files     INTEGER DEFAULT 0,
            total_dirs      INTEGER DEFAULT 0,
            total_size      INTEGER DEFAULT 0,
            files_added     INTEGER DEFAULT 0,
            files_modified  INTEGER DEFAULT 0,
            files_deleted   INTEGER DEFAULT 0,
            permission_errors INTEGER DEFAULT 0,
            status          TEXT DEFAULT 'running',
            progress        TEXT
        );

        -- Historical size snapshots per directory
        CREATE TABLE IF NOT EXISTS size_history (
            path            TEXT NOT NULL,
            scan_id         INTEGER NOT NULL,
            recorded_at     INTEGER NOT NULL,
            total_size      INTEGER NOT NULL,
            file_count      INTEGER NOT NULL,
            delta_size      INTEGER DEFAULT 0,
            delta_files     INTEGER DEFAULT 0,
            PRIMARY KEY (path, scan_id)
        );

        -- File content hashes for dedup detection
        CREATE TABLE IF NOT EXISTS file_hashes (
            file_id         INTEGER PRIMARY KEY REFERENCES files(id),
            hash_partial    BLOB,
            hash_full       BLOB,
            hash_algorithm  TEXT DEFAULT 'xxhash64'
        );

        -- Per-extension aggregate statistics
        CREATE TABLE IF NOT EXISTS extension_stats (
            extension       TEXT NOT NULL,
            scan_id         INTEGER NOT NULL,
            file_count      INTEGER NOT NULL,
            total_size      INTEGER NOT NULL,
            avg_size        INTEGER NOT NULL,
            largest_size    INTEGER NOT NULL,
            PRIMARY KEY (extension, scan_id)
        );

        -- Full-text search on file names only (not paths — saves ~1.5 GB on large indexes)
        CREATE VIRTUAL TABLE IF NOT EXISTS files_fts USING fts5(
            name,
            content=files, content_rowid=id,
            tokenize='unicode61 remove_diacritics 2'
        );

        -- FTS sync triggers
        CREATE TRIGGER IF NOT EXISTS files_ai AFTER INSERT ON files BEGIN
            INSERT INTO files_fts(rowid, name) VALUES (new.id, new.name);
        END;
        CREATE TRIGGER IF NOT EXISTS files_ad AFTER DELETE ON files BEGIN
            INSERT INTO files_fts(files_fts, rowid, name) VALUES('delete', old.id, old.name);
        END;
        CREATE TRIGGER IF NOT EXISTS files_au AFTER UPDATE ON files BEGIN
            INSERT INTO files_fts(files_fts, rowid, name) VALUES('delete', old.id, old.name);
            INSERT INTO files_fts(rowid, name) VALUES (new.id, new.name);
        END;

        -- Indexes for common query patterns
        CREATE INDEX IF NOT EXISTS idx_files_parent ON files(parent_path);
        CREATE INDEX IF NOT EXISTS idx_files_size ON files(size_bytes DESC) WHERE file_type = 0;
        CREATE INDEX IF NOT EXISTS idx_files_ext_size ON files(extension, size_bytes DESC) WHERE file_type = 0;
        CREATE INDEX IF NOT EXISTS idx_files_mtime ON files(mtime DESC);
        CREATE INDEX IF NOT EXISTS idx_files_atime ON files(atime ASC) WHERE file_type = 0;
        CREATE INDEX IF NOT EXISTS idx_files_deleted ON files(is_deleted, scan_id) WHERE is_deleted = 1;
        CREATE INDEX IF NOT EXISTS idx_files_size_exact ON files(size_bytes, inode) WHERE file_type = 0 AND size_bytes > 0;
        CREATE INDEX IF NOT EXISTS idx_dir_sizes_size ON dir_sizes(total_size DESC);
        CREATE INDEX IF NOT EXISTS idx_history_time ON size_history(recorded_at DESC);
        CREATE INDEX IF NOT EXISTS idx_history_delta ON size_history(delta_size DESC) WHERE delta_size != 0;
        ",
    )?;

    // Migration: add `progress` column to existing databases that lack it.
    // ALTER TABLE … ADD COLUMN is a no-op if the column already exists on
    // newer SQLite versions, but older ones may error — so we ignore errors.
    let _ = conn.execute_batch("ALTER TABLE scans ADD COLUMN progress TEXT;");

    // Migration: add `permission_errors` column to existing databases.
    let _ = conn.execute_batch("ALTER TABLE scans ADD COLUMN permission_errors INTEGER DEFAULT 0;");

    // Migration: if FTS5 table has a 'path' column (old schema), rebuild it with name-only.
    // Drop old triggers, old FTS table, then the CREATE VIRTUAL TABLE above will recreate.
    let has_path_in_fts: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('files_fts') WHERE name = 'path'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0)
        > 0;
    if has_path_in_fts {
        let _ = conn.execute_batch(
            "DROP TRIGGER IF EXISTS files_ai;
             DROP TRIGGER IF EXISTS files_ad;
             DROP TRIGGER IF EXISTS files_au;
             DROP TABLE IF EXISTS files_fts;",
        );
        // Recreate with name-only (re-run the schema creation for just FTS)
        let _ = conn.execute_batch(
            "CREATE VIRTUAL TABLE IF NOT EXISTS files_fts USING fts5(
                name,
                content=files, content_rowid=id,
                tokenize='unicode61 remove_diacritics 2'
            );
            CREATE TRIGGER IF NOT EXISTS files_ai AFTER INSERT ON files BEGIN
                INSERT INTO files_fts(rowid, name) VALUES (new.id, new.name);
            END;
            CREATE TRIGGER IF NOT EXISTS files_ad AFTER DELETE ON files BEGIN
                INSERT INTO files_fts(files_fts, rowid, name) VALUES('delete', old.id, old.name);
            END;
            CREATE TRIGGER IF NOT EXISTS files_au AFTER UPDATE ON files BEGIN
                INSERT INTO files_fts(files_fts, rowid, name) VALUES('delete', old.id, old.name);
                INSERT INTO files_fts(rowid, name) VALUES (new.id, new.name);
            END;
            -- Rebuild FTS index from existing data
            INSERT INTO files_fts(files_fts) VALUES('rebuild');",
        );
    }

    // Migration: drop indexes that aren't worth the space
    let _ = conn.execute_batch(
        "DROP INDEX IF EXISTS idx_files_scan;
         DROP INDEX IF EXISTS idx_files_depth;
         DROP INDEX IF EXISTS idx_files_inode;",
    );

    Ok(())
}
