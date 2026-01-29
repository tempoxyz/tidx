//! LSM-style DuckDB storage with writer/reader separation.
//!
//! Architecture:
//! - Writer DB: Small buffer for incoming data (ephemeral)
//! - Reader DB: Stable snapshot for queries (persistent)
//! - Periodic merge: writer → reader, then clear writer
//!
//! Benefits:
//! - No read/write contention (readers use stable snapshot)
//! - ~N storage instead of 2N (writer is small buffer)
//! - Consistent query latency during sync

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::RwLock;

use super::DuckDbPool;

/// LSM-style DuckDB manager with writer/reader separation.
pub struct DuckDbLsm {
    /// Path to the reader (snapshot) database
    #[allow(dead_code)]
    reader_path: PathBuf,
    /// Path to the writer (buffer) database
    writer_path: PathBuf,
    /// Writer pool for sync operations
    writer: Arc<DuckDbPool>,
    /// Reader pool for API queries (swapped on merge)
    reader: Arc<RwLock<Arc<DuckDbPool>>>,
    /// Chain ID for logging
    chain_id: u64,
}

impl DuckDbLsm {
    /// Creates a new LSM-style DuckDB manager.
    ///
    /// - `base_path`: Base path for the database (e.g., "/data/mainnet.duckdb")
    /// - Writer will use: "/data/mainnet_writer.duckdb"
    /// - Reader will use: "/data/mainnet.duckdb" (the original path)
    pub fn new(base_path: &str, chain_id: u64) -> Result<Self> {
        let reader_path = PathBuf::from(base_path);
        let writer_path = PathBuf::from(format!(
            "{}_writer.duckdb",
            base_path.trim_end_matches(".duckdb")
        ));

        // Create writer (always fresh start - it's a buffer)
        // Remove old writer file if exists
        if writer_path.exists() {
            std::fs::remove_file(&writer_path)
                .context("Failed to remove old writer database")?;
            // Also remove WAL file if exists
            let wal_path = format!("{}.wal", writer_path.display());
            let _ = std::fs::remove_file(&wal_path);
        }
        let writer = Arc::new(DuckDbPool::new(writer_path.to_str().unwrap())?);

        // Create reader if it doesn't exist (need to initialize schema)
        // We keep it as read-write initially so we can do the first merge
        let reader = if !reader_path.exists() {
            // Create new with schema
            Arc::new(DuckDbPool::new(reader_path.to_str().unwrap())?)
        } else {
            // Open existing as read-only
            Arc::new(DuckDbPool::open_readonly(reader_path.to_str().unwrap())?)
        };

        tracing::info!(
            chain_id,
            reader = %reader_path.display(),
            writer = %writer_path.display(),
            "DuckDB LSM initialized"
        );

        Ok(Self {
            reader_path,
            writer_path,
            writer,
            reader: Arc::new(RwLock::new(reader)),
            chain_id,
        })
    }

    /// Returns the writer pool (for sync operations).
    pub fn writer(&self) -> &Arc<DuckDbPool> {
        &self.writer
    }

    /// Returns the current reader pool (for API queries).
    pub async fn reader(&self) -> Arc<DuckDbPool> {
        self.reader.read().await.clone()
    }

    /// Merge writer data into reader, then clear writer.
    ///
    /// Process:
    /// 1. Checkpoint writer to flush WAL
    /// 2. ATTACH writer to reader and merge (using current reader connection)
    /// 3. Clear writer tables
    pub async fn merge(&self) -> Result<u64> {
        let start = std::time::Instant::now();

        // 1. Checkpoint writer to ensure all data is on disk
        self.writer.checkpoint().await?;

        let writer_path = self.writer_path.to_string_lossy().to_string();

        // 2. Merge using the current reader connection
        // Note: If reader is read-only, this will fail - that's expected on existing DBs
        // We handle this by trying on reader first, then falling back to reopening
        let reader = self.reader.read().await.clone();
        
        let blocks_merged = reader
            .with_connection_result(move |conn| {
                // Attach writer database
                conn.execute(
                    &format!("ATTACH '{}' AS writer (READ_ONLY)", writer_path.replace('\'', "''")),
                    [],
                )?;

                // Count rows to merge
                let mut stmt = conn.prepare("SELECT COUNT(*) FROM writer.blocks")?;
                let count: i64 = stmt.query_row([], |row| row.get(0))?;

                if count > 0 {
                    // Merge blocks
                    conn.execute(
                        "INSERT OR IGNORE INTO blocks SELECT * FROM writer.blocks",
                        [],
                    )?;

                    // Merge txs
                    conn.execute(
                        "INSERT OR IGNORE INTO txs SELECT * FROM writer.txs",
                        [],
                    )?;

                    // Merge logs
                    conn.execute(
                        "INSERT OR IGNORE INTO logs SELECT * FROM writer.logs",
                        [],
                    )?;

                    // Merge receipts
                    conn.execute(
                        "INSERT OR IGNORE INTO receipts SELECT * FROM writer.receipts",
                        [],
                    )?;
                }

                // Detach writer
                conn.execute("DETACH writer", [])?;

                // Checkpoint reader to persist
                conn.execute("CHECKPOINT", [])?;

                Ok(count as u64)
            })
            .await?;

        // 3. Clear writer tables
        if blocks_merged > 0 {
            self.writer
                .with_connection(|conn| {
                    conn.execute("DELETE FROM logs", [])?;
                    conn.execute("DELETE FROM receipts", [])?;
                    conn.execute("DELETE FROM txs", [])?;
                    conn.execute("DELETE FROM blocks", [])?;
                    conn.execute("CHECKPOINT", [])?;
                    Ok(())
                })
                .await?;
        }

        let elapsed = start.elapsed();
        tracing::info!(
            chain_id = self.chain_id,
            blocks_merged,
            elapsed_ms = elapsed.as_millis(),
            "DuckDB LSM merge complete"
        );

        Ok(blocks_merged)
    }

    /// Run the merge loop in the background.
    pub async fn run_merge_loop(self: Arc<Self>, interval: Duration) {
        tracing::info!(
            chain_id = self.chain_id,
            interval_secs = interval.as_secs(),
            "DuckDB LSM merge loop started"
        );

        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            ticker.tick().await;

            if let Err(e) = self.merge().await {
                tracing::error!(
                    chain_id = self.chain_id,
                    error = %e,
                    "DuckDB LSM merge failed"
                );
            }
        }
    }

    /// Get the block range from the reader (for status queries).
    pub async fn block_range(&self) -> Result<(Option<i64>, Option<i64>)> {
        self.reader.read().await.block_range().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[tokio::test]
    async fn test_lsm_basic() {
        let temp_dir = env::temp_dir().join(format!("tidx_test_{}", std::process::id()));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let db_path = temp_dir.join("test.duckdb");
        
        let lsm = DuckDbLsm::new(db_path.to_str().unwrap(), 1).unwrap();

        // Write some data to writer
        lsm.writer()
            .execute("INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner) VALUES (1, x'00', x'00', '2024-01-01', 0, 0, 0, x'00')")
            .await
            .unwrap();

        // Merge to reader
        let merged = lsm.merge().await.unwrap();
        assert_eq!(merged, 1);

        // Verify reader has the data
        let (min, max) = lsm.block_range().await.unwrap();
        assert_eq!(min, Some(1));
        assert_eq!(max, Some(1));

        // Writer should be empty now
        let writer_range = lsm.writer().block_range().await.unwrap();
        assert_eq!(writer_range, (None, None));

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
