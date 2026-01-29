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
    /// 2. Drop read-only reader, open read-write temp connection
    /// 3. ATTACH writer and merge data
    /// 4. Checkpoint and close temp connection
    /// 5. Reopen reader as read-only and swap
    /// 6. Clear writer tables
    pub async fn merge(&self) -> Result<u64> {
        let start = std::time::Instant::now();

        // 1. Checkpoint writer to ensure all data is on disk
        self.writer.checkpoint().await?;

        let writer_path = self.writer_path.to_string_lossy().to_string();
        let reader_path = self.reader_path.to_string_lossy().to_string();

        // 2. Drop the current read-only reader to release file locks
        {
            let mut reader_guard = self.reader.write().await;
            // Replace with a dummy in-memory pool temporarily
            // This drops the old reader, releasing the file lock
            *reader_guard = Arc::new(DuckDbPool::in_memory()?);
        }

        // 3. Open a temporary read-write connection for the merge
        let temp_pool = DuckDbPool::new(&reader_path)?;
        
        let blocks_merged = temp_pool
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

        // 4. Drop temp_pool to release the file lock
        drop(temp_pool);

        // 5. Reopen reader as read-only and swap it back
        {
            let new_reader = Arc::new(DuckDbPool::open_readonly(&self.reader_path.to_string_lossy())?);
            let mut reader_guard = self.reader.write().await;
            *reader_guard = new_reader;
        }

        // 6. Clear writer tables
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

    fn temp_dir(name: &str) -> PathBuf {
        env::temp_dir().join(format!("tidx_lsm_{}_{}", name, std::process::id()))
    }

    #[tokio::test]
    async fn test_lsm_basic() {
        let temp_dir = temp_dir("basic");
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

    #[tokio::test]
    async fn test_lsm_multiple_merges() {
        let temp_dir = temp_dir("multi_merge");
        std::fs::create_dir_all(&temp_dir).unwrap();
        let db_path = temp_dir.join("test.duckdb");
        
        let lsm = DuckDbLsm::new(db_path.to_str().unwrap(), 1).unwrap();

        // First batch
        lsm.writer()
            .execute("INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner) VALUES (1, x'01', x'00', '2024-01-01', 0, 0, 0, x'00')")
            .await
            .unwrap();
        let merged = lsm.merge().await.unwrap();
        assert_eq!(merged, 1);

        // Second batch
        lsm.writer()
            .execute("INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner) VALUES (2, x'02', x'01', '2024-01-01', 0, 0, 0, x'00')")
            .await
            .unwrap();
        lsm.writer()
            .execute("INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner) VALUES (3, x'03', x'02', '2024-01-01', 0, 0, 0, x'00')")
            .await
            .unwrap();
        let merged = lsm.merge().await.unwrap();
        assert_eq!(merged, 2);

        // Reader should have all 3 blocks
        let (min, max) = lsm.block_range().await.unwrap();
        assert_eq!(min, Some(1));
        assert_eq!(max, Some(3));

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_lsm_empty_merge() {
        let temp_dir = temp_dir("empty_merge");
        std::fs::create_dir_all(&temp_dir).unwrap();
        let db_path = temp_dir.join("test.duckdb");
        
        let lsm = DuckDbLsm::new(db_path.to_str().unwrap(), 1).unwrap();

        // Merge with no data should succeed and return 0
        let merged = lsm.merge().await.unwrap();
        assert_eq!(merged, 0);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_lsm_reader_isolation() {
        let temp_dir = temp_dir("isolation");
        std::fs::create_dir_all(&temp_dir).unwrap();
        let db_path = temp_dir.join("test.duckdb");
        
        let lsm = DuckDbLsm::new(db_path.to_str().unwrap(), 1).unwrap();

        // Write block 1 and merge
        lsm.writer()
            .execute("INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner) VALUES (1, x'01', x'00', '2024-01-01', 0, 0, 0, x'00')")
            .await
            .unwrap();
        lsm.merge().await.unwrap();

        // Get a reader reference
        let reader1 = lsm.reader().await;
        let (_, max1) = reader1.block_range().await.unwrap();
        assert_eq!(max1, Some(1));

        // Write more data to writer (not merged yet)
        lsm.writer()
            .execute("INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner) VALUES (2, x'02', x'01', '2024-01-01', 0, 0, 0, x'00')")
            .await
            .unwrap();

        // Reader should still only see block 1 (writer data not visible)
        let (_, max_before) = reader1.block_range().await.unwrap();
        assert_eq!(max_before, Some(1));

        // After merge, new reader sees both blocks
        lsm.merge().await.unwrap();
        let reader2 = lsm.reader().await;
        let (_, max2) = reader2.block_range().await.unwrap();
        assert_eq!(max2, Some(2));

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_lsm_deduplication() {
        let temp_dir = temp_dir("dedup");
        std::fs::create_dir_all(&temp_dir).unwrap();
        let db_path = temp_dir.join("test.duckdb");
        
        let lsm = DuckDbLsm::new(db_path.to_str().unwrap(), 1).unwrap();

        // Write and merge block 1
        lsm.writer()
            .execute("INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner) VALUES (1, x'01', x'00', '2024-01-01', 0, 0, 0, x'00')")
            .await
            .unwrap();
        lsm.merge().await.unwrap();

        // Try to write duplicate block 1 (same num, different hash - simulates reorg scenario)
        lsm.writer()
            .execute("INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner) VALUES (1, x'FF', x'00', '2024-01-01', 0, 0, 0, x'00')")
            .await
            .unwrap();
        lsm.merge().await.unwrap();

        // Should still only have 1 block (INSERT OR IGNORE)
        let reader = lsm.reader().await;
        let count: i64 = reader
            .with_connection_result(|conn| {
                let mut stmt = conn.prepare("SELECT COUNT(*) FROM blocks")?;
                Ok(stmt.query_row([], |row| row.get(0))?)
            })
            .await
            .unwrap();
        assert_eq!(count, 1);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_lsm_concurrent_read_during_merge() {
        let temp_dir = temp_dir("concurrent");
        std::fs::create_dir_all(&temp_dir).unwrap();
        let db_path = temp_dir.join("test.duckdb");
        
        let lsm = Arc::new(DuckDbLsm::new(db_path.to_str().unwrap(), 1).unwrap());

        // Write initial data
        lsm.writer()
            .execute("INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner) VALUES (1, x'01', x'00', '2024-01-01', 0, 0, 0, x'00')")
            .await
            .unwrap();
        lsm.merge().await.unwrap();

        // Spawn reader task
        let lsm_reader = Arc::clone(&lsm);
        let reader_handle = tokio::spawn(async move {
            for _ in 0..10 {
                let reader = lsm_reader.reader().await;
                let result = reader.block_range().await;
                assert!(result.is_ok(), "Reader should not fail during merge");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        // Perform merges concurrently
        for i in 2..=5 {
            lsm.writer()
                .execute(&format!(
                    "INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner) VALUES ({i}, x'{i:02x}', x'{:02x}', '2024-01-01', 0, 0, 0, x'00')",
                    i - 1
                ))
                .await
                .unwrap();
            lsm.merge().await.unwrap();
        }

        reader_handle.await.unwrap();

        // Final state should have all blocks
        let (min, max) = lsm.block_range().await.unwrap();
        assert_eq!(min, Some(1));
        assert_eq!(max, Some(5));

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
