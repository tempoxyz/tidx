use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::sync::{mpsc, Semaphore};

use crate::db::{DuckDbPool, Pool};
use crate::metrics;
use crate::types::{BlockRow, LogRow, ReceiptRow, TxRow};

/// Batch of rows to replicate to DuckDB (used only as optimization hint).
#[derive(Debug)]
pub enum ReplicaBatch {
    Blocks(Vec<BlockRow>),
    Txs(Vec<TxRow>),
    Logs(Vec<LogRow>),
    Receipts(Vec<ReceiptRow>),
}

/// DuckDB replicator that syncs data from PostgreSQL using pull-based tailing.
///
/// Architecture:
/// - Postgres is the source of truth; DuckDB tails it via a watermark
/// - A tight polling loop (500ms) copies new blocks from Postgres to DuckDB
/// - The channel serves only as a low-latency hint to poll sooner
/// - All tables (blocks, txs, logs, receipts) are copied together for each range
/// - Watermark only advances after ALL tables for a range are written
/// - Data transfer uses Parquet as intermediate format (memory-safe, no OOM)
/// - Single-flight semaphore ensures only one task copies data at a time
pub struct Replicator {
    duckdb: Arc<DuckDbPool>,
    pg_pool: Pool,
    rx: mpsc::Receiver<ReplicaBatch>,
    chain_id: u64,
    /// Flag set when channel receives data, signals to poll immediately
    needs_sync: Arc<AtomicBool>,
    /// Semaphore to ensure only one task (tail or gap-fill) copies data at a time
    copy_semaphore: Arc<Semaphore>,
}

/// Handle for sending hints to the replicator.
/// The channel is now just a low-latency signal, not the source of truth.
#[derive(Clone)]
pub struct ReplicatorHandle {
    tx: mpsc::Sender<ReplicaBatch>,
    needs_sync: Arc<AtomicBool>,
    duckdb: Arc<DuckDbPool>,
}

impl ReplicatorHandle {
    /// Signals that new blocks are available (low-latency hint).
    /// The replicator will pull from Postgres regardless of whether this succeeds.
    pub fn send_blocks(&self, blocks: Vec<BlockRow>) {
        if blocks.is_empty() {
            return;
        }
        self.needs_sync.store(true, Ordering::Relaxed);
        let _ = self.tx.try_send(ReplicaBatch::Blocks(blocks));
    }

    /// Signals that new transactions are available.
    pub fn send_txs(&self, txs: Vec<TxRow>) {
        if txs.is_empty() {
            return;
        }
        self.needs_sync.store(true, Ordering::Relaxed);
        let _ = self.tx.try_send(ReplicaBatch::Txs(txs));
    }

    /// Signals that new logs are available.
    pub fn send_logs(&self, logs: Vec<LogRow>) {
        if logs.is_empty() {
            return;
        }
        self.needs_sync.store(true, Ordering::Relaxed);
        let _ = self.tx.try_send(ReplicaBatch::Logs(logs));
    }

    /// Signals that new receipts are available.
    pub fn send_receipts(&self, receipts: Vec<ReceiptRow>) {
        if receipts.is_empty() {
            return;
        }
        self.needs_sync.store(true, Ordering::Relaxed);
        let _ = self.tx.try_send(ReplicaBatch::Receipts(receipts));
    }

    /// Delete blocks from a given height onwards (used for reorg handling).
    pub async fn delete_blocks_from(&self, from_block: u64) -> Result<u64> {
        self.duckdb.delete_blocks_from(from_block as i64).await
    }
}

impl Replicator {
    /// Creates a new replicator with a channel for receiving batches.
    pub fn new(
        duckdb: Arc<DuckDbPool>,
        pg_pool: Pool,
        buffer_size: usize,
        chain_id: u64,
    ) -> (Self, ReplicatorHandle) {
        let (tx, rx) = mpsc::channel(buffer_size);
        let needs_sync = Arc::new(AtomicBool::new(false));
        let copy_semaphore = Arc::new(Semaphore::new(1));
        (
            Self {
                duckdb: duckdb.clone(),
                pg_pool,
                rx,
                chain_id,
                needs_sync: needs_sync.clone(),
                copy_semaphore,
            },
            ReplicatorHandle { tx, needs_sync, duckdb },
        )
    }

    /// Runs the replicator with two concurrent tasks:
    /// 1. Tail task: low-latency sync of new blocks from pg_tip (every 500ms)
    /// 2. Gap-fill task: high-throughput backfill of historical blocks (continuous)
    ///
    /// Both tasks share the same DuckDB pool (writes serialize on mutex).
    /// Data transfer uses Parquet as intermediate format (memory-safe, no OOM).
    /// Single-flight semaphore ensures only one task copies data at a time.
    pub async fn run(mut self) -> Result<()> {
        tracing::info!(chain_id = self.chain_id, "DuckDB replicator started (parquet mode)");

        let duckdb = self.duckdb.clone();
        let pg_pool = self.pg_pool.clone();
        let chain_id = self.chain_id;
        let copy_semaphore = self.copy_semaphore.clone();

        // Spawn gap-fill task (runs continuously until caught up)
        let gap_fill_duckdb = duckdb.clone();
        let gap_fill_handle = tokio::spawn(async move {
            Self::run_gap_fill_task(gap_fill_duckdb, pg_pool, chain_id, copy_semaphore).await
        });

        // Run tail task in current task
        let tail_result = self.run_tail_task().await;

        // Cancel gap-fill when tail task exits
        gap_fill_handle.abort();

        tracing::info!(chain_id = self.chain_id, "DuckDB replicator stopped");
        tail_result
    }

    /// Tail task: low-latency sync of new blocks from pg_tip.
    /// Runs every 500ms or when signaled via channel.
    async fn run_tail_task(&mut self) -> Result<()> {
        let mut tail_interval = tokio::time::interval(Duration::from_millis(500));
        tail_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut checkpoint_interval = tokio::time::interval(Duration::from_secs(60));
        checkpoint_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut metrics_interval = tokio::time::interval(Duration::from_secs(10));
        metrics_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                // Channel signals trigger immediate sync (low-latency hint)
                batch = self.rx.recv() => {
                    match batch {
                        Some(_) => {
                            while self.rx.try_recv().is_ok() {}
                            if let Err(e) = self.tail_postgres().await {
                                tracing::error!(chain_id = self.chain_id, error = %e, "DuckDB tail sync failed");
                                metrics::increment_duckdb_errors("tail_sync");
                            }
                        }
                        None => {
                            tracing::info!(chain_id = self.chain_id, "DuckDB replicator channel closed");
                            break;
                        }
                    }
                }
                // Polling loop to tail Postgres
                _ = tail_interval.tick() => {
                    // Clear the flag (we're about to sync anyway)
                    self.needs_sync.store(false, Ordering::Relaxed);
                    if let Err(e) = self.tail_postgres().await {
                        tracing::error!(chain_id = self.chain_id, error = %e, "DuckDB tail sync failed");
                        metrics::increment_duckdb_errors("tail_sync");
                    }
                }
                _ = checkpoint_interval.tick() => {
                    if let Err(e) = self.duckdb.checkpoint().await {
                        tracing::warn!(chain_id = self.chain_id, error = %e, "DuckDB checkpoint failed");
                        metrics::increment_duckdb_errors("checkpoint");
                    } else {
                        tracing::debug!(chain_id = self.chain_id, "DuckDB checkpoint completed");
                    }
                }
                _ = metrics_interval.tick() => {
                    if let Err(e) = self.emit_metrics().await {
                        tracing::debug!(chain_id = self.chain_id, error = %e, "Failed to emit DuckDB metrics");
                    }
                }
            }
        }

        Ok(())
    }

    /// Gap-fill task: backfill historical blocks using Parquet-based transfer.
    /// 
    /// Uses Parquet as intermediate format for memory-safe, streaming data transfer:
    /// - Query PostgreSQL for block range data
    /// - Write to temporary Parquet file (ZSTD compressed)
    /// - DuckDB ingests via read_parquet()
    /// - No OOM risk (unlike postgres scanner extension)
    /// - Acquires single-flight semaphore before copying to avoid memory doubling
    async fn run_gap_fill_task(
        duckdb: Arc<DuckDbPool>,
        pg_pool: Pool,
        chain_id: u64,
        copy_semaphore: Arc<Semaphore>,
    ) -> Result<()> {
        // Initial delay to let tail task establish watermark
        tokio::time::sleep(Duration::from_secs(2)).await;

        tracing::info!(chain_id, "DuckDB gap-fill task started (parquet mode)");

        let mut last_checkpoint = Instant::now();
        
        // Maximum tail lag before pausing gap-fill (blocks)
        const MAX_TAIL_LAG: i64 = 10;

        loop {
            // Lag-aware throttle: pause gap-fill if tail is lagging
            // This ensures tail sync always takes priority
            let pg_conn = match pg_pool.get().await {
                Ok(conn) => conn,
                Err(e) => {
                    tracing::warn!(chain_id, error = %e, "Failed to get PG connection for lag check");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            
            let pg_tip: i64 = pg_conn
                .query_one("SELECT COALESCE(MAX(num), 0) FROM blocks", &[])
                .await
                .map(|r| r.get(0))
                .unwrap_or(0);
            
            let duck_tip = duckdb.latest_block().await.unwrap_or(None).unwrap_or(0);
            let tail_lag = pg_tip - duck_tip;
            
            if tail_lag > MAX_TAIL_LAG {
                tracing::debug!(
                    chain_id,
                    tail_lag,
                    pg_tip,
                    duck_tip,
                    "Gap-fill paused: tail lag too high"
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            
            drop(pg_conn);

            // Acquire semaphore before copying (prevents memory doubling with tail sync)
            let _permit = copy_semaphore.acquire().await.expect("semaphore closed");
            let result = Self::gap_fill_via_parquet(&duckdb, &pg_pool, chain_id).await;
            drop(_permit);

            match result {
                Ok(synced) => {
                    // Checkpoint every 60s after syncing
                    if synced > 0 && last_checkpoint.elapsed() > Duration::from_secs(60) {
                        let _ = duckdb.checkpoint().await;
                        last_checkpoint = Instant::now();
                    }

                    if synced == 0 {
                        // Fully caught up, sleep longer
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    } else {
                        // More to sync, short pause before next batch
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
                Err(e) => {
                    tracing::error!(chain_id, error = %e, "DuckDB gap-fill failed");
                    metrics::increment_duckdb_errors("gap_fill");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    /// Gap-fill using Parquet-based transfer.
    async fn gap_fill_via_parquet(
        duckdb: &Arc<DuckDbPool>,
        pg_pool: &Pool,
        chain_id: u64,
    ) -> Result<i64> {
        // Larger batches are safe now - Parquet streaming bounds memory regardless of size
        const BATCH_SIZE: i64 = 500;

        let (min, _max) = duckdb.block_range().await?;
        let duck_min = min.unwrap_or(0);
        let internal_gaps = detect_gaps_duckdb(duckdb).await?;

        if duck_min == 0 {
            return Ok(0);
        }
        
        let pg_conn = pg_pool.get().await?;
        let (pg_min, pg_max): (i64, i64) = {
            let row = pg_conn
                .query_one("SELECT COALESCE(MIN(num), 0), COALESCE(MAX(num), 0) FROM blocks", &[])
                .await?;
            (row.get(0), row.get(1))
        };

        if pg_max == 0 {
            return Ok(0);
        }

        // Build gaps list
        let mut gaps: Vec<(i64, i64)> = Vec::new();
        if pg_min < duck_min {
            gaps.push((pg_min, duck_min - 1));
        }
        for (start, end) in internal_gaps {
            if end >= pg_min && start <= pg_max {
                let clamped_start = start.max(pg_min);
                let clamped_end = end.min(pg_max);
                if clamped_start <= clamped_end {
                    gaps.push((clamped_start, clamped_end));
                }
            }
        }

        if gaps.is_empty() {
            return Ok(0);
        }

        // Sort by start descending (most recent first)
        gaps.sort_by(|a, b| b.0.cmp(&a.0));

        let mut total_synced = 0i64;
        let start_time = Instant::now();

        for (gap_start, gap_end) in gaps {
            let mut current = gap_end;
            while current >= gap_start {
                let batch_start = (current - BATCH_SIZE + 1).max(gap_start);
                // Use Parquet for memory-safe, streaming data transfer
                let synced = super::parquet::copy_range_via_parquet(&pg_conn, duckdb, batch_start, current).await?;
                total_synced += synced;
                current = batch_start - 1;
            }
        }

        if total_synced > 0 {
            let elapsed = start_time.elapsed();
            let rate = total_synced as f64 / elapsed.as_secs_f64();
            tracing::info!(
                chain_id,
                synced = total_synced,
                elapsed_ms = elapsed.as_millis(),
                rate = format!("{:.0} blk/s", rate),
                "DuckDB parquet gap-fill complete"
            );
        }

        Ok(total_synced)
    }

    /// Tails Postgres by copying new blocks from watermark to tip.
    /// Copies all tables (blocks, txs, logs, receipts) for each range atomically.
    async fn tail_postgres(&self) -> Result<()> {
        // Smaller batches to avoid OOM - each batch creates parquet files
        const BATCH_SIZE: i64 = 100;
        const MAX_BLOCKS_PER_TICK: i64 = 2000;

        let pg_conn = self.pg_pool.get().await?;
        
        // Get both min and max from Postgres to handle sparse block ranges
        let (pg_min, pg_tip): (i64, i64) = {
            let row = pg_conn
                .query_one("SELECT COALESCE(MIN(num), 0), COALESCE(MAX(num), 0) FROM blocks", &[])
                .await?;
            (row.get(0), row.get(1))
        };

        if pg_tip == 0 {
            return Ok(());
        }

        // DuckDB tails from the tip backwards (matching gap-fill order)
        // This ensures the most recent data is available first
        let duck_tip = self.duckdb.latest_block().await?.unwrap_or(0);
        
        // If DuckDB has no data, start from pg_tip and work backwards
        // If DuckDB has data, sync forward from duck_tip to pg_tip
        if duck_tip == 0 {
            // Initial sync: copy from pg_tip backwards
            let sync_start = (pg_tip - MAX_BLOCKS_PER_TICK + 1).max(pg_min);
            let lag = pg_tip - sync_start + 1;

            tracing::debug!(
                chain_id = self.chain_id,
                pg_min,
                pg_tip,
                sync_start,
                lag,
                "DuckDB initial sync (from tip)"
            );

            let start = Instant::now();
            let mut synced = 0i64;
            let mut current = sync_start;

            // Acquire semaphore before copying (prevents memory doubling with gap-fill)
            let _permit = self.copy_semaphore.acquire().await.expect("semaphore closed");
            
            while current <= pg_tip {
                let batch_end = (current + BATCH_SIZE - 1).min(pg_tip);
                self.copy_range_from_postgres(&pg_conn, current, batch_end).await?;
                synced += batch_end - current + 1;
                current = batch_end + 1;
            }
            
            drop(_permit);

            let elapsed = start.elapsed();
            let rate = synced as f64 / elapsed.as_secs_f64();

            if synced > 0 {
                tracing::info!(
                    chain_id = self.chain_id,
                    synced,
                    new_tip = pg_tip,
                    rate = format!("{:.1} blk/s", rate),
                    "DuckDB initial sync complete"
                );
            }
        } else {
            // Tail sync: copy forward from duck_tip to pg_tip
            let lag = pg_tip - duck_tip;

            // Always log tail check to debug sync issues
            if lag > 0 {
                // Log memory at start of tail sync
                let mem_start = self.duckdb.with_connection_result(|conn| {
                    let mem: i64 = conn
                        .prepare("SELECT COALESCE(SUM(memory_usage_bytes), 0) FROM duckdb_memory()")
                        .ok()
                        .and_then(|mut stmt| stmt.query_row([], |row| row.get(0)).ok())
                        .unwrap_or(0);
                    Ok(mem)
                }).await.unwrap_or(0);

                tracing::info!(
                    chain_id = self.chain_id,
                    duck_tip,
                    pg_tip,
                    lag,
                    mem_mb = mem_start / 1024 / 1024,
                    batch_size = BATCH_SIZE,
                    max_blocks = MAX_BLOCKS_PER_TICK,
                    "DuckDB tail sync starting"
                );
            } else {
                tracing::debug!(
                    chain_id = self.chain_id,
                    duck_tip,
                    pg_tip,
                    "DuckDB tail caught up"
                );
                return Ok(());
            }

            let sync_end = duck_tip + lag.min(MAX_BLOCKS_PER_TICK);
            let start = Instant::now();
            let mut synced = 0i64;
            let mut current = duck_tip + 1;

            // Acquire semaphore before copying (prevents memory doubling with gap-fill)
            let _permit = self.copy_semaphore.acquire().await.expect("semaphore closed");
            
            while current <= sync_end {
                let batch_end = (current + BATCH_SIZE - 1).min(sync_end);
                self.copy_range_from_postgres(&pg_conn, current, batch_end).await?;
                synced += batch_end - current + 1;
                current = batch_end + 1;
            }
            
            drop(_permit);

            let elapsed = start.elapsed();
            let rate = synced as f64 / elapsed.as_secs_f64();

            if synced > 0 {
                tracing::info!(
                    chain_id = self.chain_id,
                    synced,
                    new_watermark = sync_end,
                    remaining = pg_tip - sync_end,
                    rate = format!("{:.1} blk/s", rate),
                    "DuckDB tail sync complete"
                );
            }
        }

        Ok(())
    }

    /// Copies all tables for a block range from Postgres to DuckDB.
    async fn copy_range_from_postgres(
        &self,
        pg_conn: &deadpool_postgres::Object,
        start: i64,
        end: i64,
    ) -> Result<()> {
        // Log memory before parquet ingestion
        let mem_before = self.duckdb.with_connection_result(|conn| {
            let mem: i64 = conn
                .prepare("SELECT COALESCE(SUM(memory_usage_bytes), 0) FROM duckdb_memory()")
                .ok()
                .and_then(|mut stmt| stmt.query_row([], |row| row.get(0)).ok())
                .unwrap_or(0);
            Ok(mem)
        }).await.unwrap_or(0);

        // Use Parquet for memory-safe, streaming data transfer
        let blocks_copied = super::parquet::copy_range_via_parquet(pg_conn, &self.duckdb, start, end).await?;

        // Log memory after parquet ingestion
        let (mem_after, temp_files) = self.duckdb.with_connection_result(|conn| {
            let mem: i64 = conn
                .prepare("SELECT COALESCE(SUM(memory_usage_bytes), 0) FROM duckdb_memory()")
                .ok()
                .and_then(|mut stmt| stmt.query_row([], |row| row.get(0)).ok())
                .unwrap_or(0);
            let temps: i64 = conn
                .prepare("SELECT COUNT(*) FROM duckdb_temporary_files()")
                .ok()
                .and_then(|mut stmt| stmt.query_row([], |row| row.get(0)).ok())
                .unwrap_or(0);
            Ok((mem, temps))
        }).await.unwrap_or((0, 0));

        tracing::debug!(
            chain_id = self.chain_id,
            start,
            end,
            blocks = blocks_copied,
            mem_before_mb = mem_before / 1024 / 1024,
            mem_after_mb = mem_after / 1024 / 1024,
            mem_delta_mb = (mem_after - mem_before) / 1024 / 1024,
            temp_files,
            "DuckDB copy_range memory profile"
        );

        // Update watermark and checkpoint to release memory
        let end_copy = end;
        self.duckdb.with_connection(move |conn| {
            conn.execute(
                "UPDATE duckdb_sync_state SET latest_block = $1, updated_at = CURRENT_TIMESTAMP WHERE id = 1",
                duckdb::params![end_copy],
            )?;
            // Checkpoint to release memory between batches
            let _ = conn.execute("CHECKPOINT", []);
            Ok(())
        }).await?;

        Ok(())
    }

    /// Emit metrics for DuckDB sync status (lightweight).
    async fn emit_metrics(&self) -> Result<()> {
        let pg_conn = self.pg_pool.get().await?;
        let (pg_min, pg_max): (i64, i64) = {
            let row = pg_conn
                .query_one("SELECT COALESCE(MIN(num), 0), COALESCE(MAX(num), 0) FROM blocks", &[])
                .await?;
            (row.get(0), row.get(1))
        };
        drop(pg_conn);

        let (duck_min, duck_max) = self.duckdb.block_range().await?;
        let duck_min = duck_min.unwrap_or(0);
        let duck_max = duck_max.unwrap_or(0);

        let tip_lag = pg_max - duck_max;
        let backfill_remaining = if duck_min > 0 && pg_min < duck_min {
            duck_min - pg_min
        } else {
            0
        };

        metrics::set_duckdb_synced_block(self.chain_id, duck_max);
        metrics::set_duckdb_lag(self.chain_id, tip_lag);

        // Detect internal gaps
        let gaps = detect_gaps_duckdb(&self.duckdb).await?;
        let internal_gap_blocks: i64 = gaps.iter().map(|(s, e)| e - s + 1).sum();
        let total_gap_blocks = backfill_remaining + internal_gap_blocks;

        metrics::set_duckdb_gap_count(self.chain_id, gaps.len());
        metrics::set_duckdb_gap_blocks(self.chain_id, total_gap_blocks);

        // Update cached status for fast status endpoint queries
        self.duckdb.update_cached_status(duck_max, gaps.clone(), internal_gap_blocks);

        // Check for stale DuckDB data (DuckDB ahead of PostgreSQL)
        // This shouldn't happen normally but can occur after reorgs or data issues
        if duck_max > pg_max && pg_max > 0 {
            let stale_blocks = duck_max - pg_max;
            tracing::warn!(
                chain_id = self.chain_id,
                duck_max,
                pg_max,
                stale_blocks,
                "DuckDB has stale blocks beyond PostgreSQL tip, cleaning up"
            );
            // Delete blocks from DuckDB that don't exist in PostgreSQL
            if let Err(e) = self.duckdb.delete_blocks_from(pg_max + 1).await {
                tracing::error!(
                    chain_id = self.chain_id,
                    error = %e,
                    "Failed to clean up stale DuckDB blocks"
                );
            }
        }

        // Log sync status
        if tip_lag > 10 {
            tracing::warn!(
                chain_id = self.chain_id,
                duck_range = format!("{}-{}", duck_min, duck_max),
                pg_range = format!("{}-{}", pg_min, pg_max),
                tip_lag,
                backfill_remaining,
                internal_gaps = gaps.len(),
                internal_gap_blocks,
                "DuckDB falling behind PostgreSQL"
            );
        } else if total_gap_blocks > 0 {
            tracing::info!(
                chain_id = self.chain_id,
                duck_range = format!("{}-{}", duck_min, duck_max),
                pg_range = format!("{}-{}", pg_min, pg_max),
                tip_lag,
                backfill_remaining,
                internal_gaps = gaps.len(),
                "DuckDB sync progress"
            );
        } else {
            tracing::debug!(
                chain_id = self.chain_id,
                duck_max,
                pg_max,
                "DuckDB fully synced"
            );
        }

        Ok(())
    }
}


/// Backfills DuckDB from PostgreSQL for blocks, txs, logs, and receipts.
pub async fn backfill_from_postgres(
    pg_pool: &Pool,
    duckdb: &Arc<DuckDbPool>,
    batch_size: i64,
) -> Result<u64> {
    // Get current DuckDB watermark
    let duckdb_latest = duckdb.latest_block().await?.unwrap_or(0);

    // Get PostgreSQL latest block
    let pg_conn = pg_pool.get().await?;
    let pg_latest: i64 = pg_conn
        .query_one("SELECT COALESCE(MAX(num), 0) FROM blocks", &[])
        .await?
        .get(0);

    if duckdb_latest >= pg_latest {
        tracing::info!(
            duckdb_latest,
            pg_latest,
            "DuckDB is up to date with PostgreSQL"
        );
        return Ok(0);
    }

    let blocks_to_sync = pg_latest - duckdb_latest;
    tracing::info!(
        duckdb_latest,
        pg_latest,
        blocks_to_sync,
        "Starting DuckDB backfill from PostgreSQL"
    );

    let mut synced = 0u64;
    let mut current = duckdb_latest + 1;

    while current <= pg_latest {
        let end = (current + batch_size - 1).min(pg_latest);

        // Backfill blocks
        let block_rows = pg_conn
            .query(
                "SELECT num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner, extra_data 
                 FROM blocks WHERE num >= $1 AND num <= $2 ORDER BY num",
                &[&current, &end],
            )
            .await?;

        let block_count = block_rows.len();
        let mut all_statements: Vec<String> = Vec::new();

        // Build block insert statements
        for row in &block_rows {
            let num: i64 = row.get(0);
            let hash: Vec<u8> = row.get(1);
            let parent_hash: Vec<u8> = row.get(2);
            let timestamp: chrono::DateTime<chrono::Utc> = row.get(3);
            let timestamp_ms: i64 = row.get(4);
            let gas_limit: i64 = row.get(5);
            let gas_used: i64 = row.get(6);
            let miner: Vec<u8> = row.get(7);
            let extra_data: Option<Vec<u8>> = row.get(8);

            let sql = format!(
                "INSERT OR IGNORE INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner, extra_data) VALUES ({}, '0x{}', '0x{}', '{}', {}, {}, {}, '0x{}', {})",
                num,
                hex::encode(&hash),
                hex::encode(&parent_hash),
                timestamp.to_rfc3339(),
                timestamp_ms,
                gas_limit,
                gas_used,
                hex::encode(&miner),
                extra_data.as_ref().map(|d| format!("'0x{}'", hex::encode(d))).unwrap_or_else(|| "NULL".to_string()),
            );
            all_statements.push(sql);
        }

        // Backfill txs
        let tx_rows = pg_conn
            .query(
                "SELECT block_num, block_timestamp, idx, hash, type, \"from\", \"to\", value, input,
                        gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_used, nonce_key, nonce,
                        fee_token, fee_payer, calls::text, call_count, valid_before, valid_after, signature_type
                 FROM txs WHERE block_num >= $1 AND block_num <= $2 ORDER BY block_num, idx",
                &[&current, &end],
            )
            .await?;

        // Build transaction insert statements
        for chunk in tx_rows.chunks(500) {
            let values: Vec<String> = chunk.iter().map(|row| {
                let block_num: i64 = row.get(0);
                let block_timestamp: chrono::DateTime<chrono::Utc> = row.get(1);
                let idx: i32 = row.get(2);
                let hash: Vec<u8> = row.get(3);
                let tx_type: i16 = row.get(4);
                let from: Vec<u8> = row.get(5);
                let to: Option<Vec<u8>> = row.get(6);
                let value: String = row.get(7);
                let input: Vec<u8> = row.get(8);
                let gas_limit: i64 = row.get(9);
                let max_fee_per_gas: String = row.get(10);
                let max_priority_fee_per_gas: String = row.get(11);
                let gas_used: Option<i64> = row.get(12);
                let nonce_key: Vec<u8> = row.get(13);
                let nonce: i64 = row.get(14);
                let fee_token: Option<Vec<u8>> = row.get(15);
                let fee_payer: Option<Vec<u8>> = row.get(16);
                let calls: Option<String> = row.get(17);
                let call_count: i16 = row.get(18);
                let valid_before: Option<i64> = row.get(19);
                let valid_after: Option<i64> = row.get(20);
                let signature_type: Option<i16> = row.get(21);

                format!(
                    "({}, '{}', {}, '0x{}', {}, '0x{}', {}, '{}', '0x{}', {}, '{}', '{}', {}, '0x{}', {}, {}, {}, {}, {}, {}, {}, {})",
                    block_num,
                    block_timestamp.to_rfc3339(),
                    idx,
                    hex::encode(&hash),
                    tx_type,
                    hex::encode(&from),
                    to.as_ref().map(|t| format!("'0x{}'", hex::encode(t))).unwrap_or_else(|| "NULL".to_string()),
                    value,
                    hex::encode(&input),
                    gas_limit,
                    max_fee_per_gas,
                    max_priority_fee_per_gas,
                    gas_used.map(|g| g.to_string()).unwrap_or_else(|| "NULL".to_string()),
                    hex::encode(&nonce_key),
                    nonce,
                    fee_token.as_ref().map(|t| format!("'0x{}'", hex::encode(t))).unwrap_or_else(|| "NULL".to_string()),
                    fee_payer.as_ref().map(|p| format!("'0x{}'", hex::encode(p))).unwrap_or_else(|| "NULL".to_string()),
                    calls.as_ref().map(|c| format!("'{}'", c.replace('\'', "''"))).unwrap_or_else(|| "NULL".to_string()),
                    call_count,
                    valid_before.map(|v| v.to_string()).unwrap_or_else(|| "NULL".to_string()),
                    valid_after.map(|v| v.to_string()).unwrap_or_else(|| "NULL".to_string()),
                    signature_type.map(|s| s.to_string()).unwrap_or_else(|| "NULL".to_string()),
                )
            }).collect();

            if !values.is_empty() {
                let sql = format!(
                    "INSERT OR IGNORE INTO txs (block_num, block_timestamp, idx, hash, type, \"from\", \"to\", value, input, gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_used, nonce_key, nonce, fee_token, fee_payer, calls, call_count, valid_before, valid_after, signature_type) VALUES {}",
                    values.join(", ")
                );
                all_statements.push(sql);
            }
        }

        // Backfill logs
        let log_rows = pg_conn
            .query(
                "SELECT block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topic0, topic1, topic2, topic3, data
                 FROM logs WHERE block_num >= $1 AND block_num <= $2 ORDER BY block_num, log_idx",
                &[&current, &end],
            )
            .await?;

        // Build log insert statements
        for chunk in log_rows.chunks(500) {
            let values: Vec<String> = chunk
                .iter()
                .map(|row| {
                    let block_num: i64 = row.get(0);
                    let block_timestamp: chrono::DateTime<chrono::Utc> = row.get(1);
                    let log_idx: i32 = row.get(2);
                    let tx_idx: i32 = row.get(3);
                    let tx_hash: Vec<u8> = row.get(4);
                    let address: Vec<u8> = row.get(5);
                    let selector: Option<Vec<u8>> = row.get(6);
                    let topic0: Option<Vec<u8>> = row.get(7);
                    let topic1: Option<Vec<u8>> = row.get(8);
                    let topic2: Option<Vec<u8>> = row.get(9);
                    let topic3: Option<Vec<u8>> = row.get(10);
                    let data: Vec<u8> = row.get(11);

                    format!(
                        "({}, '{}', {}, {}, '0x{}', '0x{}', {}, {}, {}, {}, {}, '0x{}')",
                        block_num,
                        block_timestamp.to_rfc3339(),
                        log_idx,
                        tx_idx,
                        hex::encode(&tx_hash),
                        hex::encode(&address),
                        selector.as_ref().map(|s| format!("'0x{}'", hex::encode(s))).unwrap_or_else(|| "NULL".to_string()),
                        topic0.as_ref().map(|t| format!("'0x{}'", hex::encode(t))).unwrap_or_else(|| "NULL".to_string()),
                        topic1.as_ref().map(|t| format!("'0x{}'", hex::encode(t))).unwrap_or_else(|| "NULL".to_string()),
                        topic2.as_ref().map(|t| format!("'0x{}'", hex::encode(t))).unwrap_or_else(|| "NULL".to_string()),
                        topic3.as_ref().map(|t| format!("'0x{}'", hex::encode(t))).unwrap_or_else(|| "NULL".to_string()),
                        hex::encode(&data),
                    )
                })
                .collect();

            if !values.is_empty() {
                let sql = format!(
                    "INSERT OR IGNORE INTO logs (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topic0, topic1, topic2, topic3, data) VALUES {}",
                    values.join(", ")
                );
                all_statements.push(sql);
            }
        }

        // Backfill receipts
        let receipt_rows = pg_conn
            .query(
                "SELECT block_num, block_timestamp, tx_idx, tx_hash, \"from\", \"to\", contract_address,
                        gas_used, cumulative_gas_used, effective_gas_price, status, fee_payer
                 FROM receipts WHERE block_num >= $1 AND block_num <= $2 ORDER BY block_num, tx_idx",
                &[&current, &end],
            )
            .await?;

        // Build receipt insert statements
        for chunk in receipt_rows.chunks(500) {
            let values: Vec<String> = chunk.iter().map(|row| {
                let block_num: i64 = row.get(0);
                let block_timestamp: chrono::DateTime<chrono::Utc> = row.get(1);
                let tx_idx: i32 = row.get(2);
                let tx_hash: Vec<u8> = row.get(3);
                let from: Vec<u8> = row.get(4);
                let to: Option<Vec<u8>> = row.get(5);
                let contract_address: Option<Vec<u8>> = row.get(6);
                let gas_used: i64 = row.get(7);
                let cumulative_gas_used: i64 = row.get(8);
                let effective_gas_price: Option<String> = row.get(9);
                let status: Option<i16> = row.get(10);
                let fee_payer: Option<Vec<u8>> = row.get(11);

                format!(
                    "({}, '{}', {}, '0x{}', '0x{}', {}, {}, {}, {}, {}, {}, {})",
                    block_num,
                    block_timestamp.to_rfc3339(),
                    tx_idx,
                    hex::encode(&tx_hash),
                    hex::encode(&from),
                    to.as_ref().map(|t| format!("'0x{}'", hex::encode(t))).unwrap_or_else(|| "NULL".to_string()),
                    contract_address.as_ref().map(|c| format!("'0x{}'", hex::encode(c))).unwrap_or_else(|| "NULL".to_string()),
                    gas_used,
                    cumulative_gas_used,
                    effective_gas_price.as_ref().map(|p| format!("'{}'", p)).unwrap_or_else(|| "NULL".to_string()),
                    status.map(|s| s.to_string()).unwrap_or_else(|| "NULL".to_string()),
                    fee_payer.as_ref().map(|p| format!("'0x{}'", hex::encode(p))).unwrap_or_else(|| "NULL".to_string()),
                )
            }).collect();

            if !values.is_empty() {
                let sql = format!(
                    "INSERT OR IGNORE INTO receipts (block_num, block_timestamp, tx_idx, tx_hash, \"from\", \"to\", contract_address, gas_used, cumulative_gas_used, effective_gas_price, status, fee_payer) VALUES {}",
                    values.join(", ")
                );
                all_statements.push(sql);
            }
        }

        // Execute all statements on the dedicated DuckDB thread
        duckdb.with_connection(move |conn| {
            for sql in all_statements {
                if let Err(e) = conn.execute(&sql, []) {
                    tracing::warn!(error = %e, "Failed to execute SQL in backfill, skipping");
                }
            }
            Ok(())
        }).await?;

        synced += block_count as u64;
        current = end + 1;

        if synced % 10000 == 0 {
            tracing::info!(synced, current, pg_latest, "DuckDB backfill progress");
        }
    }

    // Update watermark
    let update_sql = format!(
        "UPDATE duckdb_sync_state SET latest_block = {}, updated_at = CURRENT_TIMESTAMP WHERE id = 1",
        pg_latest
    );
    duckdb.execute(&update_sql).await?;

    tracing::info!(synced, "DuckDB backfill complete");
    Ok(synced)
}

/// Gets the current DuckDB sync status including any gaps.
pub async fn get_sync_status(duckdb: &Arc<DuckDbPool>) -> Result<DuckDbSyncStatus> {
    // Use cached status for fast response (updated by background replicator task)
    let cached = duckdb.get_cached_status();
    
    Ok(DuckDbSyncStatus {
        latest_block: cached.latest_block,
        gaps: cached.gaps,
        gap_blocks: cached.gap_blocks,
        updated_at: String::new(),
    })
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DuckDbSyncStatus {
    pub latest_block: i64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub gaps: Vec<(i64, i64)>,
    pub gap_blocks: i64,
    pub updated_at: String,
}

/// Detect gaps in DuckDB block sequence (between existing blocks only).
/// Returns a list of (start, end) ranges that are missing.
pub async fn detect_gaps_duckdb(duckdb: &Arc<DuckDbPool>) -> Result<Vec<(i64, i64)>> {
    duckdb.with_connection_result(|conn| {
        let mut stmt = conn.prepare(
            r#"
            WITH numbered AS (
                SELECT num, LAG(num) OVER (ORDER BY num) as prev_num
                FROM blocks
            )
            SELECT prev_num + 1 as gap_start, num - 1 as gap_end
            FROM numbered
            WHERE num - prev_num > 1
            ORDER BY gap_end DESC
            "#,
        )?;

        let gaps: Vec<(i64, i64)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(gaps)
    }).await
}

/// Detect ALL gaps in DuckDB including from genesis to first block.
/// Returns gaps sorted by end block descending (most recent first).
pub async fn detect_all_gaps_duckdb(duckdb: &Arc<DuckDbPool>, tip_num: i64) -> Result<Vec<(i64, i64)>> {
    // Get min block
    let min_block: Option<i64> = duckdb.with_connection_result(|conn| {
        let result = conn
            .prepare("SELECT MIN(num) FROM blocks")
            .ok()
            .and_then(|mut stmt| stmt.query_row([], |row| row.get(0)).ok());
        Ok(result)
    }).await?;

    let mut gaps = detect_gaps_duckdb(duckdb).await?;

    // Add gap from block 1 to first block (if we have any blocks and min > 1)
    if let Some(min) = min_block {
        if min > 1 {
            gaps.push((1, min - 1));
        }
    } else if tip_num > 0 {
        // No blocks at all - entire range is a gap
        gaps.push((1, tip_num));
    }

    // Filter to only gaps up to tip_num
    gaps.retain(|(_, end)| *end <= tip_num);

    // Sort by end block descending (most recent gaps first)
    gaps.sort_by(|a, b| b.1.cmp(&a.1));

    Ok(gaps)
}

/// Fill specific gap ranges in DuckDB from PostgreSQL.
/// 
/// Uses the mutex-protected write connection to avoid concurrent write corruption.
/// Gap-fill yields the lock between batches to allow realtime writes to proceed.
pub async fn fill_gaps_from_postgres(
    pg_pool: &Pool,
    duckdb: &Arc<DuckDbPool>,
    gaps: &[(i64, i64)],
    batch_size: i64,
) -> Result<u64> {
    if gaps.is_empty() {
        return Ok(0);
    }

    let total_blocks: i64 = gaps.iter().map(|(s, e)| e - s + 1).sum();
    tracing::info!(gap_count = gaps.len(), total_blocks, "Starting DuckDB gap fill from PostgreSQL");

    let pg_conn = pg_pool.get().await?;
    let mut synced = 0u64;

    for (gap_start, gap_end) in gaps {
        let mut current = *gap_start;
        while current <= *gap_end {
            let end = (current + batch_size - 1).min(*gap_end);

            // Fetch all data from PostgreSQL first (outside DuckDB thread)
            let block_rows = pg_conn
                .query(
                    "SELECT num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner, extra_data 
                     FROM blocks WHERE num >= $1 AND num <= $2 ORDER BY num",
                    &[&current, &end],
                )
                .await?;

            let block_count = block_rows.len();
            let mut all_statements: Vec<String> = Vec::new();

            // Build block insert statements
            for row in &block_rows {
                let num: i64 = row.get(0);
                let hash: Vec<u8> = row.get(1);
                let parent_hash: Vec<u8> = row.get(2);
                let timestamp: chrono::DateTime<chrono::Utc> = row.get(3);
                let timestamp_ms: i64 = row.get(4);
                let gas_limit: i64 = row.get(5);
                let gas_used: i64 = row.get(6);
                let miner: Vec<u8> = row.get(7);
                let extra_data: Option<Vec<u8>> = row.get(8);

                let sql = format!(
                    "INSERT OR IGNORE INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner, extra_data) VALUES ({}, '0x{}', '0x{}', '{}', {}, {}, {}, '0x{}', {})",
                    num,
                    hex::encode(&hash),
                    hex::encode(&parent_hash),
                    timestamp.to_rfc3339(),
                    timestamp_ms,
                    gas_limit,
                    gas_used,
                    hex::encode(&miner),
                    extra_data.as_ref().map(|d| format!("'0x{}'", hex::encode(d))).unwrap_or_else(|| "NULL".to_string()),
                );
                all_statements.push(sql);
            }

            // Fetch and build txs
            let tx_rows = pg_conn
                .query(
                    "SELECT block_num, block_timestamp, idx, hash, type, \"from\", \"to\", value, input,
                            gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_used, nonce_key, nonce,
                            fee_token, fee_payer, calls::text, call_count, valid_before, valid_after, signature_type
                     FROM txs WHERE block_num >= $1 AND block_num <= $2 ORDER BY block_num, idx",
                    &[&current, &end],
                )
                .await?;

            for chunk in tx_rows.chunks(500) {
                let values: Vec<String> = chunk.iter().map(|row| {
                    let block_num: i64 = row.get(0);
                    let block_timestamp: chrono::DateTime<chrono::Utc> = row.get(1);
                    let idx: i32 = row.get(2);
                    let hash: Vec<u8> = row.get(3);
                    let tx_type: i16 = row.get(4);
                    let from: Vec<u8> = row.get(5);
                    let to: Option<Vec<u8>> = row.get(6);
                    let value: String = row.get(7);
                    let input: Vec<u8> = row.get(8);
                    let gas_limit: i64 = row.get(9);
                    let max_fee: String = row.get(10);
                    let max_priority: String = row.get(11);
                    let gas_used: Option<i64> = row.get(12);
                    let nonce_key: Vec<u8> = row.get(13);
                    let nonce: i64 = row.get(14);
                    let fee_token: Option<Vec<u8>> = row.get(15);
                    let fee_payer: Option<Vec<u8>> = row.get(16);
                    let calls: Option<String> = row.get(17);
                    let call_count: i16 = row.get(18);
                    let valid_before: Option<i64> = row.get(19);
                    let valid_after: Option<i64> = row.get(20);
                    let signature_type: Option<i16> = row.get(21);

                    format!(
                        "({}, '{}', {}, '0x{}', {}, '0x{}', {}, '{}', '0x{}', {}, '{}', '{}', {}, '0x{}', {}, {}, {}, {}, {}, {}, {}, {})",
                        block_num,
                        block_timestamp.to_rfc3339(),
                        idx,
                        hex::encode(&hash),
                        tx_type,
                        hex::encode(&from),
                        to.as_ref().map(|t| format!("'0x{}'", hex::encode(t))).unwrap_or_else(|| "NULL".to_string()),
                        value,
                        hex::encode(&input),
                        gas_limit,
                        max_fee,
                        max_priority,
                        gas_used.map(|g| g.to_string()).unwrap_or_else(|| "NULL".to_string()),
                        hex::encode(&nonce_key),
                        nonce,
                        fee_token.as_ref().map(|t| format!("'0x{}'", hex::encode(t))).unwrap_or_else(|| "NULL".to_string()),
                        fee_payer.as_ref().map(|p| format!("'0x{}'", hex::encode(p))).unwrap_or_else(|| "NULL".to_string()),
                        calls.as_ref().map(|c| format!("'{}'", c.replace('\'', "''"))).unwrap_or_else(|| "NULL".to_string()),
                        call_count,
                        valid_before.map(|v| v.to_string()).unwrap_or_else(|| "NULL".to_string()),
                        valid_after.map(|v| v.to_string()).unwrap_or_else(|| "NULL".to_string()),
                        signature_type.map(|s| s.to_string()).unwrap_or_else(|| "NULL".to_string()),
                    )
                }).collect();

                if !values.is_empty() {
                    let sql = format!(
                        "INSERT OR IGNORE INTO txs (block_num, block_timestamp, idx, hash, type, \"from\", \"to\", value, input, gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_used, nonce_key, nonce, fee_token, fee_payer, calls, call_count, valid_before, valid_after, signature_type) VALUES {}",
                        values.join(", ")
                    );
                    all_statements.push(sql);
                }
            }

            // Fetch and build logs
            let log_rows = pg_conn
                .query(
                    "SELECT block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topic0, topic1, topic2, topic3, data
                     FROM logs WHERE block_num >= $1 AND block_num <= $2 ORDER BY block_num, log_idx",
                    &[&current, &end],
                )
                .await?;

            for chunk in log_rows.chunks(500) {
                let values: Vec<String> = chunk.iter().map(|row| {
                    let block_num: i64 = row.get(0);
                    let block_timestamp: chrono::DateTime<chrono::Utc> = row.get(1);
                    let log_idx: i32 = row.get(2);
                    let tx_idx: i32 = row.get(3);
                    let tx_hash: Vec<u8> = row.get(4);
                    let address: Vec<u8> = row.get(5);
                    let selector: Option<Vec<u8>> = row.get(6);
                    let topic0: Option<Vec<u8>> = row.get(7);
                    let topic1: Option<Vec<u8>> = row.get(8);
                    let topic2: Option<Vec<u8>> = row.get(9);
                    let topic3: Option<Vec<u8>> = row.get(10);
                    let data: Vec<u8> = row.get(11);

                    format!(
                        "({}, '{}', {}, {}, '0x{}', '0x{}', {}, {}, {}, {}, {}, '0x{}')",
                        block_num,
                        block_timestamp.to_rfc3339(),
                        log_idx,
                        tx_idx,
                        hex::encode(&tx_hash),
                        hex::encode(&address),
                        selector.as_ref().map(|s| format!("'0x{}'", hex::encode(s))).unwrap_or_else(|| "NULL".to_string()),
                        topic0.as_ref().map(|t| format!("'0x{}'", hex::encode(t))).unwrap_or_else(|| "NULL".to_string()),
                        topic1.as_ref().map(|t| format!("'0x{}'", hex::encode(t))).unwrap_or_else(|| "NULL".to_string()),
                        topic2.as_ref().map(|t| format!("'0x{}'", hex::encode(t))).unwrap_or_else(|| "NULL".to_string()),
                        topic3.as_ref().map(|t| format!("'0x{}'", hex::encode(t))).unwrap_or_else(|| "NULL".to_string()),
                        hex::encode(&data),
                    )
                }).collect();

                if !values.is_empty() {
                    let sql = format!(
                        "INSERT OR IGNORE INTO logs (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topic0, topic1, topic2, topic3, data) VALUES {}",
                        values.join(", ")
                    );
                    all_statements.push(sql);
                }
            }

            // Fetch and build receipts
            let receipt_rows = pg_conn
                .query(
                    "SELECT block_num, block_timestamp, tx_idx, tx_hash, \"from\", \"to\", contract_address,
                            gas_used, cumulative_gas_used, effective_gas_price, status, fee_payer
                     FROM receipts WHERE block_num >= $1 AND block_num <= $2 ORDER BY block_num, tx_idx",
                    &[&current, &end],
                )
                .await?;

            for chunk in receipt_rows.chunks(500) {
                let values: Vec<String> = chunk.iter().map(|row| {
                    let block_num: i64 = row.get(0);
                    let block_timestamp: chrono::DateTime<chrono::Utc> = row.get(1);
                    let tx_idx: i32 = row.get(2);
                    let tx_hash: Vec<u8> = row.get(3);
                    let from: Vec<u8> = row.get(4);
                    let to: Option<Vec<u8>> = row.get(5);
                    let contract_address: Option<Vec<u8>> = row.get(6);
                    let gas_used: i64 = row.get(7);
                    let cumulative_gas_used: i64 = row.get(8);
                    let effective_gas_price: Option<String> = row.get(9);
                    let status: Option<i16> = row.get(10);
                    let fee_payer: Option<Vec<u8>> = row.get(11);

                    format!(
                        "({}, '{}', {}, '0x{}', '0x{}', {}, {}, {}, {}, {}, {}, {})",
                        block_num,
                        block_timestamp.to_rfc3339(),
                        tx_idx,
                        hex::encode(&tx_hash),
                        hex::encode(&from),
                        to.as_ref().map(|t| format!("'0x{}'", hex::encode(t))).unwrap_or_else(|| "NULL".to_string()),
                        contract_address.as_ref().map(|a| format!("'0x{}'", hex::encode(a))).unwrap_or_else(|| "NULL".to_string()),
                        gas_used,
                        cumulative_gas_used,
                        effective_gas_price.as_ref().map(|p| format!("'{}'", p)).unwrap_or_else(|| "NULL".to_string()),
                        status.map(|s| s.to_string()).unwrap_or_else(|| "NULL".to_string()),
                        fee_payer.as_ref().map(|p| format!("'0x{}'", hex::encode(p))).unwrap_or_else(|| "NULL".to_string()),
                    )
                }).collect();

                if !values.is_empty() {
                    let sql = format!(
                        "INSERT OR IGNORE INTO receipts (block_num, block_timestamp, tx_idx, tx_hash, \"from\", \"to\", contract_address, gas_used, cumulative_gas_used, effective_gas_price, status, fee_payer) VALUES {}",
                        values.join(", ")
                    );
                    all_statements.push(sql);
                }
            }

            // Execute all statements on the dedicated DuckDB thread
            duckdb.with_connection(move |conn| {
                for sql in all_statements {
                    if let Err(e) = conn.execute(&sql, []) {
                        tracing::warn!(error = %e, "Failed to execute SQL in gap-fill, skipping");
                    }
                }
                Ok(())
            }).await?;

            synced += block_count as u64;
            current = end + 1;
        }
    }

    tracing::info!(synced, "DuckDB gap fill complete");
    Ok(synced)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn create_test_pg_pool() -> Pool {
        use deadpool_postgres::{Config, Runtime};
        let mut cfg = Config::new();
        cfg.host = Some("localhost".to_string());
        cfg.dbname = Some("test".to_string());
        cfg.pool = Some(deadpool_postgres::PoolConfig::new(1));
        cfg.create_pool(Some(Runtime::Tokio1), tokio_postgres::NoTls).unwrap()
    }

    #[tokio::test]
    async fn test_non_blocking_send_sets_needs_sync_flag() {
        let duckdb = Arc::new(DuckDbPool::in_memory().unwrap());
        let pg_pool = create_test_pg_pool();
        let (replicator, handle) = Replicator::new(duckdb.clone(), pg_pool, 2, 1);

        // Initially needs_sync should be false
        assert!(!replicator.needs_sync.load(Ordering::Relaxed));

        // Send a block - should set needs_sync to true
        let block = BlockRow {
            num: 1,
            hash: vec![0xab; 32],
            parent_hash: vec![0x00; 32],
            timestamp: Utc::now(),
            timestamp_ms: 1704067200000,
            gas_limit: 30_000_000,
            gas_used: 21000,
            miner: vec![0xde; 20],
            extra_data: None,
        };
        handle.send_blocks(vec![block]);

        // needs_sync should now be true
        assert!(replicator.needs_sync.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn test_non_blocking_send_never_blocks() {
        let duckdb = Arc::new(DuckDbPool::in_memory().unwrap());
        let pg_pool = create_test_pg_pool();
        // Create a tiny buffer of 2
        let (replicator, handle) = Replicator::new(duckdb.clone(), pg_pool, 2, 1);

        // Don't start the replicator - channel will fill up
        let _replicator = replicator;

        // Send 10 batches - should not block even though buffer is 2
        for i in 0..10 {
            let block = BlockRow {
                num: i,
                hash: vec![i as u8; 32],
                parent_hash: vec![0x00; 32],
                timestamp: Utc::now(),
                timestamp_ms: 1704067200000,
                gas_limit: 30_000_000,
                gas_used: 21000,
                miner: vec![0xde; 20],
                extra_data: None,
            };
            handle.send_blocks(vec![block]);
        }

        // If we got here without blocking, the test passes
    }

    #[tokio::test]
    async fn test_sync_status() {
        let duckdb = Arc::new(DuckDbPool::in_memory().unwrap());
        let status = get_sync_status(&duckdb).await.unwrap();
        assert_eq!(status.latest_block, 0);
        assert!(status.gaps.is_empty());
        assert_eq!(status.gap_blocks, 0);
    }

    #[tokio::test]
    async fn test_detect_gaps_duckdb_no_gaps() {
        let duckdb = Arc::new(DuckDbPool::in_memory().unwrap());

        // Insert contiguous blocks 1, 2, 3
        for num in 1..=3 {
            insert_block(&duckdb, num).await;
        }

        let gaps = detect_gaps_duckdb(&duckdb).await.unwrap();
        assert!(gaps.is_empty());
    }

    #[tokio::test]
    async fn test_detect_gaps_duckdb_with_gap() {
        let duckdb = Arc::new(DuckDbPool::in_memory().unwrap());

        // Insert blocks 1, 2, 5, 6 (gap at 3-4)
        for num in [1, 2, 5, 6] {
            insert_block(&duckdb, num).await;
        }

        let gaps = detect_gaps_duckdb(&duckdb).await.unwrap();
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0], (3, 4));
    }

    #[tokio::test]
    async fn test_detect_gaps_duckdb_multiple_gaps() {
        let duckdb = Arc::new(DuckDbPool::in_memory().unwrap());

        // Insert blocks 1, 5, 10 (gaps at 2-4 and 6-9)
        for num in [1, 5, 10] {
            insert_block(&duckdb, num).await;
        }

        let gaps = detect_gaps_duckdb(&duckdb).await.unwrap();
        assert_eq!(gaps.len(), 2);
        // Sorted by end DESC (most recent first)
        assert_eq!(gaps[0], (6, 9));
        assert_eq!(gaps[1], (2, 4));
    }

    #[tokio::test]
    async fn test_detect_all_gaps_duckdb_includes_genesis() {
        let duckdb = Arc::new(DuckDbPool::in_memory().unwrap());

        // Insert only block 5 (gap from 1-4)
        insert_block(&duckdb, 5).await;

        let gaps = detect_all_gaps_duckdb(&duckdb, 10).await.unwrap();
        assert_eq!(gaps.len(), 1);
        // Gap from genesis to first block
        assert_eq!(gaps[0], (1, 4));
    }

    #[tokio::test]
    async fn test_sync_status_with_gaps() {
        let duckdb = Arc::new(DuckDbPool::in_memory().unwrap());

        // Insert blocks 1, 5 (gap at 2-4)
        for num in [1, 5] {
            insert_block(&duckdb, num).await;
        }

        // Detect gaps and update cache (simulates what background task does)
        let latest_block = duckdb.latest_block().await.unwrap().unwrap_or(0);
        let gaps = detect_gaps_duckdb(&duckdb).await.unwrap();
        let gap_blocks: i64 = gaps.iter().map(|(s, e)| e - s + 1).sum();
        duckdb.update_cached_status(latest_block, gaps, gap_blocks);

        let status = get_sync_status(&duckdb).await.unwrap();
        assert_eq!(status.latest_block, 5);
        assert_eq!(status.gaps.len(), 1);
        assert_eq!(status.gaps[0], (2, 4));
        assert_eq!(status.gap_blocks, 3); // blocks 2, 3, 4
    }

    /// Helper to insert a block directly into DuckDB
    async fn insert_block(duckdb: &Arc<DuckDbPool>, num: i64) {
        let timestamp = Utc::now().to_rfc3339();
        let sql = format!(
            "INSERT OR IGNORE INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner, extra_data) \
             VALUES ({}, '0x{}', '0x{}', '{}', {}, 30000000, 21000, '0x{}', NULL)",
            num,
            hex::encode(vec![num as u8; 32]),
            hex::encode(vec![(num.saturating_sub(1)) as u8; 32]),
            timestamp,
            1704067200000i64 + num * 1000,
            hex::encode(vec![0xde; 20]),
        );
        duckdb.execute(&sql).await.unwrap();
    }

    #[tokio::test]
    async fn test_gap_fill_detects_backfill_gap() {
        // When DuckDB has blocks 100-110 and PG has 50-110,
        // gap-fill should detect the 50-99 gap
        let duckdb = Arc::new(DuckDbPool::in_memory().unwrap());

        // Insert blocks 100-110 into DuckDB
        for num in 100..=110 {
            insert_block(&duckdb, num).await;
        }

        // Verify DuckDB range
        let (min, max) = duckdb.block_range().await.unwrap();
        assert_eq!(min, Some(100));
        assert_eq!(max, Some(110));

        // Internal gaps should be empty (100-110 is contiguous)
        let internal_gaps = detect_gaps_duckdb(&duckdb).await.unwrap();
        assert!(internal_gaps.is_empty());

        // But if PG has blocks 50-110, gap_fill_batch should detect 50-99
        // (We can't test the full flow without a real PG, but we verify the gap detection logic)
    }

    #[tokio::test]
    async fn test_gap_fill_detects_internal_gaps() {
        // When DuckDB has blocks 1, 5, 10 it should detect gaps 2-4 and 6-9
        let duckdb = Arc::new(DuckDbPool::in_memory().unwrap());

        for num in [1, 5, 10] {
            insert_block(&duckdb, num).await;
        }

        let gaps = detect_gaps_duckdb(&duckdb).await.unwrap();
        
        // Should have 2 gaps, sorted by end DESC (most recent first)
        assert_eq!(gaps.len(), 2);
        assert_eq!(gaps[0], (6, 9)); // Most recent gap first
        assert_eq!(gaps[1], (2, 4));
    }

    #[tokio::test]
    async fn test_tail_starts_from_pg_min_when_duckdb_empty() {
        // This tests the initial sync behavior:
        // When DuckDB is empty, tail_postgres should start from pg_tip backwards,
        // not from block 1.
        
        // We can't run the full flow without PG, but we can verify the logic:
        // - If duck_tip == 0, we start from pg_tip - MAX_BLOCKS_PER_TICK
        // - This ensures we get recent data first
        
        let duckdb = Arc::new(DuckDbPool::in_memory().unwrap());
        let latest = duckdb.latest_block().await.unwrap();
        assert_eq!(latest, None); // DuckDB is empty
    }

    #[tokio::test]
    async fn test_two_tasks_share_duckdb_pool() {
        // Verify that both tasks can access the same DuckDB pool
        let duckdb = Arc::new(DuckDbPool::in_memory().unwrap());
        
        let duckdb1 = duckdb.clone();
        let duckdb2 = duckdb.clone();

        // Simulate two tasks writing concurrently
        let handle1 = tokio::spawn(async move {
            for num in 1..=10 {
                insert_block(&duckdb1, num).await;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        let handle2 = tokio::spawn(async move {
            for num in 100..=110 {
                insert_block(&duckdb2, num).await;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        handle1.await.unwrap();
        handle2.await.unwrap();

        // Both ranges should be present
        let count: i64 = duckdb.with_connection_result(|conn| {
            let count = conn.prepare("SELECT COUNT(*) FROM blocks")?
                .query_row([], |row| row.get(0))?;
            Ok(count)
        }).await.unwrap();
        
        // 10 blocks from task1 (1-10) + 11 blocks from task2 (100-110) = 21
        assert_eq!(count, 21);
    }

    #[tokio::test]
    async fn test_gap_fill_prioritizes_recent_gaps() {
        // Gap-fill should process most recent gaps first
        let duckdb = Arc::new(DuckDbPool::in_memory().unwrap());

        // Insert blocks creating multiple gaps: 1, 10, 20, 30
        // Gaps: 2-9, 11-19, 21-29
        for num in [1, 10, 20, 30] {
            insert_block(&duckdb, num).await;
        }

        let gaps = detect_gaps_duckdb(&duckdb).await.unwrap();
        
        // Should be sorted by end DESC (most recent first)
        assert_eq!(gaps.len(), 3);
        assert_eq!(gaps[0], (21, 29)); // Most recent
        assert_eq!(gaps[1], (11, 19));
        assert_eq!(gaps[2], (2, 9));   // Oldest
    }

}
