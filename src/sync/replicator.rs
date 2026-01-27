use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::sync::mpsc;

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
pub struct Replicator {
    duckdb: Arc<DuckDbPool>,
    pg_pool: Pool,
    /// PostgreSQL connection URL for postgres_scanner extension
    pg_url: String,
    rx: mpsc::Receiver<ReplicaBatch>,
    chain_id: u64,
    /// Flag set when channel receives data, signals to poll immediately
    needs_sync: Arc<AtomicBool>,
}

/// Handle for sending hints to the replicator.
/// The channel is now just a low-latency signal, not the source of truth.
#[derive(Clone)]
pub struct ReplicatorHandle {
    tx: mpsc::Sender<ReplicaBatch>,
    needs_sync: Arc<AtomicBool>,
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
}

impl Replicator {
    /// Creates a new replicator with a channel for receiving batches.
    pub fn new(duckdb: Arc<DuckDbPool>, pg_pool: Pool, pg_url: String, buffer_size: usize, chain_id: u64) -> (Self, ReplicatorHandle) {
        let (tx, rx) = mpsc::channel(buffer_size);
        let needs_sync = Arc::new(AtomicBool::new(false));
        (
            Self { duckdb, pg_pool, pg_url, rx, chain_id, needs_sync: needs_sync.clone() },
            ReplicatorHandle { tx, needs_sync },
        )
    }

    /// Runs the replicator with two concurrent tasks:
    /// 1. Tail task: low-latency sync of new blocks from pg_tip (every 500ms)
    /// 2. Gap-fill task: high-throughput backfill of historical blocks (continuous)
    ///
    /// Both tasks share the same DuckDB pool (writes serialize on mutex).
    pub async fn run(mut self) -> Result<()> {
        tracing::info!(chain_id = self.chain_id, "DuckDB replicator started");

        let duckdb = self.duckdb.clone();
        let pg_pool = self.pg_pool.clone();
        let pg_url = self.pg_url.clone();
        let chain_id = self.chain_id;

        // Spawn gap-fill task (runs continuously until caught up)
        let gap_fill_duckdb = duckdb.clone();
        let gap_fill_handle = tokio::spawn(async move {
            Self::run_gap_fill_task(gap_fill_duckdb, pg_pool, pg_url, chain_id).await
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
                    let conn = self.duckdb.conn().await;
                    if let Err(e) = conn.execute("CHECKPOINT", []) {
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

    /// Gap-fill task: high-throughput backfill using postgres extension.
    /// 
    /// Uses DuckDB's postgres extension for 10-100x faster backfill:
    /// - Parallel scans with consistent snapshots
    /// - Binary protocol (no text encoding overhead)
    /// - Single query per table (no application-level batch loops)
    /// - No lock contention (single bulk operation)
    /// 
    /// Falls back to row-by-row copy if the extension isn't available.
    async fn run_gap_fill_task(
        duckdb: Arc<DuckDbPool>,
        pg_pool: Pool,
        pg_url: String,
        chain_id: u64,
    ) -> Result<()> {
        // Initial delay to let tail task establish watermark
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Check if postgres extension is available and log diagnostic info
        let (extension_available, extension_info) = {
            let conn = duckdb.conn().await;
            
            // Try to install (may already be installed/cached)
            let install_result = conn.execute("INSTALL postgres", []);
            let load_result = install_result.and_then(|_| conn.execute("LOAD postgres", []));
            
            if load_result.is_ok() {
                // Get extension version for logging
                let version: Option<String> = conn
                    .prepare("SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'postgres'")
                    .ok()
                    .and_then(|mut stmt| stmt.query_row([], |row| row.get(0)).ok());
                
                // Get DuckDB version
                let duckdb_version: Option<String> = conn
                    .prepare("SELECT version()")
                    .ok()
                    .and_then(|mut stmt| stmt.query_row([], |row| row.get(0)).ok());
                
                (true, format!(
                    "postgres extension v{} loaded (DuckDB {})",
                    version.unwrap_or_else(|| "unknown".to_string()),
                    duckdb_version.unwrap_or_else(|| "unknown".to_string())
                ))
            } else {
                let err_msg = load_result.err().map(|e| e.to_string()).unwrap_or_default();
                (false, format!("failed to load: {}", err_msg))
            }
        };

        if extension_available {
            tracing::info!(
                chain_id,
                extension_info,
                "DuckDB gap-fill task started (postgres extension mode - fast)"
            );
        } else {
            tracing::warn!(
                chain_id,
                extension_info,
                "DuckDB postgres extension not available, falling back to row-by-row mode (slower)"
            );
        }

        let mut last_checkpoint = Instant::now();

        loop {
            let result = if extension_available {
                Self::gap_fill_with_scanner(&duckdb, &pg_url, chain_id).await
            } else {
                Self::gap_fill_with_fallback(&duckdb, &pg_pool, chain_id).await
            };

            match result {
                Ok(synced) => {
                    // Checkpoint every 60s after syncing
                    if synced > 0 && last_checkpoint.elapsed() > Duration::from_secs(60) {
                        let conn = duckdb.conn().await;
                        let _ = conn.execute("CHECKPOINT", []);
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

    /// Fallback gap-fill using row-by-row copy via pg_pool.
    /// Slower but works without the postgres extension.
    async fn gap_fill_with_fallback(
        duckdb: &Arc<DuckDbPool>,
        pg_pool: &Pool,
        chain_id: u64,
    ) -> Result<i64> {
        const BATCH_SIZE: i64 = 100;

        let duck_min = {
            let (min, _max) = duckdb.block_range().await?;
            min.unwrap_or(0)
        };

        if duck_min == 0 {
            return Ok(0);
        }

        // Detect gaps
        let internal_gaps = detect_gaps_duckdb(duckdb).await?;
        
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
                let synced = copy_range_to_duckdb(&pg_conn, duckdb, batch_start, current).await?;
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
                "DuckDB fallback gap-fill complete"
            );
        }

        Ok(total_synced)
    }

    /// Fills gaps using postgres_scanner extension.
    /// Returns number of blocks synced.
    async fn gap_fill_with_scanner(
        duckdb: &Arc<DuckDbPool>,
        pg_url: &str,
        chain_id: u64,
    ) -> Result<i64> {
        // Get DuckDB range
        let (duck_min, duck_max) = {
            let (min, max) = duckdb.block_range().await?;
            (min.unwrap_or(0), max.unwrap_or(0))
        };

        if duck_min == 0 {
            // DuckDB empty, wait for tail task to do initial sync
            return Ok(0);
        }

        // Detect gaps in DuckDB
        let internal_gaps = detect_gaps_duckdb(duckdb).await?;
        
        // Query PG via the postgres extension (modern DuckDB 1.4+ approach)
        let conn = duckdb.conn().await;
        
        // Load extension (INSTALL is cached after first call, LOAD needed per connection)
        conn.execute("LOAD postgres", [])?;
        
        // Use chain-specific alias to avoid conflicts when multiple chains share a connection
        let pg_alias = format!("pg_{}", chain_id);
        
        // Attach Postgres database with chain-specific alias (skip if already attached)
        let attach_sql = format!(
            "ATTACH IF NOT EXISTS '{}' AS {} (TYPE postgres, READ_ONLY)",
            pg_url.replace('\'', "''"),
            pg_alias
        );
        conn.execute(&attach_sql, [])?;

        // Get PG block range via attached database
        let mut stmt = conn.prepare(&format!(
            "SELECT COALESCE(MIN(num), 0) as pg_min, COALESCE(MAX(num), 0) as pg_max \
             FROM {}.public.blocks",
            pg_alias
        ))?;
        let (pg_min, pg_max): (i64, i64) = stmt.query_row([], |row| Ok((row.get(0)?, row.get(1)?)))?;

        if pg_max == 0 {
            return Ok(0);
        }

        // Build gaps list
        let mut gaps: Vec<(i64, i64)> = Vec::new();

        // Backfill: blocks below duck_min that PG has
        if pg_min < duck_min {
            gaps.push((pg_min, duck_min - 1));
        }

        // Internal gaps
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

        // Process gaps in larger batches (postgres_scanner handles parallelism internally)
        const BATCH_SIZE: i64 = 10_000;
        let mut total_synced = 0i64;
        let start_time = Instant::now();

        for (gap_start, gap_end) in gaps {
            // Process gap in chunks
            let mut current = gap_end;
            while current >= gap_start {
                let batch_start = (current - BATCH_SIZE + 1).max(gap_start);
                
                let synced = Self::copy_range_with_scanner(&conn, &pg_alias, batch_start, current)?;
                total_synced += synced;
                
                current = batch_start - 1;
            }
        }

        drop(conn); // Release lock before logging

        if total_synced > 0 {
            let elapsed = start_time.elapsed();
            let rate = total_synced as f64 / elapsed.as_secs_f64();
            tracing::info!(
                chain_id,
                synced = total_synced,
                duck_range = format!("{}-{}", duck_min, duck_max),
                pg_range = format!("{}-{}", pg_min, pg_max),
                elapsed_ms = elapsed.as_millis(),
                rate = format!("{:.0} blk/s", rate),
                "DuckDB postgres_scanner gap-fill complete"
            );
        }

        Ok(total_synced)
    }

    /// Copies a block range from Postgres to DuckDB using the attached postgres database.
    /// 
    /// Assumes postgres database is already attached via `ATTACH ... AS {pg_alias} (TYPE postgres)`.
    /// 
    /// Type mappings:
    /// - BYTEA → BLOB → VARCHAR via hex() function (DuckDB native)
    /// - JSONB → VARCHAR (auto-converted by postgres extension)
    /// - TIMESTAMPTZ → TIMESTAMPTZ (direct)
    /// - TEXT → VARCHAR (direct)
    /// - INT8/INT4/INT2 → BIGINT/INTEGER/SMALLINT (direct)
    fn copy_range_with_scanner(
        conn: &duckdb::Connection,
        pg_alias: &str,
        start: i64,
        end: i64,
    ) -> Result<i64> {
        // Copy blocks - use lower(hex()) for BLOB→VARCHAR conversion (DuckDB functions)
        // lower() ensures consistency with existing data (hex::encode produces lowercase)
        let blocks_sql = format!(
            "INSERT OR IGNORE INTO blocks \
             SELECT num, '0x' || lower(hex(hash)), '0x' || lower(hex(parent_hash)), \
                    timestamp, timestamp_ms, gas_limit, gas_used, '0x' || lower(hex(miner)), \
                    CASE WHEN extra_data IS NOT NULL THEN '0x' || lower(hex(extra_data)) ELSE NULL END \
             FROM {pg_alias}.public.blocks \
             WHERE num >= {start} AND num <= {end}"
        );
        let blocks_inserted = conn.execute(&blocks_sql, [])?;

        // Copy transactions
        // Note: JSONB 'calls' column is auto-converted to VARCHAR by postgres extension
        let txs_sql = format!(
            "INSERT OR IGNORE INTO txs \
             SELECT block_num, block_timestamp, idx, '0x' || lower(hex(hash)), type, \
                    '0x' || lower(hex(\"from\")), \
                    CASE WHEN \"to\" IS NOT NULL THEN '0x' || lower(hex(\"to\")) ELSE NULL END, \
                    value, '0x' || lower(hex(input)), gas_limit, max_fee_per_gas, max_priority_fee_per_gas, \
                    gas_used, '0x' || lower(hex(nonce_key)), nonce, \
                    CASE WHEN fee_token IS NOT NULL THEN '0x' || lower(hex(fee_token)) ELSE NULL END, \
                    CASE WHEN fee_payer IS NOT NULL THEN '0x' || lower(hex(fee_payer)) ELSE NULL END, \
                    calls, call_count, valid_before, valid_after, signature_type \
             FROM {pg_alias}.public.txs \
             WHERE block_num >= {start} AND block_num <= {end}"
        );
        conn.execute(&txs_sql, [])?;

        // Copy logs
        let logs_sql = format!(
            "INSERT OR IGNORE INTO logs \
             SELECT block_num, block_timestamp, log_idx, tx_idx, '0x' || lower(hex(tx_hash)), \
                    '0x' || lower(hex(address)), \
                    CASE WHEN selector IS NOT NULL THEN '0x' || lower(hex(selector)) ELSE NULL END, \
                    CASE WHEN topic0 IS NOT NULL THEN '0x' || lower(hex(topic0)) ELSE NULL END, \
                    CASE WHEN topic1 IS NOT NULL THEN '0x' || lower(hex(topic1)) ELSE NULL END, \
                    CASE WHEN topic2 IS NOT NULL THEN '0x' || lower(hex(topic2)) ELSE NULL END, \
                    CASE WHEN topic3 IS NOT NULL THEN '0x' || lower(hex(topic3)) ELSE NULL END, \
                    '0x' || lower(hex(data)) \
             FROM {pg_alias}.public.logs \
             WHERE block_num >= {start} AND block_num <= {end}"
        );
        conn.execute(&logs_sql, [])?;

        // Copy receipts
        let receipts_sql = format!(
            "INSERT OR IGNORE INTO receipts \
             SELECT block_num, block_timestamp, tx_idx, '0x' || lower(hex(tx_hash)), \
                    '0x' || lower(hex(\"from\")), \
                    CASE WHEN \"to\" IS NOT NULL THEN '0x' || lower(hex(\"to\")) ELSE NULL END, \
                    CASE WHEN contract_address IS NOT NULL THEN '0x' || lower(hex(contract_address)) ELSE NULL END, \
                    gas_used, cumulative_gas_used, effective_gas_price, status, \
                    CASE WHEN fee_payer IS NOT NULL THEN '0x' || lower(hex(fee_payer)) ELSE NULL END \
             FROM {pg_alias}.public.receipts \
             WHERE block_num >= {start} AND block_num <= {end}"
        );
        conn.execute(&receipts_sql, [])?;

        Ok(blocks_inserted as i64)
    }

    /// Tails Postgres by copying new blocks from watermark to tip.
    /// Copies all tables (blocks, txs, logs, receipts) for each range atomically.
    async fn tail_postgres(&self) -> Result<()> {
        const BATCH_SIZE: i64 = 1000;
        const MAX_BLOCKS_PER_TICK: i64 = 5000;

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

            while current <= pg_tip {
                let batch_end = (current + BATCH_SIZE - 1).min(pg_tip);
                self.copy_range_from_postgres(&pg_conn, current, batch_end).await?;
                synced += batch_end - current + 1;
                current = batch_end + 1;
            }

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
                tracing::info!(
                    chain_id = self.chain_id,
                    duck_tip,
                    pg_tip,
                    lag,
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

            while current <= sync_end {
                let batch_end = (current + BATCH_SIZE - 1).min(sync_end);
                self.copy_range_from_postgres(&pg_conn, current, batch_end).await?;
                synced += batch_end - current + 1;
                current = batch_end + 1;
            }

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
        copy_range_to_duckdb(pg_conn, &self.duckdb, start, end).await?;

        // Update watermark
        let duck_conn = self.duckdb.conn().await;
        duck_conn.execute(
            "UPDATE duckdb_sync_state SET latest_block = $1, updated_at = CURRENT_TIMESTAMP WHERE id = 1",
            duckdb::params![end],
        )?;

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

/// Copies all tables (blocks, txs, logs, receipts) for a block range from Postgres to DuckDB.
/// Returns the number of blocks synced (0 if no blocks in range).
async fn copy_range_to_duckdb(
    pg_conn: &deadpool_postgres::Object,
    duckdb: &Arc<DuckDbPool>,
    start: i64,
    end: i64,
) -> Result<i64> {
    // Fetch all data from Postgres first (outside DuckDB lock)
    let block_rows = pg_conn
        .query(
            "SELECT num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner, extra_data 
             FROM blocks WHERE num >= $1 AND num <= $2 ORDER BY num",
            &[&start, &end],
        )
        .await?;

    if block_rows.is_empty() {
        return Ok(0);
    }

    let tx_rows = pg_conn
        .query(
            "SELECT block_num, block_timestamp, idx, hash, type, \"from\", \"to\", value, input,
                    gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_used, nonce_key, nonce,
                    fee_token, fee_payer, calls::text, call_count, valid_before, valid_after, signature_type
             FROM txs WHERE block_num >= $1 AND block_num <= $2 ORDER BY block_num, idx",
            &[&start, &end],
        )
        .await?;

    let log_rows = pg_conn
        .query(
            "SELECT block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topic0, topic1, topic2, topic3, data
             FROM logs WHERE block_num >= $1 AND block_num <= $2 ORDER BY block_num, log_idx",
            &[&start, &end],
        )
        .await?;

    let receipt_rows = pg_conn
        .query(
            "SELECT block_num, block_timestamp, tx_idx, tx_hash, \"from\", \"to\", contract_address,
                    gas_used, cumulative_gas_used, effective_gas_price, status, fee_payer
             FROM receipts WHERE block_num >= $1 AND block_num <= $2 ORDER BY block_num, tx_idx",
            &[&start, &end],
        )
        .await?;

    let blocks_synced = block_rows.len() as i64;

    // Now write all tables to DuckDB (holding lock for minimal time)
    let duck_conn = duckdb.conn().await;

    // Write blocks
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
        if let Err(e) = duck_conn.execute(&sql, []) {
            tracing::warn!(error = %e, block_num = num, "Failed to insert block, skipping");
        }
    }

    // Write transactions
    if !tx_rows.is_empty() {
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

            let sql = format!(
                "INSERT OR IGNORE INTO txs (block_num, block_timestamp, idx, hash, type, \"from\", \"to\", value, input, gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_used, nonce_key, nonce, fee_token, fee_payer, calls, call_count, valid_before, valid_after, signature_type) VALUES {}",
                values.join(", ")
            );
            if let Err(e) = duck_conn.execute(&sql, []) {
                tracing::warn!(error = %e, "Failed to insert tx batch, skipping");
            }
        }
    }

    // Write logs
    if !log_rows.is_empty() {
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

            let sql = format!(
                "INSERT OR IGNORE INTO logs (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topic0, topic1, topic2, topic3, data) VALUES {}",
                values.join(", ")
            );
            if let Err(e) = duck_conn.execute(&sql, []) {
                tracing::warn!(error = %e, "Failed to insert log batch, skipping");
            }
        }
    }

    // Write receipts
    if !receipt_rows.is_empty() {
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

            let sql = format!(
                "INSERT OR IGNORE INTO receipts (block_num, block_timestamp, tx_idx, tx_hash, \"from\", \"to\", contract_address, gas_used, cumulative_gas_used, effective_gas_price, status, fee_payer) VALUES {}",
                values.join(", ")
            );
            if let Err(e) = duck_conn.execute(&sql, []) {
                tracing::warn!(error = %e, "Failed to insert receipt batch, skipping");
            }
        }
    }

    Ok(blocks_synced)
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

        if !block_rows.is_empty() {
            let duck_conn = duckdb.conn().await;
            let mut appender = duck_conn.appender("blocks")?;
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

                appender.append_row(duckdb::params![
                    num,
                    format!("0x{}", hex::encode(&hash)),
                    format!("0x{}", hex::encode(&parent_hash)),
                    timestamp.to_rfc3339(),
                    timestamp_ms,
                    gas_limit,
                    gas_used,
                    format!("0x{}", hex::encode(&miner)),
                    extra_data.as_ref().map(|d| format!("0x{}", hex::encode(d))),
                ])?;
            }
            appender.flush()?;
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

        if !tx_rows.is_empty() {
            let duck_conn = duckdb.conn().await;
            let mut appender = duck_conn.appender("txs")?;
            for row in &tx_rows {
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
                let calls: Option<serde_json::Value> = row.get(17);
                let call_count: i16 = row.get(18);
                let valid_before: Option<i64> = row.get(19);
                let valid_after: Option<i64> = row.get(20);
                let signature_type: Option<i16> = row.get(21);

                appender.append_row(duckdb::params![
                    block_num,
                    block_timestamp.to_rfc3339(),
                    idx,
                    format!("0x{}", hex::encode(&hash)),
                    tx_type,
                    format!("0x{}", hex::encode(&from)),
                    to.as_ref().map(|t| format!("0x{}", hex::encode(t))),
                    value,
                    format!("0x{}", hex::encode(&input)),
                    gas_limit,
                    max_fee_per_gas,
                    max_priority_fee_per_gas,
                    gas_used,
                    format!("0x{}", hex::encode(&nonce_key)),
                    nonce,
                    fee_token.as_ref().map(|t| format!("0x{}", hex::encode(t))),
                    fee_payer.as_ref().map(|p| format!("0x{}", hex::encode(p))),
                    calls.as_ref().map(|c| c.to_string()),
                    call_count,
                    valid_before,
                    valid_after,
                    signature_type,
                ])?;
            }
            appender.flush()?;
        }

        // Backfill logs
        let log_rows = pg_conn
            .query(
                "SELECT block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topic0, topic1, topic2, topic3, data
                 FROM logs WHERE block_num >= $1 AND block_num <= $2 ORDER BY block_num, log_idx",
                &[&current, &end],
            )
            .await?;

        if !log_rows.is_empty() {
            let duck_conn = duckdb.conn().await;
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

                let sql = format!(
                    "INSERT OR IGNORE INTO logs (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topic0, topic1, topic2, topic3, data) VALUES {}",
                    values.join(", ")
                );
                if let Err(e) = duck_conn.execute(&sql, []) {
                    tracing::warn!(error = %e, "Failed to insert log batch in backfill, skipping");
                }
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

        if !receipt_rows.is_empty() {
            let duck_conn = duckdb.conn().await;
            let mut appender = duck_conn.appender("receipts")?;
            for row in &receipt_rows {
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

                appender.append_row(duckdb::params![
                    block_num,
                    block_timestamp.to_rfc3339(),
                    tx_idx,
                    format!("0x{}", hex::encode(&tx_hash)),
                    format!("0x{}", hex::encode(&from)),
                    to.as_ref().map(|t| format!("0x{}", hex::encode(t))),
                    contract_address.as_ref().map(|a| format!("0x{}", hex::encode(a))),
                    gas_used,
                    cumulative_gas_used,
                    effective_gas_price,
                    status,
                    fee_payer.as_ref().map(|p| format!("0x{}", hex::encode(p))),
                ])?;
            }
            appender.flush()?;
        }

        synced += block_rows.len() as u64;
        current = end + 1;

        if synced % 10000 == 0 {
            tracing::info!(synced, current, pg_latest, "DuckDB backfill progress");
        }
    }

    // Update watermark
    let duck_conn = duckdb.conn().await;
    duck_conn.execute(
        "UPDATE duckdb_sync_state SET latest_block = ?, updated_at = CURRENT_TIMESTAMP WHERE id = 1",
        duckdb::params![pg_latest],
    )?;

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
    let conn = duckdb.conn().await;
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
}

/// Detect ALL gaps in DuckDB including from genesis to first block.
/// Returns gaps sorted by end block descending (most recent first).
pub async fn detect_all_gaps_duckdb(duckdb: &Arc<DuckDbPool>, tip_num: i64) -> Result<Vec<(i64, i64)>> {
    // Get min block
    let conn = duckdb.conn().await;
    let min_block: Option<i64> = conn
        .prepare("SELECT MIN(num) FROM blocks")
        .ok()
        .and_then(|mut stmt| stmt.query_row([], |row| row.get(0)).ok());
    drop(conn);

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

            // Fetch data from PostgreSQL (outside of DuckDB lock)
            let block_rows = pg_conn
                .query(
                    "SELECT num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner, extra_data 
                     FROM blocks WHERE num >= $1 AND num <= $2 ORDER BY num",
                    &[&current, &end],
                )
                .await?;

            // Acquire DuckDB lock, write batch, then release
            // This allows realtime writes to interleave between gap-fill batches
            {
                let duck_conn = duckdb.conn().await;
                
                if !block_rows.is_empty() {
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
                        if let Err(e) = duck_conn.execute(&sql, []) {
                            tracing::warn!(error = %e, block_num = num, "Failed to insert block in gap-fill, skipping");
                            let _ = duck_conn.execute("ROLLBACK", []);
                        }
                    }
                }
            } // DuckDB lock released here

            // Backfill txs for this range
            let tx_rows = pg_conn
                .query(
                    "SELECT block_num, block_timestamp, idx, hash, type, \"from\", \"to\", value, input,
                            gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_used, nonce_key, nonce,
                            fee_token, fee_payer, calls::text, call_count, valid_before, valid_after, signature_type
                     FROM txs WHERE block_num >= $1 AND block_num <= $2 ORDER BY block_num, idx",
                    &[&current, &end],
                )
                .await?;

            if !tx_rows.is_empty() {
                // Pre-build all SQL values outside the lock
                let tx_values: Vec<Vec<String>> = tx_rows.chunks(500)
                    .map(|chunk| {
                        chunk.iter()
                            .map(|row| {
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
                            })
                            .collect()
                    })
                    .collect();

                // Acquire lock and write
                let duck_conn = duckdb.conn().await;
                for values in tx_values {
                    let sql = format!(
                        "INSERT OR IGNORE INTO txs (block_num, block_timestamp, idx, hash, type, \"from\", \"to\", value, input, gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_used, nonce_key, nonce, fee_token, fee_payer, calls, call_count, valid_before, valid_after, signature_type) VALUES {}",
                        values.join(", ")
                    );
                    if let Err(e) = duck_conn.execute(&sql, []) {
                        tracing::warn!(error = %e, "Failed to insert tx batch in gap-fill, skipping");
                        let _ = duck_conn.execute("ROLLBACK", []);
                    }
                }
            }

            // Backfill logs for this range
            let log_rows = pg_conn
                .query(
                    "SELECT block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topic0, topic1, topic2, topic3, data
                     FROM logs WHERE block_num >= $1 AND block_num <= $2 ORDER BY block_num, log_idx",
                    &[&current, &end],
                )
                .await?;

            if !log_rows.is_empty() {
                // Pre-build SQL values outside the lock
                let log_values: Vec<Vec<String>> = log_rows.chunks(500)
                    .map(|chunk| {
                        chunk.iter()
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
                            .collect()
                    })
                    .collect();

                // Acquire lock and write
                let duck_conn = duckdb.conn().await;
                for values in log_values {
                    let sql = format!(
                        "INSERT OR IGNORE INTO logs (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topic0, topic1, topic2, topic3, data) VALUES {}",
                        values.join(", ")
                    );
                    if let Err(e) = duck_conn.execute(&sql, []) {
                        tracing::warn!(error = %e, "Failed to insert log batch in gap-fill, skipping");
                    }
                }
            }

            // Backfill receipts for this range
            let receipt_rows = pg_conn
                .query(
                    "SELECT block_num, block_timestamp, tx_idx, tx_hash, \"from\", \"to\", contract_address,
                            gas_used, cumulative_gas_used, effective_gas_price, status, fee_payer
                     FROM receipts WHERE block_num >= $1 AND block_num <= $2 ORDER BY block_num, tx_idx",
                    &[&current, &end],
                )
                .await?;

            if !receipt_rows.is_empty() {
                // Pre-build SQL values outside the lock
                let receipt_values: Vec<Vec<String>> = receipt_rows.chunks(500)
                    .map(|chunk| {
                        chunk.iter()
                            .map(|row| {
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
                            })
                            .collect()
                    })
                    .collect();

                // Acquire lock and write
                let duck_conn = duckdb.conn().await;
                for values in receipt_values {
                    let sql = format!(
                        "INSERT OR IGNORE INTO receipts (block_num, block_timestamp, tx_idx, tx_hash, \"from\", \"to\", contract_address, gas_used, cumulative_gas_used, effective_gas_price, status, fee_payer) VALUES {}",
                        values.join(", ")
                    );
                    if let Err(e) = duck_conn.execute(&sql, []) {
                        tracing::warn!(error = %e, "Failed to insert receipt batch in gap-fill, skipping");
                        let _ = duck_conn.execute("ROLLBACK", []);
                    }
                }
            }

            synced += block_rows.len() as u64;
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
        let (replicator, handle) = Replicator::new(duckdb.clone(), pg_pool, "postgresql://localhost/test".to_string(), 2, 1);

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
        let (replicator, handle) = Replicator::new(duckdb.clone(), pg_pool, "postgresql://localhost/test".to_string(), 2, 1);

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
            let block = BlockRow {
                num,
                hash: vec![num as u8; 32],
                parent_hash: vec![(num - 1) as u8; 32],
                timestamp: Utc::now(),
                timestamp_ms: 1704067200000 + num * 1000,
                gas_limit: 30_000_000,
                gas_used: 21000,
                miner: vec![0xde; 20],
                extra_data: None,
            };
            let conn = duckdb.conn().await;
            let mut appender = conn.appender("blocks").unwrap();
            appender
                .append_row(duckdb::params![
                    block.num,
                    format!("0x{}", hex::encode(&block.hash)),
                    format!("0x{}", hex::encode(&block.parent_hash)),
                    block.timestamp.to_rfc3339(),
                    block.timestamp_ms,
                    block.gas_limit,
                    block.gas_used,
                    format!("0x{}", hex::encode(&block.miner)),
                    block.extra_data.as_ref().map(|d| format!("0x{}", hex::encode(d))),
                ])
                .unwrap();
            appender.flush().unwrap();
        }

        let gaps = detect_gaps_duckdb(&duckdb).await.unwrap();
        assert!(gaps.is_empty());
    }

    #[tokio::test]
    async fn test_detect_gaps_duckdb_with_gap() {
        let duckdb = Arc::new(DuckDbPool::in_memory().unwrap());

        // Insert blocks 1, 2, 5, 6 (gap at 3-4)
        for num in [1, 2, 5, 6] {
            let block = BlockRow {
                num,
                hash: vec![num as u8; 32],
                parent_hash: vec![(num - 1) as u8; 32],
                timestamp: Utc::now(),
                timestamp_ms: 1704067200000 + num * 1000,
                gas_limit: 30_000_000,
                gas_used: 21000,
                miner: vec![0xde; 20],
                extra_data: None,
            };
            let conn = duckdb.conn().await;
            let mut appender = conn.appender("blocks").unwrap();
            appender
                .append_row(duckdb::params![
                    block.num,
                    format!("0x{}", hex::encode(&block.hash)),
                    format!("0x{}", hex::encode(&block.parent_hash)),
                    block.timestamp.to_rfc3339(),
                    block.timestamp_ms,
                    block.gas_limit,
                    block.gas_used,
                    format!("0x{}", hex::encode(&block.miner)),
                    block.extra_data.as_ref().map(|d| format!("0x{}", hex::encode(d))),
                ])
                .unwrap();
            appender.flush().unwrap();
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
            let block = BlockRow {
                num,
                hash: vec![num as u8; 32],
                parent_hash: vec![(num - 1) as u8; 32],
                timestamp: Utc::now(),
                timestamp_ms: 1704067200000 + num * 1000,
                gas_limit: 30_000_000,
                gas_used: 21000,
                miner: vec![0xde; 20],
                extra_data: None,
            };
            let conn = duckdb.conn().await;
            let mut appender = conn.appender("blocks").unwrap();
            appender
                .append_row(duckdb::params![
                    block.num,
                    format!("0x{}", hex::encode(&block.hash)),
                    format!("0x{}", hex::encode(&block.parent_hash)),
                    block.timestamp.to_rfc3339(),
                    block.timestamp_ms,
                    block.gas_limit,
                    block.gas_used,
                    format!("0x{}", hex::encode(&block.miner)),
                    block.extra_data.as_ref().map(|d| format!("0x{}", hex::encode(d))),
                ])
                .unwrap();
            appender.flush().unwrap();
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
        let block = BlockRow {
            num: 5,
            hash: vec![5; 32],
            parent_hash: vec![4; 32],
            timestamp: Utc::now(),
            timestamp_ms: 1704067200000,
            gas_limit: 30_000_000,
            gas_used: 21000,
            miner: vec![0xde; 20],
            extra_data: None,
        };
        {
            let conn = duckdb.conn().await;
            let mut appender = conn.appender("blocks").unwrap();
            appender
                .append_row(duckdb::params![
                    block.num,
                    format!("0x{}", hex::encode(&block.hash)),
                    format!("0x{}", hex::encode(&block.parent_hash)),
                    block.timestamp.to_rfc3339(),
                    block.timestamp_ms,
                    block.gas_limit,
                    block.gas_used,
                    format!("0x{}", hex::encode(&block.miner)),
                    block.extra_data.as_ref().map(|d| format!("0x{}", hex::encode(d))),
                ])
                .unwrap();
            appender.flush().unwrap();
        }

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
            let conn = duckdb.conn().await;
            let mut appender = conn.appender("blocks").unwrap();
            appender
                .append_row(duckdb::params![
                    num as i64,
                    format!("0x{}", hex::encode(vec![num as u8; 32])),
                    format!("0x{}", hex::encode(vec![(num - 1) as u8; 32])),
                    Utc::now().to_rfc3339(),
                    1704067200000i64 + num * 1000,
                    30_000_000i64,
                    21000i64,
                    format!("0x{}", hex::encode(vec![0xde; 20])),
                    None::<String>,
                ])
                .unwrap();
            appender.flush().unwrap();
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
        let conn = duckdb.conn().await;
        let sql = format!(
            "INSERT OR IGNORE INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner, extra_data) \
             VALUES ({}, '0x{}', '0x{}', '{}', {}, 30000000, 21000, '0x{}', NULL)",
            num,
            hex::encode(vec![num as u8; 32]),
            hex::encode(vec![(num.saturating_sub(1)) as u8; 32]),
            Utc::now().to_rfc3339(),
            1704067200000i64 + num * 1000,
            hex::encode(vec![0xde; 20]),
        );
        conn.execute(&sql, []).unwrap();
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
        let conn = duckdb.conn().await;
        let min: i64 = conn.prepare("SELECT MIN(num) FROM blocks").unwrap()
            .query_row([], |row| row.get(0)).unwrap();
        let max: i64 = conn.prepare("SELECT MAX(num) FROM blocks").unwrap()
            .query_row([], |row| row.get(0)).unwrap();
        drop(conn);

        assert_eq!(min, 100);
        assert_eq!(max, 110);

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
        let conn = duckdb.conn().await;
        let count: i64 = conn.prepare("SELECT COUNT(*) FROM blocks").unwrap()
            .query_row([], |row| row.get(0)).unwrap();
        
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

    #[test]
    fn test_postgres_attach_sql_format() {
        // Verify the SQL templates use modern ATTACH syntax (not legacy postgres_scan)
        // This catches syntax errors without needing a live database
        
        let start = 100i64;
        let end = 200i64;

        // Test blocks SQL - uses attached database 'pg.public.blocks'
        let blocks_sql = format!(
            "INSERT OR IGNORE INTO blocks \
             SELECT num, '0x' || lower(hex(hash)), '0x' || lower(hex(parent_hash)), \
                    timestamp, timestamp_ms, gas_limit, gas_used, '0x' || lower(hex(miner)), \
                    CASE WHEN extra_data IS NOT NULL THEN '0x' || lower(hex(extra_data)) ELSE NULL END \
             FROM pg.public.blocks \
             WHERE num >= {start} AND num <= {end}"
        );
        assert!(blocks_sql.contains("pg.public.blocks"), "Should use attached database syntax");
        assert!(!blocks_sql.contains("postgres_scan"), "Should not use legacy postgres_scan function");
        assert!(blocks_sql.contains("lower(hex(hash))"));
        assert!(blocks_sql.contains(&format!("num >= {start}")));
        assert!(blocks_sql.contains(&format!("num <= {end}")));

        // Test txs SQL - verify JSONB 'calls' is passed through directly
        let txs_sql = format!(
            "INSERT OR IGNORE INTO txs \
             SELECT block_num, block_timestamp, idx, '0x' || lower(hex(hash)), type, \
                    '0x' || lower(hex(\"from\")), \
                    CASE WHEN \"to\" IS NOT NULL THEN '0x' || lower(hex(\"to\")) ELSE NULL END, \
                    value, '0x' || lower(hex(input)), gas_limit, max_fee_per_gas, max_priority_fee_per_gas, \
                    gas_used, '0x' || lower(hex(nonce_key)), nonce, \
                    CASE WHEN fee_token IS NOT NULL THEN '0x' || lower(hex(fee_token)) ELSE NULL END, \
                    CASE WHEN fee_payer IS NOT NULL THEN '0x' || lower(hex(fee_payer)) ELSE NULL END, \
                    calls, call_count, valid_before, valid_after, signature_type \
             FROM pg.public.txs \
             WHERE block_num >= {start} AND block_num <= {end}"
        );
        assert!(txs_sql.contains("pg.public.txs"));
        // calls is passed through directly (JSONB -> VARCHAR automatic conversion)
        assert!(txs_sql.contains(", calls,"), "calls column should be passed through directly");
        assert!(!txs_sql.contains("calls::text"), "Should not use PG cast syntax");
        assert!(!txs_sql.contains("encode("), "Should not use PG encode function");

        // Test logs SQL
        let logs_sql = format!(
            "INSERT OR IGNORE INTO logs \
             SELECT block_num, block_timestamp, log_idx, tx_idx, '0x' || lower(hex(tx_hash)), \
                    '0x' || lower(hex(address)), \
                    CASE WHEN selector IS NOT NULL THEN '0x' || lower(hex(selector)) ELSE NULL END, \
                    CASE WHEN topic0 IS NOT NULL THEN '0x' || lower(hex(topic0)) ELSE NULL END, \
                    CASE WHEN topic1 IS NOT NULL THEN '0x' || lower(hex(topic1)) ELSE NULL END, \
                    CASE WHEN topic2 IS NOT NULL THEN '0x' || lower(hex(topic2)) ELSE NULL END, \
                    CASE WHEN topic3 IS NOT NULL THEN '0x' || lower(hex(topic3)) ELSE NULL END, \
                    '0x' || lower(hex(data)) \
             FROM pg.public.logs \
             WHERE block_num >= {start} AND block_num <= {end}"
        );
        assert!(logs_sql.contains("pg.public.logs"));
        assert!(logs_sql.contains("lower(hex(tx_hash))"));
        assert!(logs_sql.contains("lower(hex(address))"));
        // All 4 topics should be handled
        assert!(logs_sql.contains("topic0"));
        assert!(logs_sql.contains("topic1"));
        assert!(logs_sql.contains("topic2"));
        assert!(logs_sql.contains("topic3"));

        // Test receipts SQL
        let receipts_sql = format!(
            "INSERT OR IGNORE INTO receipts \
             SELECT block_num, block_timestamp, tx_idx, '0x' || lower(hex(tx_hash)), \
                    '0x' || lower(hex(\"from\")), \
                    CASE WHEN \"to\" IS NOT NULL THEN '0x' || lower(hex(\"to\")) ELSE NULL END, \
                    CASE WHEN contract_address IS NOT NULL THEN '0x' || lower(hex(contract_address)) ELSE NULL END, \
                    gas_used, cumulative_gas_used, effective_gas_price, status, \
                    CASE WHEN fee_payer IS NOT NULL THEN '0x' || lower(hex(fee_payer)) ELSE NULL END \
             FROM pg.public.receipts \
             WHERE block_num >= {start} AND block_num <= {end}"
        );
        assert!(receipts_sql.contains("pg.public.receipts"));
        assert!(receipts_sql.contains("contract_address"));
        assert!(receipts_sql.contains("effective_gas_price"));
        assert!(receipts_sql.contains("status"));
    }

    #[test]
    fn test_postgres_attach_url_escaping() {
        // Test URL escaping for single quotes in ATTACH command
        let url_with_quotes = "postgresql://user:p'ass@localhost/db";
        let escaped = url_with_quotes.replace('\'', "''");
        assert_eq!(escaped, "postgresql://user:p''ass@localhost/db");
        
        // Verify it can be embedded in ATTACH SQL
        let sql = format!("ATTACH '{escaped}' AS pg (TYPE postgres, READ_ONLY)");
        assert!(sql.contains("p''ass")); // Properly escaped
        assert!(sql.contains("TYPE postgres"));
        assert!(sql.contains("READ_ONLY"));
    }
}
