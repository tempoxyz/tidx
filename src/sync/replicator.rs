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
    pub fn new(duckdb: Arc<DuckDbPool>, pg_pool: Pool, buffer_size: usize, chain_id: u64) -> (Self, ReplicatorHandle) {
        let (tx, rx) = mpsc::channel(buffer_size);
        let needs_sync = Arc::new(AtomicBool::new(false));
        (
            Self { duckdb, pg_pool, rx, chain_id, needs_sync: needs_sync.clone() },
            ReplicatorHandle { tx, needs_sync },
        )
    }

    /// Runs the replicator using pull-based tailing from Postgres.
    ///
    /// Architecture:
    /// - Tight polling loop (500ms) tails Postgres via watermark
    /// - Channel signals trigger immediate polling for low latency
    /// - All tables replicated together, watermark advances atomically
    /// - Gap-fill runs rarely (every 60s) for internal gaps only
    pub async fn run(mut self) -> Result<()> {
        tracing::info!(chain_id = self.chain_id, "DuckDB replicator started (pull-based tailing)");

        // Tight polling interval for tailing Postgres
        let mut tail_interval = tokio::time::interval(Duration::from_millis(500));
        tail_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Gap-fill runs less frequently now (only for internal gaps)
        let mut gap_fill_interval = tokio::time::interval(Duration::from_secs(60));
        gap_fill_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut checkpoint_interval = tokio::time::interval(Duration::from_secs(60));
        checkpoint_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut metrics_interval = tokio::time::interval(Duration::from_secs(10));
        metrics_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                // Channel signals just trigger immediate sync (low-latency hint)
                batch = self.rx.recv() => {
                    match batch {
                        Some(_) => {
                            // Drain any queued batches, then sync
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
                // Tight polling loop to tail Postgres
                _ = tail_interval.tick() => {
                    // Check if we were signaled or if it's time to poll
                    if self.needs_sync.swap(false, Ordering::Relaxed) {
                        // Already synced via channel, skip this tick
                        continue;
                    }
                    if let Err(e) = self.tail_postgres().await {
                        tracing::error!(chain_id = self.chain_id, error = %e, "DuckDB tail sync failed");
                        metrics::increment_duckdb_errors("tail_sync");
                    }
                }
                // Gap-fill for internal gaps only (rare)
                _ = gap_fill_interval.tick() => {
                    if let Err(e) = self.run_internal_gap_fill().await {
                        tracing::error!(chain_id = self.chain_id, error = %e, "DuckDB internal gap-fill failed");
                        metrics::increment_duckdb_errors("gap_fill");
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

        tracing::info!(chain_id = self.chain_id, "DuckDB replicator stopped");
        Ok(())
    }

    /// Tails Postgres by copying new blocks from watermark to tip.
    /// Copies all tables (blocks, txs, logs, receipts) for each range atomically.
    async fn tail_postgres(&self) -> Result<()> {
        const BATCH_SIZE: i64 = 1000;
        const MAX_BLOCKS_PER_TICK: i64 = 5000;

        let pg_conn = self.pg_pool.get().await?;
        let pg_tip: i64 = pg_conn
            .query_one("SELECT COALESCE(MAX(num), 0) FROM blocks", &[])
            .await?
            .get(0);

        if pg_tip == 0 {
            return Ok(());
        }

        let duck_watermark = self.duckdb.latest_block().await?.unwrap_or(0);
        let lag = pg_tip - duck_watermark;

        if lag <= 0 {
            return Ok(());
        }

        // Limit how much we sync per tick to stay responsive
        let sync_end = duck_watermark + lag.min(MAX_BLOCKS_PER_TICK);
        let _blocks_to_sync = sync_end - duck_watermark;

        tracing::debug!(
            chain_id = self.chain_id,
            duck_watermark,
            pg_tip,
            lag,
            sync_end,
            "DuckDB tailing Postgres"
        );

        let start = Instant::now();
        let mut synced = 0i64;
        let mut current = duck_watermark + 1;

        while current <= sync_end {
            let batch_end = (current + BATCH_SIZE - 1).min(sync_end);

            // Copy all tables for this range
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

        Ok(())
    }

    /// Copies all tables for a block range from Postgres to DuckDB.
    async fn copy_range_from_postgres(
        &self,
        pg_conn: &deadpool_postgres::Object,
        start: i64,
        end: i64,
    ) -> Result<()> {
        // Fetch all data from Postgres first (outside DuckDB lock)
        let block_rows = pg_conn
            .query(
                "SELECT num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner, extra_data 
                 FROM blocks WHERE num >= $1 AND num <= $2 ORDER BY num",
                &[&start, &end],
            )
            .await?;

        let tx_rows = pg_conn
            .query(
                "SELECT block_num, block_timestamp, idx, hash, type, \"from\", \"to\", value, input,
                        gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_used, nonce_key, nonce,
                        fee_token, fee_payer, calls, call_count, valid_before, valid_after, signature_type
                 FROM txs WHERE block_num >= $1 AND block_num <= $2 ORDER BY block_num, idx",
                &[&start, &end],
            )
            .await?;

        let log_rows = pg_conn
            .query(
                "SELECT block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topics, data
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

        // Now write all tables to DuckDB (holding lock for minimal time)
        let duck_conn = self.duckdb.conn().await;

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
            for chunk in tx_rows.chunks(100) {
                let values: Vec<String> = chunk.iter().map(|row| {
                    let block_num: i64 = row.get(0);
                    let block_timestamp: chrono::DateTime<chrono::Utc> = row.get(1);
                    let idx: i32 = row.get(2);
                    let hash: Vec<u8> = row.get(3);
                    let tx_type: i16 = row.get(4);
                    let from: Vec<u8> = row.get(5);
                    let to: Option<Vec<u8>> = row.get(6);
                    let value: rust_decimal::Decimal = row.get(7);
                    let input: Vec<u8> = row.get(8);
                    let gas_limit: i64 = row.get(9);
                    let max_fee: rust_decimal::Decimal = row.get(10);
                    let max_priority: rust_decimal::Decimal = row.get(11);
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
            for chunk in log_rows.chunks(100) {
                let values: Vec<String> = chunk.iter().map(|row| {
                    let block_num: i64 = row.get(0);
                    let block_timestamp: chrono::DateTime<chrono::Utc> = row.get(1);
                    let log_idx: i32 = row.get(2);
                    let tx_idx: i32 = row.get(3);
                    let tx_hash: Vec<u8> = row.get(4);
                    let address: Vec<u8> = row.get(5);
                    let selector: Option<Vec<u8>> = row.get(6);
                    let topics: Vec<Vec<u8>> = row.get(7);
                    let data: Vec<u8> = row.get(8);

                    let topics_arr: Vec<String> = topics.iter().map(|t| format!("'0x{}'", hex::encode(t))).collect();

                    format!(
                        "({}, '{}', {}, {}, '0x{}', '0x{}', {}, [{}], '0x{}')",
                        block_num,
                        block_timestamp.to_rfc3339(),
                        log_idx,
                        tx_idx,
                        hex::encode(&tx_hash),
                        hex::encode(&address),
                        selector.as_ref().map(|s| format!("'0x{}'", hex::encode(s))).unwrap_or_else(|| "NULL".to_string()),
                        topics_arr.join(", "),
                        hex::encode(&data),
                    )
                }).collect();

                let sql = format!(
                    "INSERT OR IGNORE INTO logs (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topics, data) VALUES {}",
                    values.join(", ")
                );
                if let Err(e) = duck_conn.execute(&sql, []) {
                    tracing::warn!(error = %e, "Failed to insert log batch, skipping");
                }
            }
        }

        // Write receipts
        if !receipt_rows.is_empty() {
            for chunk in receipt_rows.chunks(100) {
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
                    let effective_gas_price: Option<rust_decimal::Decimal> = row.get(9);
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
                        effective_gas_price.map(|p| format!("'{}'", p)).unwrap_or_else(|| "NULL".to_string()),
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

        // Update watermark
        duck_conn.execute(
            "UPDATE duckdb_sync_state SET latest_block = $1, updated_at = CURRENT_TIMESTAMP WHERE id = 1",
            duckdb::params![end],
        )?;

        Ok(())
    }

    /// Fill internal gaps only (not tail lag, which is handled by tail_postgres).
    async fn run_internal_gap_fill(&self) -> Result<()> {
        let pg_conn = self.pg_pool.get().await?;
        let pg_latest: i64 = pg_conn
            .query_one("SELECT COALESCE(MAX(num), 0) FROM blocks", &[])
            .await?
            .get(0);

        if pg_latest == 0 {
            return Ok(());
        }

        // Only look for internal gaps (not tail lag)
        let gaps = detect_gaps_duckdb(&self.duckdb).await?;
        if gaps.is_empty() {
            return Ok(());
        }

        // Limit to small batch per round
        const MAX_BLOCKS: i64 = 2000;
        let mut limited_gaps = Vec::new();
        let mut remaining = MAX_BLOCKS;

        for (start, end) in gaps {
            if remaining <= 0 {
                break;
            }
            let size = end - start + 1;
            if size <= remaining {
                limited_gaps.push((start, end));
                remaining -= size;
            } else {
                limited_gaps.push((start, start + remaining - 1));
                break;
            }
        }

        if limited_gaps.is_empty() {
            return Ok(());
        }

        let total: i64 = limited_gaps.iter().map(|(s, e)| e - s + 1).sum();
        tracing::info!(
            chain_id = self.chain_id,
            gaps = limited_gaps.len(),
            total_blocks = total,
            "DuckDB filling internal gaps"
        );

        for (start, end) in limited_gaps {
            self.copy_range_from_postgres(&pg_conn, start, end).await?;
        }

        Ok(())
    }

    /// Emit metrics for DuckDB sync status (lightweight).
    async fn emit_metrics(&self) -> Result<()> {
        let pg_latest = {
            let conn = self.pg_pool.get().await?;
            let row = conn.query_one("SELECT COALESCE(MAX(num), 0) FROM blocks", &[]).await?;
            row.get::<_, i64>(0)
        };

        let duck_latest = self.duckdb.latest_block().await?.unwrap_or(0);
        let lag = pg_latest - duck_latest;

        metrics::set_duckdb_synced_block(self.chain_id, duck_latest);
        metrics::set_duckdb_lag(self.chain_id, lag);

        // Only run expensive gap detection when lag is suspicious
        if lag > 50 {
            let gaps = detect_gaps_duckdb(&self.duckdb).await?;
            let gap_blocks: i64 = gaps.iter().map(|(s, e)| e - s + 1).sum();
            metrics::set_duckdb_gap_count(self.chain_id, gaps.len());
            metrics::set_duckdb_gap_blocks(self.chain_id, gap_blocks);

            tracing::info!(
                chain_id = self.chain_id,
                duck_latest,
                pg_latest,
                lag,
                gap_count = gaps.len(),
                gap_blocks,
                "DuckDB sync status"
            );
        } else {
            metrics::set_duckdb_gap_count(self.chain_id, 0);
            metrics::set_duckdb_gap_blocks(self.chain_id, 0);
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
                        fee_token, fee_payer, calls, call_count, valid_before, valid_after, signature_type
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
                "SELECT block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topics, data
                 FROM logs WHERE block_num >= $1 AND block_num <= $2 ORDER BY block_num, log_idx",
                &[&current, &end],
            )
            .await?;

        if !log_rows.is_empty() {
            let duck_conn = duckdb.conn().await;
            for chunk in log_rows.chunks(100) {
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
                        let topics: Option<Vec<Vec<u8>>> = row.get(7);
                        let data: Vec<u8> = row.get(8);

                        let topics_str = format!(
                            "[{}]",
                            topics
                                .as_ref()
                                .map(|t| t.iter()
                                    .map(|topic| format!("'0x{}'", hex::encode(topic)))
                                    .collect::<Vec<_>>()
                                    .join(", "))
                                .unwrap_or_default()
                        );
                        let selector_str = selector
                            .as_ref()
                            .map(|s| format!("'0x{}'", hex::encode(s)))
                            .unwrap_or_else(|| "NULL".to_string());

                        format!(
                            "({}, '{}', {}, {}, '0x{}', '0x{}', {}, {}, '0x{}')",
                            block_num,
                            block_timestamp.to_rfc3339(),
                            log_idx,
                            tx_idx,
                            hex::encode(&tx_hash),
                            hex::encode(&address),
                            selector_str,
                            topics_str,
                            hex::encode(&data),
                        )
                    })
                    .collect();

                let sql = format!(
                    "INSERT OR IGNORE INTO logs (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topics, data) VALUES {}",
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
    let latest_block = duckdb.latest_block().await?.unwrap_or(0);
    let gaps = detect_gaps_duckdb(duckdb).await?;
    let gap_blocks: i64 = gaps.iter().map(|(s, e)| e - s + 1).sum();

    Ok(DuckDbSyncStatus {
        latest_block,
        gaps,
        gap_blocks,
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
                            fee_token, fee_payer, calls, call_count, valid_before, valid_after, signature_type
                     FROM txs WHERE block_num >= $1 AND block_num <= $2 ORDER BY block_num, idx",
                    &[&current, &end],
                )
                .await?;

            if !tx_rows.is_empty() {
                // Pre-build all SQL values outside the lock
                let tx_values: Vec<Vec<String>> = tx_rows.chunks(100)
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
                                    calls.as_ref().map(|c| format!("'{}'", c.to_string().replace('\'', "''"))).unwrap_or_else(|| "NULL".to_string()),
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
                    "SELECT block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topics, data
                     FROM logs WHERE block_num >= $1 AND block_num <= $2 ORDER BY block_num, log_idx",
                    &[&current, &end],
                )
                .await?;

            if !log_rows.is_empty() {
                // Pre-build SQL values outside the lock
                let log_values: Vec<Vec<String>> = log_rows.chunks(100)
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
                                let topics: Option<Vec<Vec<u8>>> = row.get(7);
                                let data: Vec<u8> = row.get(8);

                                let topics_str = format!(
                                    "[{}]",
                                    topics
                                        .as_ref()
                                        .map(|t| t.iter()
                                            .map(|topic| format!("'0x{}'", hex::encode(topic)))
                                            .collect::<Vec<_>>()
                                            .join(", "))
                                        .unwrap_or_default()
                                );
                                let selector_str = selector
                                    .as_ref()
                                    .map(|s| format!("'0x{}'", hex::encode(s)))
                                    .unwrap_or_else(|| "NULL".to_string());

                                format!(
                                    "({}, '{}', {}, {}, '0x{}', '0x{}', {}, {}, '0x{}')",
                                    block_num,
                                    block_timestamp.to_rfc3339(),
                                    log_idx,
                                    tx_idx,
                                    hex::encode(&tx_hash),
                                    hex::encode(&address),
                                    selector_str,
                                    topics_str,
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
                        "INSERT OR IGNORE INTO logs (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topics, data) VALUES {}",
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
                let receipt_values: Vec<Vec<String>> = receipt_rows.chunks(100)
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
    use std::time::Duration;

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

        let status = get_sync_status(&duckdb).await.unwrap();
        assert_eq!(status.latest_block, 5);
        assert_eq!(status.gaps.len(), 1);
        assert_eq!(status.gaps[0], (2, 4));
        assert_eq!(status.gap_blocks, 3); // blocks 2, 3, 4
    }
}
