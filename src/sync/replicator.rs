use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::mpsc;

use crate::db::{DuckDbPool, Pool};
use crate::types::{BlockRow, LogRow, ReceiptRow, TxRow};

/// Batch of rows to replicate to DuckDB.
#[derive(Debug)]
pub enum ReplicaBatch {
    Blocks(Vec<BlockRow>),
    Txs(Vec<TxRow>),
    Logs(Vec<LogRow>),
    Receipts(Vec<ReceiptRow>),
}

/// DuckDB replicator that syncs data from PostgreSQL.
///
/// Uses a channel-based approach where the indexer sends batches
/// and the replicator flushes them to DuckDB in micro-batches.
/// Also runs periodic gap-fill to catch up on any dropped batches.
pub struct Replicator {
    duckdb: Arc<DuckDbPool>,
    pg_pool: Pool,
    rx: mpsc::Receiver<ReplicaBatch>,
}

/// Handle for sending batches to the replicator.
#[derive(Clone)]
pub struct ReplicatorHandle {
    tx: mpsc::Sender<ReplicaBatch>,
}

impl ReplicatorHandle {
    /// Sends a batch of blocks to be replicated (non-blocking).
    /// If the channel is full, data is dropped and DuckDB gap-fill will catch up.
    pub fn send_blocks(&self, blocks: Vec<BlockRow>) {
        if blocks.is_empty() {
            return;
        }
        if let Err(mpsc::error::TrySendError::Full(_)) = self.tx.try_send(ReplicaBatch::Blocks(blocks)) {
            tracing::debug!("DuckDB replicator channel full, dropping blocks batch");
        }
    }

    /// Sends a batch of transactions to be replicated (non-blocking).
    pub fn send_txs(&self, txs: Vec<TxRow>) {
        if txs.is_empty() {
            return;
        }
        if let Err(mpsc::error::TrySendError::Full(_)) = self.tx.try_send(ReplicaBatch::Txs(txs)) {
            tracing::debug!("DuckDB replicator channel full, dropping txs batch");
        }
    }

    /// Sends a batch of logs to be replicated (non-blocking).
    pub fn send_logs(&self, logs: Vec<LogRow>) {
        if logs.is_empty() {
            return;
        }
        if let Err(mpsc::error::TrySendError::Full(_)) = self.tx.try_send(ReplicaBatch::Logs(logs)) {
            tracing::debug!("DuckDB replicator channel full, dropping logs batch");
        }
    }

    /// Sends a batch of receipts to be replicated (non-blocking).
    pub fn send_receipts(&self, receipts: Vec<ReceiptRow>) {
        if receipts.is_empty() {
            return;
        }
        if let Err(mpsc::error::TrySendError::Full(_)) = self.tx.try_send(ReplicaBatch::Receipts(receipts)) {
            tracing::debug!("DuckDB replicator channel full, dropping receipts batch");
        }
    }
}

impl Replicator {
    /// Creates a new replicator with a channel for receiving batches.
    pub fn new(duckdb: Arc<DuckDbPool>, pg_pool: Pool, buffer_size: usize) -> (Self, ReplicatorHandle) {
        let (tx, rx) = mpsc::channel(buffer_size);
        (Self { duckdb, pg_pool, rx }, ReplicatorHandle { tx })
    }

    /// Runs the replicator, processing batches as they arrive.
    /// Also runs periodic gap-fill every 30s to catch up on dropped batches.
    pub async fn run(mut self) -> Result<()> {
        tracing::info!("DuckDB replicator started");

        let mut gap_fill_interval = tokio::time::interval(Duration::from_secs(30));
        gap_fill_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut checkpoint_interval = tokio::time::interval(Duration::from_secs(60));
        checkpoint_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                batch = self.rx.recv() => {
                    match batch {
                        Some(b) => {
                            if let Err(e) = self.process_batch(b).await {
                                tracing::error!(error = %e, "Failed to replicate batch to DuckDB");
                            }
                        }
                        None => {
                            tracing::info!("DuckDB replicator channel closed");
                            break;
                        }
                    }
                }
                _ = gap_fill_interval.tick() => {
                    if let Err(e) = self.run_gap_fill().await {
                        tracing::error!(error = %e, "DuckDB gap-fill failed");
                    }
                }
                _ = checkpoint_interval.tick() => {
                    let conn = self.duckdb.conn().await;
                    if let Err(e) = conn.execute("CHECKPOINT", []) {
                        tracing::warn!(error = %e, "DuckDB checkpoint failed");
                    } else {
                        tracing::debug!("DuckDB checkpoint completed");
                    }
                }
            }
        }

        tracing::info!("DuckDB replicator stopped");
        Ok(())
    }

    /// Detect and fill any gaps in DuckDB from PostgreSQL.
    async fn run_gap_fill(&self) -> Result<()> {
        let pg_latest = {
            let conn = self.pg_pool.get().await?;
            let row = conn.query_one("SELECT COALESCE(MAX(num), 0) FROM blocks", &[]).await?;
            row.get::<_, i64>(0)
        };

        if pg_latest == 0 {
            return Ok(());
        }

        let gaps = detect_all_gaps_duckdb(&self.duckdb, pg_latest).await?;
        if gaps.is_empty() {
            return Ok(());
        }

        let total_blocks: i64 = gaps.iter().map(|(s, e)| e - s + 1).sum();
        tracing::info!(
            gaps = gaps.len(),
            total_blocks,
            "DuckDB gap-fill: filling gaps from PostgreSQL"
        );

        fill_gaps_from_postgres(&self.pg_pool, &self.duckdb, &gaps, 500).await?;
        Ok(())
    }

    async fn process_batch(&self, batch: ReplicaBatch) -> Result<()> {
        let conn = self.duckdb.conn().await;

        // Helper to reset connection state after errors
        let rollback_if_needed = |conn: &duckdb::Connection| {
            let _ = conn.execute("ROLLBACK", []);
        };

        match batch {
            ReplicaBatch::Blocks(blocks) => {
                let count = blocks.len();
                if count > 0 {
                    // Use INSERT OR IGNORE to handle duplicates gracefully
                    for block in &blocks {
                        let sql = format!(
                            "INSERT OR IGNORE INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner, extra_data) VALUES ({}, '0x{}', '0x{}', '{}', {}, {}, {}, '0x{}', {})",
                            block.num,
                            hex::encode(&block.hash),
                            hex::encode(&block.parent_hash),
                            block.timestamp.to_rfc3339(),
                            block.timestamp_ms,
                            block.gas_limit,
                            block.gas_used,
                            hex::encode(&block.miner),
                            block.extra_data.as_ref().map(|d| format!("'0x{}'", hex::encode(d))).unwrap_or_else(|| "NULL".to_string()),
                        );
                        if let Err(e) = conn.execute(&sql, []) {
                            tracing::warn!(error = %e, block_num = block.num, "Failed to insert block, skipping");
                            rollback_if_needed(&conn);
                        }
                    }
                }
                if let Err(e) = Self::update_watermark(&conn) {
                    rollback_if_needed(&conn);
                    return Err(e);
                }
                tracing::debug!(count, "Replicated blocks to DuckDB");
            }
            ReplicaBatch::Txs(txs) => {
                let count = txs.len();
                if count > 0 {
                    // Batch txs into chunks with INSERT OR IGNORE
                    for chunk in txs.chunks(100) {
                        let values: Vec<String> = chunk
                            .iter()
                            .map(|tx| {
                                format!(
                                    "({}, '{}', {}, '0x{}', {}, '0x{}', {}, '{}', '0x{}', {}, '{}', '{}', {}, '0x{}', {}, {}, {}, {}, {}, {}, {}, {})",
                                    tx.block_num,
                                    tx.block_timestamp.to_rfc3339(),
                                    tx.idx,
                                    hex::encode(&tx.hash),
                                    tx.tx_type,
                                    hex::encode(&tx.from),
                                    tx.to.as_ref().map(|t| format!("'0x{}'", hex::encode(t))).unwrap_or_else(|| "NULL".to_string()),
                                    tx.value,
                                    hex::encode(&tx.input),
                                    tx.gas_limit,
                                    tx.max_fee_per_gas,
                                    tx.max_priority_fee_per_gas,
                                    tx.gas_used.map(|g| g.to_string()).unwrap_or_else(|| "NULL".to_string()),
                                    hex::encode(&tx.nonce_key),
                                    tx.nonce,
                                    tx.fee_token.as_ref().map(|t| format!("'0x{}'", hex::encode(t))).unwrap_or_else(|| "NULL".to_string()),
                                    tx.fee_payer.as_ref().map(|t| format!("'0x{}'", hex::encode(t))).unwrap_or_else(|| "NULL".to_string()),
                                    tx.calls.as_ref().map(|c| format!("'{}'", c.to_string().replace('\'', "''"))).unwrap_or_else(|| "NULL".to_string()),
                                    tx.call_count,
                                    tx.valid_before.map(|v| v.to_string()).unwrap_or_else(|| "NULL".to_string()),
                                    tx.valid_after.map(|v| v.to_string()).unwrap_or_else(|| "NULL".to_string()),
                                    tx.signature_type.map(|s| s.to_string()).unwrap_or_else(|| "NULL".to_string()),
                                )
                            })
                            .collect();
                        
                        let sql = format!(
                            "INSERT OR IGNORE INTO txs (block_num, block_timestamp, idx, hash, type, \"from\", \"to\", value, input, gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_used, nonce_key, nonce, fee_token, fee_payer, calls, call_count, valid_before, valid_after, signature_type) VALUES {}",
                            values.join(", ")
                        );
                        if let Err(e) = conn.execute(&sql, []) {
                            tracing::warn!(error = %e, "Failed to insert tx batch, skipping");
                            rollback_if_needed(&conn);
                        }
                    }
                }
                if let Err(e) = Self::update_watermark(&conn) {
                    rollback_if_needed(&conn);
                    return Err(e);
                }
                tracing::debug!(count, "Replicated txs to DuckDB");
            }
            ReplicaBatch::Logs(logs) => {
                let count = logs.len();
                if count > 0 {
                    // Batch logs into chunks for fewer INSERT statements
                    for chunk in logs.chunks(100) {
                        let values: Vec<String> = chunk
                            .iter()
                            .map(|log| {
                                let topics_str = format!(
                                    "[{}]",
                                    log.topics
                                        .iter()
                                        .map(|t| format!("'0x{}'", hex::encode(t)))
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                );
                                let selector = log
                                    .selector
                                    .as_ref()
                                    .map(|s| format!("'0x{}'", hex::encode(s)))
                                    .unwrap_or_else(|| "NULL".to_string());
                                format!(
                                    "({}, '{}', {}, {}, '0x{}', '0x{}', {}, {}, '0x{}')",
                                    log.block_num,
                                    log.block_timestamp.to_rfc3339(),
                                    log.log_idx,
                                    log.tx_idx,
                                    hex::encode(&log.tx_hash),
                                    hex::encode(&log.address),
                                    selector,
                                    topics_str,
                                    hex::encode(&log.data),
                                )
                            })
                            .collect();
                        
                        let sql = format!(
                            "INSERT OR IGNORE INTO logs (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topics, data) VALUES {}",
                            values.join(", ")
                        );
                        if let Err(e) = conn.execute(&sql, []) {
                            tracing::warn!(error = %e, "Failed to insert log batch, skipping");
                            rollback_if_needed(&conn);
                        }
                    }
                }
                if let Err(e) = Self::update_watermark(&conn) {
                    rollback_if_needed(&conn);
                    return Err(e);
                }
                tracing::debug!(count, "Replicated logs to DuckDB");
            }
            ReplicaBatch::Receipts(receipts) => {
                let count = receipts.len();
                if count > 0 {
                    // Batch receipts into chunks with INSERT OR IGNORE
                    for chunk in receipts.chunks(100) {
                        let values: Vec<String> = chunk
                            .iter()
                            .map(|receipt| {
                                format!(
                                    "({}, '{}', {}, '0x{}', '0x{}', {}, {}, {}, {}, {}, {}, {})",
                                    receipt.block_num,
                                    receipt.block_timestamp.to_rfc3339(),
                                    receipt.tx_idx,
                                    hex::encode(&receipt.tx_hash),
                                    hex::encode(&receipt.from),
                                    receipt.to.as_ref().map(|t| format!("'0x{}'", hex::encode(t))).unwrap_or_else(|| "NULL".to_string()),
                                    receipt.contract_address.as_ref().map(|a| format!("'0x{}'", hex::encode(a))).unwrap_or_else(|| "NULL".to_string()),
                                    receipt.gas_used,
                                    receipt.cumulative_gas_used,
                                    receipt.effective_gas_price.as_ref().map(|p| format!("'{}'", p)).unwrap_or_else(|| "NULL".to_string()),
                                    receipt.status.map(|s| s.to_string()).unwrap_or_else(|| "NULL".to_string()),
                                    receipt.fee_payer.as_ref().map(|p| format!("'0x{}'", hex::encode(p))).unwrap_or_else(|| "NULL".to_string()),
                                )
                            })
                            .collect();
                        
                        let sql = format!(
                            "INSERT OR IGNORE INTO receipts (block_num, block_timestamp, tx_idx, tx_hash, \"from\", \"to\", contract_address, gas_used, cumulative_gas_used, effective_gas_price, status, fee_payer) VALUES {}",
                            values.join(", ")
                        );
                        if let Err(e) = conn.execute(&sql, []) {
                            tracing::warn!(error = %e, "Failed to insert receipt batch, skipping");
                            rollback_if_needed(&conn);
                        }
                    }
                }
                if let Err(e) = Self::update_watermark(&conn) {
                    rollback_if_needed(&conn);
                    return Err(e);
                }
                tracing::debug!(count, "Replicated receipts to DuckDB");
            }
        }

        Ok(())
    }

    fn update_watermark(conn: &duckdb::Connection) -> Result<()> {
        conn.execute(
            "UPDATE duckdb_sync_state SET 
                latest_block = (SELECT COALESCE(MAX(num), 0) FROM blocks),
                updated_at = CURRENT_TIMESTAMP
             WHERE id = 1",
            [],
        )?;
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

    // Use a separate write connection for gap-fill to avoid blocking realtime replication
    let duck_conn = duckdb.open_write_conn()?;

    let pg_conn = pg_pool.get().await?;
    let mut synced = 0u64;

    for (gap_start, gap_end) in gaps {
        let mut current = *gap_start;
        while current <= *gap_end {
            let end = (current + batch_size - 1).min(*gap_end);

            // Backfill blocks for this range
            let block_rows = pg_conn
                .query(
                    "SELECT num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner, extra_data 
                     FROM blocks WHERE num >= $1 AND num <= $2 ORDER BY num",
                    &[&current, &end],
                )
                .await?;

            if !block_rows.is_empty() {
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

            // Backfill logs for this range
            let log_rows = pg_conn
                .query(
                    "SELECT block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topics, data
                     FROM logs WHERE block_num >= $1 AND block_num <= $2 ORDER BY block_num, log_idx",
                    &[&current, &end],
                )
                .await?;

            if !log_rows.is_empty() {
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
    async fn test_replicator_blocks() {
        let duckdb = Arc::new(DuckDbPool::in_memory().unwrap());
        let pg_pool = create_test_pg_pool();
        let (replicator, handle) = Replicator::new(duckdb.clone(), pg_pool, 100);

        let replicator_task = tokio::spawn(replicator.run());

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
        tokio::time::sleep(Duration::from_millis(50)).await;

        let conn = duckdb.conn().await;
        let mut stmt = conn.prepare("SELECT num FROM blocks WHERE num = 1").unwrap();
        let result: i64 = stmt.query_row([], |row| row.get(0)).unwrap();
        assert_eq!(result, 1);

        drop(handle);
        let _ = replicator_task.await;
    }

    #[tokio::test]
    async fn test_replicator_txs() {
        let duckdb = Arc::new(DuckDbPool::in_memory().unwrap());
        let pg_pool = create_test_pg_pool();
        let (replicator, handle) = Replicator::new(duckdb.clone(), pg_pool, 100);
        let replicator_task = tokio::spawn(replicator.run());

        let tx = TxRow {
            block_num: 1,
            block_timestamp: Utc::now(),
            idx: 0,
            hash: vec![0xab; 32],
            tx_type: 2,
            from: vec![0xde; 20],
            to: Some(vec![0xbe; 20]),
            value: "1000000000000000000".to_string(),
            input: vec![],
            gas_limit: 21000,
            max_fee_per_gas: "1000000000".to_string(),
            max_priority_fee_per_gas: "1000000".to_string(),
            gas_used: Some(21000),
            nonce_key: vec![0x00; 32],
            nonce: 0,
            fee_token: None,
            fee_payer: None,
            calls: None,
            call_count: 1,
            valid_before: None,
            valid_after: None,
            signature_type: Some(0),
        };

        handle.send_txs(vec![tx]);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let conn = duckdb.conn().await;
        let mut stmt = conn
            .prepare("SELECT block_num, idx FROM txs WHERE block_num = 1")
            .unwrap();
        let result: (i64, i32) = stmt.query_row([], |row| Ok((row.get(0)?, row.get(1)?))).unwrap();
        assert_eq!(result, (1, 0));

        drop(handle);
        let _ = replicator_task.await;
    }

    #[tokio::test]
    async fn test_replicator_logs() {
        let duckdb = Arc::new(DuckDbPool::in_memory().unwrap());
        let pg_pool = create_test_pg_pool();
        let (replicator, handle) = Replicator::new(duckdb.clone(), pg_pool, 100);
        let replicator_task = tokio::spawn(replicator.run());

        let log = LogRow {
            block_num: 1,
            block_timestamp: Utc::now(),
            log_idx: 0,
            tx_idx: 0,
            tx_hash: vec![0xab; 32],
            address: vec![0xca; 20],
            selector: Some(vec![0xdd, 0xf2, 0x52, 0xad]),
            topics: vec![vec![0xdd; 32], vec![0xee; 32]],
            data: vec![0x00; 32],
        };

        handle.send_logs(vec![log]);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let conn = duckdb.conn().await;
        let mut stmt = conn
            .prepare("SELECT block_num, log_idx FROM logs WHERE block_num = 1")
            .unwrap();
        let result: (i64, i32) = stmt.query_row([], |row| Ok((row.get(0)?, row.get(1)?))).unwrap();
        assert_eq!(result, (1, 0));

        drop(handle);
        let _ = replicator_task.await;
    }

    #[tokio::test]
    async fn test_replicator_logs_batched() {
        let duckdb = Arc::new(DuckDbPool::in_memory().unwrap());
        let pg_pool = create_test_pg_pool();
        let (replicator, handle) = Replicator::new(duckdb.clone(), pg_pool, 100);
        let replicator_task = tokio::spawn(replicator.run());

        // Send 250 logs to test batching (should be 3 batches of 100, 100, 50)
        let logs: Vec<LogRow> = (0..250)
            .map(|i| LogRow {
                block_num: 1,
                block_timestamp: Utc::now(),
                log_idx: i,
                tx_idx: 0,
                tx_hash: vec![0xab; 32],
                address: vec![0xca; 20],
                selector: Some(vec![0xdd, 0xf2, 0x52, 0xad]),
                topics: vec![vec![0xdd; 32], vec![0xee; 32]],
                data: vec![0x00; 32],
            })
            .collect();

        handle.send_logs(logs);
        tokio::time::sleep(Duration::from_millis(100)).await;

        let conn = duckdb.conn().await;
        let mut stmt = conn.prepare("SELECT COUNT(*) FROM logs").unwrap();
        let count: i64 = stmt.query_row([], |row| row.get(0)).unwrap();
        assert_eq!(count, 250);

        drop(handle);
        let _ = replicator_task.await;
    }

    #[tokio::test]
    async fn test_replicator_receipts() {
        let duckdb = Arc::new(DuckDbPool::in_memory().unwrap());
        let pg_pool = create_test_pg_pool();
        let (replicator, handle) = Replicator::new(duckdb.clone(), pg_pool, 100);
        let replicator_task = tokio::spawn(replicator.run());

        let receipt = ReceiptRow {
            block_num: 1,
            block_timestamp: Utc::now(),
            tx_idx: 0,
            tx_hash: vec![0xab; 32],
            from: vec![0xde; 20],
            to: Some(vec![0xbe; 20]),
            contract_address: None,
            gas_used: 21000,
            cumulative_gas_used: 21000,
            effective_gas_price: Some("1000000000".to_string()),
            status: Some(1),
            fee_payer: None,
        };

        handle.send_receipts(vec![receipt]);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let conn = duckdb.conn().await;
        let mut stmt = conn
            .prepare("SELECT block_num, tx_idx, gas_used FROM receipts WHERE block_num = 1")
            .unwrap();
        let result: (i64, i32, i64) = stmt
            .query_row([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap();
        assert_eq!(result, (1, 0, 21000));

        drop(handle);
        let _ = replicator_task.await;
    }

    #[tokio::test]
    async fn test_non_blocking_send_when_channel_full() {
        let duckdb = Arc::new(DuckDbPool::in_memory().unwrap());
        let pg_pool = create_test_pg_pool();
        // Create a tiny buffer of 2
        let (replicator, handle) = Replicator::new(duckdb.clone(), pg_pool, 2);

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
        // (old async API would have blocked on the 3rd send)
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
