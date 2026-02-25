//! ClickHouse direct-write sink.
//!
//! Writes blocks, transactions, logs, and receipts directly to ClickHouse
//! via the official `clickhouse` crate using RowBinary format with LZ4 compression.

use anyhow::{anyhow, Result};
use clickhouse::Row;
use serde::Serialize;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

use crate::metrics;
use crate::types::{BlockRow, LogRow, ReceiptRow, TxRow};

/// Schema SQL files embedded at compile time.
const BLOCKS_SCHEMA: &str = include_str!("../../db/clickhouse/blocks.sql");
const TXS_SCHEMA: &str = include_str!("../../db/clickhouse/txs.sql");
const LOGS_SCHEMA: &str = include_str!("../../db/clickhouse/logs.sql");
const RECEIPTS_SCHEMA: &str = include_str!("../../db/clickhouse/receipts.sql");

/// Max rows per ClickHouse INSERT to avoid unbounded memory growth during backfills.
const CH_INSERT_CHUNK_SIZE: usize = 2_000;

/// Max retry attempts for transient ClickHouse write failures.
const CH_MAX_RETRIES: u32 = 3;

/// Timeout for sending each chunk of row data to ClickHouse.
const CH_SEND_TIMEOUT: Duration = Duration::from_secs(30);

/// Timeout for waiting for ClickHouse to acknowledge the INSERT.
const CH_END_TIMEOUT: Duration = Duration::from_secs(120);

/// Direct-write ClickHouse sink using RowBinary format with LZ4 compression.
#[derive(Clone)]
pub struct ClickHouseSink {
    client: clickhouse::Client,
    /// Client without database context, used for `CREATE DATABASE` DDL.
    base_client: clickhouse::Client,
    database: String,
}

impl ClickHouseSink {
    /// Create a new ClickHouse sink.
    pub fn new(url: &str, database: &str) -> Result<Self> {
        let url = url.trim_end_matches('/');
        let base_client = clickhouse::Client::default().with_url(url);
        let client = base_client.clone().with_database(database);

        Ok(Self {
            client,
            base_client,
            database: database.to_string(),
        })
    }

    /// Create database and tables if they don't exist.
    pub async fn ensure_schema(&self) -> Result<()> {
        self.base_client
            .query(&format!(
                "CREATE DATABASE IF NOT EXISTS {}",
                self.database
            ))
            .execute()
            .await
            .map_err(|e| anyhow!("Failed to create ClickHouse database: {e}"))?;

        for (name, ddl) in [
            ("blocks", BLOCKS_SCHEMA),
            ("txs", TXS_SCHEMA),
            ("logs", LOGS_SCHEMA),
            ("receipts", RECEIPTS_SCHEMA),
        ] {
            self.client.query(ddl).execute().await.map_err(|e| {
                anyhow!("Failed to create ClickHouse table {name}: {e}")
            })?;
            debug!(table = name, database = %self.database, "ClickHouse table ready");
        }

        info!(database = %self.database, "ClickHouse schema ready");
        Ok(())
    }

    pub fn name(&self) -> &'static str {
        "clickhouse"
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub async fn write_blocks(&self, blocks: &[BlockRow]) -> Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        self.insert_chunked("blocks", blocks, ChBlockWire::from_row).await?;
        metrics::record_sink_write_duration(self.name(), "blocks", start.elapsed());
        metrics::record_sink_write_rows(self.name(), "blocks", blocks.len() as u64);
        metrics::update_sink_block_rate(self.name(), blocks.len() as u64);
        metrics::increment_sink_row_count(self.name(), "blocks", blocks.len() as u64);
        if let Some(max) = blocks.iter().map(|b| b.num).max() {
            metrics::update_sink_watermark(self.name(), "blocks", max);
        }
        Ok(())
    }

    pub async fn write_txs(&self, txs: &[TxRow]) -> Result<()> {
        if txs.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        self.insert_chunked("txs", txs, ChTxWire::from_row).await?;
        metrics::record_sink_write_duration(self.name(), "txs", start.elapsed());
        metrics::record_sink_write_rows(self.name(), "txs", txs.len() as u64);
        metrics::increment_sink_row_count(self.name(), "txs", txs.len() as u64);
        if let Some(max) = txs.iter().map(|t| t.block_num).max() {
            metrics::update_sink_watermark(self.name(), "txs", max);
        }
        Ok(())
    }

    pub async fn write_logs(&self, logs: &[LogRow]) -> Result<()> {
        if logs.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        self.insert_chunked("logs", logs, ChLogWire::from_row).await?;
        metrics::record_sink_write_duration(self.name(), "logs", start.elapsed());
        metrics::record_sink_write_rows(self.name(), "logs", logs.len() as u64);
        metrics::increment_sink_row_count(self.name(), "logs", logs.len() as u64);
        if let Some(max) = logs.iter().map(|l| l.block_num).max() {
            metrics::update_sink_watermark(self.name(), "logs", max);
        }
        Ok(())
    }

    pub async fn write_receipts(&self, receipts: &[ReceiptRow]) -> Result<()> {
        if receipts.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        self.insert_chunked("receipts", receipts, ChReceiptWire::from_row).await?;
        metrics::record_sink_write_duration(self.name(), "receipts", start.elapsed());
        metrics::record_sink_write_rows(self.name(), "receipts", receipts.len() as u64);
        metrics::increment_sink_row_count(self.name(), "receipts", receipts.len() as u64);
        if let Some(max) = receipts.iter().map(|r| r.block_num).max() {
            metrics::update_sink_watermark(self.name(), "receipts", max);
        }
        Ok(())
    }

    /// Query the highest block number in ClickHouse, or None if empty.
    pub async fn max_block_num(&self) -> Result<Option<i64>> {
        let count: u64 = self
            .client
            .query("SELECT count() FROM blocks")
            .fetch_one()
            .await
            .map_err(|e| anyhow!("ClickHouse query failed: {e}"))?;
        if count == 0 {
            return Ok(None);
        }
        let max: i64 = self
            .client
            .query("SELECT max(num) FROM blocks")
            .fetch_one()
            .await
            .map_err(|e| anyhow!("ClickHouse query failed: {e}"))?;
        Ok(Some(max))
    }

    /// Query the highest block number for a specific table.
    /// Uses "num" for blocks table, "block_num" for others.
    /// Returns None if the table is empty.
    pub async fn max_block_in_table(&self, table: &str) -> Result<Option<i64>> {
        let col = if table == "blocks" { "num" } else { "block_num" };
        let count: u64 = self
            .client
            .query(&format!("SELECT count() FROM {table}"))
            .fetch_one()
            .await
            .map_err(|e| anyhow!("ClickHouse query failed: {e}"))?;
        if count == 0 {
            return Ok(None);
        }
        let max: i64 = self
            .client
            .query(&format!("SELECT max({col}) FROM {table}"))
            .fetch_one()
            .await
            .map_err(|e| anyhow!("ClickHouse query failed: {e}"))?;
        Ok(Some(max))
    }

    /// Query the row count for a specific table.
    pub async fn row_count(&self, table: &str) -> Result<u64> {
        self.client
            .query(&format!("SELECT count() FROM {table}"))
            .fetch_one()
            .await
            .map_err(|e| anyhow!("ClickHouse query failed: {e}"))
    }

    /// Delete all data from a given block number onwards (reorg support).
    pub async fn delete_from(&self, block_num: u64) -> Result<()> {
        let tables = ["logs", "receipts", "txs", "blocks"];
        let block_col = |t: &str| if t == "blocks" { "num" } else { "block_num" };

        for table in &tables {
            let sql = format!(
                "ALTER TABLE {} DELETE WHERE {} >= {}",
                table,
                block_col(table),
                block_num
            );
            self.client
                .query(&sql)
                .with_option("mutations_sync", "1")
                .execute()
                .await
                .map_err(|e| {
                    error!(table = *table, error = %e, "ClickHouse delete failed");
                    anyhow!("ClickHouse delete from {table} failed: {e}")
                })?;
        }

        debug!(from_block = block_num, "ClickHouse reorg delete complete");
        Ok(())
    }

    /// Chunk source rows, convert each chunk to wire format, and insert with retry logic.
    /// This avoids allocating the full wire-format vec upfront, bounding peak memory
    /// to `CH_INSERT_CHUNK_SIZE` wire structs at a time.
    async fn insert_chunked<S, W, F>(&self, table: &str, rows: &[S], convert: F) -> Result<()>
    where
        W: Serialize + for<'a> Row<Value<'a> = W>,
        F: Fn(&S) -> W,
    {
        for chunk in rows.chunks(CH_INSERT_CHUNK_SIZE) {
            let wire: Vec<W> = chunk.iter().map(&convert).collect();
            let mut last_error = None;
            for attempt in 0..CH_MAX_RETRIES {
                if attempt > 0 {
                    let backoff = Duration::from_millis(100 << attempt);
                    warn!(table, attempt, "ClickHouse insert retry after {backoff:?}");
                    tokio::time::sleep(backoff).await;
                }
                match self.try_insert(table, &wire).await {
                    Ok(()) => {
                        last_error = None;
                        break;
                    }
                    Err(e) => {
                        last_error = Some(e);
                    }
                }
            }
            if let Some(e) = last_error {
                return Err(anyhow!(
                    "ClickHouse insert into {table} failed after {CH_MAX_RETRIES} attempts: {e}"
                ));
            }
        }
        Ok(())
    }

    async fn try_insert<T>(&self, table: &str, rows: &[T]) -> Result<()>
    where
        T: Serialize + for<'a> Row<Value<'a> = T>,
    {
        let mut insert = self
            .client
            .insert::<T>(table)
            .await?
            .with_timeouts(Some(CH_SEND_TIMEOUT), Some(CH_END_TIMEOUT));
        for row in rows {
            insert.write(row).await?;
        }
        insert.end().await?;
        Ok(())
    }
}

// ── ClickHouse wire-format structs ────────────────────────────────────────
//
// These derive `clickhouse::Row` for RowBinary serialization and `serde::Serialize`
// for the Row encoding. DateTime64(3) columns use the chrono serde adapter.

#[derive(Row, Serialize)]
struct ChBlockWire {
    num: i64,
    hash: String,
    parent_hash: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    timestamp: chrono::DateTime<chrono::Utc>,
    timestamp_ms: i64,
    gas_limit: i64,
    gas_used: i64,
    miner: String,
    extra_data: Option<String>,
}

impl ChBlockWire {
    fn from_row(b: &BlockRow) -> Self {
        Self {
            num: b.num,
            hash: hex_encode(&b.hash),
            parent_hash: hex_encode(&b.parent_hash),
            timestamp: b.timestamp,
            timestamp_ms: b.timestamp_ms,
            gas_limit: b.gas_limit,
            gas_used: b.gas_used,
            miner: hex_encode(&b.miner),
            extra_data: b.extra_data.as_ref().map(|v| hex_encode(v)),
        }
    }
}

#[derive(Row, Serialize)]
struct ChTxWire {
    block_num: i64,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    block_timestamp: chrono::DateTime<chrono::Utc>,
    idx: i32,
    hash: String,
    #[serde(rename = "type")]
    tx_type: i16,
    from: String,
    to: Option<String>,
    value: String,
    input: String,
    gas_limit: i64,
    max_fee_per_gas: String,
    max_priority_fee_per_gas: String,
    gas_used: Option<i64>,
    nonce_key: String,
    nonce: i64,
    fee_token: Option<String>,
    fee_payer: Option<String>,
    calls: Option<String>,
    call_count: i16,
    valid_before: Option<i64>,
    valid_after: Option<i64>,
    signature_type: Option<i16>,
}

impl ChTxWire {
    fn from_row(tx: &TxRow) -> Self {
        Self {
            block_num: tx.block_num,
            block_timestamp: tx.block_timestamp,
            idx: tx.idx,
            hash: hex_encode(&tx.hash),
            tx_type: tx.tx_type,
            from: hex_encode(&tx.from),
            to: tx.to.as_ref().map(|v| hex_encode(v)),
            value: tx.value.clone(),
            input: hex_encode(&tx.input),
            gas_limit: tx.gas_limit,
            max_fee_per_gas: tx.max_fee_per_gas.clone(),
            max_priority_fee_per_gas: tx.max_priority_fee_per_gas.clone(),
            gas_used: tx.gas_used,
            nonce_key: hex_encode(&tx.nonce_key),
            nonce: tx.nonce,
            fee_token: tx.fee_token.as_ref().map(|v| hex_encode(v)),
            fee_payer: tx.fee_payer.as_ref().map(|v| hex_encode(v)),
            calls: tx.calls.as_ref().map(|v| v.to_string()),
            call_count: tx.call_count,
            valid_before: tx.valid_before,
            valid_after: tx.valid_after,
            signature_type: tx.signature_type,
        }
    }
}

#[derive(Row, Serialize)]
struct ChLogWire {
    block_num: i64,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    block_timestamp: chrono::DateTime<chrono::Utc>,
    log_idx: i32,
    tx_idx: i32,
    tx_hash: String,
    address: String,
    selector: String,
    topic0: Option<String>,
    topic1: Option<String>,
    topic2: Option<String>,
    topic3: Option<String>,
    data: String,
}

impl ChLogWire {
    fn from_row(log: &LogRow) -> Self {
        Self {
            block_num: log.block_num,
            block_timestamp: log.block_timestamp,
            log_idx: log.log_idx,
            tx_idx: log.tx_idx,
            tx_hash: hex_encode(&log.tx_hash),
            address: hex_encode(&log.address),
            selector: log
                .selector
                .as_ref()
                .map(|v| hex_encode(v))
                .unwrap_or_default(),
            topic0: log.topic0.as_ref().map(|v| hex_encode(v)),
            topic1: log.topic1.as_ref().map(|v| hex_encode(v)),
            topic2: log.topic2.as_ref().map(|v| hex_encode(v)),
            topic3: log.topic3.as_ref().map(|v| hex_encode(v)),
            data: hex_encode(&log.data),
        }
    }
}

#[derive(Row, Serialize)]
struct ChReceiptWire {
    block_num: i64,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    block_timestamp: chrono::DateTime<chrono::Utc>,
    tx_idx: i32,
    tx_hash: String,
    from: String,
    to: Option<String>,
    contract_address: Option<String>,
    gas_used: i64,
    cumulative_gas_used: i64,
    effective_gas_price: Option<String>,
    status: Option<i16>,
    fee_payer: Option<String>,
}

impl ChReceiptWire {
    fn from_row(r: &ReceiptRow) -> Self {
        Self {
            block_num: r.block_num,
            block_timestamp: r.block_timestamp,
            tx_idx: r.tx_idx,
            tx_hash: hex_encode(&r.tx_hash),
            from: hex_encode(&r.from),
            to: r.to.as_ref().map(|v| hex_encode(v)),
            contract_address: r.contract_address.as_ref().map(|v| hex_encode(v)),
            gas_used: r.gas_used,
            cumulative_gas_used: r.cumulative_gas_used,
            effective_gas_price: r.effective_gas_price.clone(),
            status: r.status,
            fee_payer: r.fee_payer.as_ref().map(|v| hex_encode(v)),
        }
    }
}

/// Hex-encode bytes with 0x prefix.
fn hex_encode(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hex_encode() {
        assert_eq!(hex_encode(&[0xde, 0xad, 0xbe, 0xef]), "0xdeadbeef");
        assert_eq!(hex_encode(&[]), "0x");
    }

    #[test]
    fn test_wire_struct_serialization() {
        use chrono::TimeZone;
        let dt = chrono::Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap();

        let block = crate::types::BlockRow {
            num: 42,
            hash: vec![0xab; 32],
            parent_hash: vec![0xcd; 32],
            timestamp: dt,
            timestamp_ms: 1705320000000,
            gas_limit: 30_000_000,
            gas_used: 15_000_000,
            miner: vec![0xee; 20],
            extra_data: None,
        };

        let wire = ChBlockWire::from_row(&block);
        // Verify field values via the struct fields directly
        assert_eq!(wire.num, 42);
        assert_eq!(wire.hash, format!("0x{}", "ab".repeat(32)));
        assert_eq!(wire.miner, format!("0x{}", "ee".repeat(20)));
        assert_eq!(wire.timestamp, dt);
        assert!(wire.extra_data.is_none());
    }

    #[test]
    fn test_wire_struct_tx_type_rename() {
        let tx = crate::types::TxRow {
            tx_type: 2,
            ..Default::default()
        };

        let wire = ChTxWire::from_row(&tx);
        // Verify via serde JSON that the rename applies
        let json = serde_json::to_string(&wire).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["type"], 2);
        assert!(parsed.get("tx_type").is_none());
    }
}
