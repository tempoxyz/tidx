//! ClickHouse direct-write sink.
//!
//! Writes blocks, transactions, logs, and receipts directly to ClickHouse
//! via the HTTP interface using JSONEachRow format.

use anyhow::{anyhow, Result};
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

/// Max rows per ClickHouse HTTP INSERT to avoid timeouts on large backfills.
const CH_INSERT_CHUNK_SIZE: usize = 2_000;

/// Max retry attempts for transient ClickHouse write failures.
const CH_MAX_RETRIES: u32 = 3;

/// Direct-write ClickHouse sink using HTTP batch inserts.
#[derive(Clone)]
pub struct ClickHouseSink {
    http_client: reqwest::Client,
    url: String,
    database: String,
}

impl ClickHouseSink {
    /// Create a new ClickHouse sink.
    pub fn new(url: &str, database: &str) -> Result<Self> {
        let http_client = reqwest::Client::builder()
            .pool_max_idle_per_host(4)
            .build()
            .map_err(|e| anyhow!("Failed to create HTTP client: {e}"))?;

        Ok(Self {
            http_client,
            url: url.trim_end_matches('/').to_string(),
            database: database.to_string(),
        })
    }

    /// Create database and tables if they don't exist.
    pub async fn ensure_schema(&self) -> Result<()> {
        self.exec_raw(&format!("CREATE DATABASE IF NOT EXISTS {}", self.database))
            .await?;

        for (name, ddl) in [
            ("blocks", BLOCKS_SCHEMA),
            ("txs", TXS_SCHEMA),
            ("logs", LOGS_SCHEMA),
            ("receipts", RECEIPTS_SCHEMA),
        ] {
            self.exec(ddl).await.map_err(|e| {
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
        self.write_chunked("blocks", blocks, ChBlockWire::from_row).await?;
        metrics::record_sink_write_duration(self.name(), "blocks", start.elapsed());
        metrics::record_sink_write_rows(self.name(), "blocks", blocks.len() as u64);
        metrics::update_sink_block_rate(self.name(), blocks.len() as u64);
        Ok(())
    }

    pub async fn write_txs(&self, txs: &[TxRow]) -> Result<()> {
        if txs.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        self.write_chunked("txs", txs, ChTxWire::from_row).await?;
        metrics::record_sink_write_duration(self.name(), "txs", start.elapsed());
        metrics::record_sink_write_rows(self.name(), "txs", txs.len() as u64);
        Ok(())
    }

    pub async fn write_logs(&self, logs: &[LogRow]) -> Result<()> {
        if logs.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        self.write_chunked("logs", logs, ChLogWire::from_row).await?;
        metrics::record_sink_write_duration(self.name(), "logs", start.elapsed());
        metrics::record_sink_write_rows(self.name(), "logs", logs.len() as u64);
        Ok(())
    }

    pub async fn write_receipts(&self, receipts: &[ReceiptRow]) -> Result<()> {
        if receipts.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        self.write_chunked("receipts", receipts, ChReceiptWire::from_row).await?;
        metrics::record_sink_write_duration(self.name(), "receipts", start.elapsed());
        metrics::record_sink_write_rows(self.name(), "receipts", receipts.len() as u64);
        Ok(())
    }

    /// Query the highest block number in ClickHouse, or None if empty.
    pub async fn max_block_num(&self) -> Result<Option<i64>> {
        let url = format!(
            "{}/?database={}&default_format=TabSeparated",
            self.url, self.database
        );
        let resp = self
            .http_client
            .post(&url)
            .body("SELECT max(num) FROM blocks")
            .send()
            .await
            .map_err(|e| anyhow!("ClickHouse HTTP request failed: {e}"))?;

        if !resp.status().is_success() {
            let error = resp.text().await.unwrap_or_default();
            return Err(anyhow!("ClickHouse query failed: {error}"));
        }

        let text = resp.text().await.unwrap_or_default();
        let trimmed = text.trim();
        if trimmed.is_empty() || trimmed == "0" {
            // max() on empty table returns 0 in ClickHouse
            // Check if there are actually any rows
            let count_url = format!(
                "{}/?database={}&default_format=TabSeparated",
                self.url, self.database
            );
            let count_resp = self
                .http_client
                .post(&count_url)
                .body("SELECT count() FROM blocks")
                .send()
                .await
                .map_err(|e| anyhow!("ClickHouse HTTP request failed: {e}"))?;
            let count_text = count_resp.text().await.unwrap_or_default();
            if count_text.trim() == "0" {
                return Ok(None);
            }
        }
        trimmed
            .parse::<i64>()
            .map(Some)
            .map_err(|e| anyhow!("Failed to parse max block num '{trimmed}': {e}"))
    }

    /// Query the highest block number for a specific table.
    /// Uses "num" for blocks table, "block_num" for others.
    /// Returns None if the table is empty.
    pub async fn max_block_in_table(&self, table: &str) -> Result<Option<i64>> {
        let col = if table == "blocks" { "num" } else { "block_num" };
        let url = format!(
            "{}/?database={}&default_format=TabSeparated",
            self.url, self.database
        );
        let sql = format!("SELECT max({col}) FROM {table}");
        let resp = self
            .http_client
            .post(&url)
            .body(sql)
            .send()
            .await
            .map_err(|e| anyhow!("ClickHouse HTTP request failed: {e}"))?;

        if !resp.status().is_success() {
            let error = resp.text().await.unwrap_or_default();
            return Err(anyhow!("ClickHouse query failed: {error}"));
        }

        let text = resp.text().await.unwrap_or_default();
        let trimmed = text.trim();
        if trimmed.is_empty() || trimmed == "0" {
            let count_sql = format!("SELECT count() FROM {table}");
            let count_resp = self
                .http_client
                .post(&format!(
                    "{}/?database={}&default_format=TabSeparated",
                    self.url, self.database
                ))
                .body(count_sql)
                .send()
                .await
                .map_err(|e| anyhow!("ClickHouse HTTP request failed: {e}"))?;
            let count_text = count_resp.text().await.unwrap_or_default();
            if count_text.trim() == "0" {
                return Ok(None);
            }
        }
        trimmed
            .parse::<i64>()
            .map(Some)
            .map_err(|e| anyhow!("Failed to parse max block num '{trimmed}': {e}"))
    }

    /// Delete all data from a given block number onwards (reorg support).
    pub async fn delete_from(&self, block_num: u64) -> Result<()> {
        let tables = ["logs", "receipts", "txs", "blocks"];
        let block_col = |t: &str| if t == "blocks" { "num" } else { "block_num" };

        for table in &tables {
            let sql = format!(
                "ALTER TABLE {} DELETE WHERE {} >= {} SETTINGS mutations_sync = 1",
                table,
                block_col(table),
                block_num
            );
            if let Err(e) = self.exec(&sql).await {
                error!(table = *table, error = %e, "ClickHouse delete failed");
                return Err(e);
            }
        }

        debug!(from_block = block_num, "ClickHouse reorg delete complete");
        Ok(())
    }

    /// Serialize rows to JSONEachRow and insert in chunks, avoiding large single requests.
    async fn write_chunked<T, W, F>(&self, table: &str, rows: &[T], convert: F) -> Result<()>
    where
        W: Serialize,
        F: Fn(&T) -> W,
    {
        for chunk in rows.chunks(CH_INSERT_CHUNK_SIZE) {
            let mut body = Vec::with_capacity(chunk.len() * 256);
            for row in chunk {
                serde_json::to_writer(&mut body, &convert(row))?;
                body.push(b'\n');
            }
            self.insert(table, body).await?;
        }
        Ok(())
    }

    /// Execute an INSERT using JSONEachRow format with retry on transient failures.
    async fn insert(&self, table: &str, body: Vec<u8>) -> Result<()> {
        let url = format!(
            "{}/?database={}&query={}",
            self.url,
            self.database,
            urlencoded_insert(table),
        );

        let mut last_error = String::new();
        for attempt in 0..CH_MAX_RETRIES {
            if attempt > 0 {
                let backoff = Duration::from_millis(100 << attempt);
                warn!(table, attempt, "ClickHouse insert retry after {backoff:?}");
                tokio::time::sleep(backoff).await;
            }

            match self
                .http_client
                .post(&url)
                .header("Content-Type", "application/json")
                .body(body.clone())
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => return Ok(()),
                Ok(resp) => {
                    last_error = resp.text().await.unwrap_or_default();
                }
                Err(e) => {
                    last_error = e.to_string();
                }
            }
        }

        Err(anyhow!(
            "ClickHouse insert into {table} failed after {CH_MAX_RETRIES} attempts: {last_error}"
        ))
    }

    /// Execute a SQL statement in the database context.
    async fn exec(&self, sql: &str) -> Result<()> {
        let url = format!("{}/?database={}", self.url, self.database);
        let resp = self
            .http_client
            .post(&url)
            .body(sql.to_string())
            .send()
            .await
            .map_err(|e| anyhow!("ClickHouse HTTP request failed: {e}"))?;

        if !resp.status().is_success() {
            let error = resp.text().await.unwrap_or_default();
            return Err(anyhow!("ClickHouse exec failed: {error}"));
        }

        Ok(())
    }

    /// Execute a SQL statement without database context (for DDL like CREATE DATABASE).
    async fn exec_raw(&self, sql: &str) -> Result<()> {
        let resp = self
            .http_client
            .post(&self.url)
            .body(sql.to_string())
            .send()
            .await
            .map_err(|e| anyhow!("ClickHouse HTTP request failed: {e}"))?;

        if !resp.status().is_success() {
            let error = resp.text().await.unwrap_or_default();
            return Err(anyhow!("ClickHouse exec failed: {error}"));
        }

        Ok(())
    }
}

// ── ClickHouse wire-format structs ────────────────────────────────────────
//
// These derive Serialize for direct JSON serialization via serde_json::to_writer(),
// avoiding the overhead of intermediate serde_json::Value allocations that the
// serde_json::json!() macro creates.

#[derive(Serialize)]
struct ChBlockWire {
    num: i64,
    hash: String,
    parent_hash: String,
    timestamp: String,
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
            timestamp: format_datetime(&b.timestamp),
            timestamp_ms: b.timestamp_ms,
            gas_limit: b.gas_limit,
            gas_used: b.gas_used,
            miner: hex_encode(&b.miner),
            extra_data: b.extra_data.as_ref().map(|v| hex_encode(v)),
        }
    }
}

#[derive(Serialize)]
struct ChTxWire {
    block_num: i64,
    block_timestamp: String,
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
            block_timestamp: format_datetime(&tx.block_timestamp),
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

#[derive(Serialize)]
struct ChLogWire {
    block_num: i64,
    block_timestamp: String,
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
            block_timestamp: format_datetime(&log.block_timestamp),
            log_idx: log.log_idx,
            tx_idx: log.tx_idx,
            tx_hash: hex_encode(&log.tx_hash),
            address: hex_encode(&log.address),
            selector: log.selector.as_ref().map(|v| hex_encode(v)).unwrap_or_default(),
            topic0: log.topic0.as_ref().map(|v| hex_encode(v)),
            topic1: log.topic1.as_ref().map(|v| hex_encode(v)),
            topic2: log.topic2.as_ref().map(|v| hex_encode(v)),
            topic3: log.topic3.as_ref().map(|v| hex_encode(v)),
            data: hex_encode(&log.data),
        }
    }
}

#[derive(Serialize)]
struct ChReceiptWire {
    block_num: i64,
    block_timestamp: String,
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
            block_timestamp: format_datetime(&r.block_timestamp),
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

/// URL-encode an INSERT ... FORMAT JSONEachRow query for use as a query parameter.
fn urlencoded_insert(table: &str) -> String {
    let query = format!("INSERT INTO {table} FORMAT JSONEachRow");
    form_urlencoded::Serializer::new(String::new())
        .append_key_only(&query)
        .finish()
}

/// Hex-encode bytes with 0x prefix.
fn hex_encode(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

/// Format a chrono DateTime for ClickHouse DateTime64(3, 'UTC').
fn format_datetime(dt: &chrono::DateTime<chrono::Utc>) -> String {
    dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
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
    fn test_format_datetime() {
        use chrono::TimeZone;
        let dt = chrono::Utc.with_ymd_and_hms(2024, 1, 15, 12, 34, 56).unwrap();
        assert_eq!(format_datetime(&dt), "2024-01-15 12:34:56.000");
    }

    #[test]
    fn test_urlencoded_insert() {
        let encoded = urlencoded_insert("blocks");
        assert!(encoded.contains("INSERT"));
        assert!(encoded.contains("blocks"));
        assert!(encoded.contains("JSONEachRow"));
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
        let json = serde_json::to_string(&wire).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["num"], 42);
        assert_eq!(parsed["hash"], format!("0x{}", "ab".repeat(32)));
        assert_eq!(parsed["miner"], format!("0x{}", "ee".repeat(20)));
        assert_eq!(parsed["timestamp"], "2024-01-15 12:00:00.000");
        assert!(parsed["extra_data"].is_null());
    }

    #[test]
    fn test_wire_struct_tx_type_rename() {
        let tx = crate::types::TxRow {
            tx_type: 2,
            ..Default::default()
        };

        let wire = ChTxWire::from_row(&tx);
        let json = serde_json::to_string(&wire).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        // serde(rename = "type") should produce "type" not "tx_type"
        assert_eq!(parsed["type"], 2);
        assert!(parsed.get("tx_type").is_none());
    }
}
