//! ClickHouse direct-write sink.
//!
//! Writes blocks, transactions, logs, and receipts directly to ClickHouse
//! via the HTTP interface using JSONEachRow format. Replaces the old
//! Replaces the old MaterializedPostgreSQL replication approach.

use anyhow::{anyhow, Result};
use std::time::Instant;
use tracing::{debug, error, info};

use crate::metrics;
use crate::types::{BlockRow, LogRow, ReceiptRow, TxRow};

/// Schema SQL files embedded at compile time.
const BLOCKS_SCHEMA: &str = include_str!("../../db/clickhouse/blocks.sql");
const TXS_SCHEMA: &str = include_str!("../../db/clickhouse/txs.sql");
const LOGS_SCHEMA: &str = include_str!("../../db/clickhouse/logs.sql");
const RECEIPTS_SCHEMA: &str = include_str!("../../db/clickhouse/receipts.sql");

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
        let mut body = String::new();
        for b in blocks {
            let row = serde_json::json!({
                "num": b.num,
                "hash": hex_encode(&b.hash),
                "parent_hash": hex_encode(&b.parent_hash),
                "timestamp": format_datetime(&b.timestamp),
                "timestamp_ms": b.timestamp_ms,
                "gas_limit": b.gas_limit,
                "gas_used": b.gas_used,
                "miner": hex_encode(&b.miner),
                "extra_data": b.extra_data.as_ref().map(|v| hex_encode(v)),
            });
            body.push_str(&row.to_string());
            body.push('\n');
        }

        self.insert("blocks", &body).await?;

        let count = blocks.len() as u64;
        metrics::record_sink_write_duration(self.name(), "blocks", start.elapsed());
        metrics::record_sink_write_rows(self.name(), "blocks", count);
        Ok(())
    }

    pub async fn write_txs(&self, txs: &[TxRow]) -> Result<()> {
        if txs.is_empty() {
            return Ok(());
        }

        let start = Instant::now();
        let mut body = String::new();
        for tx in txs {
            let row = serde_json::json!({
                "block_num": tx.block_num,
                "block_timestamp": format_datetime(&tx.block_timestamp),
                "idx": tx.idx,
                "hash": hex_encode(&tx.hash),
                "type": tx.tx_type,
                "from": hex_encode(&tx.from),
                "to": tx.to.as_ref().map(|v| hex_encode(v)),
                "value": tx.value,
                "input": hex_encode(&tx.input),
                "gas_limit": tx.gas_limit,
                "max_fee_per_gas": tx.max_fee_per_gas,
                "max_priority_fee_per_gas": tx.max_priority_fee_per_gas,
                "gas_used": tx.gas_used,
                "nonce_key": hex_encode(&tx.nonce_key),
                "nonce": tx.nonce,
                "fee_token": tx.fee_token.as_ref().map(|v| hex_encode(v)),
                "fee_payer": tx.fee_payer.as_ref().map(|v| hex_encode(v)),
                "calls": tx.calls.as_ref().map(|v| v.to_string()),
                "call_count": tx.call_count,
                "valid_before": tx.valid_before,
                "valid_after": tx.valid_after,
                "signature_type": tx.signature_type,
            });
            body.push_str(&row.to_string());
            body.push('\n');
        }

        self.insert("txs", &body).await?;

        let count = txs.len() as u64;
        metrics::record_sink_write_duration(self.name(), "txs", start.elapsed());
        metrics::record_sink_write_rows(self.name(), "txs", count);
        Ok(())
    }

    pub async fn write_logs(&self, logs: &[LogRow]) -> Result<()> {
        if logs.is_empty() {
            return Ok(());
        }

        let start = Instant::now();
        let mut body = String::new();
        for log in logs {
            let row = serde_json::json!({
                "block_num": log.block_num,
                "block_timestamp": format_datetime(&log.block_timestamp),
                "log_idx": log.log_idx,
                "tx_idx": log.tx_idx,
                "tx_hash": hex_encode(&log.tx_hash),
                "address": hex_encode(&log.address),
                "selector": log.selector.as_ref().map(|v| hex_encode(v)).unwrap_or_default(),
                "topic0": log.topic0.as_ref().map(|v| hex_encode(v)),
                "topic1": log.topic1.as_ref().map(|v| hex_encode(v)),
                "topic2": log.topic2.as_ref().map(|v| hex_encode(v)),
                "topic3": log.topic3.as_ref().map(|v| hex_encode(v)),
                "data": hex_encode(&log.data),
            });
            body.push_str(&row.to_string());
            body.push('\n');
        }

        self.insert("logs", &body).await?;

        let count = logs.len() as u64;
        metrics::record_sink_write_duration(self.name(), "logs", start.elapsed());
        metrics::record_sink_write_rows(self.name(), "logs", count);
        Ok(())
    }

    pub async fn write_receipts(&self, receipts: &[ReceiptRow]) -> Result<()> {
        if receipts.is_empty() {
            return Ok(());
        }

        let start = Instant::now();
        let mut body = String::new();
        for r in receipts {
            let row = serde_json::json!({
                "block_num": r.block_num,
                "block_timestamp": format_datetime(&r.block_timestamp),
                "tx_idx": r.tx_idx,
                "tx_hash": hex_encode(&r.tx_hash),
                "from": hex_encode(&r.from),
                "to": r.to.as_ref().map(|v| hex_encode(v)),
                "contract_address": r.contract_address.as_ref().map(|v| hex_encode(v)),
                "gas_used": r.gas_used,
                "cumulative_gas_used": r.cumulative_gas_used,
                "effective_gas_price": r.effective_gas_price.as_ref(),
                "status": r.status,
                "fee_payer": r.fee_payer.as_ref().map(|v| hex_encode(v)),
            });
            body.push_str(&row.to_string());
            body.push('\n');
        }

        self.insert("receipts", &body).await?;

        let count = receipts.len() as u64;
        metrics::record_sink_write_duration(self.name(), "receipts", start.elapsed());
        metrics::record_sink_write_rows(self.name(), "receipts", count);
        Ok(())
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

    /// Execute an INSERT using JSONEachRow format.
    async fn insert(&self, table: &str, json_rows: &str) -> Result<()> {
        let url = format!(
            "{}/?database={}&query={}",
            self.url,
            self.database,
            urlencoded_insert(table),
        );

        let resp = self
            .http_client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(json_rows.to_string())
            .send()
            .await
            .map_err(|e| anyhow!("ClickHouse HTTP request failed: {e}"))?;

        if !resp.status().is_success() {
            let error = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "ClickHouse insert into {table} failed: {error}"
            ));
        }

        Ok(())
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
}
