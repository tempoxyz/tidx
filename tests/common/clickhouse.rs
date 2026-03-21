//! ClickHouse test infrastructure
//!
//! Provides a TestClickHouse wrapper for integration tests that need to run
//! against a local ClickHouse instance (via docker-compose).

use anyhow::Result;
use std::time::Duration;
use tokio::time::sleep;

/// ClickHouse test client wrapper.
pub struct TestClickHouse {
    pub url: String,
    pub database: String,
    http_client: reqwest::Client,
}

impl TestClickHouse {
    /// Create a new test ClickHouse connection.
    /// Uses CLICKHOUSE_URL env var or defaults to http://localhost:8123
    pub async fn new(database: &str) -> Result<Self> {
        let url = get_clickhouse_url();
        let http_client = reqwest::Client::builder()
            .pool_max_idle_per_host(4)
            .build()?;

        let ch = Self {
            url,
            database: database.to_string(),
            http_client,
        };

        Ok(ch)
    }

    /// Wait for ClickHouse to be ready (for CI/docker startup).
    pub async fn wait_for_ready(&self) -> Result<()> {
        for i in 0..30 {
            match self.query_raw("SELECT 1").await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if i == 29 {
                        return Err(anyhow::anyhow!("ClickHouse not ready after 30s: {e}"));
                    }
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
        Ok(())
    }

    /// Create the test database if it doesn't exist.
    pub async fn ensure_database(&self) -> Result<()> {
        self.query_raw(&format!("CREATE DATABASE IF NOT EXISTS {}", self.database))
            .await?;
        Ok(())
    }

    /// Drop and recreate the test database for clean state.
    pub async fn reset_database(&self) -> Result<()> {
        self.query_raw(&format!("DROP DATABASE IF EXISTS {}", self.database))
            .await?;
        self.query_raw(&format!("CREATE DATABASE {}", self.database))
            .await?;
        Ok(())
    }

    /// Execute a query without database context (for DDL).
    pub async fn query_raw(&self, sql: &str) -> Result<String> {
        let resp = self
            .http_client
            .post(&self.url)
            .body(sql.to_string())
            .send()
            .await?;

        if !resp.status().is_success() {
            let error = resp.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("ClickHouse query failed: {error}"));
        }

        resp.text().await.map_err(Into::into)
    }

    /// Execute a query in the test database context.
    pub async fn query(&self, sql: &str) -> Result<String> {
        let url = format!("{}/?database={}", self.url, self.database);
        let resp = self
            .http_client
            .post(&url)
            .body(sql.to_string())
            .send()
            .await?;

        if !resp.status().is_success() {
            let error = resp.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("ClickHouse query failed: {error}"));
        }

        resp.text().await.map_err(Into::into)
    }

    /// Execute a query and return JSON results.
    pub async fn query_json(&self, sql: &str) -> Result<serde_json::Value> {
        let url = format!(
            "{}/?database={}&default_format=JSON",
            self.url, self.database
        );
        let resp = self
            .http_client
            .post(&url)
            .body(sql.to_string())
            .send()
            .await?;

        if !resp.status().is_success() {
            let error = resp.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("ClickHouse query failed: {error}"));
        }

        let text = resp.text().await?;
        if text.trim().is_empty() {
            return Ok(serde_json::Value::Null);
        }

        serde_json::from_str(&text).map_err(Into::into)
    }

    /// Create a mock logs table for testing CTE generation.
    /// Matches the direct-write schema from db/clickhouse/logs.sql.
    pub async fn create_mock_logs_table(&self) -> Result<()> {
        let sql = r#"
            CREATE TABLE IF NOT EXISTS logs (
                block_num Int64,
                block_timestamp DateTime64(3, 'UTC'),
                log_idx Int32,
                tx_idx Int32,
                tx_hash String,
                address String,
                selector String DEFAULT '',
                topic0 Nullable(String),
                topic1 Nullable(String),
                topic2 Nullable(String),
                topic3 Nullable(String),
                data String,
                INDEX idx_selector selector TYPE bloom_filter GRANULARITY 1,
                INDEX idx_address address TYPE bloom_filter GRANULARITY 1,
                INDEX idx_topic1 topic1 TYPE bloom_filter GRANULARITY 1,
                INDEX idx_topic2 topic2 TYPE bloom_filter GRANULARITY 1
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(block_timestamp)
            ORDER BY (address, selector, block_num)
        "#;
        self.query(sql).await?;
        Ok(())
    }

    /// Insert mock log data using '0x'-prefixed hex strings (direct-write format).
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_mock_log(
        &self,
        block_num: u64,
        log_idx: u32,
        tx_idx: u32,
        tx_hash: &str,
        address: &str,
        selector: &str,
        topic1: &str,
        topic2: &str,
        topic3: &str,
        data: &str,
    ) -> Result<()> {
        let sql = format!(
            r#"INSERT INTO logs (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topic0, topic1, topic2, topic3, data) VALUES ({}, now(), {}, {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')"#,
            block_num,
            log_idx,
            tx_idx,
            tx_hash,
            address,
            selector,
            selector,
            topic1,
            topic2,
            topic3,
            data
        );
        self.query(&sql).await?;
        Ok(())
    }

    /// Insert a Transfer event log with proper encoding (0x-prefixed).
    pub async fn insert_transfer_log(
        &self,
        block_num: u64,
        log_idx: u32,
        from: &str,
        to: &str,
        value: u128,
    ) -> Result<()> {
        let from_padded = format!(
            "0x000000000000000000000000{}",
            from.trim_start_matches("0x")
        );
        let to_padded = format!("0x000000000000000000000000{}", to.trim_start_matches("0x"));
        let value_hex = format!("{:064x}", value);
        let data = format!("0x{}", value_hex);

        self.insert_mock_log(
            block_num,
            log_idx,
            0,
            "0x0000000000000000000000000000000000000000000000000000000000000001",
            "0xdAC17F958D2ee523a2206206994597C13D831ec7",
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            &from_padded,
            &to_padded,
            "0x0000000000000000000000000000000000000000000000000000000000000000",
            &data,
        )
        .await
    }

    /// Create a mock blocks table for testing.
    pub async fn create_mock_blocks_table(&self) -> Result<()> {
        let sql = r#"
            CREATE TABLE IF NOT EXISTS blocks (
                num UInt64,
                hash String,
                parent_hash String,
                timestamp_ms UInt64,
                miner String,
                gas_used UInt64,
                gas_limit UInt64,
                base_fee_per_gas UInt64,
                tx_count UInt32
            ) ENGINE = MergeTree()
            ORDER BY num
        "#;
        self.query(sql).await?;
        Ok(())
    }

    /// Get table row count.
    pub async fn table_count(&self, table: &str) -> Result<u64> {
        let result = self
            .query(&format!("SELECT count() FROM {}", table))
            .await?;
        result.trim().parse().map_err(Into::into)
    }

    /// Get table row count after ReplacingMergeTree deduplication.
    pub async fn table_count_final(&self, table: &str) -> Result<u64> {
        let result = self
            .query(&format!("SELECT count() FROM {} FINAL", table))
            .await?;
        result.trim().parse().map_err(Into::into)
    }
}

fn get_clickhouse_url() -> String {
    std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string())
}
