//! DuckDB PostgreSQL Engine - queries PostgreSQL via DuckDB's postgres extension.
//!
//! Instead of replicating data to a local DuckDB file, this engine:
//! 1. Creates in-memory DuckDB connections
//! 2. ATTACHes to PostgreSQL via the postgres extension
//! 3. Creates compatibility views that convert BYTEA→hex strings
//! 4. Routes analytical queries through DuckDB's vectorized engine

use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore, OwnedSemaphorePermit};

/// DuckDB engine that queries PostgreSQL via the postgres extension.
///
/// Manages a pool of in-memory DuckDB connections, each attached to PostgreSQL.
/// Analytical queries run through DuckDB's vectorized engine while reading from Postgres.
pub struct DuckDbPgEngine {
    /// PostgreSQL connection URL (kept for potential reconnection)
    #[allow(dead_code)]
    pg_url: String,
    /// Pool of initialized DuckDB connections
    connections: Arc<Mutex<Vec<duckdb::Connection>>>,
    /// Semaphore to limit concurrent connections
    semaphore: Arc<Semaphore>,
    /// Whether the postgres extension is available
    extension_available: bool,
}

impl DuckDbPgEngine {
    /// Creates a new DuckDB engine connected to PostgreSQL.
    ///
    /// - `pg_url`: PostgreSQL connection string
    /// - `pool_size`: Number of DuckDB connections to maintain (default: 4)
    pub fn new(pg_url: &str, pool_size: usize) -> Result<Self> {
        let mut connections = Vec::with_capacity(pool_size);
        let mut extension_available = false;

        for i in 0..pool_size {
            match Self::create_connection(pg_url) {
                Ok(conn) => {
                    if i == 0 {
                        extension_available = true;
                        tracing::info!(
                            pg_url = %pg_url.split('@').next_back().unwrap_or(pg_url),
                            pool_size,
                            "DuckDB postgres extension initialized"
                        );
                    }
                    connections.push(conn);
                }
                Err(e) => {
                    if i == 0 {
                        tracing::warn!(
                            error = %e,
                            "DuckDB postgres extension not available, analytical queries will use Postgres"
                        );
                        return Ok(Self {
                            pg_url: pg_url.to_string(),
                            connections: Arc::new(Mutex::new(vec![])),
                            semaphore: Arc::new(Semaphore::new(0)),
                            extension_available: false,
                        });
                    }
                    // Partial failure - use what we have
                    tracing::warn!(error = %e, "Failed to create DuckDB connection {}", i);
                    break;
                }
            }
        }

        let actual_size = connections.len();
        Ok(Self {
            pg_url: pg_url.to_string(),
            connections: Arc::new(Mutex::new(connections)),
            semaphore: Arc::new(Semaphore::new(actual_size)),
            extension_available,
        })
    }

    /// Creates a single DuckDB connection with postgres extension attached.
    fn create_connection(pg_url: &str) -> Result<duckdb::Connection> {
        let conn = duckdb::Connection::open_in_memory()
            .context("Failed to open in-memory DuckDB")?;

        // Performance settings
        conn.execute_batch(
            r#"
            SET memory_limit = '4GB';
            SET threads = 2;
            "#,
        )?;

        // Install and load postgres extension
        conn.execute("INSTALL postgres", [])
            .context("Failed to install postgres extension")?;
        conn.execute("LOAD postgres", [])
            .context("Failed to load postgres extension")?;

        // Limit postgres extension's internal connection pool to prevent exhausting Postgres connections
        // Default is 64 per DuckDB connection; we limit to 4 to be conservative
        // With 4 DuckDB connections per chain × 2 chains × 4 pg connections = 32 max
        let _ = conn.execute("SET pg_connection_limit = 4", []);

        // Attach PostgreSQL database
        let attach_sql = format!(
            "ATTACH '{}' AS pg (TYPE postgres, READ_ONLY)",
            pg_url.replace('\'', "''")
        );
        conn.execute(&attach_sql, [])
            .context("Failed to attach PostgreSQL database")?;

        // Create compatibility views that convert BYTEA to hex strings
        // This matches the format the old replication used
        Self::create_views(&conn)?;

        Ok(conn)
    }

    /// Creates views that convert PostgreSQL BYTEA columns to 0x-prefixed hex strings.
    fn create_views(conn: &duckdb::Connection) -> Result<()> {
        // Blocks view
        conn.execute_batch(
            r#"
            CREATE OR REPLACE VIEW blocks AS
            SELECT 
                num,
                '0x' || lower(hex(hash)) AS hash,
                '0x' || lower(hex(parent_hash)) AS parent_hash,
                timestamp,
                timestamp_ms,
                gas_limit,
                gas_used,
                '0x' || lower(hex(miner)) AS miner,
                CASE WHEN extra_data IS NOT NULL THEN '0x' || lower(hex(extra_data)) ELSE NULL END AS extra_data
            FROM pg.public.blocks;
            "#,
        )?;

        // Transactions view
        conn.execute_batch(
            r#"
            CREATE OR REPLACE VIEW txs AS
            SELECT 
                block_num,
                block_timestamp,
                idx,
                '0x' || lower(hex(hash)) AS hash,
                type,
                '0x' || lower(hex("from")) AS "from",
                CASE WHEN "to" IS NOT NULL THEN '0x' || lower(hex("to")) ELSE NULL END AS "to",
                value,
                '0x' || lower(hex(input)) AS input,
                gas_limit,
                max_fee_per_gas,
                max_priority_fee_per_gas,
                gas_used,
                '0x' || lower(hex(nonce_key)) AS nonce_key,
                nonce,
                CASE WHEN fee_token IS NOT NULL THEN '0x' || lower(hex(fee_token)) ELSE NULL END AS fee_token,
                CASE WHEN fee_payer IS NOT NULL THEN '0x' || lower(hex(fee_payer)) ELSE NULL END AS fee_payer,
                calls,
                call_count,
                valid_before,
                valid_after,
                signature_type
            FROM pg.public.txs;
            "#,
        )?;

        // Logs view
        conn.execute_batch(
            r#"
            CREATE OR REPLACE VIEW logs AS
            SELECT 
                block_num,
                block_timestamp,
                log_idx,
                tx_idx,
                '0x' || lower(hex(tx_hash)) AS tx_hash,
                '0x' || lower(hex(address)) AS address,
                CASE WHEN selector IS NOT NULL THEN '0x' || lower(hex(selector)) ELSE NULL END AS selector,
                CASE WHEN topic0 IS NOT NULL THEN '0x' || lower(hex(topic0)) ELSE NULL END AS topic0,
                CASE WHEN topic1 IS NOT NULL THEN '0x' || lower(hex(topic1)) ELSE NULL END AS topic1,
                CASE WHEN topic2 IS NOT NULL THEN '0x' || lower(hex(topic2)) ELSE NULL END AS topic2,
                CASE WHEN topic3 IS NOT NULL THEN '0x' || lower(hex(topic3)) ELSE NULL END AS topic3,
                '0x' || lower(hex(data)) AS data
            FROM pg.public.logs;
            "#,
        )?;

        // Receipts view
        conn.execute_batch(
            r#"
            CREATE OR REPLACE VIEW receipts AS
            SELECT 
                block_num,
                block_timestamp,
                tx_idx,
                '0x' || lower(hex(tx_hash)) AS tx_hash,
                '0x' || lower(hex("from")) AS "from",
                CASE WHEN "to" IS NOT NULL THEN '0x' || lower(hex("to")) ELSE NULL END AS "to",
                CASE WHEN contract_address IS NOT NULL THEN '0x' || lower(hex(contract_address)) ELSE NULL END AS contract_address,
                gas_used,
                cumulative_gas_used,
                effective_gas_price,
                status,
                CASE WHEN fee_payer IS NOT NULL THEN '0x' || lower(hex(fee_payer)) ELSE NULL END AS fee_payer
            FROM pg.public.receipts;
            "#,
        )?;

        Ok(())
    }

    /// Returns true if the postgres extension is available.
    pub fn is_available(&self) -> bool {
        self.extension_available
    }

    /// Executes a query and returns results as JSON.
    pub async fn query(&self, sql: &str) -> Result<(Vec<String>, Vec<Vec<serde_json::Value>>)> {
        if !self.extension_available {
            anyhow::bail!("DuckDB postgres extension not available");
        }

        let conn = self.checkout().await?;
        let result = execute_query(&conn, sql);
        drop(conn); // Return to pool
        result
    }

    /// Executes a streaming query and sends batches through a channel.
    pub async fn query_streaming(
        &self,
        sql: &str,
        batch_size: usize,
        tx: tokio::sync::mpsc::Sender<Result<Vec<Vec<serde_json::Value>>>>,
    ) -> Result<Vec<String>> {
        if !self.extension_available {
            anyhow::bail!("DuckDB postgres extension not available");
        }

        let conn = self.checkout().await?;
        let result = execute_query_streaming(&conn, sql, batch_size, tx);
        drop(conn); // Return to pool
        result
    }

    /// Checks out a connection from the pool.
    async fn checkout(&self) -> Result<ConnectionGuard> {
        let permit = self.semaphore.clone().acquire_owned().await
            .map_err(|_| anyhow::anyhow!("DuckDB connection pool closed"))?;
        
        let conn = {
            let mut connections = self.connections.lock().await;
            connections.pop().ok_or_else(|| anyhow::anyhow!("No DuckDB connections available"))?
        };

        Ok(ConnectionGuard {
            conn: Some(conn),
            pool: self.connections.clone(),
            _permit: permit,
        })
    }
}

/// RAII guard that returns connection to pool on drop.
struct ConnectionGuard {
    conn: Option<duckdb::Connection>,
    pool: Arc<Mutex<Vec<duckdb::Connection>>>,
    _permit: OwnedSemaphorePermit,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            // Return connection to pool - spawn a task to avoid blocking
            let pool = self.pool.clone();
            tokio::spawn(async move {
                pool.lock().await.push(conn);
            });
        }
    }
}

impl std::ops::Deref for ConnectionGuard {
    type Target = duckdb::Connection;
    fn deref(&self) -> &Self::Target {
        self.conn.as_ref().expect("Connection already returned")
    }
}

/// Executes a query on DuckDB and returns results as JSON values.
fn execute_query(
    conn: &duckdb::Connection,
    sql: &str,
) -> Result<(Vec<String>, Vec<Vec<serde_json::Value>>)> {
    let mut stmt = conn.prepare(sql)
        .with_context(|| format!("Failed to prepare DuckDB query: {sql}"))?;

    let mut rows_iter = stmt.query([])
        .with_context(|| format!("Failed to execute DuckDB query: {sql}"))?;

    // Get column info
    let column_count = rows_iter.as_ref().map(|r| r.column_count()).unwrap_or(0);
    let columns: Vec<String> = if let Some(first_row) = rows_iter.as_ref() {
        (0..column_count)
            .map(|i| {
                first_row
                    .column_name(i)
                    .map(|s| s.to_string())
                    .unwrap_or_else(|_| "?".to_string())
            })
            .collect()
    } else {
        vec![]
    };

    // Collect all rows
    let mut result_rows = Vec::new();
    while let Some(row) = rows_iter.next()? {
        let mut values = Vec::with_capacity(column_count);
        for i in 0..column_count {
            let value = row_to_json_value(row, i);
            values.push(value);
        }
        result_rows.push(values);
    }

    Ok((columns, result_rows))
}

/// Executes a streaming query on DuckDB, sending batches through a channel.
fn execute_query_streaming(
    conn: &duckdb::Connection,
    sql: &str,
    batch_size: usize,
    tx: tokio::sync::mpsc::Sender<Result<Vec<Vec<serde_json::Value>>>>,
) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(sql)
        .with_context(|| format!("Failed to prepare DuckDB query: {sql}"))?;

    let mut rows_iter = stmt.query([])
        .with_context(|| format!("Failed to execute DuckDB query: {sql}"))?;

    // Get column info
    let column_count = rows_iter.as_ref().map(|r| r.column_count()).unwrap_or(0);
    let columns: Vec<String> = if let Some(first_row) = rows_iter.as_ref() {
        (0..column_count)
            .map(|i| {
                first_row
                    .column_name(i)
                    .map(|s| s.to_string())
                    .unwrap_or_else(|_| "?".to_string())
            })
            .collect()
    } else {
        vec![]
    };

    // Stream rows in batches
    let mut batch = Vec::with_capacity(batch_size);
    while let Some(row) = rows_iter.next()? {
        let mut values = Vec::with_capacity(column_count);
        for i in 0..column_count {
            let value = row_to_json_value(row, i);
            values.push(value);
        }
        batch.push(values);

        if batch.len() >= batch_size {
            if tx.blocking_send(Ok(std::mem::take(&mut batch))).is_err() {
                break;
            }
            batch = Vec::with_capacity(batch_size);
        }
    }

    // Send remaining rows
    if !batch.is_empty() {
        let _ = tx.blocking_send(Ok(batch));
    }

    Ok(columns)
}

/// Converts a DuckDB row column to a JSON value.
fn row_to_json_value(row: &duckdb::Row<'_>, idx: usize) -> serde_json::Value {
    // Try i128 first (HUGEINT)
    if let Ok(v) = row.get::<_, i128>(idx) {
        if v >= i64::MIN as i128 && v <= i64::MAX as i128 {
            return serde_json::Value::Number((v as i64).into());
        }
        return serde_json::Value::String(v.to_string());
    }

    // Try i64
    if let Ok(v) = row.get::<_, i64>(idx) {
        return serde_json::Value::Number(v.into());
    }

    // Try i32
    if let Ok(v) = row.get::<_, i32>(idx) {
        return serde_json::Value::Number(v.into());
    }

    // Try i16
    if let Ok(v) = row.get::<_, i16>(idx) {
        return serde_json::Value::Number(v.into());
    }

    // Try f64
    if let Ok(v) = row.get::<_, f64>(idx) {
        if let Some(n) = serde_json::Number::from_f64(v) {
            return serde_json::Value::Number(n);
        }
        return serde_json::Value::String(v.to_string());
    }

    // Try bool
    if let Ok(v) = row.get::<_, bool>(idx) {
        return serde_json::Value::Bool(v);
    }

    // Try String
    if let Ok(v) = row.get::<_, String>(idx) {
        return serde_json::Value::String(v);
    }

    // NULL or unsupported type
    serde_json::Value::Null
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_engine_creation_without_postgres() {
        // This should gracefully fail since there's no Postgres to connect to
        let result = DuckDbPgEngine::new("postgresql://invalid:5432/test", 1);
        assert!(result.is_ok());
        let engine = result.unwrap();
        assert!(!engine.is_available());
    }
}
