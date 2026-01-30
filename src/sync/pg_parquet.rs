//! pg_parquet-based data transfer from PostgreSQL to DuckDB.
//!
//! Uses the pg_parquet extension for server-side Parquet generation via COPY TO STDOUT.
//! This is significantly faster than client-side Arrow building because:
//! - No row-by-row protocol overhead (COPY is Postgres's fastest export path)
//! - No client-side hex encoding or Arrow builder allocations
//! - Parquet generation happens in native code on the server
//!
//! Requirements:
//! - pg_parquet extension must be installed on PostgreSQL server
//! - Extension must be created: CREATE EXTENSION pg_parquet;

use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use futures::StreamExt;

use crate::db::{DuckDbPool, Pool};

/// Table kind for per-table gap-fill operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableKind {
    Blocks,
    Txs,
    Logs,
    Receipts,
}

impl TableKind {
    pub fn name(&self) -> &'static str {
        match self {
            TableKind::Blocks => "blocks",
            TableKind::Txs => "txs",
            TableKind::Logs => "logs",
            TableKind::Receipts => "receipts",
        }
    }

    /// Block column name for DELETE/range queries
    pub fn block_column(&self) -> &'static str {
        match self {
            TableKind::Blocks => "num",
            _ => "block_num",
        }
    }

    /// Recommended batch size (block range) for gap-fill.
    /// Tuned per table: logs are heavy, blocks are light.
    pub fn batch_size(&self) -> i64 {
        match self {
            TableKind::Blocks => 10_000,   // Very lightweight
            TableKind::Txs => 2_000,       // Medium weight
            TableKind::Logs => 2_000,      // Heavy but batched for throughput
            TableKind::Receipts => 2_000,  // Medium weight
        }
    }
}

/// Temporary directory for Parquet files
fn temp_dir() -> PathBuf {
    std::env::temp_dir().join("tidx_parquet")
}

/// Ensure temp directory exists
fn ensure_temp_dir() -> Result<PathBuf> {
    let dir = temp_dir();
    fs::create_dir_all(&dir).context("Failed to create temp directory for Parquet files")?;
    Ok(dir)
}

/// Copy a single table's block range from Postgres to DuckDB via pg_parquet COPY TO STDOUT.
///
/// This replaces the Arrow-based streaming approach with server-side Parquet generation.
/// The flow is:
/// 1. COPY (SELECT ...) TO STDOUT (FORMAT 'parquet', COMPRESSION 'zstd')
/// 2. Stream bytes to local temp file
/// 3. DuckDB ingests via read_parquet()
pub async fn copy_table_via_pg_parquet(
    pg_pool: &Pool,
    duckdb: &Arc<DuckDbPool>,
    table: TableKind,
    start: i64,
    end: i64,
) -> Result<i64> {
    
    let temp_dir = ensure_temp_dir()?;
    let batch_id = format!("{}_{}_{}_{}", table.name(), start, end, std::process::id());
    let parquet_path = temp_dir.join(format!("{batch_id}.parquet"));

    let pg_conn = pg_pool.get().await?;

    // Build the COPY query with hex encoding pushed to SQL
    let copy_query = match table {
        TableKind::Blocks => format!(
            r#"COPY (
                SELECT 
                    num,
                    '0x' || encode(hash, 'hex') as hash,
                    '0x' || encode(parent_hash, 'hex') as parent_hash,
                    to_char(timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as timestamp,
                    timestamp_ms,
                    gas_limit,
                    gas_used,
                    '0x' || encode(miner, 'hex') as miner,
                    CASE WHEN extra_data IS NOT NULL THEN '0x' || encode(extra_data, 'hex') ELSE NULL END as extra_data
                FROM blocks 
                WHERE num >= {} AND num <= {} 
                ORDER BY num
            ) TO STDOUT (FORMAT 'parquet', COMPRESSION 'zstd')"#,
            start, end
        ),
        TableKind::Txs => format!(
            r#"COPY (
                SELECT 
                    block_num,
                    to_char(block_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as block_timestamp,
                    idx,
                    '0x' || encode(hash, 'hex') as hash,
                    type,
                    '0x' || encode("from", 'hex') as "from",
                    CASE WHEN "to" IS NOT NULL THEN '0x' || encode("to", 'hex') ELSE NULL END as "to",
                    value,
                    '0x' || encode(input, 'hex') as input,
                    gas_limit,
                    max_fee_per_gas,
                    max_priority_fee_per_gas,
                    gas_used,
                    '0x' || encode(nonce_key, 'hex') as nonce_key,
                    nonce,
                    CASE WHEN fee_token IS NOT NULL THEN '0x' || encode(fee_token, 'hex') ELSE NULL END as fee_token,
                    CASE WHEN fee_payer IS NOT NULL THEN '0x' || encode(fee_payer, 'hex') ELSE NULL END as fee_payer,
                    calls::text as calls,
                    call_count,
                    valid_before,
                    valid_after,
                    signature_type
                FROM txs 
                WHERE block_num >= {} AND block_num <= {} 
                ORDER BY block_num, idx
            ) TO STDOUT (FORMAT 'parquet', COMPRESSION 'zstd')"#,
            start, end
        ),
        TableKind::Logs => format!(
            r#"COPY (
                SELECT 
                    block_num,
                    to_char(block_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as block_timestamp,
                    log_idx,
                    tx_idx,
                    '0x' || encode(tx_hash, 'hex') as tx_hash,
                    '0x' || encode(address, 'hex') as address,
                    CASE WHEN selector IS NOT NULL THEN '0x' || encode(selector, 'hex') ELSE NULL END as selector,
                    CASE WHEN topic0 IS NOT NULL THEN '0x' || encode(topic0, 'hex') ELSE NULL END as topic0,
                    CASE WHEN topic1 IS NOT NULL THEN '0x' || encode(topic1, 'hex') ELSE NULL END as topic1,
                    CASE WHEN topic2 IS NOT NULL THEN '0x' || encode(topic2, 'hex') ELSE NULL END as topic2,
                    CASE WHEN topic3 IS NOT NULL THEN '0x' || encode(topic3, 'hex') ELSE NULL END as topic3,
                    '0x' || encode(data, 'hex') as data
                FROM logs 
                WHERE block_num >= {} AND block_num <= {} 
                ORDER BY block_num, log_idx
            ) TO STDOUT (FORMAT 'parquet', COMPRESSION 'zstd')"#,
            start, end
        ),
        TableKind::Receipts => format!(
            r#"COPY (
                SELECT 
                    block_num,
                    to_char(block_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as block_timestamp,
                    tx_idx,
                    '0x' || encode(tx_hash, 'hex') as tx_hash,
                    '0x' || encode("from", 'hex') as "from",
                    CASE WHEN "to" IS NOT NULL THEN '0x' || encode("to", 'hex') ELSE NULL END as "to",
                    CASE WHEN contract_address IS NOT NULL THEN '0x' || encode(contract_address, 'hex') ELSE NULL END as contract_address,
                    gas_used,
                    cumulative_gas_used,
                    effective_gas_price,
                    status,
                    CASE WHEN fee_payer IS NOT NULL THEN '0x' || encode(fee_payer, 'hex') ELSE NULL END as fee_payer
                FROM receipts 
                WHERE block_num >= {} AND block_num <= {} 
                ORDER BY block_num, tx_idx
            ) TO STDOUT (FORMAT 'parquet', COMPRESSION 'zstd')"#,
            start, end
        ),
    };

    // Execute COPY TO STDOUT and stream bytes to file
    let stream = pg_conn.copy_out(&copy_query).await?;
    futures::pin_mut!(stream);

    let mut file = File::create(&parquet_path)
        .context("Failed to create parquet file")?;
    
    let mut total_bytes = 0usize;
    while let Some(chunk) = stream.next().await {
        let bytes = chunk?;
        file.write_all(&bytes)?;
        total_bytes += bytes.len();
    }
    file.flush()?;
    drop(file);

    if total_bytes == 0 {
        let _ = fs::remove_file(&parquet_path);
        return Ok(0);
    }

    // Count rows by querying the parquet file (DuckDB can do this efficiently)
    let parquet_path_str = parquet_path.to_string_lossy().to_string();
    let row_count: i64 = duckdb
        .with_connection_result(move |conn| {
            let count: i64 = conn
                .prepare(&format!(
                    "SELECT COUNT(*) FROM read_parquet('{}')",
                    parquet_path_str.replace('\'', "''")
                ))?
                .query_row([], |row| row.get(0))?;
            Ok(count)
        })
        .await?;

    if row_count == 0 {
        let _ = fs::remove_file(&parquet_path);
        return Ok(0);
    }

    // Ingest into DuckDB with atomic DELETE + INSERT
    let parquet_path_str = parquet_path.to_string_lossy().to_string();
    let table_name = table.name().to_string();
    let block_col = table.block_column().to_string();

    duckdb
        .with_connection(move |conn| {
            conn.execute("BEGIN TRANSACTION", [])?;

            // Delete existing data in range
            conn.execute(
                &format!(
                    "DELETE FROM {} WHERE {} BETWEEN {} AND {}",
                    table_name, block_col, start, end
                ),
                [],
            )?;

            // Insert from Parquet
            conn.execute(
                &format!(
                    "INSERT INTO {} SELECT * FROM read_parquet('{}')",
                    table_name,
                    parquet_path_str.replace('\'', "''")
                ),
                [],
            )?;

            conn.execute("COMMIT", [])?;
            Ok(())
        })
        .await?;

    // Cleanup temp file
    let _ = fs::remove_file(&parquet_path);

    Ok(row_count)
}

/// Copy all tables for a block range using pg_parquet (parallel).
pub async fn copy_range_via_pg_parquet(
    pg_pool: &Pool,
    duckdb: &Arc<DuckDbPool>,
    start: i64,
    end: i64,
) -> Result<i64> {
    // Run all 4 tables in parallel
    let (blocks, txs, logs, receipts) = tokio::try_join!(
        copy_table_via_pg_parquet(pg_pool, duckdb, TableKind::Blocks, start, end),
        copy_table_via_pg_parquet(pg_pool, duckdb, TableKind::Txs, start, end),
        copy_table_via_pg_parquet(pg_pool, duckdb, TableKind::Logs, start, end),
        copy_table_via_pg_parquet(pg_pool, duckdb, TableKind::Receipts, start, end),
    )?;

    tracing::debug!(
        start,
        end,
        blocks,
        txs,
        logs,
        receipts,
        "pg_parquet copy_range completed"
    );

    Ok(blocks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_temp_dir() {
        let dir = temp_dir();
        assert!(dir.to_string_lossy().contains("tidx_parquet"));
    }
}
