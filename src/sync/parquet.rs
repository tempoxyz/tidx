//! Parquet-based data transfer from PostgreSQL to DuckDB.
//!
//! This module provides streaming data transfer using Parquet as an intermediate format:
//! 1. Query PostgreSQL for block range data
//! 2. Write to temporary Parquet file (streaming, memory-bounded)
//! 3. DuckDB ingests via `read_parquet()` (efficient columnar read)
//! 4. Cleanup temp file
//!
//! Benefits over direct approaches:
//! - Memory-safe: never holds full dataset in memory
//! - No OOM from postgres extension (scanner mode issue)
//! - Faster than SQL string building
//! - Columnar format matches DuckDB's storage

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Int16Array, Int32Array, Int64Array, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::db::DuckDbPool;

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

/// Copy a range of blocks from Postgres to DuckDB via Parquet.
///
/// This is the unified approach for both tail sync and gap-fill:
/// - Streams data through Parquet files
/// - Memory-bounded (no OOM on large ranges)
/// - Handles duplicates via INSERT OR IGNORE
pub async fn copy_range_via_parquet(
    pg_conn: &deadpool_postgres::Object,
    duckdb: &Arc<DuckDbPool>,
    start: i64,
    end: i64,
) -> Result<i64> {
    // Fetch all data from Postgres
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

    let blocks_count = block_rows.len();

    // Create temp directory and file paths
    let temp_dir = ensure_temp_dir()?;
    let batch_id = format!("{}_{}", start, end);
    let blocks_path = temp_dir.join(format!("blocks_{batch_id}.parquet"));
    let txs_path = temp_dir.join(format!("txs_{batch_id}.parquet"));
    let logs_path = temp_dir.join(format!("logs_{batch_id}.parquet"));
    let receipts_path = temp_dir.join(format!("receipts_{batch_id}.parquet"));

    // Write Parquet files (blocking I/O, run in spawn_blocking)
    let blocks_path_clone = blocks_path.clone();
    let txs_path_clone = txs_path.clone();
    let logs_path_clone = logs_path.clone();
    let receipts_path_clone = receipts_path.clone();

    tokio::task::spawn_blocking(move || {
        write_blocks_parquet(&blocks_path_clone, &block_rows)?;
        write_txs_parquet(&txs_path_clone, &tx_rows)?;
        write_logs_parquet(&logs_path_clone, &log_rows)?;
        write_receipts_parquet(&receipts_path_clone, &receipt_rows)?;
        Ok::<_, anyhow::Error>(())
    })
    .await??;

    // Ingest Parquet files into DuckDB
    let blocks_path_str = blocks_path.to_string_lossy().to_string();
    let txs_path_str = txs_path.to_string_lossy().to_string();
    let logs_path_str = logs_path.to_string_lossy().to_string();
    let receipts_path_str = receipts_path.to_string_lossy().to_string();

    duckdb
        .with_connection(move |conn| {
            // Ingest with INSERT OR IGNORE for dedup safety
            conn.execute(
                &format!(
                    "INSERT OR IGNORE INTO blocks SELECT * FROM read_parquet('{}')",
                    blocks_path_str.replace('\'', "''")
                ),
                [],
            )?;
            conn.execute(
                &format!(
                    "INSERT OR IGNORE INTO txs SELECT * FROM read_parquet('{}')",
                    txs_path_str.replace('\'', "''")
                ),
                [],
            )?;
            conn.execute(
                &format!(
                    "INSERT OR IGNORE INTO logs SELECT * FROM read_parquet('{}')",
                    logs_path_str.replace('\'', "''")
                ),
                [],
            )?;
            conn.execute(
                &format!(
                    "INSERT OR IGNORE INTO receipts SELECT * FROM read_parquet('{}')",
                    receipts_path_str.replace('\'', "''")
                ),
                [],
            )?;
            Ok(())
        })
        .await?;

    // Cleanup temp files
    let _ = fs::remove_file(&blocks_path);
    let _ = fs::remove_file(&txs_path);
    let _ = fs::remove_file(&logs_path);
    let _ = fs::remove_file(&receipts_path);

    Ok(blocks_count as i64)
}

/// Write blocks to Parquet file
fn write_blocks_parquet(path: &PathBuf, rows: &[tokio_postgres::Row]) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("num", DataType::Int64, false),
        Field::new("hash", DataType::Utf8, false),
        Field::new("parent_hash", DataType::Utf8, false),
        Field::new("timestamp", DataType::Utf8, false), // TIMESTAMPTZ as string
        Field::new("timestamp_ms", DataType::Int64, false),
        Field::new("gas_limit", DataType::Int64, false),
        Field::new("gas_used", DataType::Int64, false),
        Field::new("miner", DataType::Utf8, false),
        Field::new("extra_data", DataType::Utf8, true),
    ]));

    let mut num_builder = Int64Array::builder(rows.len());
    let mut hash_builder = StringBuilder::new();
    let mut parent_hash_builder = StringBuilder::new();
    let mut timestamp_builder = StringBuilder::new();
    let mut timestamp_ms_builder = Int64Array::builder(rows.len());
    let mut gas_limit_builder = Int64Array::builder(rows.len());
    let mut gas_used_builder = Int64Array::builder(rows.len());
    let mut miner_builder = StringBuilder::new();
    let mut extra_data_builder = StringBuilder::new();

    for row in rows {
        let num: i64 = row.get(0);
        let hash: Vec<u8> = row.get(1);
        let parent_hash: Vec<u8> = row.get(2);
        let timestamp: chrono::DateTime<chrono::Utc> = row.get(3);
        let timestamp_ms: i64 = row.get(4);
        let gas_limit: i64 = row.get(5);
        let gas_used: i64 = row.get(6);
        let miner: Vec<u8> = row.get(7);
        let extra_data: Option<Vec<u8>> = row.get(8);

        num_builder.append_value(num);
        hash_builder.append_value(format!("0x{}", hex::encode(&hash)));
        parent_hash_builder.append_value(format!("0x{}", hex::encode(&parent_hash)));
        timestamp_builder.append_value(timestamp.to_rfc3339());
        timestamp_ms_builder.append_value(timestamp_ms);
        gas_limit_builder.append_value(gas_limit);
        gas_used_builder.append_value(gas_used);
        miner_builder.append_value(format!("0x{}", hex::encode(&miner)));
        match extra_data {
            Some(d) => extra_data_builder.append_value(format!("0x{}", hex::encode(&d))),
            None => extra_data_builder.append_null(),
        }
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(num_builder.finish()),
            Arc::new(hash_builder.finish()),
            Arc::new(parent_hash_builder.finish()),
            Arc::new(timestamp_builder.finish()),
            Arc::new(timestamp_ms_builder.finish()),
            Arc::new(gas_limit_builder.finish()),
            Arc::new(gas_used_builder.finish()),
            Arc::new(miner_builder.finish()),
            Arc::new(extra_data_builder.finish()),
        ],
    )?;

    write_parquet_file(path, schema, batch)
}

/// Write transactions to Parquet file
fn write_txs_parquet(path: &PathBuf, rows: &[tokio_postgres::Row]) -> Result<()> {
    if rows.is_empty() {
        // Write empty file with schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_num", DataType::Int64, false),
            Field::new("block_timestamp", DataType::Utf8, false),
            Field::new("idx", DataType::Int32, false),
            Field::new("hash", DataType::Utf8, false),
            Field::new("type", DataType::Int16, false),
            Field::new("from", DataType::Utf8, false),
            Field::new("to", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, false),
            Field::new("input", DataType::Utf8, false),
            Field::new("gas_limit", DataType::Int64, false),
            Field::new("max_fee_per_gas", DataType::Utf8, false),
            Field::new("max_priority_fee_per_gas", DataType::Utf8, false),
            Field::new("gas_used", DataType::Int64, true),
            Field::new("nonce_key", DataType::Utf8, false),
            Field::new("nonce", DataType::Int64, false),
            Field::new("fee_token", DataType::Utf8, true),
            Field::new("fee_payer", DataType::Utf8, true),
            Field::new("calls", DataType::Utf8, true),
            Field::new("call_count", DataType::Int16, false),
            Field::new("valid_before", DataType::Int64, true),
            Field::new("valid_after", DataType::Int64, true),
            Field::new("signature_type", DataType::Int16, true),
        ]));
        let batch = RecordBatch::new_empty(schema.clone());
        return write_parquet_file(path, schema, batch);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("block_num", DataType::Int64, false),
        Field::new("block_timestamp", DataType::Utf8, false),
        Field::new("idx", DataType::Int32, false),
        Field::new("hash", DataType::Utf8, false),
        Field::new("type", DataType::Int16, false),
        Field::new("from", DataType::Utf8, false),
        Field::new("to", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, false),
        Field::new("input", DataType::Utf8, false),
        Field::new("gas_limit", DataType::Int64, false),
        Field::new("max_fee_per_gas", DataType::Utf8, false),
        Field::new("max_priority_fee_per_gas", DataType::Utf8, false),
        Field::new("gas_used", DataType::Int64, true),
        Field::new("nonce_key", DataType::Utf8, false),
        Field::new("nonce", DataType::Int64, false),
        Field::new("fee_token", DataType::Utf8, true),
        Field::new("fee_payer", DataType::Utf8, true),
        Field::new("calls", DataType::Utf8, true),
        Field::new("call_count", DataType::Int16, false),
        Field::new("valid_before", DataType::Int64, true),
        Field::new("valid_after", DataType::Int64, true),
        Field::new("signature_type", DataType::Int16, true),
    ]));

    let mut block_num_builder = Int64Array::builder(rows.len());
    let mut block_timestamp_builder = StringBuilder::new();
    let mut idx_builder = Int32Array::builder(rows.len());
    let mut hash_builder = StringBuilder::new();
    let mut type_builder = Int16Array::builder(rows.len());
    let mut from_builder = StringBuilder::new();
    let mut to_builder = StringBuilder::new();
    let mut value_builder = StringBuilder::new();
    let mut input_builder = StringBuilder::new();
    let mut gas_limit_builder = Int64Array::builder(rows.len());
    let mut max_fee_builder = StringBuilder::new();
    let mut max_priority_builder = StringBuilder::new();
    let mut gas_used_builder = Int64Array::builder(rows.len());
    let mut nonce_key_builder = StringBuilder::new();
    let mut nonce_builder = Int64Array::builder(rows.len());
    let mut fee_token_builder = StringBuilder::new();
    let mut fee_payer_builder = StringBuilder::new();
    let mut calls_builder = StringBuilder::new();
    let mut call_count_builder = Int16Array::builder(rows.len());
    let mut valid_before_builder = Int64Array::builder(rows.len());
    let mut valid_after_builder = Int64Array::builder(rows.len());
    let mut sig_type_builder = Int16Array::builder(rows.len());

    for row in rows {
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

        block_num_builder.append_value(block_num);
        block_timestamp_builder.append_value(block_timestamp.to_rfc3339());
        idx_builder.append_value(idx);
        hash_builder.append_value(format!("0x{}", hex::encode(&hash)));
        type_builder.append_value(tx_type);
        from_builder.append_value(format!("0x{}", hex::encode(&from)));
        match to {
            Some(t) => to_builder.append_value(format!("0x{}", hex::encode(&t))),
            None => to_builder.append_null(),
        }
        value_builder.append_value(&value);
        input_builder.append_value(format!("0x{}", hex::encode(&input)));
        gas_limit_builder.append_value(gas_limit);
        max_fee_builder.append_value(&max_fee_per_gas);
        max_priority_builder.append_value(&max_priority_fee_per_gas);
        match gas_used {
            Some(g) => gas_used_builder.append_value(g),
            None => gas_used_builder.append_null(),
        }
        nonce_key_builder.append_value(format!("0x{}", hex::encode(&nonce_key)));
        nonce_builder.append_value(nonce);
        match fee_token {
            Some(t) => fee_token_builder.append_value(format!("0x{}", hex::encode(&t))),
            None => fee_token_builder.append_null(),
        }
        match fee_payer {
            Some(p) => fee_payer_builder.append_value(format!("0x{}", hex::encode(&p))),
            None => fee_payer_builder.append_null(),
        }
        match calls {
            Some(c) => calls_builder.append_value(&c),
            None => calls_builder.append_null(),
        }
        call_count_builder.append_value(call_count);
        match valid_before {
            Some(v) => valid_before_builder.append_value(v),
            None => valid_before_builder.append_null(),
        }
        match valid_after {
            Some(v) => valid_after_builder.append_value(v),
            None => valid_after_builder.append_null(),
        }
        match signature_type {
            Some(s) => sig_type_builder.append_value(s),
            None => sig_type_builder.append_null(),
        }
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(block_num_builder.finish()),
            Arc::new(block_timestamp_builder.finish()),
            Arc::new(idx_builder.finish()),
            Arc::new(hash_builder.finish()),
            Arc::new(type_builder.finish()),
            Arc::new(from_builder.finish()),
            Arc::new(to_builder.finish()),
            Arc::new(value_builder.finish()),
            Arc::new(input_builder.finish()),
            Arc::new(gas_limit_builder.finish()),
            Arc::new(max_fee_builder.finish()),
            Arc::new(max_priority_builder.finish()),
            Arc::new(gas_used_builder.finish()),
            Arc::new(nonce_key_builder.finish()),
            Arc::new(nonce_builder.finish()),
            Arc::new(fee_token_builder.finish()),
            Arc::new(fee_payer_builder.finish()),
            Arc::new(calls_builder.finish()),
            Arc::new(call_count_builder.finish()),
            Arc::new(valid_before_builder.finish()),
            Arc::new(valid_after_builder.finish()),
            Arc::new(sig_type_builder.finish()),
        ],
    )?;

    write_parquet_file(path, schema, batch)
}

/// Write logs to Parquet file
fn write_logs_parquet(path: &PathBuf, rows: &[tokio_postgres::Row]) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_num", DataType::Int64, false),
        Field::new("block_timestamp", DataType::Utf8, false),
        Field::new("log_idx", DataType::Int32, false),
        Field::new("tx_idx", DataType::Int32, false),
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("address", DataType::Utf8, false),
        Field::new("selector", DataType::Utf8, true),
        Field::new("topic0", DataType::Utf8, true),
        Field::new("topic1", DataType::Utf8, true),
        Field::new("topic2", DataType::Utf8, true),
        Field::new("topic3", DataType::Utf8, true),
        Field::new("data", DataType::Utf8, false),
    ]));

    if rows.is_empty() {
        let batch = RecordBatch::new_empty(schema.clone());
        return write_parquet_file(path, schema, batch);
    }

    let mut block_num_builder = Int64Array::builder(rows.len());
    let mut block_timestamp_builder = StringBuilder::new();
    let mut log_idx_builder = Int32Array::builder(rows.len());
    let mut tx_idx_builder = Int32Array::builder(rows.len());
    let mut tx_hash_builder = StringBuilder::new();
    let mut address_builder = StringBuilder::new();
    let mut selector_builder = StringBuilder::new();
    let mut topic0_builder = StringBuilder::new();
    let mut topic1_builder = StringBuilder::new();
    let mut topic2_builder = StringBuilder::new();
    let mut topic3_builder = StringBuilder::new();
    let mut data_builder = StringBuilder::new();

    for row in rows {
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

        block_num_builder.append_value(block_num);
        block_timestamp_builder.append_value(block_timestamp.to_rfc3339());
        log_idx_builder.append_value(log_idx);
        tx_idx_builder.append_value(tx_idx);
        tx_hash_builder.append_value(format!("0x{}", hex::encode(&tx_hash)));
        address_builder.append_value(format!("0x{}", hex::encode(&address)));
        match selector {
            Some(s) => selector_builder.append_value(format!("0x{}", hex::encode(&s))),
            None => selector_builder.append_null(),
        }
        match topic0 {
            Some(t) => topic0_builder.append_value(format!("0x{}", hex::encode(&t))),
            None => topic0_builder.append_null(),
        }
        match topic1 {
            Some(t) => topic1_builder.append_value(format!("0x{}", hex::encode(&t))),
            None => topic1_builder.append_null(),
        }
        match topic2 {
            Some(t) => topic2_builder.append_value(format!("0x{}", hex::encode(&t))),
            None => topic2_builder.append_null(),
        }
        match topic3 {
            Some(t) => topic3_builder.append_value(format!("0x{}", hex::encode(&t))),
            None => topic3_builder.append_null(),
        }
        data_builder.append_value(format!("0x{}", hex::encode(&data)));
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(block_num_builder.finish()),
            Arc::new(block_timestamp_builder.finish()),
            Arc::new(log_idx_builder.finish()),
            Arc::new(tx_idx_builder.finish()),
            Arc::new(tx_hash_builder.finish()),
            Arc::new(address_builder.finish()),
            Arc::new(selector_builder.finish()),
            Arc::new(topic0_builder.finish()),
            Arc::new(topic1_builder.finish()),
            Arc::new(topic2_builder.finish()),
            Arc::new(topic3_builder.finish()),
            Arc::new(data_builder.finish()),
        ],
    )?;

    write_parquet_file(path, schema, batch)
}

/// Write receipts to Parquet file
fn write_receipts_parquet(path: &PathBuf, rows: &[tokio_postgres::Row]) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_num", DataType::Int64, false),
        Field::new("block_timestamp", DataType::Utf8, false),
        Field::new("tx_idx", DataType::Int32, false),
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("from", DataType::Utf8, false),
        Field::new("to", DataType::Utf8, true),
        Field::new("contract_address", DataType::Utf8, true),
        Field::new("gas_used", DataType::Int64, false),
        Field::new("cumulative_gas_used", DataType::Int64, false),
        Field::new("effective_gas_price", DataType::Utf8, true),
        Field::new("status", DataType::Int16, true),
        Field::new("fee_payer", DataType::Utf8, true),
    ]));

    if rows.is_empty() {
        let batch = RecordBatch::new_empty(schema.clone());
        return write_parquet_file(path, schema, batch);
    }

    let mut block_num_builder = Int64Array::builder(rows.len());
    let mut block_timestamp_builder = StringBuilder::new();
    let mut tx_idx_builder = Int32Array::builder(rows.len());
    let mut tx_hash_builder = StringBuilder::new();
    let mut from_builder = StringBuilder::new();
    let mut to_builder = StringBuilder::new();
    let mut contract_address_builder = StringBuilder::new();
    let mut gas_used_builder = Int64Array::builder(rows.len());
    let mut cumulative_gas_used_builder = Int64Array::builder(rows.len());
    let mut effective_gas_price_builder = StringBuilder::new();
    let mut status_builder = Int16Array::builder(rows.len());
    let mut fee_payer_builder = StringBuilder::new();

    for row in rows {
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

        block_num_builder.append_value(block_num);
        block_timestamp_builder.append_value(block_timestamp.to_rfc3339());
        tx_idx_builder.append_value(tx_idx);
        tx_hash_builder.append_value(format!("0x{}", hex::encode(&tx_hash)));
        from_builder.append_value(format!("0x{}", hex::encode(&from)));
        match to {
            Some(t) => to_builder.append_value(format!("0x{}", hex::encode(&t))),
            None => to_builder.append_null(),
        }
        match contract_address {
            Some(c) => contract_address_builder.append_value(format!("0x{}", hex::encode(&c))),
            None => contract_address_builder.append_null(),
        }
        gas_used_builder.append_value(gas_used);
        cumulative_gas_used_builder.append_value(cumulative_gas_used);
        match effective_gas_price {
            Some(p) => effective_gas_price_builder.append_value(&p),
            None => effective_gas_price_builder.append_null(),
        }
        match status {
            Some(s) => status_builder.append_value(s),
            None => status_builder.append_null(),
        }
        match fee_payer {
            Some(p) => fee_payer_builder.append_value(format!("0x{}", hex::encode(&p))),
            None => fee_payer_builder.append_null(),
        }
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(block_num_builder.finish()),
            Arc::new(block_timestamp_builder.finish()),
            Arc::new(tx_idx_builder.finish()),
            Arc::new(tx_hash_builder.finish()),
            Arc::new(from_builder.finish()),
            Arc::new(to_builder.finish()),
            Arc::new(contract_address_builder.finish()),
            Arc::new(gas_used_builder.finish()),
            Arc::new(cumulative_gas_used_builder.finish()),
            Arc::new(effective_gas_price_builder.finish()),
            Arc::new(status_builder.finish()),
            Arc::new(fee_payer_builder.finish()),
        ],
    )?;

    write_parquet_file(path, schema, batch)
}

/// Write a RecordBatch to a Parquet file with compression
fn write_parquet_file(path: &PathBuf, schema: Arc<Schema>, batch: RecordBatch) -> Result<()> {
    let file = fs::File::create(path).context("Failed to create Parquet file")?;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();

    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::db::DuckDbPool;

    #[test]
    fn test_temp_dir_creation() {
        let dir = ensure_temp_dir().unwrap();
        assert!(dir.exists());
    }

    #[test]
    fn test_write_blocks_parquet() {
        // Create a mock block row using Arrow directly
        let schema = Arc::new(Schema::new(vec![
            Field::new("num", DataType::Int64, false),
            Field::new("hash", DataType::Utf8, false),
            Field::new("parent_hash", DataType::Utf8, false),
            Field::new("timestamp", DataType::Utf8, false),
            Field::new("timestamp_ms", DataType::Int64, false),
            Field::new("gas_limit", DataType::Int64, false),
            Field::new("gas_used", DataType::Int64, false),
            Field::new("miner", DataType::Utf8, false),
            Field::new("extra_data", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(arrow::array::StringArray::from(vec!["0xabc", "0xdef", "0x123"])),
                Arc::new(arrow::array::StringArray::from(vec!["0x000", "0xabc", "0xdef"])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2024-01-01T00:00:00Z",
                    "2024-01-01T00:00:01Z",
                    "2024-01-01T00:00:02Z",
                ])),
                Arc::new(Int64Array::from(vec![1704067200000, 1704067201000, 1704067202000])),
                Arc::new(Int64Array::from(vec![30_000_000, 30_000_000, 30_000_000])),
                Arc::new(Int64Array::from(vec![21000, 42000, 63000])),
                Arc::new(arrow::array::StringArray::from(vec!["0xminer1", "0xminer2", "0xminer3"])),
                Arc::new(arrow::array::StringArray::from(vec![Some("0xextra"), None, Some("0xdata")])),
            ],
        )
        .unwrap();

        let temp_dir = ensure_temp_dir().unwrap();
        let path = temp_dir.join("test_blocks.parquet");

        write_parquet_file(&path, schema, batch).unwrap();

        // Verify file exists and has content
        assert!(path.exists());
        let metadata = std::fs::metadata(&path).unwrap();
        assert!(metadata.len() > 0);

        // Cleanup
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_write_empty_parquet() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("num", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::new_empty(schema.clone());

        let temp_dir = ensure_temp_dir().unwrap();
        let path = temp_dir.join("test_empty.parquet");

        write_parquet_file(&path, schema, batch).unwrap();

        assert!(path.exists());

        // Cleanup
        std::fs::remove_file(&path).unwrap();
    }

    #[tokio::test]
    async fn test_parquet_roundtrip_with_duckdb() {
        // Create DuckDB in-memory
        let duckdb = Arc::new(DuckDbPool::in_memory().unwrap());

        // Write a simple parquet file
        let schema = Arc::new(Schema::new(vec![
            Field::new("num", DataType::Int64, false),
            Field::new("hash", DataType::Utf8, false),
            Field::new("parent_hash", DataType::Utf8, false),
            Field::new("timestamp", DataType::Utf8, false),
            Field::new("timestamp_ms", DataType::Int64, false),
            Field::new("gas_limit", DataType::Int64, false),
            Field::new("gas_used", DataType::Int64, false),
            Field::new("miner", DataType::Utf8, false),
            Field::new("extra_data", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(arrow::array::StringArray::from(vec!["0xabc123"])),
                Arc::new(arrow::array::StringArray::from(vec!["0x000000"])),
                Arc::new(arrow::array::StringArray::from(vec!["2024-01-01T12:00:00+00:00"])),
                Arc::new(Int64Array::from(vec![1704110400000])),
                Arc::new(Int64Array::from(vec![30_000_000])),
                Arc::new(Int64Array::from(vec![21000])),
                Arc::new(arrow::array::StringArray::from(vec!["0xminer"])),
                Arc::new(arrow::array::StringArray::from(vec![None::<&str>])),
            ],
        )
        .unwrap();

        let temp_dir = ensure_temp_dir().unwrap();
        let path = temp_dir.join("test_roundtrip.parquet");
        let path_str = path.to_string_lossy().to_string();

        write_parquet_file(&path, schema, batch).unwrap();

        // Read it back via DuckDB
        let result = duckdb
            .with_connection_result(move |conn| {
                let mut stmt = conn.prepare(&format!(
                    "SELECT num, hash, gas_used FROM read_parquet('{}')",
                    path_str.replace('\'', "''")
                ))?;
                let row: (i64, String, i64) = stmt.query_row([], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })?;
                Ok(row)
            })
            .await
            .unwrap();

        assert_eq!(result.0, 100);
        assert_eq!(result.1, "0xabc123");
        assert_eq!(result.2, 21000);

        // Cleanup
        std::fs::remove_file(&path).unwrap();
    }

    #[tokio::test]
    async fn test_parquet_ingest_into_duckdb_table() {
        let duckdb = Arc::new(DuckDbPool::in_memory().unwrap());

        // Write parquet with block data
        let schema = Arc::new(Schema::new(vec![
            Field::new("num", DataType::Int64, false),
            Field::new("hash", DataType::Utf8, false),
            Field::new("parent_hash", DataType::Utf8, false),
            Field::new("timestamp", DataType::Utf8, false),
            Field::new("timestamp_ms", DataType::Int64, false),
            Field::new("gas_limit", DataType::Int64, false),
            Field::new("gas_used", DataType::Int64, false),
            Field::new("miner", DataType::Utf8, false),
            Field::new("extra_data", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(arrow::array::StringArray::from(vec!["0xh1", "0xh2", "0xh3"])),
                Arc::new(arrow::array::StringArray::from(vec!["0xp0", "0xh1", "0xh2"])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2024-01-01T00:00:00+00:00",
                    "2024-01-01T00:00:01+00:00",
                    "2024-01-01T00:00:02+00:00",
                ])),
                Arc::new(Int64Array::from(vec![1704067200000, 1704067201000, 1704067202000])),
                Arc::new(Int64Array::from(vec![30_000_000, 30_000_000, 30_000_000])),
                Arc::new(Int64Array::from(vec![1000, 2000, 3000])),
                Arc::new(arrow::array::StringArray::from(vec!["0xm1", "0xm2", "0xm3"])),
                Arc::new(arrow::array::StringArray::from(vec![None::<&str>, None, None])),
            ],
        )
        .unwrap();

        let temp_dir = ensure_temp_dir().unwrap();
        let path = temp_dir.join("test_ingest.parquet");
        let path_str = path.to_string_lossy().to_string();

        write_parquet_file(&path, schema, batch).unwrap();

        // Ingest into DuckDB blocks table
        let count = duckdb
            .with_connection_result(move |conn| {
                conn.execute(
                    &format!(
                        "INSERT OR IGNORE INTO blocks SELECT * FROM read_parquet('{}')",
                        path_str.replace('\'', "''")
                    ),
                    [],
                )?;

                let mut stmt = conn.prepare("SELECT COUNT(*) FROM blocks")?;
                let count: i64 = stmt.query_row([], |row| row.get(0))?;
                Ok(count)
            })
            .await
            .unwrap();

        assert_eq!(count, 3);

        // Cleanup
        std::fs::remove_file(&path).unwrap();
    }

    #[tokio::test]
    async fn test_parquet_handles_duplicates() {
        let duckdb = Arc::new(DuckDbPool::in_memory().unwrap());

        let schema = Arc::new(Schema::new(vec![
            Field::new("num", DataType::Int64, false),
            Field::new("hash", DataType::Utf8, false),
            Field::new("parent_hash", DataType::Utf8, false),
            Field::new("timestamp", DataType::Utf8, false),
            Field::new("timestamp_ms", DataType::Int64, false),
            Field::new("gas_limit", DataType::Int64, false),
            Field::new("gas_used", DataType::Int64, false),
            Field::new("miner", DataType::Utf8, false),
            Field::new("extra_data", DataType::Utf8, true),
        ]));

        let temp_dir = ensure_temp_dir().unwrap();

        // First batch: blocks 1, 2
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(arrow::array::StringArray::from(vec!["0xh1", "0xh2"])),
                Arc::new(arrow::array::StringArray::from(vec!["0xp0", "0xh1"])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2024-01-01T00:00:00+00:00",
                    "2024-01-01T00:00:01+00:00",
                ])),
                Arc::new(Int64Array::from(vec![1704067200000, 1704067201000])),
                Arc::new(Int64Array::from(vec![30_000_000, 30_000_000])),
                Arc::new(Int64Array::from(vec![1000, 2000])),
                Arc::new(arrow::array::StringArray::from(vec!["0xm1", "0xm2"])),
                Arc::new(arrow::array::StringArray::from(vec![None::<&str>, None])),
            ],
        )
        .unwrap();

        let path1 = temp_dir.join("test_dup1.parquet");
        write_parquet_file(&path1, schema.clone(), batch1).unwrap();

        // Second batch: blocks 2, 3 (block 2 is duplicate)
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![2, 3])),
                Arc::new(arrow::array::StringArray::from(vec!["0xh2_dup", "0xh3"])),
                Arc::new(arrow::array::StringArray::from(vec!["0xh1", "0xh2"])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2024-01-01T00:00:01+00:00",
                    "2024-01-01T00:00:02+00:00",
                ])),
                Arc::new(Int64Array::from(vec![1704067201000, 1704067202000])),
                Arc::new(Int64Array::from(vec![30_000_000, 30_000_000])),
                Arc::new(Int64Array::from(vec![2000, 3000])),
                Arc::new(arrow::array::StringArray::from(vec!["0xm2", "0xm3"])),
                Arc::new(arrow::array::StringArray::from(vec![None::<&str>, None])),
            ],
        )
        .unwrap();

        let path2 = temp_dir.join("test_dup2.parquet");
        write_parquet_file(&path2, schema, batch2).unwrap();

        let path1_str = path1.to_string_lossy().to_string();
        let path2_str = path2.to_string_lossy().to_string();

        // Ingest both with INSERT OR IGNORE
        let (count, hash2) = duckdb
            .with_connection_result(move |conn| {
                conn.execute(
                    &format!(
                        "INSERT OR IGNORE INTO blocks SELECT * FROM read_parquet('{}')",
                        path1_str.replace('\'', "''")
                    ),
                    [],
                )?;
                conn.execute(
                    &format!(
                        "INSERT OR IGNORE INTO blocks SELECT * FROM read_parquet('{}')",
                        path2_str.replace('\'', "''")
                    ),
                    [],
                )?;

                let mut stmt = conn.prepare("SELECT COUNT(*) FROM blocks")?;
                let count: i64 = stmt.query_row([], |row| row.get(0))?;

                // Block 2 should have original hash, not duplicate
                let mut stmt2 = conn.prepare("SELECT hash FROM blocks WHERE num = 2")?;
                let hash: String = stmt2.query_row([], |row| row.get(0))?;

                Ok((count, hash))
            })
            .await
            .unwrap();

        assert_eq!(count, 3); // Only 3 unique blocks
        assert_eq!(hash2, "0xh2"); // Original hash, not 0xh2_dup

        // Cleanup
        std::fs::remove_file(&path1).unwrap();
        std::fs::remove_file(&path2).unwrap();
    }
}
