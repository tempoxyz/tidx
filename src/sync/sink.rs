use anyhow::Result;
use tracing::info;

use crate::db::Pool;
use crate::types::{BlockRow, LogRow, ReceiptRow, TxRow};

use super::ch_sink::ClickHouseSink;
use super::writer;

/// Number of blocks worth of data to fetch per query during backfill.
/// Uses block-range pagination (no long-lived transactions).
const BACKFILL_BLOCK_BATCH: i64 = 5_000;

/// Fan-out writer that sends data to all configured sinks.
///
/// PostgreSQL is always present. ClickHouse is optional.
/// Write failures from either sink are fatal and propagate to the caller.
#[derive(Clone)]
pub struct SinkSet {
    pool: Pool,
    ch: Option<ClickHouseSink>,
}

impl SinkSet {
    pub fn new(pool: Pool) -> Self {
        Self { pool, ch: None }
    }

    pub fn with_clickhouse(mut self, ch: ClickHouseSink) -> Self {
        self.ch = Some(ch);
        self
    }

    /// Access the underlying PG pool for read operations (sync state, gap detection, etc.)
    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    pub async fn write_blocks(&self, blocks: &[BlockRow]) -> Result<()> {
        writer::write_blocks(&self.pool, blocks).await?;
        if let Some(ch) = &self.ch {
            ch.write_blocks(blocks).await?;
        }
        Ok(())
    }

    pub async fn write_txs(&self, txs: &[TxRow]) -> Result<()> {
        writer::write_txs(&self.pool, txs).await?;
        if let Some(ch) = &self.ch {
            ch.write_txs(txs).await?;
        }
        Ok(())
    }

    pub async fn write_logs(&self, logs: &[LogRow]) -> Result<()> {
        writer::write_logs(&self.pool, logs).await?;
        if let Some(ch) = &self.ch {
            ch.write_logs(logs).await?;
        }
        Ok(())
    }

    pub async fn write_receipts(&self, receipts: &[ReceiptRow]) -> Result<()> {
        writer::write_receipts(&self.pool, receipts).await?;
        if let Some(ch) = &self.ch {
            ch.write_receipts(receipts).await?;
        }
        Ok(())
    }

    /// Delete all data from a given block number onwards (reorg support).
    /// Returns the number of blocks deleted from PostgreSQL.
    pub async fn delete_from(&self, block_num: u64) -> Result<u64> {
        let deleted = writer::delete_blocks_from(&self.pool, block_num).await?;
        if let Some(ch) = &self.ch {
            ch.delete_from(block_num).await?;
        }
        Ok(deleted)
    }

    /// Automatically backfill ClickHouse from PostgreSQL if CH is behind.
    ///
    /// Each table is backfilled independently from its own high-water mark
    /// so that txs/logs/receipts are repaired even if blocks are already present.
    /// Uses block-range pagination (no long-lived transactions) to avoid
    /// holding PG snapshots that block autovacuum.
    pub async fn backfill_clickhouse(&self) -> Result<()> {
        let ch = match &self.ch {
            Some(ch) => ch,
            None => return Ok(()),
        };

        let pg_max = pg_max_block_num(&self.pool).await?;
        let pg_max = match pg_max {
            Some(n) => n,
            None => return Ok(()), // PG is empty, nothing to backfill
        };

        let start = std::time::Instant::now();

        let blocks_written = backfill_table(
            &self.pool, ch, "blocks", pg_max,
            |conn, from, to| Box::pin(fetch_blocks(conn, from, to)),
            |ch, rows| Box::pin(async move { ch.write_blocks(&rows).await }),
        ).await?;

        backfill_table(
            &self.pool, ch, "txs", pg_max,
            |conn, from, to| Box::pin(fetch_txs(conn, from, to)),
            |ch, rows| Box::pin(async move { ch.write_txs(&rows).await }),
        ).await?;

        backfill_table(
            &self.pool, ch, "logs", pg_max,
            |conn, from, to| Box::pin(fetch_logs(conn, from, to)),
            |ch, rows| Box::pin(async move { ch.write_logs(&rows).await }),
        ).await?;

        backfill_table(
            &self.pool, ch, "receipts", pg_max,
            |conn, from, to| Box::pin(fetch_receipts(conn, from, to)),
            |ch, rows| Box::pin(async move { ch.write_receipts(&rows).await }),
        ).await?;

        let elapsed = start.elapsed();
        if blocks_written > 0 {
            let rate = if elapsed.as_secs() > 0 {
                blocks_written as f64 / elapsed.as_secs_f64()
            } else {
                blocks_written as f64
            };
            info!(
                blocks = blocks_written,
                elapsed_secs = elapsed.as_secs(),
                rate = format!("{rate:.0} blk/s"),
                "ClickHouse backfill complete"
            );
        }

        Ok(())
    }
}

/// Get the max block number in PostgreSQL, or None if empty.
async fn pg_max_block_num(pool: &Pool) -> Result<Option<i64>> {
    let conn = pool.get().await?;
    let row = conn.query_one("SELECT MAX(num) FROM blocks", &[]).await?;
    Ok(row.get::<_, Option<i64>>(0))
}

/// Generic backfill: queries CH for per-table high-water mark, then pages through
/// PG data in block-range batches. Each query is autocommit (no long transactions).
/// Returns the number of rows written.
async fn backfill_table<T, F, W>(
    pool: &Pool,
    ch: &ClickHouseSink,
    table: &str,
    pg_max: i64,
    fetch_fn: F,
    write_fn: W,
) -> Result<i64>
where
    T: Send + 'static,
    F: Fn(&deadpool_postgres::Object, i64, i64) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<T>>> + Send + '_>>,
    W: Fn(&ClickHouseSink, Vec<T>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>>,
{
    let ch_max = ch.max_block_in_table(table).await?;

    let from_block = match ch_max {
        Some(n) if n >= pg_max => {
            info!(table, ch_max = n, pg_max, "ClickHouse {table} is up to date");
            return Ok(0);
        }
        Some(n) => n + 1,
        None => 1,
    };

    let total = pg_max - from_block + 1;
    info!(table, from_block, pg_max, total_blocks = total, "Backfilling ClickHouse table");

    let mut written: i64 = 0;
    let mut current = from_block;

    while current <= pg_max {
        let batch_end = (current + BACKFILL_BLOCK_BATCH - 1).min(pg_max);

        // Short-lived connection per batch — no long transactions
        let conn = pool.get().await?;
        let rows = fetch_fn(&conn, current, batch_end).await?;
        drop(conn); // release connection immediately

        let count = rows.len() as i64;
        if count > 0 {
            write_fn(ch, rows).await?;
            written += count;

            if written % 100_000 < count {
                let pct = (((current - from_block + 1) as f64 / total as f64) * 100.0) as u64;
                info!(table, rows_written = written, pct, "ClickHouse backfill progress");
            }
        }

        current = batch_end + 1;
    }

    if written > 0 {
        info!(table, rows = written, "ClickHouse backfill: {table} complete");
    }

    Ok(written)
}

// ── PG fetch functions (one query per batch, no cursors) ──────────────────

async fn fetch_blocks(conn: &deadpool_postgres::Object, from: i64, to: i64) -> Result<Vec<BlockRow>> {
    let rows = conn
        .query(
            "SELECT num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner, extra_data \
             FROM blocks WHERE num >= $1 AND num <= $2 ORDER BY num",
            &[&from, &to],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| BlockRow {
            num: r.get(0),
            hash: r.get(1),
            parent_hash: r.get(2),
            timestamp: r.get(3),
            timestamp_ms: r.get(4),
            gas_limit: r.get(5),
            gas_used: r.get(6),
            miner: r.get(7),
            extra_data: r.get(8),
        })
        .collect())
}

async fn fetch_txs(conn: &deadpool_postgres::Object, from: i64, to: i64) -> Result<Vec<TxRow>> {
    let rows = conn
        .query(
            "SELECT block_num, block_timestamp, idx, hash, type, \"from\", \"to\", value, input, \
             gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_used, \
             nonce_key, nonce, fee_token, fee_payer, calls, call_count, \
             valid_before, valid_after, signature_type \
             FROM txs WHERE block_num >= $1 AND block_num <= $2 ORDER BY block_num, idx",
            &[&from, &to],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| TxRow {
            block_num: r.get(0),
            block_timestamp: r.get(1),
            idx: r.get(2),
            hash: r.get(3),
            tx_type: r.get(4),
            from: r.get(5),
            to: r.get(6),
            value: r.get(7),
            input: r.get(8),
            gas_limit: r.get(9),
            max_fee_per_gas: r.get(10),
            max_priority_fee_per_gas: r.get(11),
            gas_used: r.get(12),
            nonce_key: r.get(13),
            nonce: r.get(14),
            fee_token: r.get(15),
            fee_payer: r.get(16),
            calls: r.get(17),
            call_count: r.get(18),
            valid_before: r.get(19),
            valid_after: r.get(20),
            signature_type: r.get(21),
        })
        .collect())
}

async fn fetch_logs(conn: &deadpool_postgres::Object, from: i64, to: i64) -> Result<Vec<LogRow>> {
    let rows = conn
        .query(
            "SELECT block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, \
             selector, topic0, topic1, topic2, topic3, data \
             FROM logs WHERE block_num >= $1 AND block_num <= $2 ORDER BY block_num, log_idx",
            &[&from, &to],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| LogRow {
            block_num: r.get(0),
            block_timestamp: r.get(1),
            log_idx: r.get(2),
            tx_idx: r.get(3),
            tx_hash: r.get(4),
            address: r.get(5),
            selector: r.get(6),
            topic0: r.get(7),
            topic1: r.get(8),
            topic2: r.get(9),
            topic3: r.get(10),
            data: r.get(11),
        })
        .collect())
}

async fn fetch_receipts(conn: &deadpool_postgres::Object, from: i64, to: i64) -> Result<Vec<ReceiptRow>> {
    let rows = conn
        .query(
            "SELECT block_num, block_timestamp, tx_idx, tx_hash, \"from\", \"to\", \
             contract_address, gas_used, cumulative_gas_used, effective_gas_price, \
             status, fee_payer \
             FROM receipts WHERE block_num >= $1 AND block_num <= $2 ORDER BY block_num, tx_idx",
            &[&from, &to],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|r| ReceiptRow {
            block_num: r.get(0),
            block_timestamp: r.get(1),
            tx_idx: r.get(2),
            tx_hash: r.get(3),
            from: r.get(4),
            to: r.get(5),
            contract_address: r.get(6),
            gas_used: r.get(7),
            cumulative_gas_used: r.get(8),
            effective_gas_price: r.get(9),
            status: r.get(10),
            fee_payer: r.get(11),
        })
        .collect())
}
