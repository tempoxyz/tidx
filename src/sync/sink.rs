use anyhow::Result;
use tracing::{error, info, warn};

use crate::db::Pool;
use crate::metrics;
use crate::types::{BlockRow, LogRow, ReceiptRow, TxRow};

use super::ch_sink::ClickHouseSink;
use super::writer;

/// Default batch size for PG→CH backfill (rows per cursor FETCH).
const BACKFILL_BATCH_SIZE: i64 = 10_000;

/// Fan-out writer that sends data to all configured sinks.
///
/// PostgreSQL is always present and authoritative — PG write failures are fatal.
/// ClickHouse is optional — CH write failures are logged and swallowed.
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
        // PG is authoritative — failures propagate
        writer::write_blocks(&self.pool, blocks).await?;

        // CH is best-effort
        if let Some(ch) = &self.ch {
            if let Err(e) = ch.write_blocks(blocks).await {
                error!(error = %e, "ClickHouse write_blocks failed (non-fatal)");
                metrics::record_sink_error(ch.name());
            }
        }

        Ok(())
    }

    pub async fn write_txs(&self, txs: &[TxRow]) -> Result<()> {
        writer::write_txs(&self.pool, txs).await?;

        if let Some(ch) = &self.ch {
            if let Err(e) = ch.write_txs(txs).await {
                error!(error = %e, "ClickHouse write_txs failed (non-fatal)");
                metrics::record_sink_error(ch.name());
            }
        }

        Ok(())
    }

    pub async fn write_logs(&self, logs: &[LogRow]) -> Result<()> {
        writer::write_logs(&self.pool, logs).await?;

        if let Some(ch) = &self.ch {
            if let Err(e) = ch.write_logs(logs).await {
                error!(error = %e, "ClickHouse write_logs failed (non-fatal)");
                metrics::record_sink_error(ch.name());
            }
        }

        Ok(())
    }

    pub async fn write_receipts(&self, receipts: &[ReceiptRow]) -> Result<()> {
        writer::write_receipts(&self.pool, receipts).await?;

        if let Some(ch) = &self.ch {
            if let Err(e) = ch.write_receipts(receipts).await {
                error!(error = %e, "ClickHouse write_receipts failed (non-fatal)");
                metrics::record_sink_error(ch.name());
            }
        }

        Ok(())
    }

    /// Delete all data from a given block number onwards (reorg support).
    /// Returns the number of blocks deleted from PostgreSQL.
    pub async fn delete_from(&self, block_num: u64) -> Result<u64> {
        let deleted = writer::delete_blocks_from(&self.pool, block_num).await?;

        if let Some(ch) = &self.ch {
            if let Err(e) = ch.delete_from(block_num).await {
                warn!(error = %e, block_num, "ClickHouse delete_from failed (non-fatal)");
                metrics::record_sink_error(ch.name());
            }
        }

        Ok(deleted)
    }

    /// Automatically backfill ClickHouse from PostgreSQL if CH is behind.
    ///
    /// Compares the max block in CH vs PG. If CH is behind (or empty),
    /// streams the missing blocks from PG using a server-side cursor and
    /// writes them to CH in batches. This runs at startup before the sync
    /// engine begins, so CH catches up without re-fetching from RPC.
    pub async fn backfill_clickhouse(&self) -> Result<()> {
        let ch = match &self.ch {
            Some(ch) => ch,
            None => return Ok(()),
        };

        // Determine high-water marks
        let ch_max = ch.max_block_num().await?;
        let pg_max = pg_max_block_num(&self.pool).await?;

        let pg_max = match pg_max {
            Some(n) => n,
            None => return Ok(()), // PG is empty, nothing to backfill
        };

        let from_block = match ch_max {
            Some(n) if n >= pg_max => {
                info!(ch_max = n, pg_max, "ClickHouse is up to date with PostgreSQL");
                return Ok(());
            }
            Some(n) => n + 1, // Start after the last CH block
            None => 1,        // CH is empty, start from block 1
        };

        let total = pg_max - from_block + 1;
        info!(
            from_block,
            pg_max,
            total_blocks = total,
            "Backfilling ClickHouse from PostgreSQL"
        );

        let start = std::time::Instant::now();
        let mut written: i64 = 0;

        // Use a dedicated connection for the cursor transaction
        let conn = self.pool.get().await?;
        conn.execute("SET statement_timeout = 0", &[]).await?;

        // Stream blocks in batches using a cursor
        backfill_table_blocks(&conn, ch, from_block, pg_max, &mut written, total).await?;

        // Now backfill txs, logs, receipts for the same block range
        backfill_table_txs(&conn, ch, from_block, pg_max).await?;
        backfill_table_logs(&conn, ch, from_block, pg_max).await?;
        backfill_table_receipts(&conn, ch, from_block, pg_max).await?;

        let elapsed = start.elapsed();
        let rate = if elapsed.as_secs() > 0 {
            written as f64 / elapsed.as_secs_f64()
        } else {
            written as f64
        };

        info!(
            blocks = written,
            elapsed_secs = elapsed.as_secs(),
            rate = format!("{rate:.0} blk/s"),
            "ClickHouse backfill complete"
        );

        Ok(())
    }
}

/// Get the max block number in PostgreSQL, or None if empty.
async fn pg_max_block_num(pool: &Pool) -> Result<Option<i64>> {
    let conn = pool.get().await?;
    let row = conn.query_one("SELECT MAX(num) FROM blocks", &[]).await?;
    Ok(row.get::<_, Option<i64>>(0))
}

/// Backfill blocks from PG to CH using a server-side cursor.
async fn backfill_table_blocks(
    conn: &deadpool_postgres::Object,
    ch: &ClickHouseSink,
    from_block: i64,
    to_block: i64,
    written: &mut i64,
    total: i64,
) -> Result<()> {
    conn.execute("BEGIN", &[]).await?;
    conn.execute(
        &format!(
            "DECLARE backfill_blocks CURSOR FOR \
             SELECT num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner, extra_data \
             FROM blocks WHERE num >= {} AND num <= {} ORDER BY num",
            from_block, to_block
        ),
        &[],
    )
    .await?;

    loop {
        let rows = conn
            .query(
                &format!("FETCH {} FROM backfill_blocks", BACKFILL_BATCH_SIZE),
                &[],
            )
            .await?;

        if rows.is_empty() {
            break;
        }

        let blocks: Vec<BlockRow> = rows
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
            .collect();

        let count = blocks.len() as i64;
        ch.write_blocks(&blocks).await?;
        *written += count;

        if *written % 100_000 < count {
            let pct = (*written as f64 / total as f64 * 100.0) as u64;
            info!(
                blocks_written = *written,
                total,
                pct,
                "ClickHouse backfill progress"
            );
        }
    }

    conn.execute("CLOSE backfill_blocks", &[]).await?;
    conn.execute("COMMIT", &[]).await?;
    Ok(())
}

/// Backfill txs from PG to CH using a server-side cursor.
async fn backfill_table_txs(
    conn: &deadpool_postgres::Object,
    ch: &ClickHouseSink,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    conn.execute("BEGIN", &[]).await?;
    conn.execute(
        &format!(
            "DECLARE backfill_txs CURSOR FOR \
             SELECT block_num, block_timestamp, idx, hash, type, \"from\", \"to\", value, input, \
             gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_used, \
             nonce_key, nonce, fee_token, fee_payer, calls, call_count, \
             valid_before, valid_after, signature_type \
             FROM txs WHERE block_num >= {} AND block_num <= {} ORDER BY block_num, idx",
            from_block, to_block
        ),
        &[],
    )
    .await?;

    let mut total_written: i64 = 0;
    loop {
        let rows = conn
            .query(
                &format!("FETCH {} FROM backfill_txs", BACKFILL_BATCH_SIZE),
                &[],
            )
            .await?;

        if rows.is_empty() {
            break;
        }

        let txs: Vec<TxRow> = rows
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
            .collect();

        total_written += txs.len() as i64;
        ch.write_txs(&txs).await?;
    }

    conn.execute("CLOSE backfill_txs", &[]).await?;
    conn.execute("COMMIT", &[]).await?;
    info!(txs = total_written, "ClickHouse backfill: txs complete");
    Ok(())
}

/// Backfill logs from PG to CH using a server-side cursor.
async fn backfill_table_logs(
    conn: &deadpool_postgres::Object,
    ch: &ClickHouseSink,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    conn.execute("BEGIN", &[]).await?;
    conn.execute(
        &format!(
            "DECLARE backfill_logs CURSOR FOR \
             SELECT block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, \
             selector, topic0, topic1, topic2, topic3, data \
             FROM logs WHERE block_num >= {} AND block_num <= {} ORDER BY block_num, log_idx",
            from_block, to_block
        ),
        &[],
    )
    .await?;

    let mut total_written: i64 = 0;
    loop {
        let rows = conn
            .query(
                &format!("FETCH {} FROM backfill_logs", BACKFILL_BATCH_SIZE),
                &[],
            )
            .await?;

        if rows.is_empty() {
            break;
        }

        let logs: Vec<LogRow> = rows
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
            .collect();

        total_written += logs.len() as i64;
        ch.write_logs(&logs).await?;
    }

    conn.execute("CLOSE backfill_logs", &[]).await?;
    conn.execute("COMMIT", &[]).await?;
    info!(logs = total_written, "ClickHouse backfill: logs complete");
    Ok(())
}

/// Backfill receipts from PG to CH using a server-side cursor.
async fn backfill_table_receipts(
    conn: &deadpool_postgres::Object,
    ch: &ClickHouseSink,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    conn.execute("BEGIN", &[]).await?;
    conn.execute(
        &format!(
            "DECLARE backfill_receipts CURSOR FOR \
             SELECT block_num, block_timestamp, tx_idx, tx_hash, \"from\", \"to\", \
             contract_address, gas_used, cumulative_gas_used, effective_gas_price, \
             status, fee_payer \
             FROM receipts WHERE block_num >= {} AND block_num <= {} ORDER BY block_num, tx_idx",
            from_block, to_block
        ),
        &[],
    )
    .await?;

    let mut total_written: i64 = 0;
    loop {
        let rows = conn
            .query(
                &format!("FETCH {} FROM backfill_receipts", BACKFILL_BATCH_SIZE),
                &[],
            )
            .await?;

        if rows.is_empty() {
            break;
        }

        let receipts: Vec<ReceiptRow> = rows
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
            .collect();

        total_written += receipts.len() as i64;
        ch.write_receipts(&receipts).await?;
    }

    conn.execute("CLOSE backfill_receipts", &[]).await?;
    conn.execute("COMMIT", &[]).await?;
    info!(receipts = total_written, "ClickHouse backfill: receipts complete");
    Ok(())
}
