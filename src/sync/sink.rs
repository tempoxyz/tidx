use anyhow::Result;
use tracing::info;

use crate::db::Pool;
use crate::metrics;
use crate::types::{BlockRow, LogRow, ReceiptRow, TxRow};

use super::ch_sink::ClickHouseSink;
use super::store::SyncStore;
use super::writer;

/// Number of blocks worth of data to fetch per query during backfill.
/// Uses block-range pagination (no long-lived transactions).
const BACKFILL_BLOCK_BATCH: i64 = 5_000;

/// Fan-out writer that sends data to all configured sinks.
///
/// At least one of PostgreSQL or ClickHouse is present; both are optional.
/// Write failures from either sink are fatal and propagate to the caller.
#[derive(Clone)]
pub struct SinkSet {
    pg: Option<Pool>,
    ch: Option<ClickHouseSink>,
}

impl SinkSet {
    pub fn new(pool: Pool) -> Self {
        Self {
            pg: Some(pool),
            ch: None,
        }
    }

    /// ClickHouse-only sink set (no PostgreSQL).
    pub fn clickhouse_only(ch: ClickHouseSink) -> Self {
        Self {
            pg: None,
            ch: Some(ch),
        }
    }

    pub fn with_clickhouse(mut self, ch: ClickHouseSink) -> Self {
        self.ch = Some(ch);
        self
    }

    /// The PG pool, when PostgreSQL is configured.
    pub fn pg(&self) -> Option<&Pool> {
        self.pg.as_ref()
    }

    /// The ClickHouse sink, when configured.
    pub fn clickhouse(&self) -> Option<&ClickHouseSink> {
        self.ch.as_ref()
    }

    /// Store backing sync-engine state: PostgreSQL when present, else ClickHouse.
    pub fn store(&self) -> SyncStore<'_> {
        if let Some(pg) = &self.pg {
            SyncStore::Postgres(pg)
        } else if let Some(ch) = &self.ch {
            SyncStore::ClickHouse(ch)
        } else {
            unreachable!("SinkSet constructed without any sink")
        }
    }

    pub async fn write_blocks(&self, blocks: &[BlockRow]) -> Result<()> {
        match (&self.pg, &self.ch) {
            (Some(pg), Some(ch)) => {
                tokio::try_join!(writer::write_blocks(pg, blocks), ch.write_blocks(blocks))?;
            }
            (Some(pg), None) => writer::write_blocks(pg, blocks).await?,
            (None, Some(ch)) => ch.write_blocks(blocks).await?,
            (None, None) => unreachable!("SinkSet constructed without any sink"),
        }
        Ok(())
    }

    pub async fn write_txs(&self, txs: &[TxRow]) -> Result<()> {
        match (&self.pg, &self.ch) {
            (Some(pg), Some(ch)) => {
                tokio::try_join!(writer::write_txs(pg, txs), ch.write_txs(txs))?;
            }
            (Some(pg), None) => writer::write_txs(pg, txs).await?,
            (None, Some(ch)) => ch.write_txs(txs).await?,
            (None, None) => unreachable!("SinkSet constructed without any sink"),
        }
        Ok(())
    }

    pub async fn write_logs(&self, logs: &[LogRow]) -> Result<()> {
        match (&self.pg, &self.ch) {
            (Some(pg), Some(ch)) => {
                tokio::try_join!(writer::write_logs(pg, logs), ch.write_logs(logs))?;
            }
            (Some(pg), None) => writer::write_logs(pg, logs).await?,
            (None, Some(ch)) => ch.write_logs(logs).await?,
            (None, None) => unreachable!("SinkSet constructed without any sink"),
        }
        Ok(())
    }

    pub async fn write_receipts(&self, receipts: &[ReceiptRow]) -> Result<()> {
        match (&self.pg, &self.ch) {
            (Some(pg), Some(ch)) => {
                tokio::try_join!(
                    writer::write_receipts(pg, receipts),
                    ch.write_receipts(receipts),
                )?;
            }
            (Some(pg), None) => writer::write_receipts(pg, receipts).await?,
            (None, Some(ch)) => ch.write_receipts(receipts).await?,
            (None, None) => unreachable!("SinkSet constructed without any sink"),
        }
        Ok(())
    }

    /// Write all four tables in a single PG transaction, with CH writes concurrent.
    pub async fn write_all(
        &self,
        blocks: &[BlockRow],
        txs: &[TxRow],
        logs: &[LogRow],
        receipts: &[ReceiptRow],
    ) -> Result<()> {
        self.write_all_inner(blocks, txs, logs, receipts, None)
            .await
    }

    pub async fn write_all_with_application_name(
        &self,
        blocks: &[BlockRow],
        txs: &[TxRow],
        logs: &[LogRow],
        receipts: &[ReceiptRow],
        application_name: &str,
    ) -> Result<()> {
        self.write_all_inner(blocks, txs, logs, receipts, Some(application_name))
            .await
    }

    async fn write_all_inner(
        &self,
        blocks: &[BlockRow],
        txs: &[TxRow],
        logs: &[LogRow],
        receipts: &[ReceiptRow],
        application_name: Option<&str>,
    ) -> Result<()> {
        match (&self.pg, &self.ch) {
            (Some(pg), Some(ch)) => {
                tokio::try_join!(
                    write_pg_batch(pg, blocks, txs, logs, receipts, application_name),
                    ch.write_blocks(blocks),
                    ch.write_txs(txs),
                    ch.write_logs(logs),
                    ch.write_receipts(receipts),
                )?;
            }
            (Some(pg), None) => {
                write_pg_batch(pg, blocks, txs, logs, receipts, application_name).await?;
            }
            (None, Some(ch)) => {
                tokio::try_join!(
                    ch.write_blocks(blocks),
                    ch.write_txs(txs),
                    ch.write_logs(logs),
                    ch.write_receipts(receipts),
                )?;
            }
            (None, None) => unreachable!("SinkSet constructed without any sink"),
        }
        Ok(())
    }

    /// Delete all data from a given block number onwards (reorg support).
    /// Returns the number of blocks deleted.
    pub async fn delete_from(&self, block_num: u64) -> Result<u64> {
        match (&self.pg, &self.ch) {
            (Some(pg), Some(ch)) => {
                let (deleted, ()) = tokio::try_join!(
                    writer::delete_blocks_from(pg, block_num),
                    ch.delete_from(block_num),
                )?;
                Ok(deleted)
            }
            (Some(pg), None) => writer::delete_blocks_from(pg, block_num).await,
            (None, Some(ch)) => {
                let deleted = ch.count_blocks_from(block_num).await.unwrap_or(0);
                ch.delete_from(block_num).await?;
                Ok(deleted)
            }
            (None, None) => unreachable!("SinkSet constructed without any sink"),
        }
    }

    /// Automatically backfill ClickHouse from PostgreSQL if CH is behind.
    ///
    /// Uses a persistent cursor (`ch_backfill_block`) in the PG `sync_state` table
    /// to track progress. This avoids the race condition where realtime sync writes
    /// blocks ahead of the backfill position, causing `max(block_num)` to skip gaps.
    ///
    /// All four tables (blocks, txs, logs, receipts) are backfilled together per
    /// block range. The cursor advances only after all tables succeed for that range.
    /// Tables use ReplacingMergeTree so re-inserts after a crash are safe.
    pub async fn backfill_clickhouse(&self, chain_id: u64) -> Result<()> {
        // Only meaningful when mirroring PG into CH; ClickHouse-only chains
        // write directly to CH and have no source to backfill from.
        let (Some(pool), Some(ch)) = (&self.pg, &self.ch) else {
            return Ok(());
        };

        let pg_max = pg_max_block_num(pool).await?;
        let pg_max = match pg_max {
            Some(n) => n,
            None => return Ok(()), // PG is empty, nothing to backfill
        };

        // Load persisted cursor from PG (survives restarts)
        let cursor = load_ch_backfill_cursor(pool, chain_id).await?;
        let from_block = cursor + 1;

        if from_block > pg_max {
            info!(
                ch_backfill_block = cursor,
                pg_max, "ClickHouse backfill up to date"
            );
            metrics::set_backfill_remaining(chain_id, "clickhouse", 0);
            return Ok(());
        }

        let total = pg_max - from_block + 1;
        metrics::set_backfill_remaining(chain_id, "clickhouse", total as u64);
        info!(
            chain_id,
            from_block,
            pg_max,
            total_blocks = total,
            "Starting ClickHouse backfill"
        );

        let start = std::time::Instant::now();
        let mut blocks_written: i64 = 0;
        let mut current = from_block;

        // Pre-fetch the first batch so we can pipeline fetch(N+1) with write(N)
        let mut pending = {
            let batch_end = (current + BACKFILL_BLOCK_BATCH - 1).min(pg_max);
            let conn = pool.get().await?;
            let data = tokio::try_join!(
                fetch_blocks(&conn, current, batch_end),
                fetch_txs(&conn, current, batch_end),
                fetch_logs(&conn, current, batch_end),
                fetch_receipts(&conn, current, batch_end),
            )?;
            current = batch_end + 1;
            Some((batch_end, data))
        };

        while let Some((batch_end, (blocks, txs, logs, mut receipts))) = pending.take() {
            let block_count = blocks.len() as i64;

            // Postgres receipts lack the denormalized tx-level type/fee_token;
            // populate them from txs so the ClickHouse mirror matches the
            // live-sync path before writing.
            super::decoder::enrich_receipts_from_txs(&mut receipts, &txs);

            // Pipeline: fetch next batch from PG while writing current batch to CH
            let next_fetch = async {
                if current > pg_max {
                    return Ok(None);
                }
                let next_end = (current + BACKFILL_BLOCK_BATCH - 1).min(pg_max);
                let conn = pool.get().await?;
                let data = tokio::try_join!(
                    fetch_blocks(&conn, current, next_end),
                    fetch_txs(&conn, current, next_end),
                    fetch_logs(&conn, current, next_end),
                    fetch_receipts(&conn, current, next_end),
                )?;
                Ok::<_, anyhow::Error>(Some((next_end, data)))
            };

            let ch_write = async {
                tokio::try_join!(
                    async {
                        if !blocks.is_empty() {
                            ch.write_blocks(&blocks).await
                        } else {
                            Ok(())
                        }
                    },
                    async {
                        if !txs.is_empty() {
                            ch.write_txs(&txs).await
                        } else {
                            Ok(())
                        }
                    },
                    async {
                        if !logs.is_empty() {
                            ch.write_logs(&logs).await
                        } else {
                            Ok(())
                        }
                    },
                    async {
                        if !receipts.is_empty() {
                            ch.write_receipts(&receipts).await
                        } else {
                            Ok(())
                        }
                    },
                )
            };

            let (next_data, _) = tokio::try_join!(next_fetch, ch_write)?;

            // Advance cursor only after all tables written successfully
            save_ch_backfill_cursor(pool, chain_id, batch_end).await?;

            blocks_written += block_count;
            let remaining = pg_max - batch_end;
            metrics::set_backfill_block(chain_id, "clickhouse", batch_end as u64);
            metrics::set_backfill_remaining(chain_id, "clickhouse", remaining as u64);

            if blocks_written % 100_000 < block_count {
                let pct = (((batch_end - from_block + 1) as f64 / total as f64) * 100.0) as u64;
                info!(
                    chain_id,
                    blocks_written, pct, batch_end, "ClickHouse backfill progress"
                );
            }

            if let Some(ref nd) = next_data {
                current = nd.0 + 1;
            }
            pending = next_data;
        }

        let elapsed = start.elapsed();
        if blocks_written > 0 {
            let rate = if elapsed.as_secs() > 0 {
                blocks_written as f64 / elapsed.as_secs_f64()
            } else {
                blocks_written as f64
            };
            info!(
                chain_id,
                blocks = blocks_written,
                elapsed_secs = elapsed.as_secs(),
                rate = format!("{rate:.0} blk/s"),
                "ClickHouse backfill complete"
            );
        }

        Ok(())
    }
}

/// Write a full batch to PostgreSQL, optionally tagged with an application name.
async fn write_pg_batch(
    pg: &Pool,
    blocks: &[BlockRow],
    txs: &[TxRow],
    logs: &[LogRow],
    receipts: &[ReceiptRow],
    application_name: Option<&str>,
) -> Result<()> {
    if let Some(application_name) = application_name {
        writer::write_batch_with_application_name(pg, blocks, txs, logs, receipts, application_name)
            .await
    } else {
        writer::write_batch(pg, blocks, txs, logs, receipts).await
    }
}

/// Get the max block number in PostgreSQL, or None if empty.
async fn pg_max_block_num(pool: &Pool) -> Result<Option<i64>> {
    let conn = pool.get().await?;
    let row = conn.query_one("SELECT MAX(num) FROM blocks", &[]).await?;
    Ok(row.get::<_, Option<i64>>(0))
}

/// Load the CH backfill cursor for a chain. Returns 0 if no row exists.
async fn load_ch_backfill_cursor(pool: &Pool, chain_id: u64) -> Result<i64> {
    let conn = pool.get().await?;
    let row = conn
        .query_opt(
            "SELECT ch_backfill_block FROM sync_state WHERE chain_id = $1",
            &[&(chain_id as i64)],
        )
        .await?;
    Ok(row.map(|r| r.get::<_, i64>(0)).unwrap_or(0))
}

/// Save the CH backfill cursor for a chain (upsert, only advances).
async fn save_ch_backfill_cursor(pool: &Pool, chain_id: u64, block: i64) -> Result<()> {
    let conn = pool.get().await?;
    conn.execute(
        r#"
        INSERT INTO sync_state (chain_id, ch_backfill_block)
        VALUES ($1, $2)
        ON CONFLICT (chain_id) DO UPDATE SET
            ch_backfill_block = GREATEST(sync_state.ch_backfill_block, EXCLUDED.ch_backfill_block),
            updated_at = NOW()
        "#,
        &[&(chain_id as i64), &block],
    )
    .await?;
    Ok(())
}

// ── PG fetch functions (one query per batch, no cursors) ──────────────────

async fn fetch_blocks(
    conn: &deadpool_postgres::Object,
    from: i64,
    to: i64,
) -> Result<Vec<BlockRow>> {
    let rows = conn
        .query(
            "SELECT num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner, extra_data, consensus_proposer \
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
            consensus_proposer: r.get(9),
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
             selector, topic0, topic1, topic2, topic3, data, is_virtual_forward \
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
            is_virtual_forward: r.get(12),
        })
        .collect())
}

async fn fetch_receipts(
    conn: &deadpool_postgres::Object,
    from: i64,
    to: i64,
) -> Result<Vec<ReceiptRow>> {
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
            // Postgres `receipts` has no type/fee_token; these are denormalized
            // from txs by `enrich_receipts_from_txs` before the ClickHouse write.
            tx_type: None,
            fee_token: None,
        })
        .collect())
}
