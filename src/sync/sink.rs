use anyhow::Result;
use chrono::{DateTime, Utc};
use std::collections::HashSet;
use tracing::info;

use crate::db::Pool;
use crate::db::partitions::PartitionCoverage;
use crate::metrics;
use crate::types::{BlockRow, LogRow, ReceiptRow, TxRow};

use super::ch_sink::ClickHouseSink;
use super::writer;

/// Storage target for a decoded sync batch.
///
/// Realtime writes use `All`. Tiered deployments use `ClickHouse` for the
/// full archive backfill and `Postgres` only as the no-ClickHouse fallback for
/// hot-window materialization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WriteTarget {
    All,
    Postgres,
    ClickHouse,
}

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
    partitions: PartitionCoverage,
}

impl SinkSet {
    pub fn new(pool: Pool) -> Self {
        Self {
            pool,
            ch: None,
            partitions: PartitionCoverage::default(),
        }
    }

    pub fn with_clickhouse(mut self, ch: ClickHouseSink) -> Self {
        self.ch = Some(ch);
        self
    }

    /// Access the underlying PG pool for read operations (sync state, gap detection, etc.)
    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// Whether a ClickHouse sink is active.
    pub fn has_clickhouse(&self) -> bool {
        self.ch.is_some()
    }

    /// Access the ClickHouse sink, if configured.
    pub(crate) fn clickhouse(&self) -> Option<&ClickHouseSink> {
        self.ch.as_ref()
    }

    /// Forget cached partition coverage (call after partitions are dropped).
    pub async fn reset_partition_coverage(&self) {
        self.partitions.reset().await;
    }

    /// Ensure weekly partitions exist for every row timestamp in the batch.
    /// No-op on legacy (non-partitioned) deployments; cached per week.
    async fn ensure_partitions(
        &self,
        timestamps: impl Iterator<Item = DateTime<Utc>>,
    ) -> Result<()> {
        let mut min: Option<DateTime<Utc>> = None;
        let mut max: Option<DateTime<Utc>> = None;
        for ts in timestamps {
            min = Some(min.map_or(ts, |m| m.min(ts)));
            max = Some(max.map_or(ts, |m| m.max(ts)));
        }
        if let (Some(min), Some(max)) = (min, max) {
            self.partitions.ensure(&self.pool, min, max).await?;
        }
        Ok(())
    }

    pub async fn write_blocks(&self, blocks: &[BlockRow]) -> Result<()> {
        self.ensure_partitions(blocks.iter().map(|b| b.timestamp))
            .await?;
        if let Some(ch) = &self.ch {
            tokio::try_join!(
                writer::write_blocks(&self.pool, blocks),
                ch.write_blocks(blocks),
            )?;
        } else {
            writer::write_blocks(&self.pool, blocks).await?;
        }
        Ok(())
    }

    pub async fn write_txs(&self, txs: &[TxRow]) -> Result<()> {
        self.ensure_partitions(txs.iter().map(|t| t.block_timestamp))
            .await?;
        if let Some(ch) = &self.ch {
            tokio::try_join!(writer::write_txs(&self.pool, txs), ch.write_txs(txs),)?;
        } else {
            writer::write_txs(&self.pool, txs).await?;
        }
        Ok(())
    }

    pub async fn write_logs(&self, logs: &[LogRow]) -> Result<()> {
        self.ensure_partitions(logs.iter().map(|l| l.block_timestamp))
            .await?;
        if let Some(ch) = &self.ch {
            tokio::try_join!(writer::write_logs(&self.pool, logs), ch.write_logs(logs),)?;
        } else {
            writer::write_logs(&self.pool, logs).await?;
        }
        Ok(())
    }

    pub async fn write_receipts(&self, receipts: &[ReceiptRow]) -> Result<()> {
        self.ensure_partitions(receipts.iter().map(|r| r.block_timestamp))
            .await?;
        if let Some(ch) = &self.ch {
            tokio::try_join!(
                writer::write_receipts(&self.pool, receipts),
                ch.write_receipts(receipts),
            )?;
        } else {
            writer::write_receipts(&self.pool, receipts).await?;
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
        self.write_all_to(blocks, txs, logs, receipts, None, WriteTarget::All)
            .await
    }

    pub(crate) async fn write_all_postgres(
        &self,
        blocks: &[BlockRow],
        txs: &[TxRow],
        logs: &[LogRow],
        receipts: &[ReceiptRow],
    ) -> Result<()> {
        self.write_all_to(
            blocks,
            txs,
            logs,
            receipts,
            Some("tidx postgres hot backfill"),
            WriteTarget::Postgres,
        )
        .await
    }

    pub(crate) async fn write_all_clickhouse(
        &self,
        blocks: &[BlockRow],
        txs: &[TxRow],
        logs: &[LogRow],
        receipts: &[ReceiptRow],
    ) -> Result<()> {
        self.write_all_to(blocks, txs, logs, receipts, None, WriteTarget::ClickHouse)
            .await
    }

    /// Hydrate a PostgreSQL block range from the canonical ClickHouse archive.
    ///
    /// The caller is responsible for checking the durable archive watermark
    /// before requesting the range. A missing block is treated as checkpoint
    /// corruption and aborts the write rather than creating a PostgreSQL gap.
    pub async fn hydrate_postgres_from_clickhouse(&self, from: u64, to: u64) -> Result<()> {
        let ch = self
            .ch
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PostgreSQL archive hydration requires ClickHouse"))?;
        let batch = ch.read_archive_range(from, to).await?;
        let expected = to.saturating_sub(from).saturating_add(1);
        if batch.blocks.len() as u64 != expected {
            anyhow::bail!(
                "ClickHouse archive checkpoint claimed {from}..={to}, but returned {} of {expected} blocks",
                batch.blocks.len()
            );
        }

        self.write_all_postgres(&batch.blocks, &batch.txs, &batch.logs, &batch.receipts)
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
        self.write_all_to(
            blocks,
            txs,
            logs,
            receipts,
            Some(application_name),
            WriteTarget::All,
        )
        .await
    }

    async fn write_all_to(
        &self,
        blocks: &[BlockRow],
        txs: &[TxRow],
        logs: &[LogRow],
        receipts: &[ReceiptRow],
        application_name: Option<&str>,
        target: WriteTarget,
    ) -> Result<()> {
        if target != WriteTarget::ClickHouse {
            self.ensure_partitions(
                blocks
                    .iter()
                    .map(|b| b.timestamp)
                    .chain(txs.iter().map(|t| t.block_timestamp))
                    .chain(logs.iter().map(|l| l.block_timestamp))
                    .chain(receipts.iter().map(|r| r.block_timestamp)),
            )
            .await?;
        }

        let write_postgres = || async {
            if let Some(application_name) = application_name {
                writer::write_batch_with_application_name(
                    &self.pool,
                    blocks,
                    txs,
                    logs,
                    receipts,
                    application_name,
                )
                .await
            } else {
                writer::write_batch(&self.pool, blocks, txs, logs, receipts).await
            }
        };

        match (target, &self.ch) {
            (WriteTarget::All, Some(ch)) => {
                tokio::try_join!(
                    write_postgres(),
                    write_clickhouse_batch(ch, blocks, txs, logs, receipts)
                )?;
            }
            (WriteTarget::All | WriteTarget::Postgres, _) => {
                write_postgres().await?;
            }
            (WriteTarget::ClickHouse, Some(ch)) => {
                write_clickhouse_batch(ch, blocks, txs, logs, receipts).await?;
            }
            (WriteTarget::ClickHouse, None) => {
                anyhow::bail!("ClickHouse archive backfill requested without an active sink");
            }
        }
        Ok(())
    }

    /// Delete all data from a given block number onwards (reorg support).
    /// Returns the number of blocks deleted from PostgreSQL.
    pub async fn delete_from(&self, block_num: u64) -> Result<u64> {
        if let Some(ch) = &self.ch {
            let (deleted, ()) = tokio::try_join!(
                writer::delete_blocks_from(&self.pool, block_num),
                ch.delete_from(block_num),
            )?;
            Ok(deleted)
        } else {
            writer::delete_blocks_from(&self.pool, block_num).await
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
        let ch = match &self.ch {
            Some(ch) => ch,
            None => return Ok(()),
        };

        let pg_max = pg_max_block_num(&self.pool).await?;
        let pg_max = match pg_max {
            Some(n) => n,
            None => return Ok(()), // PG is empty, nothing to backfill
        };

        // Load persisted cursor from PG (survives restarts)
        let cursor = load_ch_backfill_cursor(&self.pool, chain_id).await?;
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
            let conn = self.pool.get().await?;
            let data = tokio::try_join!(
                fetch_blocks(&conn, current, batch_end),
                fetch_txs(&conn, current, batch_end),
                fetch_logs(&conn, current, batch_end),
                fetch_receipts(&conn, current, batch_end),
            )?;
            current = batch_end + 1;
            Some((batch_end, data))
        };

        while let Some((batch_end, (mut blocks, mut txs, mut logs, mut receipts))) = pending.take()
        {
            let block_count = blocks.len() as i64;

            // Postgres receipts lack the denormalized tx-level type/fee_token;
            // populate them from txs so the ClickHouse mirror matches the
            // live-sync path before writing.
            super::decoder::enrich_receipts_from_txs(&mut receipts, &txs);

            // Rows already in ClickHouse duplicate ReplacingMergeTree rows
            // until a merge if re-inserted. Filter per table so a legacy
            // partial write (blocks without children) still gets its missing
            // child rows healed from PostgreSQL.
            if !blocks.is_empty() {
                let lo = blocks.first().expect("non-empty batch").num.max(0) as u64;
                let hi = blocks.last().expect("non-empty batch").num.max(0) as u64;
                let as_set = |nums: Vec<u64>| -> HashSet<i64> {
                    nums.into_iter().map(|n| n as i64).collect()
                };
                let (in_blocks, in_txs, in_logs, in_receipts) = tokio::try_join!(
                    ch.block_nums_in_table_range("blocks", lo, hi),
                    ch.block_nums_in_table_range("txs", lo, hi),
                    ch.block_nums_in_table_range("logs", lo, hi),
                    ch.block_nums_in_table_range("receipts", lo, hi),
                )?;
                let (in_blocks, in_txs, in_logs, in_receipts) = (
                    as_set(in_blocks),
                    as_set(in_txs),
                    as_set(in_logs),
                    as_set(in_receipts),
                );
                blocks.retain(|b| !in_blocks.contains(&b.num));
                txs.retain(|t| !in_txs.contains(&t.block_num));
                logs.retain(|l| !in_logs.contains(&l.block_num));
                receipts.retain(|r| !in_receipts.contains(&r.block_num));
            }

            // Pipeline: fetch next batch from PG while writing current batch to CH
            let next_fetch = async {
                if current > pg_max {
                    return Ok(None);
                }
                let next_end = (current + BACKFILL_BLOCK_BATCH - 1).min(pg_max);
                let conn = self.pool.get().await?;
                let data = tokio::try_join!(
                    fetch_blocks(&conn, current, next_end),
                    fetch_txs(&conn, current, next_end),
                    fetch_logs(&conn, current, next_end),
                    fetch_receipts(&conn, current, next_end),
                )?;
                Ok::<_, anyhow::Error>(Some((next_end, data)))
            };

            // Children first, blocks last: a block row in ClickHouse then
            // proves the whole batch landed.
            let ch_write = async {
                tokio::try_join!(
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
                )?;
                if !blocks.is_empty() {
                    ch.write_blocks(&blocks).await?;
                }
                Ok::<_, anyhow::Error>(())
            };

            let (next_data, ()) = tokio::try_join!(next_fetch, ch_write)?;

            // Advance cursor only after all tables written successfully
            save_ch_backfill_cursor(&self.pool, chain_id, batch_end).await?;

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

async fn write_clickhouse_batch(
    ch: &ClickHouseSink,
    blocks: &[BlockRow],
    txs: &[TxRow],
    logs: &[LogRow],
    receipts: &[ReceiptRow],
) -> Result<()> {
    // Children first, blocks last: a block row in ClickHouse then proves the
    // whole batch landed, keeping block presence checks sound when a write
    // fails partway.
    tokio::try_join!(
        ch.write_txs(txs),
        ch.write_logs(logs),
        ch.write_receipts(receipts),
    )?;
    ch.write_blocks(blocks).await
}

/// Get the max block number in PostgreSQL, or None if empty.
async fn pg_max_block_num(pool: &Pool) -> Result<Option<i64>> {
    let conn = pool.get().await?;
    let row = conn.query_one("SELECT MAX(num) FROM blocks", &[]).await?;
    Ok(row.get::<_, Option<i64>>(0))
}

/// Load the CH backfill cursor for a chain. Returns 0 if no row exists.
pub(crate) async fn load_ch_backfill_cursor(pool: &Pool, chain_id: u64) -> Result<i64> {
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
