use anyhow::Result;
use chrono::{DateTime, Utc};
use std::collections::HashSet;
use tracing::info;

use crate::db::Pool;
use crate::db::partitions::PartitionCoverage;
use crate::metrics;
use crate::types::{BlockRow, LogRow, ReceiptRow, TxRow};

use super::ch_sink::{ClickHouseSink, batch_deduplication_seed, replay_deduplication_seed};
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

/// Startup snapshot for the legacy PostgreSQL-to-ClickHouse backfill.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClickHouseBackfillPlan {
    pub upper_bound: Option<i64>,
    pub complete_before_realtime: bool,
}

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
        let ch = self.ch.as_ref().ok_or_else(|| {
            anyhow::anyhow!("ClickHouse archive backfill requested without an active sink")
        })?;
        let (mut blocks, mut txs, mut logs, mut receipts) = (
            blocks.to_vec(),
            txs.to_vec(),
            logs.to_vec(),
            receipts.to_vec(),
        );
        let repair_blocks =
            filter_clickhouse_rows(ch, &mut blocks, &mut txs, &mut logs, &mut receipts).await?;
        write_filtered_clickhouse_batch(ch, &blocks, &txs, &logs, &receipts, &repair_blocks).await
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
                let seed = batch_deduplication_seed(blocks);
                tokio::try_join!(
                    write_postgres(),
                    write_clickhouse_batch(ch, blocks, txs, logs, receipts, seed.as_deref())
                )?;
            }
            (WriteTarget::All | WriteTarget::Postgres, _) => {
                write_postgres().await?;
            }
            (WriteTarget::ClickHouse, Some(ch)) => {
                let seed = batch_deduplication_seed(blocks);
                write_clickhouse_batch(ch, blocks, txs, logs, receipts, seed.as_deref()).await?;
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
        let plan = self.clickhouse_backfill_plan(chain_id).await?;
        self.backfill_clickhouse_through(chain_id, plan.upper_bound)
            .await
    }

    /// Plan the startup catch-up against a stable PostgreSQL snapshot.
    ///
    /// `tip_num` advances only after both realtime sinks complete, so capping
    /// the legacy PG→CH backfill there prevents it from racing a PG-first
    /// realtime commit. Legacy databases with no positive tip still need all
    /// existing PostgreSQL rows copied, but that first copy must finish before
    /// realtime starts because no non-overlapping handoff point exists yet.
    #[doc(hidden)]
    pub async fn clickhouse_backfill_plan(&self, chain_id: u64) -> Result<ClickHouseBackfillPlan> {
        let (pg_max, tip_num) = pg_backfill_snapshot(&self.pool, chain_id).await?;
        let Some(pg_max) = pg_max else {
            return Ok(ClickHouseBackfillPlan {
                upper_bound: None,
                complete_before_realtime: false,
            });
        };
        let durable_tip = tip_num.filter(|tip| *tip > 0);
        Ok(ClickHouseBackfillPlan {
            upper_bound: Some(durable_tip.map_or(pg_max, |tip| pg_max.min(tip))),
            complete_before_realtime: durable_tip.is_none(),
        })
    }

    /// Execute a startup plan and establish its realtime handoff when needed.
    ///
    /// For an absent or zero tip, the PostgreSQL maximum becomes durable only
    /// after the bounded copy finishes. Advancing `tip_num` at that point makes
    /// the realtime engine start after the copied range instead of replaying it.
    #[doc(hidden)]
    pub async fn run_clickhouse_startup_backfill(
        &self,
        chain_id: u64,
        plan: ClickHouseBackfillPlan,
    ) -> Result<()> {
        if self.ch.is_none() {
            return Ok(());
        }
        self.backfill_clickhouse_through(chain_id, plan.upper_bound)
            .await?;
        if plan.complete_before_realtime
            && let Some(tip_num) = plan.upper_bound.filter(|tip| *tip > 0)
        {
            writer::update_tip_num(&self.pool, chain_id, tip_num as u64, tip_num as u64).await?;
        }
        Ok(())
    }

    #[doc(hidden)]
    pub async fn backfill_clickhouse_through(
        &self,
        chain_id: u64,
        upper_bound: Option<i64>,
    ) -> Result<()> {
        let ch = match &self.ch {
            Some(ch) => ch,
            None => return Ok(()),
        };
        let pg_max = match upper_bound {
            Some(n) => n,
            None => return Ok(()),
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

            let repair_blocks =
                filter_clickhouse_rows(ch, &mut blocks, &mut txs, &mut logs, &mut receipts).await?;

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

            let ch_write = write_filtered_clickhouse_batch(
                ch,
                &blocks,
                &txs,
                &logs,
                &receipts,
                &repair_blocks,
            );

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
    deduplication_seed: Option<&str>,
) -> Result<()> {
    // Children first, blocks last: a block row in ClickHouse then proves the
    // whole batch landed, keeping block presence checks sound when a write
    // fails partway.
    if let Some(seed) = deduplication_seed {
        tokio::try_join!(
            ch.write_txs_deduplicated(txs, &seed),
            ch.write_logs_deduplicated(logs, &seed),
            ch.write_receipts_deduplicated(receipts, &seed),
        )?;
        ch.write_blocks_deduplicated(blocks, &seed).await
    } else {
        tokio::try_join!(
            ch.write_txs(txs),
            ch.write_logs(logs),
            ch.write_receipts(receipts),
        )?;
        ch.write_blocks(blocks).await
    }
}

async fn write_filtered_clickhouse_batch(
    ch: &ClickHouseSink,
    blocks: &[BlockRow],
    txs: &[TxRow],
    logs: &[LogRow],
    receipts: &[ReceiptRow],
    repair_blocks: &[i64],
) -> Result<()> {
    if blocks.is_empty() && txs.is_empty() && logs.is_empty() && receipts.is_empty() {
        return Ok(());
    }

    // Presence filtering makes retries across invocations safe. A fresh seed
    // prevents ClickHouse from suppressing rows that were deliberately deleted.
    let seed = replay_deduplication_seed();
    if repair_blocks.is_empty() {
        return write_clickhouse_batch(ch, blocks, txs, logs, receipts, Some(&seed)).await;
    }

    let guard = ch.maintenance_guard().await;
    ch.delete_blocks_exact(&guard, repair_blocks).await?;
    write_clickhouse_batch(ch, blocks, txs, logs, receipts, Some(&seed)).await
}

async fn filter_clickhouse_rows(
    ch: &ClickHouseSink,
    blocks: &mut Vec<BlockRow>,
    txs: &mut Vec<TxRow>,
    logs: &mut Vec<LogRow>,
    receipts: &mut Vec<ReceiptRow>,
) -> Result<Vec<i64>> {
    blocks.sort_unstable_by_key(|row| row.num);
    sort_by_natural_key(txs, |row| (row.block_num, row.idx));
    sort_by_natural_key(logs, |row| (row.block_num, row.log_idx));
    sort_by_natural_key(receipts, |row| (row.block_num, row.tx_idx));

    let mut block_nums = blocks
        .iter()
        .map(|row| row.num)
        .chain(txs.iter().map(|row| row.block_num))
        .chain(logs.iter().map(|row| row.block_num))
        .chain(receipts.iter().map(|row| row.block_num))
        .collect::<Vec<_>>();
    block_nums.sort_unstable();
    block_nums.dedup();
    if block_nums.is_empty() {
        return Ok(Vec::new());
    }

    let (block_check, tx_check, log_check, receipt_check) = tokio::try_join!(
        ch.canonical_blocks_present(blocks),
        ch.canonical_txs_present(txs, &block_nums),
        ch.canonical_logs_present(logs, &block_nums),
        ch.canonical_receipts_present(receipts, &block_nums),
    )?;

    let mut repair_blocks = block_check.stale_blocks.clone();
    repair_blocks.extend(tx_check.stale_blocks.iter().copied());
    repair_blocks.extend(log_check.stale_blocks.iter().copied());
    repair_blocks.extend(receipt_check.stale_blocks.iter().copied());
    for (block, is_present) in blocks.iter().zip(&block_check.present) {
        if !is_present
            && !block_check.occupied_blocks.contains(&block.num)
            && (tx_check.occupied_blocks.contains(&block.num)
                || log_check.occupied_blocks.contains(&block.num)
                || receipt_check.occupied_blocks.contains(&block.num))
        {
            repair_blocks.insert(block.num);
        }
    }
    let mut repair_block_nums = repair_blocks.iter().copied().collect::<Vec<_>>();
    repair_block_nums.sort_unstable();
    if !repair_block_nums.is_empty() {
        let source_blocks = blocks.iter().map(|block| block.num).collect::<HashSet<_>>();
        if let Some(block_num) = repair_blocks
            .iter()
            .find(|block_num| !source_blocks.contains(block_num))
        {
            anyhow::bail!(
                "cannot repair stale ClickHouse rows for block {block_num} without its canonical block row"
            );
        }
    }

    retain_missing_or_repaired(blocks, block_check.present, &repair_blocks, |row| row.num);
    retain_missing_or_repaired(txs, tx_check.present, &repair_blocks, |row| row.block_num);
    retain_missing_or_repaired(logs, log_check.present, &repair_blocks, |row| row.block_num);
    retain_missing_or_repaired(receipts, receipt_check.present, &repair_blocks, |row| {
        row.block_num
    });
    Ok(repair_block_nums)
}

fn sort_by_natural_key<T>(rows: &mut [T], key: impl FnMut(&T) -> (i64, i32)) {
    rows.sort_unstable_by_key(key);
}

fn retain_missing_or_repaired<T>(
    rows: &mut Vec<T>,
    present: Vec<bool>,
    repair_blocks: &HashSet<i64>,
    block_num: impl Fn(&T) -> i64,
) {
    assert_eq!(rows.len(), present.len());
    let mut present = present.into_iter();
    rows.retain(|row| {
        let is_present = present
            .next()
            .expect("canonical row check must match the input length");
        repair_blocks.contains(&block_num(row)) || !is_present
    });
}

/// Read the source high-water mark and durable dual-write tip together.
async fn pg_backfill_snapshot(pool: &Pool, chain_id: u64) -> Result<(Option<i64>, Option<i64>)> {
    let conn = pool.get().await?;
    let row = conn
        .query_one(
            "SELECT
                (SELECT MAX(num) FROM blocks),
                (SELECT tip_num FROM sync_state WHERE chain_id = $1)",
            &[&(chain_id as i64)],
        )
        .await?;
    Ok((row.get(0), row.get(1)))
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

#[cfg(test)]
mod tests {
    use super::sort_by_natural_key;

    #[test]
    fn replay_rows_are_sorted_by_natural_key() {
        let mut rows = vec![(3, 1), (1, 2), (2, 0), (1, 0)];
        sort_by_natural_key(&mut rows, |row| *row);
        assert_eq!(rows, vec![(1, 0), (1, 2), (2, 0), (3, 1)]);
    }
}
