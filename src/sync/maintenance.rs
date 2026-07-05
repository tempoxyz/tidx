//! Partition lifecycle maintenance.
//!
//! A background pass per chain that:
//!
//! 1. **Pre-creates partitions ahead of the chain head**, so crossing a
//!    partition boundary during live sync never runs DDL inside the hot
//!    write path (the writer's `ensure_block_partitions` call becomes a
//!    no-op).
//! 2. **Seals partitions that have fully left the hot window**: `CLUSTER`
//!    by primary key (physical block order — locality for history scans and
//!    a better compression ratio when the seal hook later gains compression),
//!    `ANALYZE`, `VACUUM (FREEZE)` (cold partitions never need
//!    anti-wraparound vacuums again), and a `partition_state` record so the
//!    partition is sealed exactly once.
//!
//! A partition is sealable only when it lies entirely below both
//! `synced_num` (contiguous from genesis — backfill and gap-fill will never
//! write into it again) and `tip_num - hot_window_blocks` (far behind any
//! possible reorg). Sealing is advisory: stray writes into a sealed
//! partition (e.g. deferred receipt backfill) remain allowed and merely
//! reduce its clustering.
//!
//! On pre-partitioning deployments (regular tables) every pass is a no-op.

use anyhow::{Context, Result};
use std::time::{Duration, Instant};
use tracing::{info, warn};

use crate::db::Pool;
use crate::metrics;
use crate::sync::writer::load_sync_state;

/// The partitioned chain tables, in seal order.
const CHAIN_TABLES: [&str; 5] = ["blocks", "txs", "tx_calls", "logs", "receipts"];

/// How often the maintenance pass runs.
const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(600);

/// Run partition maintenance forever (spawned per chain).
pub async fn run_maintenance_loop(pool: Pool, chain_id: u64, hot_window_blocks: u64) {
    loop {
        match run_maintenance_pass(&pool, chain_id, hot_window_blocks).await {
            Ok(sealed) if !sealed.is_empty() => {
                info!(chain_id, count = sealed.len(), "Sealed partitions");
            }
            Ok(_) => {}
            Err(e) => warn!(error = %e, chain_id, "Partition maintenance pass failed"),
        }
        tokio::time::sleep(MAINTENANCE_INTERVAL).await;
    }
}

/// One maintenance pass: pre-create partitions ahead of the head, then seal
/// every partition that has fully left the hot window. Returns the sealed
/// `(table, partition_idx)` pairs.
pub async fn run_maintenance_pass(
    pool: &Pool,
    chain_id: u64,
    hot_window_blocks: u64,
) -> Result<Vec<(String, i64)>> {
    // Pre-partitioning deployments (regular tables) have nothing to manage.
    if !is_partitioned(pool, "blocks").await? {
        return Ok(Vec::new());
    }

    let Some(state) = load_sync_state(pool, chain_id).await? else {
        return Ok(Vec::new());
    };

    let width = partition_width(pool).await?;

    precreate_ahead_of_head(pool, state.head_num.max(state.tip_num), width).await?;

    // Seal everything below both the contiguous-sync watermark (backfill and
    // gap-fill never write below it again) and the hot window (far behind
    // any reorg).
    let seal_bound = state
        .synced_num
        .min(state.tip_num.saturating_sub(hot_window_blocks));

    let mut sealed = Vec::new();
    for table in CHAIN_TABLES {
        for partition_idx in seal_candidates(pool, table, width, seal_bound).await? {
            seal_partition(pool, table, partition_idx).await?;
            metrics::record_partition_sealed(chain_id, table);
            sealed.push((table.to_string(), partition_idx));
        }
    }

    Ok(sealed)
}

async fn is_partitioned(pool: &Pool, table: &str) -> Result<bool> {
    let conn = pool.get().await?;
    Ok(conn
        .query_one(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_partitioned_table pt
                 JOIN pg_class c ON c.oid = pt.partrelid
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE c.relname = $1 AND n.nspname = current_schema()
             )",
            &[&table],
        )
        .await?
        .get(0))
}

async fn partition_width(pool: &Pool) -> Result<i64> {
    let conn = pool.get().await?;
    let width: i64 = conn
        .query_one("SELECT partition_blocks FROM storage_config", &[])
        .await
        .context("storage_config.partition_blocks is not set")?
        .get(0);
    Ok(width)
}

/// Make sure the partitions covering [head, head + width] exist, so boundary
/// crossings during live sync never create tables inside the write path.
async fn precreate_ahead_of_head(pool: &Pool, head: u64, width: i64) -> Result<()> {
    let conn = pool.get().await?;
    let from = head as i64;
    let to = from.saturating_add(width);
    for table in CHAIN_TABLES {
        conn.execute(
            "SELECT ensure_block_partitions($1, $2, $3)",
            &[&table, &from, &to],
        )
        .await?;
    }
    Ok(())
}

/// Unsealed partitions of `table` lying entirely at or below `seal_bound`.
async fn seal_candidates(
    pool: &Pool,
    table: &str,
    width: i64,
    seal_bound: u64,
) -> Result<Vec<i64>> {
    let conn = pool.get().await?;
    let rows = conn
        .query(
            "SELECT c.relname FROM pg_inherits i
             JOIN pg_class c ON c.oid = i.inhrelid
             JOIN pg_class p ON p.oid = i.inhparent
             JOIN pg_namespace n ON n.oid = p.relnamespace
             WHERE p.relname = $1 AND n.nspname = current_schema()
               AND NOT EXISTS (
                   SELECT 1 FROM partition_state s
                   WHERE s.table_name = $1
                     AND c.relname = $1 || '_p' || s.partition_idx
               )
             ORDER BY c.relname",
            &[&table],
        )
        .await?;

    let prefix = format!("{table}_p");
    let mut candidates = Vec::new();
    for row in &rows {
        let name: String = row.get(0);
        // Partitions are always created as {table}_p{k}; skip anything else
        // (e.g. a manually attached partition) rather than guessing bounds.
        let Some(idx) = name
            .strip_prefix(&prefix)
            .and_then(|s| s.parse::<i64>().ok())
        else {
            continue;
        };
        // Partition k covers [k * width, (k + 1) * width); sealable when its
        // last block is at or below the bound.
        let last_block = (idx + 1).saturating_mul(width).saturating_sub(1);
        if last_block >= 0 && (last_block as u64) <= seal_bound {
            candidates.push(idx);
        }
    }
    Ok(candidates)
}

/// Seal one partition and record it in partition_state.
///
/// Heap partitions get the full physical optimization: CLUSTER by the
/// primary-key index (exclusive lock on the cold, no-longer-written
/// partition for the duration of the rewrite), ANALYZE, VACUUM (FREEZE).
///
/// orioledb partitions only need ANALYZE: the access method is
/// index-organized (rows are always physically ordered by primary key) and
/// uses undo-log MVCC (no freeze debt), and it does not support CLUSTER or
/// VACUUM FULL at all.
async fn seal_partition(pool: &Pool, table: &str, partition_idx: i64) -> Result<()> {
    let partition = format!("{table}_p{partition_idx}");
    let conn = pool.get().await?;

    let am: String = conn
        .query_one(
            "SELECT am.amname FROM pg_class c
             JOIN pg_am am ON am.oid = c.relam
             WHERE c.oid = to_regclass($1::text)",
            &[&partition],
        )
        .await
        .with_context(|| format!("no access method for {partition}"))?
        .get(0);

    let start = Instant::now();
    // CLUSTER / ANALYZE / VACUUM can't run inside a transaction block; each
    // executes standalone. Record the seal only after everything succeeded
    // so a failed seal is retried on the next pass.
    if am != "orioledb" {
        let pk_index: String = conn
            .query_one(
                "SELECT ci.relname FROM pg_index i
                 JOIN pg_class ci ON ci.oid = i.indexrelid
                 WHERE i.indrelid = to_regclass($1::text) AND i.indisprimary",
                &[&partition],
            )
            .await
            .with_context(|| format!("no primary-key index on {partition}"))?
            .get(0);

        conn.batch_execute(&format!("CLUSTER \"{partition}\" USING \"{pk_index}\""))
            .await
            .with_context(|| format!("CLUSTER {partition} failed"))?;
    }
    conn.batch_execute(&format!("ANALYZE \"{partition}\""))
        .await
        .with_context(|| format!("ANALYZE {partition} failed"))?;
    if am != "orioledb" {
        conn.batch_execute(&format!("VACUUM (FREEZE) \"{partition}\""))
            .await
            .with_context(|| format!("VACUUM (FREEZE) {partition} failed"))?;
    }

    conn.execute(
        "INSERT INTO partition_state (table_name, partition_idx) VALUES ($1, $2)
         ON CONFLICT DO NOTHING",
        &[&table, &partition_idx],
    )
    .await?;

    info!(
        partition = %partition,
        am = %am,
        elapsed_ms = start.elapsed().as_millis() as u64,
        "Sealed partition"
    );

    Ok(())
}
