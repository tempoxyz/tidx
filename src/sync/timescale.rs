//! TimescaleDB columnstore cold tier: chain tables become hypertables
//! chunked by block number, and chunks that fall fully behind the hot
//! window are converted to compressed columnstore — locally, inside the
//! same Postgres. There is no FDW, no pushdown allowlist and no view
//! layer: cold rows keep Postgres types and stay joinable/pageable like
//! any other row.
//!
//! Hot chunks are plain rowstore with the complete, unchanged index set,
//! so the write path (COPY into staging + `INSERT … ON CONFLICT DO
//! NOTHING`) and hot serving behave exactly as on stock Postgres. Cold
//! chunks rely on columnstore metadata instead of B-trees: bloom sparse
//! indexes over the point-lookup columns (tx/block hashes, addresses,
//! topics) prune compressed batches for equality probes, and minmax
//! metadata over the orderby columns (block number/timestamp) prunes
//! range scans. Reorg deletes and idempotent re-inserts remain valid on
//! compressed chunks (slower, but reorgs only ever touch the rowstore
//! window in practice).
//!
//! A chunk is converted only when it is:
//!
//! - entirely behind the reorg horizon (`tip_num - hot_window_blocks`), and
//! - fully populated: every block number in its range is present in
//!   `blocks`, so gap-fill has nothing left to write into it.
//!
//! Coverage is checked per chunk rather than against the global
//! `synced_num` watermark so compression can chase the gap-fill frontier
//! during an initial backfill (which proceeds tip-down): Postgres never
//! has to hold the whole uncompressed archive before the first chunk
//! compresses.
//!
//! Conversions run one chunk at a time from a maintenance loop owned by
//! tidx (not a Timescale background policy) so gating on sync progress
//! stays in one place. Every step is idempotent: a failed or interrupted
//! conversion leaves the chunk uncompressed for the next pass.

use anyhow::{Context, Result, bail};
use std::time::Instant;
use tracing::{info, warn};

use crate::config::TimescaleConfig;
use crate::db::Pool;
use crate::metrics;

/// Tables managed by the cold tier, with their block-number column.
const TIERED_TABLES: [(&str, &str); 4] = [
    ("blocks", "num"),
    ("txs", "block_num"),
    ("logs", "block_num"),
    ("receipts", "block_num"),
];

/// Columnstore settings per table: `(orderby, sparse index)`.
///
/// `orderby` leads with the block number — the dimension tidx actually
/// range-scans (partitioning, keyset pagination, gap/coverage checks) —
/// and keeps every primary-key column so unique-conflict checks against
/// compressed chunks stay cheap; orderby columns get minmax/firstlast
/// metadata automatically. Blooms mirror the rowstore lookup indexes
/// (equality point-lookup columns). `segmentby` is empty everywhere:
/// addresses/hashes are near-unique per chunk, so segmenting by them
/// would fragment compression.
fn columnstore_settings(table: &str) -> (&'static str, &'static str) {
    match table {
        "blocks" => (r#"num, "timestamp""#, "bloom(hash)"),
        "txs" => (
            "block_num, idx, block_timestamp",
            r#"bloom(hash),bloom("from"),bloom("to")"#,
        ),
        "logs" => (
            "block_num, log_idx, block_timestamp",
            "bloom(selector),bloom(tx_hash),bloom(address),bloom(topic1),bloom(topic2),bloom(topic3)",
        ),
        "receipts" => (
            "block_num, tx_idx, block_timestamp",
            r#"bloom(tx_hash),bloom("from"),bloom(fee_payer),bloom(contract_address)"#,
        ),
        other => unreachable!("no columnstore settings for table {other}"),
    }
}

/// Set up the cold tier: create the extension, convert the chain tables
/// to hypertables and configure columnstore settings.
///
/// Returns `false` (with a warning) when the timescaledb extension is not
/// available on the server, so deployments on stock Postgres images keep
/// working with the flag on. Errors when a table already holds data but
/// is not a hypertable — enabling the cold tier requires a fresh database
/// (blue/green re-sync), matching the no-in-place-migration convention.
pub async fn setup(pool: &Pool, cfg: &TimescaleConfig) -> Result<bool> {
    let conn = pool.get().await?;

    let available = conn
        .query_opt(
            "SELECT 1 FROM pg_available_extensions WHERE name = 'timescaledb'",
            &[],
        )
        .await?
        .is_some();
    if !available {
        warn!(
            "timescale.enabled is set but the timescaledb extension is not available on this \
             Postgres server; continuing without the columnstore cold tier"
        );
        return Ok(false);
    }

    conn.batch_execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
        .await
        .context("creating timescaledb extension")?;

    // Columnstore compression is a TSL-licensed feature (free
    // self-hosted). Apache-only builds would fail much later with an
    // obscure error, so check up front.
    let license: String = conn
        .query_one("SHOW timescaledb.license", &[])
        .await?
        .get(0);
    if license != "timescale" {
        bail!(
            "timescale.enabled requires the TSL-licensed timescaledb build \
             (timescaledb.license = 'timescale'); this server reports '{license}', \
             which cannot use columnstore compression"
        );
    }

    for (table, block_col) in TIERED_TABLES {
        if is_hypertable(&conn, table).await? {
            continue;
        }

        let has_rows: bool = conn
            .query_one(&format!("SELECT EXISTS (SELECT 1 FROM {table})"), &[])
            .await?
            .get(0);
        if has_rows {
            bail!(
                "timescale.enabled requires converting {table} to a hypertable, but it already \
                 contains data; start from a fresh database (blue/green re-sync) or disable \
                 timescale"
            );
        }

        conn.query_one(
            "SELECT FROM create_hypertable($1::text::regclass, by_range($2::text::name, $3::int8), \
             create_default_indexes => false)",
            &[&table, &block_col, &(cfg.chunk_blocks as i64)],
        )
        .await
        .with_context(|| format!("converting {table} to a hypertable"))?;

        let (orderby, index) = columnstore_settings(table);
        conn.batch_execute(&format!(
            "ALTER TABLE {table} SET (\
                 timescaledb.enable_columnstore, \
                 timescaledb.orderby = '{orderby}', \
                 timescaledb.segmentby = '', \
                 timescaledb.sparse_index = '{index}')"
        ))
        .await
        .with_context(|| format!("configuring columnstore settings on {table}"))?;

        info!(
            table,
            chunk_blocks = cfg.chunk_blocks,
            orderby,
            index,
            "Converted table to hypertable with columnstore cold tier"
        );
    }

    Ok(true)
}

async fn is_hypertable(conn: &deadpool_postgres::Object, table: &str) -> Result<bool> {
    Ok(conn
        .query_opt(
            "SELECT 1 FROM timescaledb_information.hypertables \
             WHERE hypertable_schema = 'public' AND hypertable_name = $1",
            &[&table],
        )
        .await?
        .is_some())
}

/// First block that must stay in rowstore (exclusive ceiling for
/// conversion): chunks whose `[range_start, range_end)` lies entirely
/// below it are behind the reorg horizon. `None` while there is nothing
/// safe to convert.
async fn cold_end_exclusive(
    conn: &deadpool_postgres::Object,
    chain_id: u64,
    hot_window_blocks: u64,
) -> Result<Option<i64>> {
    let Some(row) = conn
        .query_opt(
            "SELECT tip_num FROM sync_state WHERE chain_id = $1",
            &[&(chain_id as i64)],
        )
        .await?
    else {
        return Ok(None);
    };

    let tip: i64 = row.get(0);
    let cold_max = tip - hot_window_blocks as i64;
    Ok((cold_max > 0).then_some(cold_max + 1))
}

/// Whether every block in `[range_start, range_end)` is present locally,
/// i.e. gap-fill has nothing left to write into this range. Follows the
/// gap detector's convention that the chain starts at block 1. DISTINCT
/// guards against transient duplicate block numbers around reorgs.
async fn range_fully_covered(
    conn: &deadpool_postgres::Object,
    range_start: i64,
    range_end: i64,
) -> Result<bool> {
    let floor = range_start.max(1);
    let expected = range_end - floor;
    if expected <= 0 {
        return Ok(true);
    }
    let present: i64 = conn
        .query_one(
            "SELECT count(DISTINCT num) FROM blocks WHERE num >= $1 AND num < $2",
            &[&floor, &range_end],
        )
        .await?
        .get(0);
    Ok(present >= expected)
}

/// One maintenance pass: convert every eligible chunk to columnstore.
/// Returns the converted `(table, range_start, range_end)` triples.
pub async fn run_pass(
    pool: &Pool,
    chain_id: u64,
    cfg: &TimescaleConfig,
) -> Result<Vec<(String, i64, i64)>> {
    let conn = pool.get().await?;

    // Converting a multi-GB chunk far exceeds the pool's default
    // per-statement timeout. Session state is discarded on recycle.
    conn.batch_execute("SET statement_timeout = 0").await?;

    let Some(end) = cold_end_exclusive(&conn, chain_id, cfg.hot_window_blocks).await? else {
        return Ok(Vec::new());
    };
    metrics::set_timescale_cold_ceiling(chain_id, (end - 1) as u64);

    let mut converted = Vec::new();
    for (table, _) in TIERED_TABLES {
        let chunks = conn
            .query(
                "SELECT format('%I.%I', chunk_schema, chunk_name), \
                        range_start_integer, range_end_integer \
                 FROM timescaledb_information.chunks \
                 WHERE hypertable_schema = 'public' AND hypertable_name = $1 \
                   AND NOT is_compressed AND range_end_integer <= $2 \
                 ORDER BY range_start_integer",
                &[&table, &end],
            )
            .await?;

        for row in chunks {
            let chunk: String = row.get(0);
            let range_start: i64 = row.get(1);
            let range_end: i64 = row.get(2);

            // Gap-fill may still be writing into this range (initial
            // backfill proceeds tip-down); leave it in rowstore until
            // every block is present.
            if !range_fully_covered(&conn, range_start, range_end).await? {
                continue;
            }

            let start = Instant::now();
            // Simple-query protocol: the procedure manages its own
            // transactions. Chunk identifiers come pre-quoted from the
            // catalog via format('%I.%I', …).
            let result = conn
                .batch_execute(&format!(
                    "CALL convert_to_columnstore('{chunk}'::regclass, if_not_columnstore => true)"
                ))
                .await;

            match result {
                Ok(()) => {
                    info!(
                        table,
                        chunk = %chunk,
                        range_start,
                        range_end,
                        elapsed_secs = start.elapsed().as_secs_f64(),
                        "Converted chunk to columnstore"
                    );
                    metrics::record_timescale_chunk_compressed(chain_id, table);
                    converted.push((table.to_string(), range_start, range_end));
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        table,
                        chunk = %chunk,
                        range_start,
                        range_end,
                        "Columnstore conversion failed; will retry next pass"
                    );
                }
            }
        }
    }

    Ok(converted)
}

/// Log per-table hypertable sizes (cheap catalog lookups).
async fn log_sizes(pool: &Pool) -> Result<()> {
    let conn = pool.get().await?;
    for (table, _) in TIERED_TABLES {
        let row = conn
            .query_one(
                "SELECT pg_size_pretty(hypertable_size($1::regclass)), \
                        count(*) FILTER (WHERE is_compressed), count(*) \
                 FROM timescaledb_information.chunks \
                 WHERE hypertable_schema = 'public' AND hypertable_name = $1",
                &[&table],
            )
            .await?;
        let size: String = row.get(0);
        let compressed: i64 = row.get(1);
        let total: i64 = row.get(2);
        info!(
            table,
            size = %size,
            compressed_chunks = compressed,
            total_chunks = total,
            "Cold-tier size"
        );
    }
    Ok(())
}

/// Cold-tier maintenance loop: runs [`run_pass`] every `interval_secs`.
/// Never exits; conversions are idempotent, so an interrupted pass (or
/// process restart) simply resumes on the next tick.
pub async fn run_maintenance_loop(pool: Pool, chain_id: u64, cfg: TimescaleConfig) {
    info!(
        chain_id,
        chunk_blocks = cfg.chunk_blocks,
        hot_window_blocks = cfg.hot_window_blocks,
        interval_secs = cfg.interval_secs,
        "Timescale cold-tier maintenance started"
    );

    let mut interval =
        tokio::time::interval(std::time::Duration::from_secs(cfg.interval_secs.max(1)));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        interval.tick().await;
        match run_pass(&pool, chain_id, &cfg).await {
            Ok(converted) if !converted.is_empty() => {
                info!(
                    chain_id,
                    chunks = converted.len(),
                    "Cold-tier pass converted chunks to columnstore"
                );
                if let Err(e) = log_sizes(&pool).await {
                    warn!(chain_id, error = %e, "Failed to report cold-tier sizes");
                }
            }
            Ok(_) => {}
            Err(e) => {
                warn!(chain_id, error = %e, "Cold-tier pass failed; will retry");
            }
        }
    }
}
