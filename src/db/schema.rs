use anyhow::Result;
use tracing::{info, warn};

use super::Pool;

const VIRTUAL_FORWARD_INDEX_SQL: &str =
    include_str!("../../db/migrations/20260417_add_logs_virtual_forward_indexes.sql");
const VIRTUAL_FORWARD_TX_HASH_INDEX_SQL: &str =
    include_str!("../../db/migrations/20260417_add_logs_tx_hash_virtual_forward_index.sql");
const NORMALIZE_TX_CALLS_SQL: &str =
    include_str!("../../db/migrations/20260705_normalize_tx_calls.sql");

/// Default number of blocks per partition of the chain tables.
pub const DEFAULT_PARTITION_BLOCKS: u64 = 1_000_000;

pub async fn run_migrations(pool: &Pool, partition_blocks: u64) -> Result<()> {
    let conn = pool.get().await?;

    // Kill ALL other connections to this database before running migrations.
    // On container restart, any existing connections are stale (from the old process)
    // and may hold locks that block DDL (e.g., COPY mid-flight blocks CREATE INDEX).
    let terminated: Vec<_> = conn
        .query(
            r#"
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE pid != pg_backend_pid()
              AND datname = current_database()
            "#,
            &[],
        )
        .await?;

    if !terminated.is_empty() {
        warn!(
            count = terminated.len(),
            "Terminated stale connections before migrations"
        );
    }

    info!("Running schema migrations");

    // Extensions first: table and partition DDL below branches on whether
    // the orioledb access method is available.
    conn.batch_execute(include_str!("../../db/extensions.sql"))
        .await?;

    // Storage layout settings + partition machinery first: the partition
    // width is locked at first boot (existing boundaries are fixed; a
    // different width would overlap them), and ensure_block_partitions is
    // called by the writer before every batch.
    conn.batch_execute(include_str!("../../db/storage.sql"))
        .await?;
    conn.execute(
        "INSERT INTO storage_config (partition_blocks) VALUES ($1)
         ON CONFLICT (single_row) DO NOTHING",
        &[&(partition_blocks as i64)],
    )
    .await?;
    let locked: i64 = conn
        .query_one("SELECT partition_blocks FROM storage_config", &[])
        .await?
        .get(0);
    if locked != partition_blocks as i64 {
        warn!(
            configured = partition_blocks,
            locked,
            "partition_blocks differs from the value locked at first boot; using the locked value"
        );
    }
    conn.batch_execute(include_str!("../../db/partitions.sql"))
        .await?;

    conn.batch_execute(include_str!("../../db/blocks.sql"))
        .await?;
    conn.batch_execute(include_str!("../../db/txs.sql")).await?;
    conn.batch_execute(include_str!("../../db/tx_calls.sql"))
        .await?;
    conn.batch_execute(include_str!("../../db/logs.sql"))
        .await?;
    conn.batch_execute(include_str!("../../db/receipts.sql"))
        .await?;
    conn.batch_execute(include_str!("../../db/sync_state.sql"))
        .await?;
    conn.batch_execute(include_str!("../../db/functions.sql"))
        .await?;

    // Apply lightweight additive upgrades for existing deployments whose
    // tables were created before newer columns were introduced.
    conn.batch_execute(include_str!(
        "../../db/migrations/20260416_add_is_virtual_forward.sql"
    ))
    .await?;
    conn.batch_execute(include_str!(
        "../../db/migrations/20260430_add_blocks_consensus_proposer.sql"
    ))
    .await?;

    // Heavyweight upgrades such as concurrent index creation run in a
    // best-effort post-startup task so normal boot isn't blocked.

    Ok(())
}

pub async fn run_post_startup_migrations(pool: &Pool) -> Result<()> {
    let conn = pool.get().await?;

    // CREATE INDEX CONCURRENTLY is not supported on partitioned tables, so
    // on the partitioned layout create the same indexes non-concurrently
    // (no-op once they exist; partitioned deployments start empty, so the
    // first creation is cheap).
    let logs_partitioned: bool = conn
        .query_one(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_partitioned_table pt
                 JOIN pg_class c ON c.oid = pt.partrelid
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE c.relname = 'logs' AND n.nspname = current_schema()
             )",
            &[],
        )
        .await?
        .get(0);

    if logs_partitioned {
        conn.batch_execute(&VIRTUAL_FORWARD_INDEX_SQL.replace("CONCURRENTLY ", ""))
            .await?;
        conn.batch_execute(&VIRTUAL_FORWARD_TX_HASH_INDEX_SQL.replace("CONCURRENTLY ", ""))
            .await?;
    } else {
        conn.batch_execute(VIRTUAL_FORWARD_INDEX_SQL).await?;
        conn.batch_execute(VIRTUAL_FORWARD_TX_HASH_INDEX_SQL)
            .await?;
    }

    // Legacy upgrade: backfill tx_calls from the old txs.calls JSONB column,
    // then drop it. Potentially long-running on large deployments, hence
    // post-startup. No-op once the column is gone.
    conn.batch_execute(NORMALIZE_TX_CALLS_SQL).await?;

    Ok(())
}
