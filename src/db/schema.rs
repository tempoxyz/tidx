use anyhow::Result;
use tracing::{info, warn};

use super::Pool;

const VIRTUAL_FORWARD_INDEX_SQL: &str =
    include_str!("../../db/migrations/20260417_add_logs_virtual_forward_indexes.sql");
const VIRTUAL_FORWARD_TX_HASH_INDEX_SQL: &str = "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_logs_tx_hash_virtual_forward \
ON logs (tx_hash, log_idx) \
WHERE is_virtual_forward = TRUE;";

pub async fn run_migrations(pool: &Pool) -> Result<()> {
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
    conn.batch_execute(include_str!("../../db/blocks.sql"))
        .await?;
    conn.batch_execute(include_str!("../../db/txs.sql")).await?;
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

    // TIP-1031: ed25519 consensus proposer pubkey on blocks.
    conn.batch_execute(include_str!(
        "../../db/migrations/20260430_add_blocks_consensus_proposer.sql"
    ))
    .await?;

    // Heavyweight upgrades such as concurrent index creation run in a
    // best-effort post-startup task so normal boot isn't blocked. Production
    // should still apply them in a pre-deploy migration flow.

    // Load any optional extensions
    conn.batch_execute(include_str!("../../db/extensions.sql"))
        .await?;

    Ok(())
}

pub async fn run_post_startup_migrations(pool: &Pool) -> Result<()> {
    let conn = pool.get().await?;

    conn.batch_execute(VIRTUAL_FORWARD_INDEX_SQL).await?;
    conn.batch_execute(VIRTUAL_FORWARD_TX_HASH_INDEX_SQL)
        .await?;

    Ok(())
}
