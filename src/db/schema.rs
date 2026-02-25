use anyhow::Result;
use tracing::{info, warn};

use super::Pool;

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
        warn!(count = terminated.len(), "Terminated stale connections before migrations");
    }

    info!("Running schema migrations");
    conn.batch_execute(include_str!("../../db/blocks.sql")).await?;
    conn.batch_execute(include_str!("../../db/txs.sql")).await?;
    conn.batch_execute(include_str!("../../db/logs.sql")).await?;
    conn.batch_execute(include_str!("../../db/receipts.sql")).await?;
    conn.batch_execute(include_str!("../../db/sync_state.sql")).await?;
    conn.batch_execute(include_str!("../../db/functions.sql")).await?;

    // Load any optional extensions
    conn.batch_execute(include_str!("../../db/extensions.sql")).await?;

    Ok(())
}


