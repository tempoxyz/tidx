use anyhow::Result;
use tracing::{info, warn};

use super::Pool;

pub async fn run_migrations(pool: &Pool) -> Result<()> {
    let conn = pool.get().await?;

    // Kill any stale connections that might be holding locks (e.g., from a previous crash mid-COPY)
    // This prevents migrations from blocking indefinitely on lock acquisition
    let terminated = conn
        .execute(
            r#"
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE pid != pg_backend_pid()
              AND datname = current_database()
              AND state = 'active'
              AND wait_event_type = 'Client'
              AND wait_event = 'ClientRead'
              AND query_start < NOW() - INTERVAL '30 seconds'
            "#,
            &[],
        )
        .await?;

    if terminated > 0 {
        warn!(terminated, "Terminated stale connections holding locks");
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

    // Create read-only API role with SELECT-only access to indexed tables
    conn.batch_execute(include_str!("../../db/api_role.sql")).await?;

    Ok(())
}


