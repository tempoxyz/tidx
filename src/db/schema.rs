use anyhow::{Result, bail};
use tracing::{info, warn};

use super::Pool;

/// Storage options applied when chain tables are first created.
#[derive(Debug, Default, Clone)]
pub struct StorageOptions {
    /// Table access method (e.g. "orioledb"). None = server default.
    pub table_am: Option<String>,
    /// OrioleDB zstd compression level (-1..=22). Requires table_am = "orioledb".
    pub compress: Option<i32>,
}

impl StorageOptions {
    fn validate(&self) -> Result<()> {
        if let Some(am) = &self.table_am {
            if am.is_empty() || !am.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                bail!("Invalid pg_table_am {am:?}: must be an identifier");
            }
        }
        if let Some(level) = self.compress {
            if self.table_am.as_deref() != Some("orioledb") {
                bail!("pg_compress requires pg_table_am = \"orioledb\"");
            }
            if !(-1..=22).contains(&level) {
                bail!("pg_compress must be between -1 and 22, got {level}");
            }
        }
        Ok(())
    }
}

const VIRTUAL_FORWARD_INDEX_SQL: &str =
    include_str!("../../db/migrations/20260417_add_logs_virtual_forward_indexes.sql");
const VIRTUAL_FORWARD_TX_HASH_INDEX_SQL: &str =
    include_str!("../../db/migrations/20260417_add_logs_tx_hash_virtual_forward_index.sql");

pub async fn run_migrations(pool: &Pool) -> Result<()> {
    run_migrations_with_storage(pool, &StorageOptions::default()).await
}

/// Run schema migrations, creating chain tables with the given storage options.
///
/// The table access method only applies to tables created by this run
/// (`CREATE TABLE IF NOT EXISTS`); pre-existing tables keep theirs.
pub async fn run_migrations_with_storage(pool: &Pool, storage: &StorageOptions) -> Result<()> {
    storage.validate()?;
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

    if storage.table_am.as_deref() == Some("orioledb") {
        conn.batch_execute("CREATE EXTENSION IF NOT EXISTS orioledb")
            .await?;
    }

    // Base schema runs in one transaction so SET LOCAL scopes the table
    // access method to this DDL without leaking into the pooled connection.
    let mut ddl = String::from("BEGIN;\n");
    if let Some(am) = &storage.table_am {
        ddl.push_str(&format!(
            "SET LOCAL default_table_access_method = '{am}';\n"
        ));
        if let Some(level) = storage.compress {
            ddl.push_str(&format!("SET LOCAL orioledb.default_compress = {level};\n"));
        }
    }
    for sql in [
        include_str!("../../db/blocks.sql"),
        include_str!("../../db/txs.sql"),
        include_str!("../../db/logs.sql"),
        include_str!("../../db/receipts.sql"),
        include_str!("../../db/sync_state.sql"),
        include_str!("../../db/functions.sql"),
    ] {
        ddl.push_str(sql);
        ddl.push('\n');
    }
    ddl.push_str("COMMIT;\n");
    conn.batch_execute(&ddl).await?;

    // Warn when the requested AM didn't take effect (tables pre-existed).
    if let Some(am) = &storage.table_am {
        let actual: String = conn
            .query_one(
                "SELECT am.amname FROM pg_class c JOIN pg_am am ON am.oid = c.relam \
                 WHERE c.oid = 'blocks'::regclass",
                &[],
            )
            .await?
            .get(0);
        if &actual != am {
            warn!(
                requested = %am,
                actual = %actual,
                "Table access method differs from pg_table_am (tables existed before this setting)"
            );
        }
    }

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
