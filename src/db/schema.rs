use anyhow::{Context, Result, anyhow, bail};
use tracing::{info, warn};

use super::Pool;

const VIRTUAL_FORWARD_INDEX_SQL: &str =
    include_str!("../../db/migrations/20260417_add_logs_virtual_forward_indexes.sql");
const VIRTUAL_FORWARD_TX_HASH_INDEX_SQL: &str =
    include_str!("../../db/migrations/20260417_add_logs_tx_hash_virtual_forward_index.sql");
const LOGS_ADDRESS_TOPIC0_INDEX_SQL: &str =
    include_str!("../../db/migrations/20260722_add_logs_address_topic0_index.sql");
const LOGS_SELECTOR_INDEXED_ADDRESS_INDEX_SQL: &str =
    include_str!("../../db/migrations/20260722_add_logs_selector_indexed_address_index.sql");
const TXS_FROM_BLOCK_INDEX_SQL: &str =
    include_str!("../../db/migrations/20260723_add_txs_from_block_index.sql");
const TXS_FEE_PAYER_BLOCK_INDEX_SQL: &str =
    include_str!("../../db/migrations/20260723_add_txs_fee_payer_block_index.sql");
const RECEIPTS_FEE_PAYER_BLOCK_INDEX_SQL: &str =
    include_str!("../../db/migrations/20260723_add_receipts_fee_payer_block_index.sql");
const RECEIPTS_FROM_BLOCK_INDEX_SQL: &str =
    include_str!("../../db/migrations/20260828_add_receipts_from_block_index.sql");
const LOGS_SELECTOR_TOPIC1_BLOCK_INDEX_SQL: &str =
    include_str!("../../db/migrations/20260723_add_logs_selector_topic1_block_index.sql");
const LOGS_SELECTOR_TOPIC2_BLOCK_INDEX_SQL: &str =
    include_str!("../../db/migrations/20260723_add_logs_selector_topic2_block_index.sql");
const LOGS_SELECTOR_TOPIC3_BLOCK_INDEX_SQL: &str =
    include_str!("../../db/migrations/20260723_add_logs_selector_topic3_block_index.sql");

const POST_STARTUP_INDEXES: &[&str] = &[
    VIRTUAL_FORWARD_INDEX_SQL,
    VIRTUAL_FORWARD_TX_HASH_INDEX_SQL,
    LOGS_ADDRESS_TOPIC0_INDEX_SQL,
    LOGS_SELECTOR_INDEXED_ADDRESS_INDEX_SQL,
    TXS_FROM_BLOCK_INDEX_SQL,
    TXS_FEE_PAYER_BLOCK_INDEX_SQL,
    RECEIPTS_FEE_PAYER_BLOCK_INDEX_SQL,
    RECEIPTS_FROM_BLOCK_INDEX_SQL,
    LOGS_SELECTOR_TOPIC1_BLOCK_INDEX_SQL,
    LOGS_SELECTOR_TOPIC2_BLOCK_INDEX_SQL,
    LOGS_SELECTOR_TOPIC3_BLOCK_INDEX_SQL,
];

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
    conn.batch_execute(include_str!("../../db/receipt_repair.sql"))
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

    // Load any optional extensions
    conn.batch_execute(include_str!("../../db/extensions.sql"))
        .await?;

    drop(conn);

    // Pre-create weekly partitions around now (partitioned installs only)
    // so realtime writes never wait on partition DDL.
    let now = chrono::Utc::now();
    super::partitions::ensure_partitions_covering(
        pool,
        now - chrono::Duration::days(7),
        now + chrono::Duration::days(35),
    )
    .await?;

    Ok(())
}

pub async fn run_post_startup_migrations(pool: &Pool) -> Result<()> {
    let conn = pool.get().await?;
    // General pool connections have a 60-second statement timeout. Historical
    // index builds routinely exceed that and a cancelled concurrent build
    // leaves behind an unusable same-name index.
    conn.batch_execute("SET statement_timeout = 0").await?;

    let migration_result = run_post_startup_index_migrations(&conn).await;
    let reset_result = conn
        .batch_execute("RESET statement_timeout")
        .await
        .context("Failed to reset statement timeout after post-startup migrations");

    match (migration_result, reset_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(reset_error)) => Err(error.context(format!(
            "Additionally failed to reset statement timeout: {reset_error:#}"
        ))),
    }
}

async fn run_post_startup_index_migrations(conn: &tokio_postgres::Client) -> Result<()> {
    let partitioned = core_tables_partitioned(conn).await?;
    for sql in POST_STARTUP_INDEXES {
        let migration = IndexMigration::parse(sql)?;
        if partitioned {
            ensure_partitioned_index(conn, &migration).await?;
        } else {
            ensure_concurrent_index(conn, &migration).await?;
        }
    }
    Ok(())
}

async fn core_tables_partitioned(conn: &tokio_postgres::Client) -> Result<bool> {
    let row = conn
        .query_opt(
            "SELECT relkind = 'p' FROM pg_class
             WHERE relname = 'blocks' AND relnamespace = 'public'::regnamespace",
            &[],
        )
        .await?;
    Ok(row.map(|row| row.get(0)).unwrap_or(false))
}

struct IndexMigration<'a> {
    sql: &'a str,
    name: &'a str,
    table: &'a str,
    suffix: &'a str,
}

impl<'a> IndexMigration<'a> {
    fn parse(sql: &'a str) -> Result<Self> {
        const PREFIX: &str = "CREATE INDEX CONCURRENTLY IF NOT EXISTS ";

        let sql = sql.trim().trim_end_matches(';');
        let rest = sql
            .strip_prefix(PREFIX)
            .ok_or_else(|| anyhow!("Unsupported post-startup index migration: {sql}"))?;
        let name_end = rest
            .find(char::is_whitespace)
            .ok_or_else(|| anyhow!("Index migration is missing ON clause: {sql}"))?;
        let name = &rest[..name_end];
        let rest = rest[name_end..]
            .trim_start()
            .strip_prefix("ON ")
            .ok_or_else(|| anyhow!("Index migration is missing ON clause: {sql}"))?;
        let table_end = rest
            .find(char::is_whitespace)
            .ok_or_else(|| anyhow!("Index migration is missing a definition: {sql}"))?;
        let table = &rest[..table_end];
        let suffix = rest[table_end..].trim_start();

        if !is_safe_identifier(name) || !is_safe_identifier(table) || !suffix.starts_with('(') {
            bail!("Unsupported post-startup index migration: {sql}");
        }

        Ok(Self {
            sql,
            name,
            table,
            suffix,
        })
    }

    fn parent_sql(&self) -> String {
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON ONLY {} {}",
            quote_ident(self.name),
            quote_relation("public", self.table),
            self.suffix
        )
    }

    fn child_sql(&self, child_name: &str, partition: &Partition) -> String {
        format!(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS {} ON {} {}",
            quote_ident(child_name),
            quote_relation(&partition.schema, &partition.name),
            self.suffix
        )
    }
}

#[derive(Clone)]
struct Partition {
    oid: u32,
    schema: String,
    name: String,
}

struct IndexState {
    valid: bool,
    ready: bool,
    partitioned: bool,
}

async fn ensure_concurrent_index(
    conn: &tokio_postgres::Client,
    migration: &IndexMigration<'_>,
) -> Result<()> {
    if let Some(state) = index_state(conn, "public", migration.name).await? {
        if state.partitioned {
            bail!(
                "Expected regular index {}, found partitioned index",
                migration.name
            );
        }
        if state.valid && state.ready {
            return Ok(());
        }

        conn.batch_execute(&format!(
            "DROP INDEX CONCURRENTLY {}",
            quote_relation("public", migration.name)
        ))
        .await
        .with_context(|| format!("Failed to drop invalid index {}", migration.name))?;
    }

    conn.batch_execute(migration.sql)
        .await
        .with_context(|| format!("Failed to create index {}", migration.name))?;
    require_valid_index(conn, "public", migration.name, false).await
}

async fn ensure_partitioned_index(
    conn: &tokio_postgres::Client,
    migration: &IndexMigration<'_>,
) -> Result<()> {
    match index_state(conn, "public", migration.name).await? {
        Some(state) if !state.partitioned => {
            bail!(
                "Expected partitioned index {}, found regular index",
                migration.name
            );
        }
        Some(state) if state.valid && state.ready => return Ok(()),
        Some(_) => {}
        None => {
            conn.batch_execute(&migration.parent_sql())
                .await
                .with_context(|| {
                    format!("Failed to create partitioned index {}", migration.name)
                })?;
        }
    }

    // A partition can be added while its siblings are being indexed. Re-scan
    // until PostgreSQL marks the parent valid; future partitions then inherit
    // the completed index automatically.
    for _ in 0..3 {
        for partition in table_partitions(conn, "public", migration.table).await? {
            if let Some((schema, name, state)) =
                attached_partition_index(conn, "public", migration.name, partition.oid).await?
            {
                if !state.valid || !state.ready {
                    conn.batch_execute(&format!(
                        "REINDEX INDEX CONCURRENTLY {}",
                        quote_relation(&schema, &name)
                    ))
                    .await
                    .with_context(|| {
                        format!("Failed to rebuild invalid attached index {schema}.{name}")
                    })?;
                    require_valid_index(conn, &schema, &name, false).await?;
                }
                continue;
            }

            let child_name = child_index_name(migration.name, &partition);
            if let Some(state) = index_state(conn, &partition.schema, &child_name).await? {
                if state.partitioned {
                    bail!(
                        "Expected regular child index {}.{}, found partitioned index",
                        partition.schema,
                        child_name
                    );
                }
                if !state.valid || !state.ready {
                    conn.batch_execute(&format!(
                        "DROP INDEX CONCURRENTLY {}",
                        quote_relation(&partition.schema, &child_name)
                    ))
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to drop invalid child index {}.{}",
                            partition.schema, child_name
                        )
                    })?;
                }
            }

            if index_state(conn, &partition.schema, &child_name)
                .await?
                .is_none()
            {
                conn.batch_execute(&migration.child_sql(&child_name, &partition))
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to create child index {}.{}",
                            partition.schema, child_name
                        )
                    })?;
            }
            require_valid_index(conn, &partition.schema, &child_name, false).await?;

            conn.batch_execute(&format!(
                "ALTER INDEX {} ATTACH PARTITION {}",
                quote_relation("public", migration.name),
                quote_relation(&partition.schema, &child_name)
            ))
            .await
            .with_context(|| {
                format!(
                    "Failed to attach {}.{} to {}",
                    partition.schema, child_name, migration.name
                )
            })?;
        }

        if let Some(state) = index_state(conn, "public", migration.name).await?
            && state.valid
            && state.ready
        {
            return Ok(());
        }
    }

    bail!(
        "Partitioned index {} remained invalid after indexing all current partitions",
        migration.name
    )
}

async fn table_partitions(
    conn: &tokio_postgres::Client,
    schema: &str,
    table: &str,
) -> Result<Vec<Partition>> {
    let rows = conn
        .query(
            r#"
            SELECT child.oid, child_ns.nspname, child.relname
            FROM pg_inherits inheritance
            JOIN pg_class parent ON parent.oid = inheritance.inhparent
            JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
            JOIN pg_class child ON child.oid = inheritance.inhrelid
            JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
            WHERE parent_ns.nspname = $1
              AND parent.relname = $2
            ORDER BY child_ns.nspname, child.relname
            "#,
            &[&schema, &table],
        )
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| Partition {
            oid: row.get(0),
            schema: row.get(1),
            name: row.get(2),
        })
        .collect())
}

async fn attached_partition_index(
    conn: &tokio_postgres::Client,
    schema: &str,
    parent_index: &str,
    partition_oid: u32,
) -> Result<Option<(String, String, IndexState)>> {
    let row = conn
        .query_opt(
            r#"
            SELECT child_ns.nspname, child.relname,
                   child_index.indisvalid, child_index.indisready,
                   child.relkind = 'I'
            FROM pg_inherits inheritance
            JOIN pg_class parent ON parent.oid = inheritance.inhparent
            JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
            JOIN pg_class child ON child.oid = inheritance.inhrelid
            JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
            JOIN pg_index child_index ON child_index.indexrelid = child.oid
            WHERE parent_ns.nspname = $1
              AND parent.relname = $2
              AND child_index.indrelid = $3
            "#,
            &[&schema, &parent_index, &partition_oid],
        )
        .await?;

    Ok(row.map(|row| {
        (
            row.get(0),
            row.get(1),
            IndexState {
                valid: row.get(2),
                ready: row.get(3),
                partitioned: row.get(4),
            },
        )
    }))
}

async fn index_state(
    conn: &tokio_postgres::Client,
    schema: &str,
    index: &str,
) -> Result<Option<IndexState>> {
    let row = conn
        .query_opt(
            r#"
            SELECT index_state.indisvalid, index_state.indisready,
                   relation.relkind = 'I'
            FROM pg_class relation
            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
            JOIN pg_index index_state ON index_state.indexrelid = relation.oid
            WHERE namespace.nspname = $1
              AND relation.relname = $2
            "#,
            &[&schema, &index],
        )
        .await?;

    Ok(row.map(|row| IndexState {
        valid: row.get(0),
        ready: row.get(1),
        partitioned: row.get(2),
    }))
}

async fn require_valid_index(
    conn: &tokio_postgres::Client,
    schema: &str,
    index: &str,
    partitioned: bool,
) -> Result<()> {
    let state = index_state(conn, schema, index)
        .await?
        .ok_or_else(|| anyhow!("Index {schema}.{index} was not created"))?;
    if !state.valid || !state.ready || state.partitioned != partitioned {
        bail!("Index {schema}.{index} is not valid and ready after migration");
    }
    Ok(())
}

fn child_index_name(parent_index: &str, partition: &Partition) -> String {
    let full = format!("{parent_index}_{}", partition.name);
    if full.len() <= 63 {
        full
    } else {
        let suffix = format!("_p{}", partition.oid);
        let keep = 63 - suffix.len();
        format!(
            "{}{}",
            &parent_index[..parent_index.len().min(keep)],
            suffix
        )
    }
}

fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn quote_relation(schema: &str, relation: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(relation))
}

fn is_safe_identifier(identifier: &str) -> bool {
    !identifier.is_empty()
        && identifier.len() <= 63
        && identifier
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && identifier
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}
