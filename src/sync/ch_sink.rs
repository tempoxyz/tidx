//! ClickHouse direct-write sink.
//!
//! Writes blocks, transactions, logs, and receipts directly to ClickHouse
//! via the official `clickhouse` crate using RowBinary format with LZ4 compression.

use anyhow::{Result, anyhow};
use clickhouse::{Row, RowOwned, RowRead};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

use crate::clickhouse_schema::{
    BackfillPolicy, ClickHouseObject, ClickHouseObjectKind, base_objects, derived_backfills,
    derived_objects, migrations, reorg_tables,
};
use crate::metrics;
use crate::types::{BlockRow, LogRow, ReceiptRow, TxRow};

/// DDL for the catalog state table that records the checksum of every
/// migration / view / materialized view the sink has applied. Used to detect
/// definition drift on subsequent `ensure_schema()` calls.
const SCHEMA_OBJECTS_TABLE_DDL: &str = "
    CREATE TABLE IF NOT EXISTS tidx_schema_objects (
        name       String,
        checksum   String,
        kind       String,
        applied_at DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(applied_at)
    ORDER BY name
";

/// Max rows per ClickHouse INSERT to avoid unbounded memory growth during backfills.
const CH_INSERT_CHUNK_SIZE: usize = 10_000;

/// Max retry attempts for transient ClickHouse write failures.
const CH_MAX_RETRIES: u32 = 3;

/// Timeout for sending each chunk of row data to ClickHouse.
const CH_SEND_TIMEOUT: Duration = Duration::from_secs(30);

/// Timeout for waiting for ClickHouse to acknowledge the INSERT.
const CH_END_TIMEOUT: Duration = Duration::from_secs(120);
const DERIVED_BACKFILL_BLOCK_BATCH_SIZE: i64 = 100_000;
const CH_DERIVED_QUERY_MAX_ATTEMPTS: u32 = 6;
const CH_DERIVED_QUERY_RETRY_BASE_MS: u64 = 500;
const CH_DERIVED_QUERY_RETRY_MAX_MS: u64 = 10_000;

/// Direct-write ClickHouse sink using RowBinary format with LZ4 compression.
#[derive(Clone)]
pub struct ClickHouseSink {
    client: clickhouse::Client,
    /// Client without database context, used for `CREATE DATABASE` DDL.
    base_client: clickhouse::Client,
    database: String,
}

/// A historical derived-table repair planned from the schema state observed
/// at startup. Materialized views handle rows written after the planned range,
/// so these jobs can run in the background without holding up the sync engine.
#[derive(Clone, Debug)]
pub struct DerivedBackfillPlan {
    target: &'static str,
    select_sql: &'static str,
    block_column: &'static str,
    from_block: i64,
    to_block_exclusive: i64,
    source_rows: u64,
    target_rows: u64,
}

impl ClickHouseSink {
    /// Create a new ClickHouse sink.
    ///
    /// The database name is validated to prevent SQL injection in DDL statements
    /// that interpolate it (e.g., `CREATE DATABASE IF NOT EXISTS {database}`).
    ///
    /// Optional `user` and `password` enable HTTP basic auth for secured instances.
    pub fn new(
        url: &str,
        database: &str,
        user: Option<&str>,
        password: Option<&str>,
    ) -> Result<Self> {
        if !is_valid_identifier(database) {
            return Err(anyhow!(
                "Invalid ClickHouse database name '{database}': must be alphanumeric/underscore, \
                 start with a letter or underscore, and be 1-64 chars"
            ));
        }

        let url = url.trim_end_matches('/');
        let mut base_client = clickhouse::Client::default().with_url(url);
        if let Some(user) = user {
            base_client = base_client.with_user(user);
        }
        if let Some(password) = password {
            base_client = base_client.with_password(password);
        }
        let client = base_client.clone().with_database(database);

        Ok(Self {
            client,
            base_client,
            database: database.to_string(),
        })
    }

    /// Reconcile the ClickHouse schema:
    ///
    /// 1. Create the database and base tables (idempotent).
    /// 2. Apply migrations once, tracking their checksum in
    ///    `tidx_schema_objects`. A modified migration body fails loudly rather
    ///    than silently skipping or replaying.
    /// 3. Reconcile derived views / materialized views: if a definition's
    ///    checksum has changed since the last `ensure_schema()`, drop and
    ///    recreate it so SELECT-body edits actually take effect.
    /// 4. Backfill any detected gaps in derived tables.
    pub async fn ensure_schema(&self) -> Result<()> {
        self.ensure_schema_only().await?;
        self.repair_derived_backfill_gaps().await
    }

    /// Reconcile schema objects without scanning or repairing derived data.
    /// The sync engine uses this path so regular writes can start before any
    /// potentially large historical derived-table repair work.
    pub async fn ensure_schema_only(&self) -> Result<()> {
        self.ensure_schema_objects().await
    }

    /// Detect and repair historical gaps in managed derived tables.
    pub async fn repair_derived_backfill_gaps(&self) -> Result<()> {
        let plans = self.plan_derived_backfills().await?;
        self.run_derived_backfill_plan(plans).await
    }

    async fn ensure_schema_objects(&self) -> Result<()> {
        self.base_client
            .query(&format!("CREATE DATABASE IF NOT EXISTS {}", self.database))
            .execute()
            .await
            .map_err(|e| anyhow!("Failed to create ClickHouse database: {e}"))?;

        for object in base_objects() {
            let ddl = object.ddl();
            self.client
                .query(&ddl)
                .execute()
                .await
                .map_err(|e| anyhow!("Failed to create ClickHouse table {}: {e}", object.name))?;
            debug!(table = object.name, database = %self.database, "ClickHouse table ready");
        }

        self.ensure_schema_objects_table().await?;
        let mut tracking = self.load_applied_checksums().await?;

        for migration in migrations() {
            self.apply_migration(migration, &mut tracking).await?;
        }

        self.ensure_derived_objects(&mut tracking).await?;

        info!(database = %self.database, "ClickHouse schema ready");
        Ok(())
    }

    async fn ensure_schema_objects_table(&self) -> Result<()> {
        self.client
            .query(SCHEMA_OBJECTS_TABLE_DDL)
            .execute()
            .await
            .map_err(|e| anyhow!("Failed to create tidx_schema_objects: {e}"))?;
        Ok(())
    }

    async fn load_applied_checksums(&self) -> Result<HashMap<String, String>> {
        let rows: Vec<ChSchemaObjectRow> = self
            .client
            .query("SELECT name, checksum FROM tidx_schema_objects FINAL")
            .fetch_all()
            .await
            .map_err(|e| anyhow!("Failed to load tidx_schema_objects: {e}"))?;
        Ok(rows.into_iter().map(|r| (r.name, r.checksum)).collect())
    }

    async fn apply_migration(
        &self,
        migration: &ClickHouseObject,
        tracking: &mut HashMap<String, String>,
    ) -> Result<()> {
        let checksum = checksum_of(&migration.ddl());
        if let Some(applied) = tracking.get(migration.name) {
            if applied != &checksum {
                return Err(anyhow!(
                    "ClickHouse migration {} has been modified since it was applied \
                     (recorded checksum {} != current {}). Migrations are append-only; \
                     add a new migration instead of editing the existing one.",
                    migration.name,
                    applied,
                    checksum
                ));
            }
            return Ok(());
        }

        self.client
            .query(&migration.ddl())
            .execute()
            .await
            .map_err(|e| anyhow!("Failed to run ClickHouse migration {}: {e}", migration.name))?;
        self.record_applied(migration.name, &checksum, "migration")
            .await?;
        tracking.insert(migration.name.to_string(), checksum);
        Ok(())
    }

    async fn ensure_derived_objects(&self, tracking: &mut HashMap<String, String>) -> Result<()> {
        for object in derived_objects() {
            let ddl = object.ddl();
            let checksum = checksum_of(&ddl);
            let needs_recreate = match tracking.get(object.name) {
                Some(applied) => applied != &checksum,
                None => false,
            };

            if needs_recreate {
                if let Some(drop_sql) = object.drop_sql() {
                    warn!(
                        object = object.name,
                        "ClickHouse derived object definition changed; dropping and re-creating. \
                         Historical rows in the target table still reflect the OLD definition — \
                         add a migration to truncate + rebackfill if you need them rewritten."
                    );
                    self.client.query(&drop_sql).execute().await.map_err(|e| {
                        anyhow!("Failed to drop ClickHouse object {}: {e}", object.name)
                    })?;
                }
            }

            let mut create = self.client.query(&ddl);
            if object.is_refreshable_materialized_view() {
                // Refreshable materialized views are still gated behind an
                // experimental setting in ClickHouse 25.x. It must be set on the
                // same statement that runs the CREATE.
                create =
                    create.with_option("allow_experimental_refreshable_materialized_view", "1");
            }
            create
                .execute()
                .await
                .map_err(|e| anyhow!("Failed to create ClickHouse object {}: {e}", object.name))?;

            let kind_label = match object.kind {
                ClickHouseObjectKind::Table(_) => "table",
                ClickHouseObjectKind::View(_) => "view",
                ClickHouseObjectKind::MaterializedView { .. } => "materialized_view",
                ClickHouseObjectKind::RefreshableMaterializedView(_) => {
                    "refreshable_materialized_view"
                }
                ClickHouseObjectKind::Migration(_) => "migration",
            };
            self.record_applied(object.name, &checksum, kind_label)
                .await?;
            tracking.insert(object.name.to_string(), checksum);
            debug!(object = object.name, database = %self.database, "ClickHouse object ready");
        }

        Ok(())
    }

    async fn record_applied(&self, name: &str, checksum: &str, kind: &str) -> Result<()> {
        // ReplacingMergeTree on (name) collapses prior entries during merges.
        // All inputs are catalog-controlled (object names, hex checksums,
        // kind labels) so direct interpolation is safe.
        let sql = format!(
            "INSERT INTO tidx_schema_objects (name, checksum, kind) VALUES ('{}', '{}', '{}')",
            name, checksum, kind
        );
        self.client
            .query(&sql)
            .execute()
            .await
            .map_err(|e| anyhow!("Failed to record schema object {name}: {e}"))?;
        Ok(())
    }

    /// Execute a planned derived-table backfill.
    pub async fn run_derived_backfill_plan(&self, plans: Vec<DerivedBackfillPlan>) -> Result<()> {
        if plans.is_empty() {
            return Ok(());
        }

        info!(
            database = %self.database,
            backfills = plans.len(),
            "Starting ClickHouse derived table backfills"
        );

        for plan in plans {
            info!(
                database = %self.database,
                table = plan.target,
                from_block = plan.from_block,
                to_block = plan.to_block_exclusive - 1,
                source_rows = plan.source_rows,
                target_rows = plan.target_rows,
                "Backfilling ClickHouse derived table"
            );

            self.execute_derived_query_with_retry(
                &bounded_backfill_sql(&plan),
                &format!("ClickHouse table {} backfill", plan.target),
            )
            .await?;
        }

        info!(
            database = %self.database,
            "ClickHouse derived table backfills complete"
        );
        Ok(())
    }

    async fn plan_derived_backfills(&self) -> Result<Vec<DerivedBackfillPlan>> {
        let mut plans = Vec::new();

        for object in derived_backfills() {
            let Some(BackfillPolicy::Ranged { select_sql }) = object.backfill else {
                continue;
            };
            let Some(block_column) = object.block_column else {
                return Err(anyhow!(
                    "ClickHouse derived backfill table {} has no block column",
                    object.name
                ));
            };

            let Some((source_min, source_max)) = self
                .source_min_max_for_select(select_sql, block_column)
                .await?
            else {
                continue;
            };

            let mut lo = source_min;
            let end_exclusive = source_max.saturating_add(1);
            while lo < end_exclusive {
                let hi = lo
                    .saturating_add(DERIVED_BACKFILL_BLOCK_BATCH_SIZE)
                    .min(end_exclusive);
                let source_rows = self
                    .count_source_rows(select_sql, block_column, lo, hi)
                    .await?;
                if source_rows > 0 {
                    let target_rows = self
                        .count_target_rows(object.name, block_column, lo, hi)
                        .await?;
                    if target_rows < source_rows {
                        warn!(
                            database = %self.database,
                            table = object.name,
                            from_block = lo,
                            to_block = hi - 1,
                            source_rows,
                            target_rows,
                            "Detected ClickHouse derived table backfill gap"
                        );
                        plans.push(DerivedBackfillPlan {
                            target: object.name,
                            select_sql,
                            block_column,
                            from_block: lo,
                            to_block_exclusive: hi,
                            source_rows,
                            target_rows,
                        });
                    }
                }
                lo = hi;
            }
        }

        Ok(plans)
    }

    pub fn name(&self) -> &'static str {
        "clickhouse"
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub async fn write_blocks(&self, blocks: &[BlockRow]) -> Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        self.insert_chunked("blocks", blocks, ChBlockWire::from_row)
            .await?;
        metrics::record_sink_write_duration(self.name(), "blocks", start.elapsed());
        metrics::record_sink_write_rows(self.name(), "blocks", blocks.len() as u64);
        metrics::update_sink_block_rate(self.name(), blocks.len() as u64);
        metrics::increment_sink_row_count(self.name(), "blocks", blocks.len() as u64);
        if let Some(max) = blocks.iter().map(|b| b.num).max() {
            metrics::update_sink_watermark(self.name(), "blocks", max);
        }
        Ok(())
    }

    pub async fn write_txs(&self, txs: &[TxRow]) -> Result<()> {
        if txs.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        self.insert_chunked("txs", txs, ChTxWire::from_row).await?;
        metrics::record_sink_write_duration(self.name(), "txs", start.elapsed());
        metrics::record_sink_write_rows(self.name(), "txs", txs.len() as u64);
        metrics::increment_sink_row_count(self.name(), "txs", txs.len() as u64);
        if let Some(max) = txs.iter().map(|t| t.block_num).max() {
            metrics::update_sink_watermark(self.name(), "txs", max);
        }
        Ok(())
    }

    pub async fn write_logs(&self, logs: &[LogRow]) -> Result<()> {
        if logs.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        self.insert_chunked("logs", logs, ChLogWire::from_row)
            .await?;
        metrics::record_sink_write_duration(self.name(), "logs", start.elapsed());
        metrics::record_sink_write_rows(self.name(), "logs", logs.len() as u64);
        metrics::increment_sink_row_count(self.name(), "logs", logs.len() as u64);
        if let Some(max) = logs.iter().map(|l| l.block_num).max() {
            metrics::update_sink_watermark(self.name(), "logs", max);
        }
        Ok(())
    }

    pub async fn write_receipts(&self, receipts: &[ReceiptRow]) -> Result<()> {
        if receipts.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        self.insert_chunked("receipts", receipts, ChReceiptWire::from_row)
            .await?;
        metrics::record_sink_write_duration(self.name(), "receipts", start.elapsed());
        metrics::record_sink_write_rows(self.name(), "receipts", receipts.len() as u64);
        metrics::increment_sink_row_count(self.name(), "receipts", receipts.len() as u64);
        if let Some(max) = receipts.iter().map(|r| r.block_num).max() {
            metrics::update_sink_watermark(self.name(), "receipts", max);
        }
        Ok(())
    }

    /// Query the highest block number in ClickHouse, or None if empty.
    pub async fn max_block_num(&self) -> Result<Option<i64>> {
        let count: u64 = self
            .client
            .query("SELECT count() FROM blocks")
            .fetch_one()
            .await
            .map_err(|e| anyhow!("ClickHouse query failed: {e}"))?;
        if count == 0 {
            return Ok(None);
        }
        let max: i64 = self
            .client
            .query("SELECT max(num) FROM blocks")
            .fetch_one()
            .await
            .map_err(|e| anyhow!("ClickHouse query failed: {e}"))?;
        Ok(Some(max))
    }

    /// Query the highest block number for a specific table.
    /// Uses the block column declared in the ClickHouse schema registry.
    /// Returns None if the table is empty.
    pub async fn max_block_in_table(&self, table: &str) -> Result<Option<i64>> {
        let table = validate_table_name(table)?;
        let col = crate::clickhouse_schema::block_column(table)
            .ok_or_else(|| anyhow!("ClickHouse table has no block column: {table}"))?;
        let count: u64 = self
            .client
            .query(&format!("SELECT count() FROM {table}"))
            .fetch_one()
            .await
            .map_err(|e| anyhow!("ClickHouse query failed: {e}"))?;
        if count == 0 {
            return Ok(None);
        }
        let max: i64 = self
            .client
            .query(&format!("SELECT max({col}) FROM {table}"))
            .fetch_one()
            .await
            .map_err(|e| anyhow!("ClickHouse query failed: {e}"))?;
        Ok(Some(max))
    }

    /// Query the row count for a specific table.
    pub async fn row_count(&self, table: &str) -> Result<u64> {
        let table = validate_table_name(table)?;
        self.client
            .query(&format!("SELECT count() FROM {table}"))
            .fetch_one()
            .await
            .map_err(|e| anyhow!("ClickHouse query failed: {e}"))
    }

    async fn source_min_max_for_select(
        &self,
        select_sql: &str,
        block_column: &str,
    ) -> Result<Option<(i64, i64)>> {
        let sql = format!(
            "SELECT count(), ifNull(minOrNull({block_column}), 0), ifNull(maxOrNull({block_column}), 0) FROM ({})",
            select_sql.trim()
        );
        let (count, min, max): (u64, i64, i64) = self
            .fetch_one_derived_query_with_retry(&sql, "ClickHouse source range query")
            .await?;
        if count == 0 {
            Ok(None)
        } else {
            Ok(Some((min, max)))
        }
    }

    async fn count_source_rows(
        &self,
        select_sql: &str,
        block_column: &str,
        lo: i64,
        hi: i64,
    ) -> Result<u64> {
        self.fetch_one_derived_query_with_retry(
            &source_count_sql(select_sql, block_column, lo, hi),
            "ClickHouse source count query",
        )
        .await
    }

    async fn count_target_rows(
        &self,
        table: &str,
        block_column: &str,
        lo: i64,
        hi: i64,
    ) -> Result<u64> {
        let table = validate_table_name(table)?;
        self.fetch_one_derived_query_with_retry(
            &target_count_sql(table, block_column, lo, hi),
            "ClickHouse target count query",
        )
        .await
    }

    async fn fetch_one_derived_query_with_retry<T>(&self, sql: &str, operation: &str) -> Result<T>
    where
        T: RowOwned + RowRead,
    {
        let mut attempt = 0;
        loop {
            match self.client.query(sql).fetch_one::<T>().await {
                Ok(row) => return Ok(row),
                Err(e) => {
                    attempt += 1;
                    if attempt >= CH_DERIVED_QUERY_MAX_ATTEMPTS
                        || !is_retryable_clickhouse_error(&e)
                    {
                        return Err(anyhow!("{operation} failed: {e}"));
                    }

                    let delay = derived_query_retry_delay(attempt);
                    warn!(
                        operation,
                        attempt,
                        max_attempts = CH_DERIVED_QUERY_MAX_ATTEMPTS,
                        retry_in_ms = delay.as_millis() as u64,
                        error = %e,
                        "ClickHouse derived repair query retry"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    async fn execute_derived_query_with_retry(&self, sql: &str, operation: &str) -> Result<()> {
        let mut attempt = 0;
        loop {
            match self.client.query(sql).execute().await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    attempt += 1;
                    if attempt >= CH_DERIVED_QUERY_MAX_ATTEMPTS
                        || !is_retryable_clickhouse_error(&e)
                    {
                        return Err(anyhow!("{operation} failed: {e}"));
                    }

                    let delay = derived_query_retry_delay(attempt);
                    warn!(
                        operation,
                        attempt,
                        max_attempts = CH_DERIVED_QUERY_MAX_ATTEMPTS,
                        retry_in_ms = delay.as_millis() as u64,
                        error = %e,
                        "ClickHouse derived repair query retry"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// Delete all data from a given block number onwards (reorg support).
    ///
    /// Uses `mutations_sync=1` so the ALTER ... DELETE completes before this
    /// returns, then asserts the affected range is actually empty before
    /// moving to the next table. This catches the case where a mutation
    /// silently fails (or where a replicated cluster reports synchronous
    /// completion but a replica still serves stale rows) — without the
    /// assertion, replay would happily start atop ghost rows.
    pub async fn delete_from(&self, block_num: u64) -> Result<()> {
        for table in reorg_tables() {
            let sql = format!(
                "ALTER TABLE {} DELETE WHERE {} >= {}",
                table.name, table.block_column, block_num
            );
            self.client
                .query(&sql)
                .with_option("mutations_sync", "1")
                .execute()
                .await
                .map_err(|e| {
                    error!(table = table.name, error = %e, "ClickHouse delete failed");
                    anyhow!("ClickHouse delete from {} failed: {e}", table.name)
                })?;

            let remaining: u64 = self
                .client
                .query(&format!(
                    "SELECT count() FROM {} WHERE {} >= {}",
                    table.name, table.block_column, block_num
                ))
                .fetch_one()
                .await
                .map_err(|e| {
                    anyhow!(
                        "ClickHouse reorg verification query for {} failed: {e}",
                        table.name
                    )
                })?;
            if remaining > 0 {
                return Err(anyhow!(
                    "ClickHouse reorg delete on {} left {remaining} row(s) at \
                     {} >= {block_num}; refusing to replay atop stale rows",
                    table.name,
                    table.block_column
                ));
            }
        }

        debug!(from_block = block_num, "ClickHouse reorg delete complete");
        Ok(())
    }

    /// Chunk source rows, convert each chunk to wire format, and insert with retry logic.
    /// This avoids allocating the full wire-format vec upfront, bounding peak memory
    /// to `CH_INSERT_CHUNK_SIZE` wire structs at a time.
    async fn insert_chunked<S, W, F>(&self, table: &str, rows: &[S], convert: F) -> Result<()>
    where
        W: Serialize + for<'a> Row<Value<'a> = W>,
        F: Fn(&S) -> W,
    {
        for chunk in rows.chunks(CH_INSERT_CHUNK_SIZE) {
            let wire: Vec<W> = chunk.iter().map(&convert).collect();
            let mut last_error = None;
            for attempt in 0..CH_MAX_RETRIES {
                if attempt > 0 {
                    let backoff = Duration::from_millis(100 << attempt);
                    warn!(table, attempt, "ClickHouse insert retry after {backoff:?}");
                    tokio::time::sleep(backoff).await;
                }
                match self.try_insert(table, &wire).await {
                    Ok(()) => {
                        last_error = None;
                        break;
                    }
                    Err(e) => {
                        last_error = Some(e);
                    }
                }
            }
            if let Some(e) = last_error {
                return Err(anyhow!(
                    "ClickHouse insert into {table} failed after {CH_MAX_RETRIES} attempts: {e}"
                ));
            }
        }
        Ok(())
    }

    async fn try_insert<T>(&self, table: &str, rows: &[T]) -> Result<()>
    where
        T: Serialize + for<'a> Row<Value<'a> = T>,
    {
        let mut insert = self
            .client
            .insert::<T>(table)
            .await?
            .with_timeouts(Some(CH_SEND_TIMEOUT), Some(CH_END_TIMEOUT));
        for row in rows {
            insert.write(row).await?;
        }
        insert.end().await?;
        Ok(())
    }
}

// ── ClickHouse wire-format structs ────────────────────────────────────────
//
// These derive `clickhouse::Row` for RowBinary serialization and `serde::Serialize`
// for the Row encoding. DateTime64(3) columns use the chrono serde adapter.

#[derive(Row, Deserialize)]
struct ChSchemaObjectRow {
    name: String,
    checksum: String,
}

#[derive(Row, Serialize)]
struct ChBlockWire {
    num: i64,
    hash: String,
    parent_hash: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    timestamp: chrono::DateTime<chrono::Utc>,
    timestamp_ms: i64,
    gas_limit: i64,
    gas_used: i64,
    miner: String,
    extra_data: Option<String>,
    consensus_proposer: Option<String>,
}

impl ChBlockWire {
    fn from_row(b: &BlockRow) -> Self {
        Self {
            num: b.num,
            hash: hex_encode(&b.hash),
            parent_hash: hex_encode(&b.parent_hash),
            timestamp: b.timestamp,
            timestamp_ms: b.timestamp_ms,
            gas_limit: b.gas_limit,
            gas_used: b.gas_used,
            miner: hex_encode(&b.miner),
            extra_data: b.extra_data.as_ref().map(|v| hex_encode(v)),
            consensus_proposer: b.consensus_proposer.as_ref().map(|v| hex_encode(v)),
        }
    }
}

#[derive(Row, Serialize)]
struct ChTxWire {
    block_num: i64,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    block_timestamp: chrono::DateTime<chrono::Utc>,
    idx: i32,
    hash: String,
    #[serde(rename = "type")]
    tx_type: i16,
    from: String,
    to: Option<String>,
    value: String,
    input: String,
    gas_limit: i64,
    max_fee_per_gas: String,
    max_priority_fee_per_gas: String,
    gas_used: Option<i64>,
    nonce_key: String,
    nonce: i64,
    fee_token: Option<String>,
    fee_payer: Option<String>,
    calls: Option<String>,
    call_count: i16,
    valid_before: Option<i64>,
    valid_after: Option<i64>,
    signature_type: Option<i16>,
}

impl ChTxWire {
    fn from_row(tx: &TxRow) -> Self {
        Self {
            block_num: tx.block_num,
            block_timestamp: tx.block_timestamp,
            idx: tx.idx,
            hash: hex_encode(&tx.hash),
            tx_type: tx.tx_type,
            from: hex_encode(&tx.from),
            to: tx.to.as_ref().map(|v| hex_encode(v)),
            value: tx.value.clone(),
            input: hex_encode(&tx.input),
            gas_limit: tx.gas_limit,
            max_fee_per_gas: tx.max_fee_per_gas.clone(),
            max_priority_fee_per_gas: tx.max_priority_fee_per_gas.clone(),
            gas_used: tx.gas_used,
            nonce_key: hex_encode(&tx.nonce_key),
            nonce: tx.nonce,
            fee_token: tx.fee_token.as_ref().map(|v| hex_encode(v)),
            fee_payer: tx.fee_payer.as_ref().map(|v| hex_encode(v)),
            calls: tx.calls.as_ref().map(|v| v.to_string()),
            call_count: tx.call_count,
            valid_before: tx.valid_before,
            valid_after: tx.valid_after,
            signature_type: tx.signature_type,
        }
    }
}

#[derive(Row, Serialize)]
struct ChLogWire {
    block_num: i64,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    block_timestamp: chrono::DateTime<chrono::Utc>,
    log_idx: i32,
    tx_idx: i32,
    tx_hash: String,
    address: String,
    selector: String,
    topic0: Option<String>,
    topic1: Option<String>,
    topic2: Option<String>,
    topic3: Option<String>,
    data: String,
    is_virtual_forward: u8,
}

impl ChLogWire {
    fn from_row(log: &LogRow) -> Self {
        Self {
            block_num: log.block_num,
            block_timestamp: log.block_timestamp,
            log_idx: log.log_idx,
            tx_idx: log.tx_idx,
            tx_hash: hex_encode(&log.tx_hash),
            address: hex_encode(&log.address),
            selector: log
                .selector
                .as_ref()
                .map(|v| hex_encode(v))
                .unwrap_or_default(),
            topic0: log.topic0.as_ref().map(|v| hex_encode(v)),
            topic1: log.topic1.as_ref().map(|v| hex_encode(v)),
            topic2: log.topic2.as_ref().map(|v| hex_encode(v)),
            topic3: log.topic3.as_ref().map(|v| hex_encode(v)),
            data: hex_encode(&log.data),
            is_virtual_forward: log.is_virtual_forward as u8,
        }
    }
}

#[derive(Row, Serialize)]
struct ChReceiptWire {
    block_num: i64,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    block_timestamp: chrono::DateTime<chrono::Utc>,
    tx_idx: i32,
    tx_hash: String,
    from: String,
    to: Option<String>,
    contract_address: Option<String>,
    gas_used: i64,
    cumulative_gas_used: i64,
    effective_gas_price: Option<String>,
    status: Option<i16>,
    fee_payer: Option<String>,
}

impl ChReceiptWire {
    fn from_row(r: &ReceiptRow) -> Self {
        Self {
            block_num: r.block_num,
            block_timestamp: r.block_timestamp,
            tx_idx: r.tx_idx,
            tx_hash: hex_encode(&r.tx_hash),
            from: hex_encode(&r.from),
            to: r.to.as_ref().map(|v| hex_encode(v)),
            contract_address: r.contract_address.as_ref().map(|v| hex_encode(v)),
            gas_used: r.gas_used,
            cumulative_gas_used: r.cumulative_gas_used,
            effective_gas_price: r.effective_gas_price.clone(),
            status: r.status,
            fee_payer: r.fee_payer.as_ref().map(|v| hex_encode(v)),
        }
    }
}

/// Hex-encode bytes with 0x prefix.
fn hex_encode(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

/// Stable non-cryptographic checksum of a DDL string. Used only to detect
/// whether a managed object's definition has drifted since the last
/// `ensure_schema()`. Collisions are not security-relevant here.
fn checksum_of(ddl: &str) -> String {
    let mut hasher = DefaultHasher::new();
    ddl.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn bounded_backfill_sql(plan: &DerivedBackfillPlan) -> String {
    ranged_backfill_sql(
        plan.target,
        plan.select_sql,
        plan.block_column,
        plan.from_block,
        plan.to_block_exclusive,
    )
}

fn ranged_backfill_sql(
    target: &str,
    select_sql: &str,
    block_column: &str,
    from_block: i64,
    to_block_exclusive: i64,
) -> String {
    format!(
        "INSERT INTO {target} SELECT DISTINCT * FROM ({}) WHERE {block_column} >= {from_block} AND {block_column} < {to_block_exclusive}",
        select_sql.trim()
    )
}

fn source_count_sql(
    select_sql: &str,
    block_column: &str,
    from_block: i64,
    to_block_exclusive: i64,
) -> String {
    format!(
        "SELECT count() FROM (SELECT DISTINCT * FROM ({}) WHERE {block_column} >= {from_block} AND {block_column} < {to_block_exclusive})",
        select_sql.trim()
    )
}

fn target_count_sql(
    table: &str,
    block_column: &str,
    from_block: i64,
    to_block_exclusive: i64,
) -> String {
    format!(
        "SELECT count() FROM {table} FINAL WHERE {block_column} >= {from_block} AND {block_column} < {to_block_exclusive}"
    )
}

fn is_retryable_clickhouse_error(error: &impl std::fmt::Display) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("network error")
        || message.contains("connect")
        || message.contains("connection")
        || message.contains("timeout")
        || message.contains("timed out")
}

fn derived_query_retry_delay(attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1);
    let multiplier = 2u64.saturating_pow(exponent);
    let millis = CH_DERIVED_QUERY_RETRY_BASE_MS
        .saturating_mul(multiplier)
        .min(CH_DERIVED_QUERY_RETRY_MAX_MS);
    Duration::from_millis(millis)
}

/// Validate that a table name is one of the known tables.
/// Returns the validated name or an error for unknown tables.
fn validate_table_name(table: &str) -> Result<&str> {
    crate::clickhouse_schema::is_known_table(table)
        .then_some(table)
        .ok_or_else(|| anyhow!("Unknown ClickHouse table: {table}"))
}

/// Validate that a string is a safe SQL identifier (for table/database names
/// interpolated into DDL/queries). Allows `[a-zA-Z_][a-zA-Z0-9_]{0,63}`.
fn is_valid_identifier(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hex_encode() {
        assert_eq!(hex_encode(&[0xde, 0xad, 0xbe, 0xef]), "0xdeadbeef");
        assert_eq!(hex_encode(&[]), "0x");
    }

    #[test]
    fn test_wire_struct_serialization() {
        use chrono::TimeZone;
        let dt = chrono::Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap();

        let block = crate::types::BlockRow {
            num: 42,
            hash: vec![0xab; 32],
            parent_hash: vec![0xcd; 32],
            timestamp: dt,
            timestamp_ms: 1705320000000,
            gas_limit: 30_000_000,
            gas_used: 15_000_000,
            miner: vec![0xee; 20],
            extra_data: None,
            consensus_proposer: None,
        };

        let wire = ChBlockWire::from_row(&block);
        // Verify field values via the struct fields directly
        assert_eq!(wire.num, 42);
        assert_eq!(wire.hash, format!("0x{}", "ab".repeat(32)));
        assert_eq!(wire.miner, format!("0x{}", "ee".repeat(20)));
        assert_eq!(wire.timestamp, dt);
        assert!(wire.extra_data.is_none());
    }

    #[test]
    fn test_wire_struct_tx_type_rename() {
        let tx = crate::types::TxRow {
            tx_type: 2,
            ..Default::default()
        };

        let wire = ChTxWire::from_row(&tx);
        // Verify via serde JSON that the rename applies
        let json = serde_json::to_string(&wire).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["type"], 2);
        assert!(parsed.get("tx_type").is_none());
    }

    #[test]
    fn test_valid_identifier() {
        assert!(is_valid_identifier("tidx_4217"));
        assert!(is_valid_identifier("blocks"));
        assert!(is_valid_identifier("_private"));
        assert!(is_valid_identifier("A"));

        assert!(!is_valid_identifier(""));
        assert!(!is_valid_identifier("123abc"));
        assert!(!is_valid_identifier("my-db"));
        assert!(!is_valid_identifier("db; DROP TABLE x"));
        assert!(!is_valid_identifier("db name"));
        assert!(!is_valid_identifier(&"a".repeat(65)));
    }

    #[test]
    fn test_new_rejects_bad_database_name() {
        assert!(ClickHouseSink::new("http://localhost:8123", "tidx_4217", None, None).is_ok());
        assert!(
            ClickHouseSink::new(
                "http://localhost:8123",
                "foo; DROP TABLE blocks",
                None,
                None
            )
            .is_err()
        );
        assert!(ClickHouseSink::new("http://localhost:8123", "123bad", None, None).is_err());
        assert!(ClickHouseSink::new("http://localhost:8123", "", None, None).is_err());
    }

    #[test]
    fn test_bounded_backfill_sql_wraps_select_with_range() {
        let plan = DerivedBackfillPlan {
            target: "address_txs",
            select_sql: "SELECT block_num, tx_hash FROM txs\n",
            block_column: "block_num",
            from_block: 100,
            to_block_exclusive: 200,
            source_rows: 10,
            target_rows: 5,
        };

        assert_eq!(
            bounded_backfill_sql(&plan),
            "INSERT INTO address_txs SELECT DISTINCT * FROM (SELECT block_num, tx_hash FROM txs) WHERE block_num >= 100 AND block_num < 200"
        );
    }

    #[test]
    fn test_derived_backfill_count_sql_uses_distinct_source_and_final_target() {
        assert_eq!(
            source_count_sql(
                "SELECT block_num, tx_hash FROM txs\n",
                "block_num",
                100,
                200
            ),
            "SELECT count() FROM (SELECT DISTINCT * FROM (SELECT block_num, tx_hash FROM txs) WHERE block_num >= 100 AND block_num < 200)"
        );
        assert_eq!(
            target_count_sql("address_txs", "block_num", 100, 200),
            "SELECT count() FROM address_txs FINAL WHERE block_num >= 100 AND block_num < 200"
        );
    }

    #[test]
    fn test_derived_query_retry_classification() {
        assert!(is_retryable_clickhouse_error(
            &"network error: client error (Connect)"
        ));
        assert!(is_retryable_clickhouse_error(&"request timed out"));
        assert!(is_retryable_clickhouse_error(&"connection closed"));

        assert!(!is_retryable_clickhouse_error(
            &"MEMORY_LIMIT_EXCEEDED: would use too much memory"
        ));
        assert!(!is_retryable_clickhouse_error(&"Syntax error near SELECT"));
    }

    #[test]
    fn test_derived_query_retry_delay_caps() {
        assert_eq!(derived_query_retry_delay(1), Duration::from_millis(500));
        assert_eq!(derived_query_retry_delay(2), Duration::from_secs(1));
        assert_eq!(derived_query_retry_delay(5), Duration::from_secs(8));
        assert_eq!(derived_query_retry_delay(6), Duration::from_secs(10));
        assert_eq!(derived_query_retry_delay(127), Duration::from_secs(10));
    }

    #[test]
    fn test_token_holder_deltas_table_is_known() {
        assert!(validate_table_name("token_transfers").is_ok());
        assert!(validate_table_name("token_holder_deltas").is_ok());
        assert!(validate_table_name("token_balances").is_ok());
    }
}
