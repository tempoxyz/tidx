//! ClickHouse direct-write sink.
//!
//! Writes blocks, transactions, logs, and receipts directly to ClickHouse
//! via the official `clickhouse` crate using RowBinary format with LZ4 compression.

use anyhow::{Context, Result, anyhow};
use clickhouse::types::UInt256 as ChUInt256;
use clickhouse::{Row, RowOwned, RowRead};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Keccak256};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, MutexGuard};
use tracing::{debug, error, info, warn};

use crate::clickhouse_schema::{
    BackfillPolicy, BlockScopedTable, ClickHouseObject, ClickHouseObjectKind, base_objects,
    derived_backfills, derived_objects, migrations, post_derived_migrations, reorg_tables,
    retired_object_drops,
};
use crate::metrics;
use crate::sync::earn_share_prices::{
    EARN_STACK_DEPLOYED_SELECTORS, EarnSharePriceCandidate, EarnSharePriceObservation, EarnVault,
    QUOTED_SHARES,
};
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
    SETTINGS default_compression_codec = 'ZSTD(1)'
";

/// Max rows per ClickHouse INSERT to avoid unbounded memory growth during backfills.
const CH_INSERT_CHUNK_SIZE: usize = 10_000;

/// Max block numbers included in one ClickHouse replay query or mutation.
const CH_REPLAY_BLOCK_CHUNK_SIZE: usize = 1_000;

/// Target rows per streamed populated-block replay query.
const CH_REPLAY_RESULT_CHUNK_SIZE: u64 = 50_000;

/// Max retry attempts for transient ClickHouse write failures.
const CH_MAX_RETRIES: u32 = 3;

/// Timeout for sending each chunk of row data to ClickHouse.
const CH_SEND_TIMEOUT: Duration = Duration::from_secs(30);

/// Timeout for waiting for ClickHouse to acknowledge the INSERT.
const CH_END_TIMEOUT: Duration = Duration::from_secs(120);
const MIN_CLICKHOUSE_VERSION: (u64, u64) = (25, 11);
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
    /// Create the database with `ENGINE = Replicated` and rewrite
    /// MergeTree-family table engines to their `Replicated*` counterparts.
    replicated_database: bool,
    /// Serializes destructive repairs with derived-table backfills.
    maintenance_lock: Arc<Mutex<()>>,
}

/// Canonical base-table rows read from a completed ClickHouse archive range.
#[derive(Default)]
pub(crate) struct ArchiveBatch {
    pub blocks: Vec<BlockRow>,
    pub txs: Vec<TxRow>,
    pub logs: Vec<LogRow>,
    pub receipts: Vec<ReceiptRow>,
}

pub(crate) struct CanonicalRowCheck {
    pub present: Vec<bool>,
    pub stale_blocks: HashSet<i64>,
    pub occupied_blocks: HashSet<i64>,
}

struct CanonicalChildQuery<'a> {
    table: &'a str,
    position_col: &'a str,
    columns: &'a str,
}

fn earn_share_price_candidates_sql(from_block: i64, limit: usize) -> String {
    format!(
        "WITH toDateTime64(\
             toStartOfInterval(\
                 timestamp - INTERVAL 1 MILLISECOND, \
                 INTERVAL 15 MINUTE\
             ) + INTERVAL 15 MINUTE, \
             3, 'UTC'\
         ) AS bucket \
         SELECT bucket, \
                argMax(num, tuple(timestamp, num)) AS block_num, \
                argMax(hash, tuple(timestamp, num)) AS block_hash, \
                argMax(timestamp, tuple(timestamp, num)) AS block_timestamp \
         FROM blocks FINAL \
         WHERE num >= {from_block} \
           AND timestamp <= now64(3) - INTERVAL 30 SECOND \
         GROUP BY bucket \
         HAVING bucket <= toStartOfInterval(\
             now64(3) - INTERVAL 30 SECOND, INTERVAL 15 MINUTE\
         ) \
         ORDER BY bucket \
         LIMIT {limit}"
    )
}

fn exact_block_delete_tables() -> Vec<BlockScopedTable> {
    let mut tables = reorg_tables().collect::<Vec<_>>();
    let blocks = tables
        .iter()
        .position(|table| table.name == "blocks")
        .map(|index| tables.remove(index))
        .expect("blocks must be registered for reorg cleanup");
    tables.insert(0, blocks);
    tables
}

/// Stable seed for one logical realtime base-table write.
pub(crate) fn batch_deduplication_seed(blocks: &[BlockRow]) -> Option<String> {
    if blocks.is_empty() {
        return None;
    }

    let mut hasher = Keccak256::new();
    for block in blocks {
        hasher.update(block.num.to_le_bytes());
        hasher.update(&block.hash);
    }
    Some(hex::encode(hasher.finalize()))
}

/// Fresh seed for a presence-filtered write or destructive replay.
pub(crate) fn replay_deduplication_seed() -> String {
    format!("tidx-replay-{}", hex::encode(rand::random::<[u8; 16]>()))
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
            replicated_database: false,
            maintenance_lock: Arc::new(Mutex::new(())),
        })
    }

    /// Enable replicated schema DDL for self-hosted multi-replica clusters.
    ///
    /// The database is created with `ENGINE = Replicated` and MergeTree-family
    /// table engines are rewritten to their `Replicated*` counterparts at
    /// execution time. A `Replicated` database only replicates DDL — without
    /// replicated table engines each replica would hold independent data.
    /// Existing databases and tables are unaffected (`IF NOT EXISTS` DDL).
    pub fn with_replicated_database(mut self, enabled: bool) -> Self {
        self.replicated_database = enabled;
        self
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
    /// 4. Remove retired objects, including anything recreated by older code.
    /// 5. Backfill any detected gaps in derived tables.
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
        let _guard = self.maintenance_lock.lock().await;
        let plans = self.plan_derived_backfills().await?;
        self.run_derived_backfill_plan_locked(plans).await
    }

    async fn ensure_schema_objects(&self) -> Result<()> {
        self.ensure_supported_server_version().await?;

        self.base_client
            .query(&self.create_database_ddl())
            .execute()
            .await
            .map_err(|e| anyhow!("Failed to create ClickHouse database: {e}"))?;

        for object in base_objects() {
            let raw_ddl = object.ddl();
            let ddl = self.prepare_ddl(&raw_ddl);
            self.client
                .query(&ddl)
                .execute()
                .await
                .map_err(|e| anyhow!("Failed to create ClickHouse table {}: {e}", object.name))?;
            debug!(table = object.name, database = %self.database, "ClickHouse table ready");
        }

        // A retried insert re-sends an identical part; the dedup window drops
        // it instead of duplicating rows. Replicated tables already
        // deduplicate through the Keeper-backed window.
        if !self.replicated_database {
            for table in ["blocks", "txs", "logs", "receipts"] {
                self.client
                    .query(&format!(
                        "ALTER TABLE {table} MODIFY SETTING non_replicated_deduplication_window = 100"
                    ))
                    .execute()
                    .await
                    .map_err(|e| {
                        anyhow!("Failed to set deduplication window on {table}: {e}")
                    })?;
            }
        }

        self.ensure_schema_objects_table().await?;
        let mut tracking = self.load_applied_checksums().await?;

        for migration in migrations() {
            self.apply_migration(migration, &mut tracking).await?;
        }

        self.ensure_derived_objects(&mut tracking).await?;

        // This RPC-backed derived table is also written through RowBinary and
        // uses stable insert tokens for retry safety. It is created after the
        // base-table dedup settings above, so configure its window here.
        if !self.replicated_database {
            self.client
                .query(
                    "ALTER TABLE earn_share_prices MODIFY SETTING \
                     non_replicated_deduplication_window = 100",
                )
                .execute()
                .await
                .map_err(|e| {
                    anyhow!("Failed to set deduplication window on earn_share_prices: {e}")
                })?;
        }

        // Run after derived tables exist, since these migrations mutate them.
        for migration in post_derived_migrations() {
            self.apply_migration(migration, &mut tracking).await?;
        }

        self.ensure_retired_objects_absent().await?;

        info!(database = %self.database, "ClickHouse schema ready");
        Ok(())
    }

    async fn ensure_supported_server_version(&self) -> Result<()> {
        let row: ChVersionRow = self
            .base_client
            .query("SELECT version() AS version")
            .fetch_one()
            .await
            .map_err(|e| anyhow!("Failed to query ClickHouse server version: {e}"))?;
        validate_clickhouse_version(&row.version)
    }

    async fn ensure_retired_objects_absent(&self) -> Result<()> {
        for ddl in retired_object_drops() {
            self.client.query(ddl).execute().await.map_err(|e| {
                anyhow!(
                    "Failed to run retired ClickHouse cleanup `{}`: {e}",
                    ddl.trim()
                )
            })?;
        }
        Ok(())
    }

    async fn ensure_schema_objects_table(&self) -> Result<()> {
        self.client
            .query(&self.prepare_ddl(SCHEMA_OBJECTS_TABLE_DDL))
            .execute()
            .await
            .map_err(|e| anyhow!("Failed to create tidx_schema_objects: {e}"))?;
        Ok(())
    }

    /// `CREATE DATABASE` DDL honoring `replicated_database`.
    ///
    /// The ZooKeeper path is derived from the database name (instead of the
    /// `{uuid}` default) so that creating the same database on another replica
    /// converges on the same replication group. `{shard}`/`{replica}` macros
    /// come from server configuration.
    fn create_database_ddl(&self) -> String {
        if self.replicated_database {
            format!(
                "CREATE DATABASE IF NOT EXISTS {db} \
                 ENGINE = Replicated('/clickhouse/databases/{db}', '{{shard}}', '{{replica}}')",
                db = self.database
            )
        } else {
            format!("CREATE DATABASE IF NOT EXISTS {}", self.database)
        }
    }

    /// DDL as executed against the server, honoring `replicated_database`.
    ///
    /// Checksums are always computed on the raw catalog DDL, so toggling the
    /// flag never triggers drop/recreate cycles for derived objects.
    fn prepare_ddl<'a>(&self, ddl: &'a str) -> Cow<'a, str> {
        if self.replicated_database {
            Cow::Owned(to_replicated_engine_ddl(ddl))
        } else {
            Cow::Borrowed(ddl)
        }
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

        let raw_ddl = migration.ddl();
        self.client
            .query(&self.prepare_ddl(&raw_ddl))
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

            let mut create = self.client.query(&self.prepare_ddl(&ddl));
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
        let _guard = self.maintenance_lock.lock().await;
        self.run_derived_backfill_plan_locked(plans).await
    }

    async fn run_derived_backfill_plan_locked(
        &self,
        plans: Vec<DerivedBackfillPlan>,
    ) -> Result<()> {
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

    pub(crate) async fn maintenance_guard(&self) -> MutexGuard<'_, ()> {
        self.maintenance_lock.lock().await
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
        self.write_blocks_with_deduplication_seed(blocks, None)
            .await
    }

    pub(crate) async fn write_blocks_deduplicated(
        &self,
        blocks: &[BlockRow],
        seed: &str,
    ) -> Result<()> {
        self.write_blocks_with_deduplication_seed(blocks, Some(seed))
            .await
    }

    async fn write_blocks_with_deduplication_seed(
        &self,
        blocks: &[BlockRow],
        seed: Option<&str>,
    ) -> Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        self.insert_chunked("blocks", blocks, ChBlockWire::from_row, seed)
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
        self.write_txs_with_deduplication_seed(txs, None).await
    }

    pub(crate) async fn write_txs_deduplicated(&self, txs: &[TxRow], seed: &str) -> Result<()> {
        self.write_txs_with_deduplication_seed(txs, Some(seed))
            .await
    }

    async fn write_txs_with_deduplication_seed(
        &self,
        txs: &[TxRow],
        seed: Option<&str>,
    ) -> Result<()> {
        if txs.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        self.insert_chunked("txs", txs, ChTxWire::from_row, seed)
            .await?;
        metrics::record_sink_write_duration(self.name(), "txs", start.elapsed());
        metrics::record_sink_write_rows(self.name(), "txs", txs.len() as u64);
        metrics::increment_sink_row_count(self.name(), "txs", txs.len() as u64);
        if let Some(max) = txs.iter().map(|t| t.block_num).max() {
            metrics::update_sink_watermark(self.name(), "txs", max);
        }
        Ok(())
    }

    pub async fn write_logs(&self, logs: &[LogRow]) -> Result<()> {
        self.write_logs_with_deduplication_seed(logs, None).await
    }

    pub(crate) async fn write_logs_deduplicated(&self, logs: &[LogRow], seed: &str) -> Result<()> {
        self.write_logs_with_deduplication_seed(logs, Some(seed))
            .await
    }

    async fn write_logs_with_deduplication_seed(
        &self,
        logs: &[LogRow],
        seed: Option<&str>,
    ) -> Result<()> {
        if logs.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        self.insert_chunked("logs", logs, ChLogWire::from_row, seed)
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
        self.write_receipts_with_deduplication_seed(receipts, None)
            .await
    }

    pub(crate) async fn write_receipts_deduplicated(
        &self,
        receipts: &[ReceiptRow],
        seed: &str,
    ) -> Result<()> {
        self.write_receipts_with_deduplication_seed(receipts, Some(seed))
            .await
    }

    async fn write_receipts_with_deduplication_seed(
        &self,
        receipts: &[ReceiptRow],
        seed: Option<&str>,
    ) -> Result<()> {
        if receipts.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        self.insert_chunked("receipts", receipts, ChReceiptWire::from_row, seed)
            .await?;
        metrics::record_sink_write_duration(self.name(), "receipts", start.elapsed());
        metrics::record_sink_write_rows(self.name(), "receipts", receipts.len() as u64);
        metrics::increment_sink_row_count(self.name(), "receipts", receipts.len() as u64);
        if let Some(max) = receipts.iter().map(|r| r.block_num).max() {
            metrics::update_sink_watermark(self.name(), "receipts", max);
        }
        Ok(())
    }

    /// Discover Earn vaults from the factory's final atomic deployment event.
    pub(crate) async fn earn_vaults(&self) -> Result<Vec<EarnVault>> {
        let selectors = EARN_STACK_DEPLOYED_SELECTORS
            .iter()
            .map(|selector| format!("'{selector}'"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT concat('0x', lower(right(assumeNotNull(topic1), 40))) AS address, \
             min(block_num) AS deployment_block \
             FROM logs FINAL \
             WHERE selector IN ({selectors}) AND topic1 IS NOT NULL \
             GROUP BY address ORDER BY deployment_block, address"
        );
        let rows = self
            .client
            .query(&sql)
            .fetch_all::<ChEarnVaultRow>()
            .await
            .context("discover Earn vaults from ClickHouse logs")?;
        Ok(rows
            .into_iter()
            .map(|row| EarnVault {
                address: row.address,
                deployment_block: row.deployment_block,
            })
            .collect())
    }

    /// Return the next confirmed 15-minute sample points after a vault's
    /// durable high-water mark. Reorg cleanup deletes observations by block,
    /// which automatically rewinds this cursor before the replacement replay.
    pub(crate) async fn earn_share_price_candidates(
        &self,
        vault: &EarnVault,
        limit: usize,
    ) -> Result<Vec<EarnSharePriceCandidate>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        if vault.address.len() != 42
            || !vault.address.starts_with("0x")
            || !vault.address[2..]
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(anyhow!(
                "Invalid Earn vault address in indexed deployment log"
            ));
        }

        let cursor_sql = format!(
            "SELECT maxOrNull(block_num) AS block_num \
             FROM earn_share_prices FINAL WHERE vault = '{}'",
            vault.address
        );
        let cursor = self
            .client
            .query(&cursor_sql)
            .fetch_one::<ChOptionalBlockNumber>()
            .await
            .context("read Earn share-price high-water mark")?;
        let from_block = cursor
            .block_num
            .map_or(vault.deployment_block, |block_num| {
                block_num.saturating_add(1)
            });

        let sql = earn_share_price_candidates_sql(from_block, limit);
        let rows = self
            .client
            .query(&sql)
            .fetch_all::<ChEarnSharePriceCandidateRow>()
            .await
            .with_context(|| {
                format!(
                    "select Earn share-price sample blocks for {}",
                    vault.address
                )
            })?;
        Ok(rows
            .into_iter()
            .map(|row| EarnSharePriceCandidate {
                vault: vault.address.clone(),
                bucket: row.bucket,
                block_num: row.block_num,
                block_hash: row.block_hash,
                block_timestamp: row.block_timestamp,
            })
            .collect())
    }

    pub(crate) async fn write_earn_share_prices(
        &self,
        observations: &[EarnSharePriceObservation],
    ) -> Result<usize> {
        if observations.is_empty() {
            return Ok(0);
        }
        let _guard = self.maintenance_guard().await;

        // Candidate discovery and historical RPC execution happen outside the
        // maintenance lock. Re-check hashes after acquiring it so a reorg that
        // landed in between cannot reinsert an orphaned observation after the
        // reorg mutation has completed.
        let block_nums = observations
            .iter()
            .map(|observation| observation.block_num.to_string())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .join(", ");
        let canonical = self
            .client
            .query(&format!(
                "SELECT num, hash FROM blocks FINAL WHERE num IN ({block_nums})"
            ))
            .fetch_all::<ChCanonicalEarnBlock>()
            .await
            .context("verify Earn sample blocks before insert")?
            .into_iter()
            .map(|block| (block.num, block.hash))
            .collect::<HashMap<_, _>>();
        let canonical_observations = observations
            .iter()
            .filter(|observation| {
                canonical.get(&observation.block_num) == Some(&observation.block_hash)
            })
            .collect::<Vec<_>>();
        if canonical_observations.is_empty() {
            return Ok(0);
        }

        let seed = canonical_observations
            .iter()
            .map(|observation| {
                format!(
                    "{}:{}:{}",
                    observation.vault,
                    observation.bucket.timestamp_millis(),
                    observation.block_hash
                )
            })
            .collect::<Vec<_>>()
            .join("|");
        let start = Instant::now();
        self.insert_chunked(
            "earn_share_prices",
            &canonical_observations,
            |observation| ChEarnSharePriceWire::from_observation(observation),
            Some(&seed),
        )
        .await?;
        let written = canonical_observations.len();
        metrics::record_sink_write_duration(self.name(), "earn_share_prices", start.elapsed());
        metrics::record_sink_write_rows(self.name(), "earn_share_prices", written as u64);
        metrics::increment_sink_row_count(self.name(), "earn_share_prices", written as u64);
        if let Some(max) = canonical_observations
            .iter()
            .map(|observation| observation.block_num)
            .max()
        {
            metrics::update_sink_watermark(self.name(), "earn_share_prices", max);
        }
        Ok(written)
    }

    /// Read a canonical base-table range for PostgreSQL hot-tier hydration.
    ///
    /// Callers must gate this on the durable archive checkpoint. `FINAL`
    /// collapses identical crash-replay rows before they cross into the
    /// PostgreSQL tables, which enforce one canonical row per primary key.
    pub(crate) async fn read_archive_range(&self, from: u64, to: u64) -> Result<ArchiveBatch> {
        if from > to {
            return Ok(ArchiveBatch::default());
        }
        let from = i64::try_from(from).context("archive range start exceeds Int64")?;
        let to = i64::try_from(to).context("archive range end exceeds Int64")?;

        let blocks_sql = format!(
            "SELECT num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner, \
             extra_data, consensus_proposer FROM blocks FINAL \
             WHERE num >= {from} AND num <= {to} ORDER BY num"
        );
        // Fetch blocks first so their canonical timestamps can constrain the
        // monthly partitions used by the denormalized child tables. This also
        // avoids launching three archive reads when the checkpoint is corrupt
        // and the requested block range is absent.
        let blocks = self
            .fetch_archive_rows::<ChBlockWire>(&blocks_sql, "blocks")
            .await?;
        let Some(from_timestamp_ms) = blocks.iter().map(|block| block.timestamp_ms).min() else {
            return Ok(ArchiveBatch::default());
        };
        let to_timestamp_ms = blocks
            .iter()
            .map(|block| block.timestamp_ms)
            .max()
            .expect("non-empty blocks have a maximum timestamp");
        let range_predicate = archive_range_predicate(from, to, from_timestamp_ms, to_timestamp_ms);

        let txs_sql = format!(
            "SELECT block_num, block_timestamp, idx, hash, `type`, `from`, `to`, value, input, \
             gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_used, nonce_key, nonce, \
             fee_token, fee_payer, calls, call_count, valid_before, valid_after, signature_type \
             FROM txs FINAL WHERE {range_predicate} \
             ORDER BY block_num, idx"
        );
        let logs_sql = format!(
            "SELECT block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, \
             topic0, topic1, topic2, topic3, data, is_virtual_forward FROM logs FINAL \
             WHERE {range_predicate} ORDER BY block_num, log_idx"
        );
        let receipts_sql = format!(
            "SELECT block_num, block_timestamp, tx_idx, tx_hash, `from`, `to`, contract_address, \
             gas_used, cumulative_gas_used, effective_gas_price, status, fee_payer, `type`, \
             fee_token FROM receipts FINAL WHERE {range_predicate} \
             ORDER BY block_num, tx_idx"
        );

        let (txs, logs, receipts) = tokio::try_join!(
            self.fetch_archive_rows::<ChTxWire>(&txs_sql, "txs"),
            self.fetch_archive_rows::<ChLogWire>(&logs_sql, "logs"),
            self.fetch_archive_rows::<ChReceiptWire>(&receipts_sql, "receipts"),
        )?;

        Ok(ArchiveBatch {
            blocks: blocks
                .into_iter()
                .map(ChBlockWire::into_row)
                .collect::<Result<_>>()?,
            txs: txs
                .into_iter()
                .map(ChTxWire::into_row)
                .collect::<Result<_>>()?,
            logs: logs
                .into_iter()
                .map(ChLogWire::into_row)
                .collect::<Result<_>>()?,
            receipts: receipts
                .into_iter()
                .map(ChReceiptWire::into_row)
                .collect::<Result<_>>()?,
        })
    }

    async fn fetch_archive_rows<T>(&self, sql: &str, table: &str) -> Result<Vec<T>>
    where
        T: RowOwned + RowRead,
    {
        self.client
            .query(sql)
            .fetch_all()
            .await
            .map_err(|e| anyhow!("ClickHouse archive read from {table} failed: {e}"))
    }

    /// Distinct block numbers present in `blocks` within `[from, to]`,
    /// ascending. Presence in `blocks` proves the whole batch landed:
    /// writers commit the child tables before blocks.
    pub(crate) async fn block_nums_in_range(&self, from: u64, to: u64) -> Result<Vec<u64>> {
        self.block_nums_in_table_range("blocks", from, to).await
    }

    /// Distinct block numbers with rows in `table` within `[from, to]`,
    /// ascending.
    pub(crate) async fn block_nums_in_table_range(
        &self,
        table: &str,
        from: u64,
        to: u64,
    ) -> Result<Vec<u64>> {
        if from > to {
            return Ok(Vec::new());
        }
        let table = validate_table_name(table)?;
        let col = crate::clickhouse_schema::block_column(table)
            .ok_or_else(|| anyhow!("ClickHouse table has no block column: {table}"))?;
        let nums: Vec<i64> = self
            .client
            .query(&format!(
                "SELECT DISTINCT {col} FROM {table} WHERE {col} >= {from} AND {col} <= {to} ORDER BY {col}"
            ))
            .fetch_all()
            .await
            .map_err(|e| anyhow!("ClickHouse query failed: {e}"))?;
        Ok(nums.into_iter().map(|n| n.max(0) as u64).collect())
    }

    /// Compare incoming blocks with the canonical rows currently stored by number.
    pub(crate) async fn canonical_blocks_present(
        &self,
        blocks: &[BlockRow],
    ) -> Result<CanonicalRowCheck> {
        let mut present = Vec::with_capacity(blocks.len());
        let mut stale_blocks = HashSet::new();
        let mut occupied_blocks = HashSet::new();

        for chunk in blocks.chunks(CH_REPLAY_BLOCK_CHUNK_SIZE) {
            let nums = chunk
                .iter()
                .map(|block| block.num.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT DISTINCT num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, \
                 gas_used, miner, extra_data, consensus_proposer FROM blocks \
                 WHERE num IN ({nums})"
            );
            let existing: Vec<ChBlockWire> =
                self.client.query(&sql).fetch_all().await.map_err(|e| {
                    anyhow!("ClickHouse canonical-row query for blocks failed: {e}")
                })?;
            let mut existing_by_num = HashMap::<i64, HashSet<ChBlockWire>>::new();
            for block in existing {
                existing_by_num.entry(block.num).or_default().insert(block);
            }
            occupied_blocks.extend(existing_by_num.keys().copied());

            for block in chunk {
                let existing = existing_by_num.get(&block.num);
                let is_present = existing
                    .is_some_and(|versions| versions.contains(&ChBlockWire::from_row(block)));
                present.push(is_present);
                if existing.is_some_and(|versions| versions.len() > 1 || !is_present) {
                    stale_blocks.insert(block.num);
                }
            }
        }

        Ok(CanonicalRowCheck {
            present,
            stale_blocks,
            occupied_blocks,
        })
    }

    /// Compare transactions with complete canonical values and block row counts.
    pub(crate) async fn canonical_txs_present(
        &self,
        txs: &[TxRow],
        block_nums: &[i64],
    ) -> Result<CanonicalRowCheck> {
        self.canonical_child_rows_present::<_, ChTxWire, _, _, _>(
            CanonicalChildQuery {
                table: "txs",
                position_col: "idx",
                columns: "block_num, block_timestamp, idx, hash, `type`, `from`, `to`, value, \
                          input, gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_used, \
                          nonce_key, nonce, fee_token, fee_payer, calls, call_count, valid_before, \
                          valid_after, signature_type",
            },
            txs,
            block_nums,
            ChTxWire::from_row,
            |row| (row.block_num, row.idx),
            |row| (row.block_num, row.idx),
        )
        .await
    }

    /// Compare logs with complete canonical values and block row counts.
    pub(crate) async fn canonical_logs_present(
        &self,
        logs: &[LogRow],
        block_nums: &[i64],
    ) -> Result<CanonicalRowCheck> {
        self.canonical_child_rows_present::<_, ChLogWire, _, _, _>(
            CanonicalChildQuery {
                table: "logs",
                position_col: "log_idx",
                columns: "block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, \
                          topic0, topic1, topic2, topic3, data, is_virtual_forward",
            },
            logs,
            block_nums,
            ChLogWire::from_row,
            |row| (row.block_num, row.log_idx),
            |row| (row.block_num, row.log_idx),
        )
        .await
    }

    /// Compare receipts with complete canonical values and block row counts.
    pub(crate) async fn canonical_receipts_present(
        &self,
        receipts: &[ReceiptRow],
        block_nums: &[i64],
    ) -> Result<CanonicalRowCheck> {
        self.canonical_child_rows_present::<_, ChReceiptWire, _, _, _>(
            CanonicalChildQuery {
                table: "receipts",
                position_col: "tx_idx",
                columns: "block_num, block_timestamp, tx_idx, tx_hash, `from`, `to`, \
                          contract_address, gas_used, cumulative_gas_used, effective_gas_price, \
                          status, fee_payer, `type`, fee_token",
            },
            receipts,
            block_nums,
            ChReceiptWire::from_row,
            |row| (row.block_num, row.tx_idx),
            |row| (row.block_num, row.tx_idx),
        )
        .await
    }

    async fn canonical_child_rows_present<S, W, F, K, G>(
        &self,
        query: CanonicalChildQuery<'_>,
        rows: &[S],
        block_nums: &[i64],
        to_wire: F,
        row_key: K,
        wire_key: G,
    ) -> Result<CanonicalRowCheck>
    where
        W: Eq + RowOwned + RowRead,
        F: Fn(&S) -> W,
        K: Fn(&S) -> (i64, i32),
        G: Fn(&W) -> (i64, i32),
    {
        let CanonicalChildQuery {
            table,
            position_col,
            columns,
        } = query;
        let table = validate_table_name(table)?;
        let actual_counts = self
            .row_counts_in_blocks(table, "block_num", block_nums)
            .await?;
        let occupied_blocks = actual_counts.keys().copied().collect::<HashSet<_>>();
        let mut present = vec![false; rows.len()];
        let mut expected_counts = HashMap::<i64, u64>::new();
        for pair in rows.windows(2) {
            if row_key(&pair[0]) >= row_key(&pair[1]) {
                anyhow::bail!(
                    "canonical source rows for {table} must be strictly ordered by block and position"
                );
            }
        }
        for row in rows {
            *expected_counts.entry(row_key(row).0).or_default() += 1;
        }

        // Group populated blocks by FINAL row counts, then stream each result.
        // Blocks with extras are already stale and need no full-row query.
        let mut block_batches = Vec::<Vec<i64>>::new();
        let mut batch = Vec::new();
        let mut batch_rows = 0_u64;
        for block_num in block_nums.iter().copied() {
            let Some(&actual) = actual_counts.get(&block_num) else {
                continue;
            };
            let expected = expected_counts.get(&block_num).copied().unwrap_or_default();
            if actual > expected {
                continue;
            }
            if actual > CH_REPLAY_RESULT_CHUNK_SIZE {
                if !batch.is_empty() {
                    block_batches.push(std::mem::take(&mut batch));
                    batch_rows = 0;
                }
                block_batches.push(vec![block_num]);
                continue;
            }
            if !batch.is_empty()
                && (batch_rows + actual > CH_REPLAY_RESULT_CHUNK_SIZE
                    || batch.len() == CH_REPLAY_BLOCK_CHUNK_SIZE)
            {
                block_batches.push(std::mem::take(&mut batch));
                batch_rows = 0;
            }
            batch.push(block_num);
            batch_rows += actual;
        }
        if !batch.is_empty() {
            block_batches.push(batch);
        }

        let mut source_index = 0;
        for block_batch in block_batches {
            let nums = block_batch
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT {columns} FROM {table} FINAL WHERE block_num IN ({nums}) \
                 ORDER BY block_num, {position_col}"
            );
            let mut cursor =
                self.client.query(&sql).fetch::<W>().map_err(|e| {
                    anyhow!("ClickHouse canonical-row query for {table} failed: {e}")
                })?;
            while let Some(existing) = cursor
                .next()
                .await
                .map_err(|e| anyhow!("ClickHouse canonical-row query for {table} failed: {e}"))?
            {
                let key = wire_key(&existing);
                while source_index < rows.len() && row_key(&rows[source_index]) < key {
                    source_index += 1;
                }
                if source_index < rows.len()
                    && row_key(&rows[source_index]) == key
                    && existing == to_wire(&rows[source_index])
                {
                    present[source_index] = true;
                }
            }
        }

        let mut exact_counts = HashMap::<i64, u64>::new();
        for (row, is_present) in rows.iter().zip(&present) {
            if *is_present {
                *exact_counts.entry(row_key(row).0).or_default() += 1;
            }
        }

        let stale_blocks = actual_counts
            .into_iter()
            .filter_map(|(block_num, actual)| {
                (actual > exact_counts.get(&block_num).copied().unwrap_or_default())
                    .then_some(block_num)
            })
            .collect();

        Ok(CanonicalRowCheck {
            present,
            stale_blocks,
            occupied_blocks,
        })
    }

    async fn row_counts_in_blocks(
        &self,
        table: &str,
        block_column: &str,
        block_nums: &[i64],
    ) -> Result<HashMap<i64, u64>> {
        let mut counts = HashMap::new();
        for chunk in block_nums.chunks(CH_REPLAY_BLOCK_CHUNK_SIZE) {
            let nums = chunk
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT {block_column} AS block_num, count() AS row_count FROM {table} FINAL \
                 WHERE {block_column} IN ({nums}) GROUP BY {block_column}"
            );
            let rows: Vec<ChBlockRowCount> = self
                .client
                .query(&sql)
                .fetch_all()
                .await
                .map_err(|e| anyhow!("ClickHouse row-count query for {table} failed: {e}"))?;
            counts.extend(rows.into_iter().map(|row| (row.block_num, row.row_count)));
        }
        Ok(counts)
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

    /// Delete exact blocks from every block-scoped table before replaying them.
    pub(crate) async fn delete_blocks_exact(
        &self,
        _guard: &MutexGuard<'_, ()>,
        block_nums: &[i64],
    ) -> Result<()> {
        if block_nums.is_empty() {
            return Ok(());
        }

        // Remove the completion marker before dependent data. A retry can then
        // recognize interrupted cleanup and replay the entire canonical block.
        for table in exact_block_delete_tables() {
            for chunk in block_nums.chunks(CH_REPLAY_BLOCK_CHUNK_SIZE) {
                let nums = chunk
                    .iter()
                    .map(i64::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                let predicate = format!("{} IN ({nums})", table.block_column);
                self.client
                    .query(&format!(
                        "ALTER TABLE {} DELETE WHERE {predicate}",
                        table.name
                    ))
                    .with_option("mutations_sync", "1")
                    .execute()
                    .await
                    .map_err(|e| {
                        anyhow!(
                            "ClickHouse exact-block delete from {} failed: {e}",
                            table.name
                        )
                    })?;

                let remaining: u64 = self
                    .client
                    .query(&format!(
                        "SELECT count() FROM {} WHERE {predicate}",
                        table.name
                    ))
                    .fetch_one()
                    .await
                    .map_err(|e| {
                        anyhow!(
                            "ClickHouse exact-block verification query for {} failed: {e}",
                            table.name
                        )
                    })?;
                if remaining > 0 {
                    return Err(anyhow!(
                        "ClickHouse exact-block delete on {} left {remaining} row(s); \
                         refusing to replay atop stale rows",
                        table.name
                    ));
                }
            }
        }

        debug!(
            blocks = block_nums.len(),
            "ClickHouse exact-block delete complete"
        );
        Ok(())
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
        let _guard = self.maintenance_lock.lock().await;
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
    async fn insert_chunked<S, W, F>(
        &self,
        table: &str,
        rows: &[S],
        convert: F,
        deduplication_seed: Option<&str>,
    ) -> Result<()>
    where
        W: Hash + Serialize + for<'a> Row<Value<'a> = W>,
        F: Fn(&S) -> W,
    {
        for (chunk_index, chunk) in rows.chunks(CH_INSERT_CHUNK_SIZE).enumerate() {
            let wire: Vec<W> = chunk.iter().map(&convert).collect();
            // Retries reuse a token, while a reorg's replacement block hash
            // gives otherwise identical child rows a new token.
            let deduplication_token = deduplication_seed.map(|seed| {
                let mut hasher = DefaultHasher::new();
                seed.hash(&mut hasher);
                table.hash(&mut hasher);
                chunk_index.hash(&mut hasher);
                wire.hash(&mut hasher);
                format!("tidx-{:016x}", hasher.finish())
            });
            let mut last_error = None;
            for attempt in 0..CH_MAX_RETRIES {
                if attempt > 0 {
                    let backoff = Duration::from_millis(100 << attempt);
                    warn!(table, attempt, "ClickHouse insert retry after {backoff:?}");
                    tokio::time::sleep(backoff).await;
                }
                match self
                    .try_insert(table, &wire, deduplication_token.as_deref())
                    .await
                {
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

    async fn try_insert<T>(
        &self,
        table: &str,
        rows: &[T],
        deduplication_token: Option<&str>,
    ) -> Result<()>
    where
        T: Serialize + for<'a> Row<Value<'a> = T>,
    {
        let mut insert = self
            .client
            .insert::<T>(table)
            .await?
            .with_timeouts(Some(CH_SEND_TIMEOUT), Some(CH_END_TIMEOUT));
        if let Some(token) = deduplication_token {
            insert = insert.with_option("insert_deduplication_token", token);
        }
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

#[derive(Row, Deserialize)]
struct ChVersionRow {
    version: String,
}

#[derive(Row, Deserialize)]
struct ChBlockRowCount {
    block_num: i64,
    row_count: u64,
}

#[derive(Row, Deserialize)]
struct ChEarnVaultRow {
    address: String,
    deployment_block: i64,
}

#[derive(Row, Deserialize)]
struct ChOptionalBlockNumber {
    block_num: Option<i64>,
}

#[derive(Row, Deserialize)]
struct ChEarnSharePriceCandidateRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    bucket: chrono::DateTime<chrono::Utc>,
    block_num: i64,
    block_hash: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    block_timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Row, Deserialize)]
struct ChCanonicalEarnBlock {
    num: i64,
    hash: String,
}

#[derive(Eq, PartialEq, Row, Serialize)]
struct ChEarnSharePriceWire {
    vault: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    bucket: chrono::DateTime<chrono::Utc>,
    block_num: i64,
    block_hash: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    block_timestamp: chrono::DateTime<chrono::Utc>,
    quoted_shares: ChUInt256,
    quoted_assets: ChUInt256,
}

impl Hash for ChEarnSharePriceWire {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.vault.hash(state);
        self.bucket.hash(state);
        self.block_num.hash(state);
        self.block_hash.hash(state);
        self.block_timestamp.hash(state);
        self.quoted_shares.to_string().hash(state);
        self.quoted_assets.to_string().hash(state);
    }
}

impl ChEarnSharePriceWire {
    fn from_observation(observation: &EarnSharePriceObservation) -> Self {
        Self {
            vault: observation.vault.clone(),
            bucket: observation.bucket,
            block_num: observation.block_num,
            block_hash: observation.block_hash.clone(),
            block_timestamp: observation.block_timestamp,
            quoted_shares: ChUInt256::from(QUOTED_SHARES),
            quoted_assets: ChUInt256::from_le_bytes(observation.quoted_assets.to_le_bytes()),
        }
    }
}

#[derive(Eq, Hash, PartialEq, Row, Serialize, Deserialize)]
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

    fn into_row(self) -> Result<BlockRow> {
        Ok(BlockRow {
            num: self.num,
            hash: hex_decode(&self.hash).context("decode ClickHouse block hash")?,
            parent_hash: hex_decode(&self.parent_hash)
                .context("decode ClickHouse block parent hash")?,
            timestamp: self.timestamp,
            timestamp_ms: self.timestamp_ms,
            gas_limit: self.gas_limit,
            gas_used: self.gas_used,
            miner: hex_decode(&self.miner).context("decode ClickHouse block miner")?,
            extra_data: decode_optional_hex(self.extra_data)
                .context("decode ClickHouse block extra_data")?,
            consensus_proposer: decode_optional_hex(self.consensus_proposer)
                .context("decode ClickHouse block consensus_proposer")?,
        })
    }
}

#[derive(Eq, Hash, PartialEq, Row, Serialize, Deserialize)]
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

    fn into_row(self) -> Result<TxRow> {
        Ok(TxRow {
            block_num: self.block_num,
            block_timestamp: self.block_timestamp,
            idx: self.idx,
            hash: hex_decode(&self.hash).context("decode ClickHouse transaction hash")?,
            tx_type: self.tx_type,
            from: hex_decode(&self.from).context("decode ClickHouse transaction sender")?,
            to: decode_optional_hex(self.to).context("decode ClickHouse transaction recipient")?,
            value: self.value,
            input: hex_decode(&self.input).context("decode ClickHouse transaction input")?,
            gas_limit: self.gas_limit,
            max_fee_per_gas: self.max_fee_per_gas,
            max_priority_fee_per_gas: self.max_priority_fee_per_gas,
            gas_used: self.gas_used,
            nonce_key: hex_decode(&self.nonce_key)
                .context("decode ClickHouse transaction nonce_key")?,
            nonce: self.nonce,
            fee_token: decode_optional_hex(self.fee_token)
                .context("decode ClickHouse transaction fee_token")?,
            fee_payer: decode_optional_hex(self.fee_payer)
                .context("decode ClickHouse transaction fee_payer")?,
            calls: self
                .calls
                .map(|calls| serde_json::from_str(&calls))
                .transpose()
                .context("decode ClickHouse transaction calls JSON")?,
            call_count: self.call_count,
            valid_before: self.valid_before,
            valid_after: self.valid_after,
            signature_type: self.signature_type,
        })
    }
}

#[derive(Eq, Hash, PartialEq, Row, Serialize, Deserialize)]
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

    fn into_row(self) -> Result<LogRow> {
        Ok(LogRow {
            block_num: self.block_num,
            block_timestamp: self.block_timestamp,
            log_idx: self.log_idx,
            tx_idx: self.tx_idx,
            tx_hash: hex_decode(&self.tx_hash).context("decode ClickHouse log tx_hash")?,
            address: hex_decode(&self.address).context("decode ClickHouse log address")?,
            selector: if self.selector.is_empty() {
                None
            } else {
                Some(hex_decode(&self.selector).context("decode ClickHouse log selector")?)
            },
            topic0: decode_optional_hex(self.topic0).context("decode ClickHouse log topic0")?,
            topic1: decode_optional_hex(self.topic1).context("decode ClickHouse log topic1")?,
            topic2: decode_optional_hex(self.topic2).context("decode ClickHouse log topic2")?,
            topic3: decode_optional_hex(self.topic3).context("decode ClickHouse log topic3")?,
            data: hex_decode(&self.data).context("decode ClickHouse log data")?,
            is_virtual_forward: self.is_virtual_forward != 0,
        })
    }
}

#[derive(Eq, Hash, PartialEq, Row, Serialize, Deserialize)]
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
    #[serde(rename = "type")]
    tx_type: Option<i16>,
    fee_token: Option<String>,
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
            tx_type: r.tx_type,
            fee_token: r.fee_token.as_ref().map(|v| hex_encode(v)),
        }
    }

    fn into_row(self) -> Result<ReceiptRow> {
        Ok(ReceiptRow {
            block_num: self.block_num,
            block_timestamp: self.block_timestamp,
            tx_idx: self.tx_idx,
            tx_hash: hex_decode(&self.tx_hash).context("decode ClickHouse receipt tx_hash")?,
            from: hex_decode(&self.from).context("decode ClickHouse receipt sender")?,
            to: decode_optional_hex(self.to).context("decode ClickHouse receipt recipient")?,
            contract_address: decode_optional_hex(self.contract_address)
                .context("decode ClickHouse receipt contract_address")?,
            gas_used: self.gas_used,
            cumulative_gas_used: self.cumulative_gas_used,
            effective_gas_price: self.effective_gas_price,
            status: self.status,
            fee_payer: decode_optional_hex(self.fee_payer)
                .context("decode ClickHouse receipt fee_payer")?,
            tx_type: self.tx_type,
            fee_token: decode_optional_hex(self.fee_token)
                .context("decode ClickHouse receipt fee_token")?,
        })
    }
}

/// Hex-encode bytes with 0x prefix.
fn hex_encode(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

fn hex_decode(value: &str) -> Result<Vec<u8>> {
    let value = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value);
    hex::decode(value).map_err(|e| anyhow!("invalid hex value: {e}"))
}

fn decode_optional_hex(value: Option<String>) -> Result<Option<Vec<u8>>> {
    value.map(|value| hex_decode(&value)).transpose()
}

/// Stable non-cryptographic checksum of a DDL string. Used only to detect
/// whether a managed object's definition has drifted since the last
/// `ensure_schema()`. Collisions are not security-relevant here.
fn checksum_of(ddl: &str) -> String {
    let mut hasher = DefaultHasher::new();
    ddl.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn validate_clickhouse_version(version: &str) -> Result<()> {
    let mut components = version.split('.');
    let major = components
        .next()
        .and_then(|value| value.parse::<u64>().ok());
    let minor = components
        .next()
        .and_then(|value| value.parse::<u64>().ok());
    let Some(found) = major.zip(minor) else {
        return Err(anyhow!(
            "Could not parse ClickHouse server version `{version}`"
        ));
    };
    if found < MIN_CLICKHOUSE_VERSION {
        return Err(anyhow!(
            "Unsupported ClickHouse server version {version}: tidx requires ClickHouse {}.{} or newer",
            MIN_CLICKHOUSE_VERSION.0,
            MIN_CLICKHOUSE_VERSION.1
        ));
    }
    Ok(())
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

/// Rewrite MergeTree-family engines to their `Replicated*` counterparts.
///
/// ZooKeeper path and replica-name arguments are intentionally omitted:
/// inside a `Replicated` database ClickHouse forbids explicit path arguments
/// and substitutes server defaults (`default_replica_path` /
/// `default_replica_name`). Engine-specific arguments (e.g. the
/// `ReplacingMergeTree` version column) are preserved, and engines that are
/// already `Replicated*` are left untouched.
fn to_replicated_engine_ddl(ddl: &str) -> String {
    let engine = regex_lite::Regex::new(
        r"\bENGINE\s*=\s*((?:Replacing|Summing|Aggregating|Collapsing|VersionedCollapsing|Graphite)?MergeTree)\b",
    )
    .expect("static engine rewrite regex must compile");
    engine
        .replace_all(ddl, "ENGINE = Replicated$1")
        .into_owned()
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

fn archive_range_predicate(
    from: i64,
    to: i64,
    from_timestamp_ms: i64,
    to_timestamp_ms: i64,
) -> String {
    format!(
        "block_num >= {from} AND block_num <= {to} \
         AND block_timestamp >= fromUnixTimestamp64Milli({from_timestamp_ms}, 'UTC') \
         AND block_timestamp <= fromUnixTimestamp64Milli({to_timestamp_ms}, 'UTC')"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn earn_candidates_withhold_the_boundary_without_shifting_the_sample() {
        let sql = earn_share_price_candidates_sql(42, 7);

        assert!(sql.contains("timestamp - INTERVAL 1 MILLISECOND"));
        assert!(!sql.contains("timestamp + INTERVAL 30 SECOND"));
        assert!(sql.contains("HAVING bucket <= toStartOfInterval(now64(3) - INTERVAL 30 SECOND"));
        assert!(sql.contains("WHERE num >= 42"));
        assert!(sql.ends_with("LIMIT 7"));
    }

    #[test]
    fn validates_minimum_clickhouse_version() {
        validate_clickhouse_version("25.11.1.1").unwrap();
        validate_clickhouse_version("26.7.2.59").unwrap();

        let error = validate_clickhouse_version("25.10.9.1").unwrap_err();
        assert!(error.to_string().contains("requires ClickHouse 25.11"));
        assert!(validate_clickhouse_version("invalid").is_err());
    }

    #[test]
    fn archive_wire_conversion_restores_postgres_values() {
        let timestamp = chrono::DateTime::from_timestamp_millis(1_750_000_000_123).unwrap();
        let tx = TxRow {
            block_num: 42,
            block_timestamp: timestamp,
            idx: 3,
            hash: vec![0xaa; 32],
            tx_type: 2,
            from: vec![0x11; 20],
            to: Some(vec![0x22; 20]),
            value: "123".into(),
            input: vec![0xde, 0xad],
            gas_limit: 21_000,
            max_fee_per_gas: "10".into(),
            max_priority_fee_per_gas: "1".into(),
            gas_used: Some(20_000),
            nonce_key: vec![0x33; 20],
            nonce: 7,
            fee_token: Some(vec![0x44; 20]),
            fee_payer: Some(vec![0x55; 20]),
            calls: Some(serde_json::json!([{"to": "0x1234"}])),
            call_count: 2,
            valid_before: Some(100),
            valid_after: Some(50),
            signature_type: Some(1),
        };
        let restored_tx = ChTxWire::from_row(&tx).into_row().unwrap();
        assert_eq!(restored_tx.hash, tx.hash);
        assert_eq!(restored_tx.to, tx.to);
        assert_eq!(restored_tx.input, tx.input);
        assert_eq!(restored_tx.fee_token, tx.fee_token);
        assert_eq!(restored_tx.fee_payer, tx.fee_payer);
        assert_eq!(restored_tx.calls, tx.calls);

        let log = LogRow {
            block_num: 42,
            block_timestamp: timestamp,
            log_idx: 1,
            tx_idx: 3,
            tx_hash: vec![0xaa; 32],
            address: vec![0x66; 20],
            selector: None,
            topic0: Some(vec![0x77; 32]),
            topic1: None,
            topic2: None,
            topic3: None,
            data: vec![0x88; 16],
            is_virtual_forward: true,
        };
        let restored_log = ChLogWire::from_row(&log).into_row().unwrap();
        assert_eq!(restored_log.address, log.address);
        assert_eq!(restored_log.selector, None);
        assert_eq!(restored_log.topic0, log.topic0);
        assert!(restored_log.is_virtual_forward);
    }

    #[test]
    fn test_to_replicated_engine_ddl() {
        // Bare and parameterized MergeTree-family engines are rewritten.
        assert_eq!(
            to_replicated_engine_ddl("CREATE TABLE t (x Int64) ENGINE = MergeTree ORDER BY x"),
            "CREATE TABLE t (x Int64) ENGINE = ReplicatedMergeTree ORDER BY x"
        );
        assert_eq!(
            to_replicated_engine_ddl(") ENGINE = ReplacingMergeTree()\nORDER BY (a, b)"),
            ") ENGINE = ReplicatedReplacingMergeTree()\nORDER BY (a, b)"
        );
        // Engine-specific arguments (version column) are preserved.
        assert_eq!(
            to_replicated_engine_ddl("ENGINE = ReplacingMergeTree(applied_at)"),
            "ENGINE = ReplicatedReplacingMergeTree(applied_at)"
        );
        // Already-replicated engines and non-engine DDL are untouched.
        assert_eq!(
            to_replicated_engine_ddl("ENGINE = ReplicatedReplacingMergeTree(applied_at)"),
            "ENGINE = ReplicatedReplacingMergeTree(applied_at)"
        );
        assert_eq!(
            to_replicated_engine_ddl("ALTER TABLE logs ADD COLUMN IF NOT EXISTS x Int64"),
            "ALTER TABLE logs ADD COLUMN IF NOT EXISTS x Int64"
        );
        assert_eq!(
            to_replicated_engine_ddl("CREATE MATERIALIZED VIEW mv TO t AS SELECT 1"),
            "CREATE MATERIALIZED VIEW mv TO t AS SELECT 1"
        );
    }

    #[test]
    fn test_schema_ddl_covers_all_catalog_engines() {
        // Every MergeTree-family engine in the catalog must be rewritten when
        // replication is enabled — a new engine variant outside the rewrite
        // list would silently create non-replicated tables.
        for object in base_objects().iter().chain(derived_objects()) {
            let raw = object.ddl();
            let rewritten = to_replicated_engine_ddl(&raw);
            assert!(
                !rewritten.contains("= MergeTree") && !rewritten.contains("= Replacing"),
                "object {} still has a non-replicated MergeTree engine after rewrite: {raw}",
                object.name
            );
        }
        assert!(
            !to_replicated_engine_ddl(SCHEMA_OBJECTS_TABLE_DDL).contains("= ReplacingMergeTree")
        );
        assert!(
            SCHEMA_OBJECTS_TABLE_DDL.contains("SETTINGS default_compression_codec = 'ZSTD(1)'")
        );
    }

    #[test]
    fn test_create_database_ddl() {
        let sink = ClickHouseSink::new("http://localhost:8123", "tidx_4217", None, None).unwrap();
        assert_eq!(
            sink.create_database_ddl(),
            "CREATE DATABASE IF NOT EXISTS tidx_4217"
        );

        let sink = sink.with_replicated_database(true);
        assert_eq!(
            sink.create_database_ddl(),
            "CREATE DATABASE IF NOT EXISTS tidx_4217 \
             ENGINE = Replicated('/clickhouse/databases/tidx_4217', '{shard}', '{replica}')"
        );
    }

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
    fn batch_deduplication_seed_tracks_block_generation() {
        use chrono::TimeZone;

        let block = BlockRow {
            num: 42,
            hash: vec![0xcc; 32],
            parent_hash: vec![0xbb; 32],
            timestamp: chrono::Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap(),
            timestamp_ms: 1_705_320_000_000,
            gas_limit: 30_000_000,
            gas_used: 15_000_000,
            miner: vec![0xaa; 20],
            extra_data: None,
            consensus_proposer: None,
        };
        let ordinary = batch_deduplication_seed(std::slice::from_ref(&block))
            .expect("non-empty writes need a seed");
        let repeated = batch_deduplication_seed(std::slice::from_ref(&block)).unwrap();
        assert_eq!(ordinary, repeated);

        assert_eq!(batch_deduplication_seed(&[]), None);

        let mut changed_block = block.clone();
        changed_block.hash[0] ^= 0xff;
        let changed = batch_deduplication_seed(&[changed_block]).unwrap();
        assert_ne!(ordinary, changed);
    }

    #[test]
    fn exact_block_cleanup_deletes_marker_first() {
        let tables = exact_block_delete_tables();
        assert_eq!(tables.first().map(|table| table.name), Some("blocks"));
        assert_eq!(
            tables.iter().filter(|table| table.name == "blocks").count(),
            1
        );
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
    fn archive_range_predicate_prunes_block_and_timestamp_ranges() {
        assert_eq!(
            archive_range_predicate(100, 199, 1_750_000_000_000, 1_750_000_099_000),
            "block_num >= 100 AND block_num <= 199 \
             AND block_timestamp >= fromUnixTimestamp64Milli(1750000000000, 'UTC') \
             AND block_timestamp <= fromUnixTimestamp64Milli(1750000099000, 'UTC')"
        );
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
            target: "token_transfers",
            select_sql: "SELECT block_num, tx_hash FROM logs\n",
            block_column: "block_num",
            from_block: 100,
            to_block_exclusive: 200,
            source_rows: 10,
            target_rows: 5,
        };

        assert_eq!(
            bounded_backfill_sql(&plan),
            "INSERT INTO token_transfers SELECT DISTINCT * FROM (SELECT block_num, tx_hash FROM logs) WHERE block_num >= 100 AND block_num < 200"
        );
    }

    #[test]
    fn test_derived_backfill_count_sql_uses_distinct_source_and_final_target() {
        assert_eq!(
            source_count_sql(
                "SELECT block_num, tx_hash FROM logs\n",
                "block_num",
                100,
                200
            ),
            "SELECT count() FROM (SELECT DISTINCT * FROM (SELECT block_num, tx_hash FROM logs) WHERE block_num >= 100 AND block_num < 200)"
        );
        assert_eq!(
            target_count_sql("token_transfers", "block_num", 100, 200),
            "SELECT count() FROM token_transfers FINAL WHERE block_num >= 100 AND block_num < 200"
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
