//! ClickHouse OLAP engine for analytical queries
//!
//! Uses MaterializedPostgreSQL for real-time WAL-based CDC replication
//! from PostgreSQL. Provides vectorized columnar execution for OLAP queries.
//!
//! Supports multiple ClickHouse instances per chain with failover:
//! queries go to the primary instance and automatically fail over
//! to secondary instances if the primary is unavailable.

use anyhow::{anyhow, Result};
use clickhouse::Client;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex;
use tokio_postgres::NoTls;
use tracing::{debug, error, info, warn};

use crate::config::ClickHouseConfig;
use crate::query::EventSignature;

/// A single ClickHouse instance (connection + URL).
struct Instance {
    admin_client: Client,
    http_client: reqwest::Client,
    url: String,
    /// Database name for this instance (includes instance index suffix)
    database: String,
}

/// ClickHouse engine for OLAP queries.
/// Uses MaterializedPostgreSQL for real-time replication from PostgreSQL.
///
/// When multiple instances are configured, queries are sent to the active
/// instance (starting with the primary). On connection failure the engine
/// automatically tries the next instance in order.
///
/// Each instance gets its own database name (e.g., `tidx_4217_0`, `tidx_4217_1`)
/// to ensure unique PostgreSQL replication slots per instance.
pub struct ClickHouseEngine {
    instances: Vec<Instance>,
    /// Index of the currently active instance (0 = primary).
    active: AtomicUsize,
    /// Base database name for this chain (e.g., "tidx_4217" for chain 4217)
    database: String,
    /// Chain ID
    chain_id: u64,
}

impl ClickHouseEngine {
    /// Create a new ClickHouse engine for the given chain.
    /// The primary URL comes from `config.url`; additional failover URLs
    /// come from `config.failover_urls`.
    pub fn new(config: &ClickHouseConfig, chain_id: u64, _pg_url: &str) -> Result<Self> {
        let database = format!("tidx_{chain_id}");

        let mut instances = Vec::new();
        for (i, url) in config.all_urls().iter().enumerate() {
            // Each instance gets a unique database name to avoid replication slot conflicts
            let instance_database = format!("{}_{}", database, i);
            instances.push(Self::make_instance(url, instance_database)?);
        }

        Ok(Self {
            instances,
            active: AtomicUsize::new(0),
            database,
            chain_id,
        })
    }

    fn make_instance(url: &str, database: String) -> Result<Instance> {
        let admin_client = Client::default().with_url(url);
        let http_client = reqwest::Client::builder()
            .pool_max_idle_per_host(4)
            .build()
            .map_err(|e| anyhow!("Failed to create HTTP client: {e}"))?;
        Ok(Instance {
            admin_client,
            http_client,
            url: url.to_string(),
            database,
        })
    }

    /// Get the admin client of the *primary* instance (index 0) for DDL operations.
    pub fn admin_client(&self) -> &Client {
        &self.instances[0].admin_client
    }

    /// Get the database name.
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Execute a query and return results as JSON values.
    /// On connection failure the engine automatically retries with the next
    /// instance (failover). Only connection-level errors trigger failover;
    /// ClickHouse query errors (syntax, missing table, etc.) are returned
    /// immediately.
    pub async fn query(
        &self,
        sql: &str,
        signature: Option<&str>,
    ) -> Result<QueryResult> {
        let sql = if let Some(sig_str) = signature {
            let sig = EventSignature::parse(sig_str)?;
            let sql = sig.normalize_table_references(sql);
            let sql = sig.rewrite_filters_for_pushdown(&sql);
            let cte = sig.to_cte_sql_clickhouse();
            format!("WITH {cte} {sql}")
        } else {
            sql.to_string()
        };

        let sql = crate::query::convert_hex_literals_clickhouse(&sql);
        let start = std::time::Instant::now();
        let n = self.instances.len();
        let starting = self.active.load(Ordering::Relaxed);

        for attempt in 0..n {
            let idx = (starting + attempt) % n;
            let inst = &self.instances[idx];

            match self.try_query(inst, &sql, start).await {
                Ok(result) => {
                    if attempt > 0 {
                        self.active.store(idx, Ordering::Relaxed);
                        warn!(
                            url = %inst.url,
                            database = %self.database,
                            "ClickHouse failed over to instance {}",
                            idx
                        );
                    }
                    return Ok(result);
                }
                Err(e) if is_connection_error(&e) && attempt + 1 < n => {
                    error!(
                        url = %inst.url,
                        error = %e,
                        database = %self.database,
                        "ClickHouse instance unreachable, trying next"
                    );
                }
                Err(e) => return Err(e),
            }
        }

        Err(anyhow!("All ClickHouse instances unreachable"))
    }

    async fn try_query(
        &self,
        inst: &Instance,
        sql: &str,
        start: std::time::Instant,
    ) -> Result<QueryResult> {
        let url = format!(
            "{}/?database={}&default_format=JSON",
            inst.url.trim_end_matches('/'),
            inst.database
        );

        let resp = inst
            .http_client
            .post(&url)
            .body(sql.to_string())
            .send()
            .await
            .map_err(|e| anyhow!("ClickHouse HTTP request failed: {e}"))?;

        if !resp.status().is_success() {
            let error_text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("ClickHouse query failed: {error_text}"));
        }

        let json_response = resp
            .text()
            .await
            .map_err(|e| anyhow!("Failed to read response: {e}"))?;

        if json_response.trim().is_empty() {
            return Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                row_count: 0,
                engine: Some("clickhouse".to_string()),
                query_time_ms: Some(start.elapsed().as_secs_f64() * 1000.0),
            });
        }

        let parsed: serde_json::Value = serde_json::from_str(&json_response)
            .map_err(|e| anyhow!("Failed to parse ClickHouse JSON response: {e}"))?;

        let meta = parsed.get("meta").and_then(|m| m.as_array());
        let data = parsed.get("data").and_then(|d| d.as_array());

        let columns: Vec<String> = meta
            .map(|m| {
                m.iter()
                    .filter_map(|col| col.get("name").and_then(|n| n.as_str()).map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let rows: Vec<Vec<serde_json::Value>> = data
            .map(|d| {
                d.iter()
                    .map(|row| {
                        columns
                            .iter()
                            .map(|col| {
                                let value =
                                    row.get(col).cloned().unwrap_or(serde_json::Value::Null);
                                normalize_hex_output(value)
                            })
                            .collect()
                    })
                    .collect()
            })
            .unwrap_or_default();

        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        let row_count = rows.len();

        Ok(QueryResult {
            columns,
            rows,
            row_count,
            engine: Some("clickhouse".to_string()),
            query_time_ms: Some(elapsed_ms),
        })
    }

    /// Ensure MaterializedPostgreSQL replication is set up on **all** instances.
    /// Each instance gets its own replication stream from Postgres via a unique
    /// database name (e.g., `tidx_4217_0`, `tidx_4217_1`) and pre-created replication
    /// slots to avoid slot conflicts between instances.
    ///
    /// Uses the PostgreSQL replication protocol to create slots with EXPORT_SNAPSHOT
    /// and FAILOVER flags. The snapshot is passed to ClickHouse for consistent initial
    /// sync, and FAILOVER enables CNPG to synchronize slots to standbys.
    pub async fn ensure_replication(&self, pg_url: &str) -> Result<()> {
        let pg = parse_pg_url(pg_url)?;

        for (i, inst) in self.instances.iter().enumerate() {
            let exists: u8 = inst
                .admin_client
                .query("SELECT count() FROM system.databases WHERE name = ?")
                .bind(&inst.database)
                .fetch_one()
                .await
                .unwrap_or(0);

            if exists > 0 {
                debug!(
                    database = %inst.database,
                    url = %inst.url,
                    "ClickHouse database already exists on instance {}",
                    i
                );
                continue;
            }

            info!(
                database = %inst.database,
                chain_id = self.chain_id,
                url = %inst.url,
                "Creating MaterializedPostgreSQL database on instance {}",
                i
            );

            // Use unique slot name per ClickHouse instance
            let slot_name = &inst.database;

            // Connect to PostgreSQL using replication protocol to create slot with
            // EXPORT_SNAPSHOT (required by ClickHouse) and FAILOVER (required by CNPG)
            // Build key=value connection string with proper quoting for special chars
            let pg_repl_conn_str = format!(
                "host='{}' port='{}' dbname='{}' user='{}' password='{}' replication=database",
                pg.host.replace('\'', "\\'"),
                pg.port,
                pg.database.replace('\'', "\\'"),
                pg.user.replace('\'', "\\'"),
                pg.password.replace('\'', "\\'")
            );

            let (pg_client, connection) =
                tokio_postgres::connect(&pg_repl_conn_str, NoTls).await?;

            // Spawn connection handler - keep alive until ClickHouse starts using the slot
            let conn_handle = tokio::spawn(async move {
                if let Err(e) = connection.await {
                    error!(error = %e, "PostgreSQL replication connection error");
                }
            });

            // Check if slot already exists (reuse if so - ClickHouse will ATTACH)
            let slot_row = pg_client
                .query_opt(
                    "SELECT slot_name FROM pg_replication_slots WHERE slot_name = $1",
                    &[&slot_name],
                )
                .await?;

            let snapshot_name = if slot_row.is_some() {
                // Slot exists - we can't get a new snapshot for an existing slot
                // Drop and recreate to get a fresh snapshot
                info!(
                    slot_name = %slot_name,
                    "Dropping existing replication slot to recreate with snapshot"
                );

                // Use simple query for replication protocol commands
                pg_client
                    .simple_query(&format!("DROP_REPLICATION_SLOT {} WAIT", slot_name))
                    .await
                    .map_err(|e| anyhow!("Failed to drop replication slot: {}", e))?;

                // Create new slot with EXPORT_SNAPSHOT and FAILOVER
                self.create_replication_slot_with_snapshot(&pg_client, slot_name).await?
            } else {
                // Create new slot with EXPORT_SNAPSHOT and FAILOVER
                self.create_replication_slot_with_snapshot(&pg_client, slot_name).await?
            };

            info!(
                slot_name = %slot_name,
                snapshot_name = %snapshot_name,
                "Created PostgreSQL replication slot with exported snapshot"
            );

            // Create ClickHouse database with slot AND snapshot
            // The snapshot ensures consistent initial sync from the slot's LSN
            let create_sql = format!(
                r#"CREATE DATABASE IF NOT EXISTS {database}
ENGINE = MaterializedPostgreSQL(
    '{host}:{port}',
    '{pg_database}',
    '{user}',
    '{password}'
)
SETTINGS materialized_postgresql_tables_list = 'logs,blocks,txs,receipts',
         materialized_postgresql_replication_slot = '{slot_name}',
         materialized_postgresql_snapshot = '{snapshot_name}'"#,
                database = inst.database,
                host = pg.host,
                port = pg.port,
                pg_database = pg.database,
                user = pg.user,
                password = pg.password,
                slot_name = slot_name,
                snapshot_name = snapshot_name,
            );

            inst.admin_client.query(&create_sql).execute().await?;

            info!(
                database = %inst.database,
                url = %inst.url,
                "MaterializedPostgreSQL database created on instance {}, replication starting",
                i
            );

            // Drop the replication connection - ClickHouse now owns the slot
            // The snapshot was only needed for the CREATE DATABASE command
            drop(pg_client);
            conn_handle.abort();

            tokio::spawn({
                let client = inst.admin_client.clone();
                let database = inst.database.clone();
                async move {
                    tokio::time::sleep(std::time::Duration::from_secs(10)).await;

                    if let Err(e) = add_bloom_indexes(&client, &database).await {
                        warn!(error = %e, "Failed to add bloom filter indexes (non-fatal)");
                    }
                }
            });
        }

        Ok(())
    }

    /// Create a replication slot using the replication protocol with EXPORT_SNAPSHOT
    /// and FAILOVER flags. Returns the snapshot name for use in ClickHouse.
    async fn create_replication_slot_with_snapshot(
        &self,
        pg_client: &tokio_postgres::Client,
        slot_name: &str,
    ) -> Result<String> {
        // Use replication protocol command (not SQL function) to get EXPORT_SNAPSHOT
        // Format: CREATE_REPLICATION_SLOT slot_name LOGICAL pgoutput EXPORT_SNAPSHOT FAILOVER
        let cmd = format!(
            "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput EXPORT_SNAPSHOT FAILOVER",
            slot_name
        );

        let results = pg_client
            .simple_query(&cmd)
            .await
            .map_err(|e| anyhow!("Failed to create replication slot: {}", e))?;

        // Parse the response to extract snapshot_name
        // Response format: slot_name, consistent_point, snapshot_name, output_plugin
        for msg in results {
            if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
                // Column order: slot_name, consistent_point, snapshot_name, output_plugin
                if let Some(snapshot) = row.get(2) {
                    return Ok(snapshot.to_string());
                }
            }
        }

        Err(anyhow!(
            "Failed to get snapshot name from CREATE_REPLICATION_SLOT response"
        ))
    }

    /// Return the URL of the currently active instance (for observability).
    pub fn active_url(&self) -> &str {
        let idx = self.active.load(Ordering::Relaxed);
        &self.instances[idx].url
    }

    /// Return the number of configured instances.
    pub fn instance_count(&self) -> usize {
        self.instances.len()
    }
}

/// Returns true for errors that indicate the ClickHouse instance is unreachable
/// (connection refused, timeout, DNS failure, etc.) — as opposed to query-level
/// errors that would happen on any instance.
fn is_connection_error(err: &anyhow::Error) -> bool {
    let msg = err.to_string();
    msg.contains("HTTP request failed")
        || msg.contains("connection refused")
        || msg.contains("Connection refused")
        || msg.contains("connect error")
        || msg.contains("dns error")
        || msg.contains("timed out")
        || msg.contains("hyper::Error")
}

/// Query result from ClickHouse.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub row_count: usize,
    pub engine: Option<String>,
    pub query_time_ms: Option<f64>,
}

/// Parsed PostgreSQL connection URL.
struct PgConnection {
    host: String,
    port: u16,
    database: String,
    user: String,
    password: String,
}

/// Parse a PostgreSQL URL into components.
fn parse_pg_url(url: &str) -> Result<PgConnection> {
    let url = url::Url::parse(url).map_err(|e| anyhow!("Invalid PostgreSQL URL: {e}"))?;

    Ok(PgConnection {
        host: url.host_str().unwrap_or("localhost").to_string(),
        port: url.port().unwrap_or(5432),
        database: url.path().trim_start_matches('/').to_string(),
        user: url.username().to_string(),
        password: url.password().unwrap_or("").to_string(),
    })
}

/// Add bloom filter indexes to the logs table for efficient filtering.
async fn add_bloom_indexes(client: &Client, database: &str) -> Result<()> {
    let indexes = [
        ("idx_selector", "selector", "bloom_filter"),
        ("idx_address", "address", "bloom_filter"),
        ("idx_topic1", "topic1", "bloom_filter"),
        ("idx_topic2", "topic2", "bloom_filter"),
    ];

    for (name, column, type_) in indexes {
        let sql = format!(
            "ALTER TABLE {database}.logs ADD INDEX IF NOT EXISTS {name} {column} TYPE {type_} GRANULARITY 1"
        );

        if let Err(e) = client.query(&sql).execute().await {
            debug!(error = %e, index = name, "Failed to add index (may already exist)");
        } else {
            info!(index = name, "Added bloom filter index on logs.{column}");
        }
    }

    Ok(())
}

/// Convert '\x...' hex strings to '0x...' format for output.
/// MaterializedPostgreSQL stores bytea as '\x'-prefixed strings, but we want
/// standard Ethereum '0x...' format in API responses.
fn normalize_hex_output(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::String(s) if s.starts_with("\\x") => {
            serde_json::Value::String(format!("0x{}", &s[2..]))
        }
        other => other,
    }
}

/// Global engine registry for multiple chains.
pub struct ClickHouseRegistry {
    engines: Mutex<HashMap<u64, Arc<ClickHouseEngine>>>,
    config: ClickHouseConfig,
}

impl ClickHouseRegistry {
    /// Create a new registry.
    pub fn new(config: ClickHouseConfig) -> Self {
        Self {
            engines: Mutex::new(HashMap::new()),
            config,
        }
    }

    /// Get or create an engine for the given chain.
    pub async fn get_or_create(
        &self,
        chain_id: u64,
        pg_url: &str,
    ) -> Result<Arc<ClickHouseEngine>> {
        let mut engines = self.engines.lock().await;

        if let Some(engine) = engines.get(&chain_id) {
            return Ok(Arc::clone(engine));
        }

        let engine = Arc::new(ClickHouseEngine::new(&self.config, chain_id, pg_url)?);
        engines.insert(chain_id, Arc::clone(&engine));
        Ok(engine)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_pg_url() {
        let url = "postgres://user:pass@host:5432/database";
        let pg = parse_pg_url(url).unwrap();

        assert_eq!(pg.host, "host");
        assert_eq!(pg.port, 5432);
        assert_eq!(pg.database, "database");
        assert_eq!(pg.user, "user");
        assert_eq!(pg.password, "pass");
    }

    #[test]
    fn test_parse_pg_url_default_port() {
        let url = "postgres://user:pass@host/database";
        let pg = parse_pg_url(url).unwrap();

        assert_eq!(pg.port, 5432);
    }

    #[test]
    fn test_is_connection_error() {
        let conn_err = anyhow!("ClickHouse HTTP request failed: connection refused");
        assert!(is_connection_error(&conn_err));

        let query_err = anyhow!("ClickHouse query failed: Code: 60. DB::Exception: Table logs doesn't exist");
        assert!(!is_connection_error(&query_err));
    }

    #[test]
    fn test_engine_single_instance() {
        let config = ClickHouseConfig {
            enabled: true,
            url: "http://clickhouse-1:8123".to_string(),
            failover_urls: vec![],
        };

        let engine = ClickHouseEngine::new(&config, 4217, "postgres://localhost/test").unwrap();
        assert_eq!(engine.instance_count(), 1);
        assert_eq!(engine.active_url(), "http://clickhouse-1:8123");
        // Single instance should have database name with _0 suffix
        assert_eq!(engine.instances[0].database, "tidx_4217_0");
    }

    #[test]
    fn test_engine_multiple_instances() {
        let config = ClickHouseConfig {
            enabled: true,
            url: "http://clickhouse-1:8123".to_string(),
            failover_urls: vec!["http://clickhouse-2:8123".to_string()],
        };

        let engine = ClickHouseEngine::new(&config, 4217, "postgres://localhost/test").unwrap();
        assert_eq!(engine.instance_count(), 2);
        assert_eq!(engine.active_url(), "http://clickhouse-1:8123");
        // Each instance should have a unique database name
        assert_eq!(engine.instances[0].database, "tidx_4217_0");
        assert_eq!(engine.instances[1].database, "tidx_4217_1");
    }

    #[test]
    fn test_config_all_urls() {
        let config = ClickHouseConfig {
            enabled: true,
            url: "http://primary:8123".to_string(),
            failover_urls: vec![
                "http://secondary:8123".to_string(),
                "http://tertiary:8123".to_string(),
            ],
        };
        assert_eq!(
            config.all_urls(),
            vec!["http://primary:8123", "http://secondary:8123", "http://tertiary:8123"]
        );
    }

    #[test]
    fn test_config_all_urls_single() {
        let config = ClickHouseConfig::default();
        assert_eq!(config.all_urls(), vec!["http://clickhouse:8123"]);
    }
}
