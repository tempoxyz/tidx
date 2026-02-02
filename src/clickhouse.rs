//! ClickHouse OLAP engine for analytical queries
//!
//! Uses MaterializedPostgreSQL for real-time WAL-based CDC replication
//! from PostgreSQL. Provides vectorized columnar execution for OLAP queries.

use anyhow::{anyhow, Result};
use clickhouse::Client;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::config::ClickHouseConfig;
use crate::query::EventSignature;

/// ClickHouse engine for OLAP queries.
/// Uses MaterializedPostgreSQL for real-time replication from PostgreSQL.
pub struct ClickHouseEngine {
    /// Client without database set (for DDL operations)
    admin_client: Client,
    /// Pooled HTTP client for queries (reuses connections)
    http_client: reqwest::Client,
    /// ClickHouse HTTP URL
    url: String,
    /// Database name for this chain (e.g., "tidx_4217" for chain 4217)
    database: String,
    /// Chain ID
    chain_id: u64,
}

impl ClickHouseEngine {
    /// Create a new ClickHouse engine for the given chain.
    pub fn new(config: &ClickHouseConfig, chain_id: u64, _pg_url: &str) -> Result<Self> {
        let database = format!("tidx_{chain_id}");
        
        // Admin client without database (for CREATE DATABASE and other DDL)
        let admin_client = Client::default()
            .with_url(&config.url);
        
        // Pooled HTTP client for queries (connection pooling, keep-alive)
        let http_client = reqwest::Client::builder()
            .pool_max_idle_per_host(4)
            .build()
            .map_err(|e| anyhow!("Failed to create HTTP client: {e}"))?;
        
        Ok(Self {
            admin_client,
            http_client,
            url: config.url.clone(),
            database,
            chain_id,
        })
    }
    
    /// Get the admin client for DDL operations.
    pub fn admin_client(&self) -> &Client {
        &self.admin_client
    }
    
    /// Get the database name.
    pub fn database(&self) -> &str {
        &self.database
    }
    
    /// Execute a query and return results as JSON values.
    pub async fn query(
        &self,
        sql: &str,
        signature: Option<&str>,
    ) -> Result<QueryResult> {
        // Generate CTE if signature provided
        let sql = if let Some(sig_str) = signature {
            let sig = EventSignature::parse(sig_str)?;
            let cte = sig.to_cte_sql_clickhouse();
            format!("WITH {cte} {sql}")
        } else {
            sql.to_string()
        };
        
        let start = std::time::Instant::now();
        
        // Query ClickHouse via HTTP with JSON format (connection pooled)
        let url = format!(
            "{}/?database={}&default_format=JSON",
            self.url.trim_end_matches('/'),
            self.database
        );
        
        let resp = self.http_client
            .post(&url)
            .body(sql.clone())
            .send()
            .await
            .map_err(|e| anyhow!("ClickHouse HTTP request failed: {e}"))?;
        
        if !resp.status().is_success() {
            let error_text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("ClickHouse query failed: {error_text}"));
        }
        
        let json_response = resp.text().await
            .map_err(|e| anyhow!("Failed to read response: {e}"))?;
        
        // Parse the JSON response
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
                            .map(|col| row.get(col).cloned().unwrap_or(serde_json::Value::Null))
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
    
    /// Ensure MaterializedPostgreSQL replication is set up.
    /// Creates the database and starts replication if not already running.
    pub async fn ensure_replication(&self, pg_url: &str) -> Result<()> {
        // Parse PostgreSQL URL to extract components
        let pg = parse_pg_url(pg_url)?;
        
        // Check if database exists (use admin client - no database context needed)
        let exists: u8 = self.admin_client
            .query("SELECT count() FROM system.databases WHERE name = ?")
            .bind(&self.database)
            .fetch_one()
            .await
            .unwrap_or(0);
        
        if exists > 0 {
            debug!(database = %self.database, "ClickHouse database already exists");
            return Ok(());
        }
        
        info!(
            database = %self.database,
            chain_id = self.chain_id,
            "Creating MaterializedPostgreSQL database for real-time replication"
        );
        
        // Create MaterializedPostgreSQL database
        let create_sql = format!(
            r#"CREATE DATABASE IF NOT EXISTS {database}
ENGINE = MaterializedPostgreSQL(
    '{host}:{port}',
    '{pg_database}',
    '{user}',
    '{password}'
)
SETTINGS materialized_postgresql_tables_list = 'logs,blocks,txs,receipts'"#,
            database = self.database,
            host = pg.host,
            port = pg.port,
            pg_database = pg.database,
            user = pg.user,
            password = pg.password,
        );
        
        self.admin_client.query(&create_sql).execute().await?;
        
        info!(
            database = %self.database,
            "MaterializedPostgreSQL database created, replication starting"
        );
        
        // Add bloom filter indexes for common query patterns
        // Note: This needs to be done after replication starts and tables exist
        tokio::spawn({
            let client = self.admin_client.clone();
            let database = self.database.clone();
            async move {
                // Wait for tables to be created by replication
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                
                if let Err(e) = add_bloom_indexes(&client, &database).await {
                    warn!(error = %e, "Failed to add bloom filter indexes (non-fatal)");
                }
            }
        });
        
        Ok(())
    }
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
    pub async fn get_or_create(&self, chain_id: u64, pg_url: &str) -> Result<Arc<ClickHouseEngine>> {
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
}
