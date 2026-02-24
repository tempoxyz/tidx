//! ClickHouse OLAP engine for analytical queries.
//!
//! Reads from tables populated by the direct-write ClickHouseSink.
//! Provides vectorized columnar execution for OLAP queries.
//!
//! Supports multiple ClickHouse instances per chain with failover:
//! queries go to the primary instance and automatically fail over
//! to secondary instances if the primary is unavailable.

use anyhow::{anyhow, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{error, warn};

use crate::config::ClickHouseConfig;
use crate::query::EventSignature;

/// A single ClickHouse instance (connection + URL).
struct Instance {
    http_client: reqwest::Client,
    url: String,
}

/// ClickHouse engine for OLAP queries.
///
/// When multiple instances are configured, queries are sent to the active
/// instance (starting with the primary). On connection failure the engine
/// automatically tries the next instance in order.
pub struct ClickHouseEngine {
    instances: Vec<Instance>,
    /// Index of the currently active instance (0 = primary).
    active: AtomicUsize,
    /// Database name for this chain (e.g., "tidx_4217" for chain 4217)
    database: String,
}

impl ClickHouseEngine {
    /// Create a new ClickHouse engine for the given chain.
    /// The primary URL comes from `config.url`; additional failover URLs
    /// come from `config.failover_urls`.
    pub fn new(config: &ClickHouseConfig, chain_id: u64) -> Result<Self> {
        let database = config
            .database
            .clone()
            .unwrap_or_else(|| format!("tidx_{chain_id}"));

        let mut instances = Vec::new();
        for url in config.all_urls() {
            instances.push(Self::make_instance(url)?);
        }

        Ok(Self {
            instances,
            active: AtomicUsize::new(0),
            database,
        })
    }

    fn make_instance(url: &str) -> Result<Instance> {
        let http_client = reqwest::Client::builder()
            .pool_max_idle_per_host(4)
            .build()
            .map_err(|e| anyhow!("Failed to create HTTP client: {e}"))?;
        Ok(Instance {
            http_client,
            url: url.to_string(),
        })
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
        signatures: &[&str],
    ) -> Result<QueryResult> {
        let sql = if !signatures.is_empty() {
            let sigs: Vec<EventSignature> = signatures
                .iter()
                .map(|s| EventSignature::parse(s))
                .collect::<Result<_>>()?;

            let mut sql = sql.to_string();
            for sig in &sigs {
                sql = sig.normalize_table_references(&sql);
                sql = sig.rewrite_filters_for_pushdown(&sql);
            }

            let ctes: Vec<String> = sigs
                .iter()
                .map(|sig| sig.to_cte_sql_clickhouse())
                .collect();
            format!("WITH {} {sql}", ctes.join(", "))
        } else {
            sql.to_string()
        };

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
            self.database
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
                                row.get(col).cloned().unwrap_or(serde_json::Value::Null)
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

#[cfg(test)]
mod tests {
    use super::*;

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
            database: None,
        };

        let engine = ClickHouseEngine::new(&config, 4217).unwrap();
        assert_eq!(engine.instance_count(), 1);
        assert_eq!(engine.active_url(), "http://clickhouse-1:8123");
    }

    #[test]
    fn test_engine_multiple_instances() {
        let config = ClickHouseConfig {
            enabled: true,
            url: "http://clickhouse-1:8123".to_string(),
            failover_urls: vec!["http://clickhouse-2:8123".to_string()],
            database: None,
        };

        let engine = ClickHouseEngine::new(&config, 4217).unwrap();
        assert_eq!(engine.instance_count(), 2);
        assert_eq!(engine.active_url(), "http://clickhouse-1:8123");
    }

    #[test]
    fn test_engine_database_override() {
        let config = ClickHouseConfig {
            enabled: true,
            url: "http://clickhouse-1:8123".to_string(),
            failover_urls: vec![],
            database: Some("custom_db".to_string()),
        };

        let engine = ClickHouseEngine::new(&config, 4217).unwrap();
        assert_eq!(engine.database(), "custom_db");
    }

    #[test]
    fn test_engine_database_default() {
        let config = ClickHouseConfig {
            enabled: true,
            url: "http://clickhouse-1:8123".to_string(),
            failover_urls: vec![],
            database: None,
        };

        let engine = ClickHouseEngine::new(&config, 4217).unwrap();
        assert_eq!(engine.database(), "tidx_4217");
    }
}
