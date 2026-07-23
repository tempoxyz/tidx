//! ClickHouse OLAP engine for analytical queries.
//!
//! Reads from tables populated by the direct-write ClickHouseSink.
//! Provides vectorized columnar execution for OLAP queries.
//!
//! Supports multiple ClickHouse instances per chain with failover:
//! queries go to the primary instance and automatically fail over
//! to secondary instances if the primary is unavailable.

use anyhow::{Result, anyhow};
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{error, warn};

use crate::config::ClickHouseConfig;
use crate::query::{
    HARD_LIMIT_MAX, apply_event_signature_ctes_clickhouse, convert_timestamp_literals_clickhouse,
    hoist_set_operation_order_by_clickhouse, validate_clickhouse_query,
};

const MAX_QUERY_RESULT_BYTES: usize = 10 * 1024 * 1024;

/// A single ClickHouse instance (connection + URL).
struct Instance {
    http_client: reqwest::Client,
    url: String,
    user: Option<String>,
    password: Option<String>,
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

        let password = config.resolved_password()?;
        let mut instances = Vec::new();
        for url in config.all_urls() {
            instances.push(Self::make_instance(
                url,
                config.user.clone(),
                password.clone(),
            )?);
        }

        Ok(Self {
            instances,
            active: AtomicUsize::new(0),
            database,
        })
    }

    fn make_instance(
        url: &str,
        user: Option<String>,
        password: Option<String>,
    ) -> Result<Instance> {
        let http_client = reqwest::Client::builder()
            .pool_max_idle_per_host(4)
            .build()
            .map_err(|e| anyhow!("Failed to create HTTP client: {e}"))?;
        Ok(Instance {
            http_client,
            url: url.to_string(),
            user,
            password,
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
    pub async fn query(&self, sql: &str, signatures: &[&str]) -> Result<QueryResult> {
        let sql = Self::prepare_query(sql, signatures)?;
        self.execute_prepared_query(&sql, None).await
    }

    /// Execute a public user query after applying signature rewrites, SQL
    /// validation, and caller-provided timeout limits.
    pub async fn query_user(
        &self,
        sql: &str,
        signatures: &[&str],
        timeout_ms: u64,
        limit: i64,
    ) -> Result<QueryResult> {
        self.query_user_with_settings(sql, signatures, timeout_ms, limit, &[])
            .await
    }

    /// [`Self::query_user`] with extra ClickHouse settings appended to the
    /// request URL (e.g. output formatting for the tiered cold arm). Keys and
    /// values must be URL-safe tokens.
    pub async fn query_user_with_settings(
        &self,
        sql: &str,
        signatures: &[&str],
        timeout_ms: u64,
        limit: i64,
        settings: &[(&str, &str)],
    ) -> Result<QueryResult> {
        let sql = Self::prepare_query(sql, signatures)?;
        validate_clickhouse_query(&sql)?;
        let sql = hoist_set_operation_order_by_clickhouse(&sql);
        let sql = Self::wrap_user_query_with_limit(&sql, limit.clamp(1, HARD_LIMIT_MAX));
        self.execute_prepared_query_with_settings(&sql, Some(timeout_ms), settings)
            .await
    }

    pub async fn query_with_timeout(
        &self,
        sql: &str,
        signatures: &[&str],
        timeout_ms: u64,
    ) -> Result<QueryResult> {
        let sql = Self::prepare_query(sql, signatures)?;
        self.execute_prepared_query(&sql, Some(timeout_ms)).await
    }

    fn prepare_query(sql: &str, signatures: &[&str]) -> Result<String> {
        let sql = convert_timestamp_literals_clickhouse(sql);
        apply_event_signature_ctes_clickhouse(&sql, signatures)
    }

    fn wrap_user_query_with_limit(sql: &str, limit: i64) -> String {
        // Always apply an outer LIMIT so the public API's row cap is enforced
        // even when the inner query omits LIMIT or requests a larger result set.
        format!("SELECT * FROM ({sql}) AS tidx_query LIMIT {limit}")
    }

    async fn execute_prepared_query(
        &self,
        sql: &str,
        timeout_ms: Option<u64>,
    ) -> Result<QueryResult> {
        self.execute_prepared_query_with_settings(sql, timeout_ms, &[])
            .await
    }

    async fn execute_prepared_query_with_settings(
        &self,
        sql: &str,
        timeout_ms: Option<u64>,
        settings: &[(&str, &str)],
    ) -> Result<QueryResult> {
        let start = std::time::Instant::now();
        let n = self.instances.len();
        let starting = self.active.load(Ordering::Relaxed);

        for attempt in 0..n {
            let idx = (starting + attempt) % n;
            let inst = &self.instances[idx];

            match self.try_query(inst, sql, start, timeout_ms, settings).await {
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

    fn query_url(
        &self,
        inst: &Instance,
        timeout_ms: Option<u64>,
        settings: &[(&str, &str)],
    ) -> String {
        // union_default_mode: bare UNION behaves as UNION DISTINCT
        // (PostgreSQL semantics); ClickHouse otherwise rejects it.
        let mut url = format!(
            "{}/?database={}&default_format=JSON&max_result_bytes={}&result_overflow_mode=throw&union_default_mode=DISTINCT",
            inst.url.trim_end_matches('/'),
            self.database,
            MAX_QUERY_RESULT_BYTES
        );
        if let Some(timeout_ms) = timeout_ms {
            let max_execution_time = timeout_ms.div_ceil(1000).max(1);
            url.push_str(&format!("&max_execution_time={max_execution_time}"));
        }
        for (key, value) in settings {
            url.push_str(&format!("&{key}={value}"));
        }
        url
    }

    async fn try_query(
        &self,
        inst: &Instance,
        sql: &str,
        start: std::time::Instant,
        timeout_ms: Option<u64>,
        settings: &[(&str, &str)],
    ) -> Result<QueryResult> {
        let url = self.query_url(inst, timeout_ms, settings);

        let request_timeout = timeout_ms.map(clickhouse_request_timeout);
        let mut req = inst.http_client.post(&url).body(sql.to_string());
        if let Some(timeout) = request_timeout {
            req = req.timeout(timeout);
        }
        if let Some(ref user) = inst.user {
            req = req.header("X-ClickHouse-User", user);
        }
        if let Some(ref password) = inst.password {
            req = req.header("X-ClickHouse-Key", password);
        }
        let send = req.send();
        let resp = if let Some(timeout) = request_timeout {
            tokio::time::timeout(timeout, send)
                .await
                .map_err(|_| timeout_error(timeout))?
                .map_err(|e| send_error(e, Some(timeout)))?
        } else {
            send.await.map_err(|e| send_error(e, None))?
        };

        if !resp.status().is_success() {
            let error_text = read_limited_response(resp).await.unwrap_or_default();
            return Err(anyhow!("ClickHouse query failed: {error_text}"));
        }

        let json_response = read_limited_response(resp).await?;

        if json_response.trim().is_empty() {
            return Ok(QueryResult {
                columns: vec![],
                column_types: vec![],
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

        let column_types: Vec<String> = meta
            .map(|m| {
                m.iter()
                    .map(|col| {
                        col.get("type")
                            .and_then(|t| t.as_str())
                            .unwrap_or_default()
                            .to_string()
                    })
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
            column_types,
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

fn clickhouse_request_timeout(timeout_ms: u64) -> std::time::Duration {
    // Wide margin past the server's max_execution_time so its precise
    // TIMEOUT_EXCEEDED error normally arrives before the client deadline.
    std::time::Duration::from_millis(
        timeout_ms
            .div_ceil(1000)
            .max(1)
            .saturating_mul(1000)
            .saturating_add(1000),
    )
}

/// Error for a client-side deadline expiry: a slow query, not an
/// unreachable instance.
fn timeout_error(timeout: std::time::Duration) -> anyhow::Error {
    anyhow!(
        "ClickHouse request timed out after {}ms",
        timeout.as_millis()
    )
}

/// Wrap a reqwest send failure, keeping the typed source so
/// [`is_connection_error`] can classify it precisely.
fn send_error(e: reqwest::Error, timeout: Option<std::time::Duration>) -> anyhow::Error {
    let msg = if e.is_timeout() {
        match timeout {
            Some(t) => format!("ClickHouse request timed out after {}ms", t.as_millis()),
            None => "ClickHouse request timed out".to_string(),
        }
    } else {
        format!("ClickHouse HTTP request failed: {e}")
    };
    anyhow::Error::new(e).context(msg)
}

async fn read_limited_response(mut resp: reqwest::Response) -> Result<String> {
    let mut body = Vec::new();

    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| anyhow!("Failed to read response: {e}"))?
    {
        if body.len().saturating_add(chunk.len()) > MAX_QUERY_RESULT_BYTES {
            return Err(anyhow!(
                "ClickHouse response exceeded {} bytes",
                MAX_QUERY_RESULT_BYTES
            ));
        }
        body.extend_from_slice(&chunk);
    }

    String::from_utf8(body).map_err(|e| anyhow!("ClickHouse response was not valid UTF-8: {e}"))
}

/// Returns true for errors that indicate the ClickHouse instance is
/// unreachable or unhealthy (connection refused, DNS failure, connection
/// reset or closed before a response), as opposed to client timeouts or
/// query-level errors that would happen on any instance.
pub(crate) fn is_connection_error(err: &anyhow::Error) -> bool {
    if let Some(e) = err.downcast_ref::<reqwest::Error>() {
        if e.is_connect() {
            return true;
        }
        return !e.is_timeout() && source_is_connection_reset(e);
    }
    let msg = err.to_string();
    msg.contains("connection refused")
        || msg.contains("Connection refused")
        || msg.contains("connect error")
        || msg.contains("dns error")
}

/// Walks the source chain for an instance that accepted TCP but reset or
/// closed the connection before responding.
fn source_is_connection_reset(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut source = err.source();
    while let Some(e) = source {
        if let Some(io) = e.downcast_ref::<std::io::Error>()
            && matches!(
                io.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
            )
        {
            return true;
        }
        if let Some(h) = e.downcast_ref::<hyper::Error>()
            && h.is_incomplete_message()
        {
            return true;
        }
        source = e.source();
    }
    false
}

/// Query result from ClickHouse.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    /// ClickHouse type per column (JSON `meta`), e.g. `Int64`, `Nullable(String)`.
    pub column_types: Vec<String>,
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

        let query_err =
            anyhow!("ClickHouse query failed: Code: 60. DB::Exception: Table logs doesn't exist");
        assert!(!is_connection_error(&query_err));

        let timeout_err = timeout_error(std::time::Duration::from_secs(2));
        assert!(!is_connection_error(&timeout_err));
    }

    #[tokio::test]
    async fn test_connect_failure_is_connection_error() {
        // Port 9 (discard) is closed locally; a real refused connection.
        let e = reqwest::Client::new()
            .post("http://127.0.0.1:9/")
            .send()
            .await
            .expect_err("connect must fail");
        let err = send_error(e, None);
        assert!(is_connection_error(&err), "got: {err:#}");
    }

    #[tokio::test]
    async fn test_reset_connection_is_connection_error() {
        // An instance that accepts TCP then closes before responding must
        // still classify as a connection error so failover triggers.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                drop(stream);
            }
        });

        let e = reqwest::Client::new()
            .post(format!("http://{addr}/"))
            .body("SELECT 1")
            .send()
            .await
            .expect_err("send must fail");
        let err = send_error(e, None);
        assert!(is_connection_error(&err), "got: {err:#}");
    }

    #[test]
    fn test_io_reset_in_source_chain_is_connection_reset() {
        for kind in [
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::ConnectionAborted,
            std::io::ErrorKind::BrokenPipe,
        ] {
            let err = anyhow::Error::new(std::io::Error::from(kind))
                .context("ClickHouse HTTP request failed");
            assert!(source_is_connection_reset(err.as_ref()), "kind: {kind:?}");
        }

        let err = anyhow::Error::new(std::io::Error::from(std::io::ErrorKind::TimedOut))
            .context("ClickHouse HTTP request failed");
        assert!(!source_is_connection_reset(err.as_ref()));
    }

    #[test]
    fn test_clickhouse_request_timeout_exceeds_server_timeout() {
        assert_eq!(
            clickhouse_request_timeout(1_001),
            std::time::Duration::from_secs(3)
        );
        assert_eq!(
            clickhouse_request_timeout(100),
            std::time::Duration::from_secs(2)
        );
    }

    #[test]
    fn test_engine_single_instance() {
        let config = ClickHouseConfig {
            enabled: true,
            url: "http://clickhouse-1:8123".to_string(),
            failover_urls: vec![],
            database: None,
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
        };

        let engine = ClickHouseEngine::new(&config, 4217).unwrap();
        assert_eq!(engine.database(), "tidx_4217");
    }

    #[test]
    fn test_prepare_query_merges_signature_cte_with_user_cte() {
        let sql = ClickHouseEngine::prepare_query(
            r#"WITH recent AS (SELECT * FROM transfer WHERE block_num > 10) SELECT * FROM recent"#,
            &["Transfer(address indexed from, address indexed to, uint256 value)"],
        )
        .unwrap();

        assert!(sql.starts_with("WITH Transfer AS ("));
        assert!(sql.contains("), recent AS ("));
        assert_eq!(sql.matches("WITH ").count(), 1);
        assert!(validate_clickhouse_query(&sql).is_ok(), "got: {sql}");
    }

    #[test]
    fn test_prepare_query_rejects_signature_cte_collision() {
        let err = ClickHouseEngine::prepare_query(
            "WITH Transfer AS (SELECT * FROM logs) SELECT * FROM Transfer",
            &["Transfer(address indexed from, address indexed to, uint256 value)"],
        )
        .unwrap_err();

        assert!(err.to_string().contains("conflicts"));
    }

    #[test]
    fn test_internal_query_url_omits_timeout() {
        let config = ClickHouseConfig {
            enabled: true,
            url: "http://clickhouse-1:8123".to_string(),
            failover_urls: vec![],
            database: None,
            ..Default::default()
        };

        let engine = ClickHouseEngine::new(&config, 4217).unwrap();
        let url = engine.query_url(&engine.instances[0], None, &[]);

        assert_eq!(
            url,
            "http://clickhouse-1:8123/?database=tidx_4217&default_format=JSON&max_result_bytes=10485760&result_overflow_mode=throw&union_default_mode=DISTINCT"
        );
        assert!(!url.contains("max_execution_time"));
    }

    #[test]
    fn test_user_query_url_sets_ceiled_timeout_seconds() {
        let config = ClickHouseConfig {
            enabled: true,
            url: "http://clickhouse-1:8123".to_string(),
            failover_urls: vec![],
            database: None,
            ..Default::default()
        };

        let engine = ClickHouseEngine::new(&config, 4217).unwrap();
        let url = engine.query_url(&engine.instances[0], Some(1_001), &[]);

        assert_eq!(
            url,
            "http://clickhouse-1:8123/?database=tidx_4217&default_format=JSON&max_result_bytes=10485760&result_overflow_mode=throw&union_default_mode=DISTINCT&max_execution_time=2"
        );
    }

    #[tokio::test]
    async fn test_client_timeout_is_not_a_connection_error() {
        let url =
            std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());
        let config = ClickHouseConfig {
            enabled: true,
            url,
            failover_urls: vec![],
            database: Some("default".to_string()),
            ..Default::default()
        };
        let engine = ClickHouseEngine::new(&config, 4217).unwrap();

        // Skip when ClickHouse is unavailable (e.g. CI's unit-test job runs
        // without services); `make test` boots ClickHouse and runs the full
        // suite, so integration runs still exercise this.
        if engine.query("SELECT 1", &[]).await.is_err() {
            println!("ClickHouse not available, skipping test");
            return;
        }

        // The trailing setting overrides the server timeout to 30s, so the
        // 2s client deadline reliably fires first (as when the server-side
        // check overshoots in production).
        let err = engine
            .execute_prepared_query_with_settings(
                "SELECT sleep(3)",
                Some(1_000),
                &[("max_execution_time", "30")],
            )
            .await
            .expect_err("query must exceed the client deadline");

        // A slow query is a timeout, not an unreachable instance; classifying
        // it as a connection error burns failover retries.
        assert!(
            !is_connection_error(&err),
            "client timeout misclassified as connection error: {err}"
        );
    }

    #[test]
    fn test_wrap_user_query_with_limit_caps_public_results() {
        let sql = ClickHouseEngine::wrap_user_query_with_limit(
            "SELECT num, hash FROM blocks ORDER BY num DESC LIMIT 5000",
            100,
        );

        assert_eq!(
            sql,
            "SELECT * FROM (SELECT num, hash FROM blocks ORDER BY num DESC LIMIT 5000) AS tidx_query LIMIT 100"
        );
    }
}
