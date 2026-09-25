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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QueryFailureKind {
    Timeout,
    Unavailable,
    BadGateway,
    InvalidQuery,
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub(crate) struct QueryFailure {
    pub(crate) kind: QueryFailureKind,
    pub(crate) upstream_status: Option<reqwest::StatusCode>,
    message: String,
}

impl QueryFailure {
    pub(crate) fn new(kind: QueryFailureKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            upstream_status: None,
            message: message.into(),
        }
    }

    fn with_status(mut self, status: reqwest::StatusCode) -> Self {
        self.upstream_status = Some(status);
        self
    }
}

fn query_failure(kind: QueryFailureKind, message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(QueryFailure::new(kind, message))
}

/// ClickHouse reports SQL errors as HTTP 500, so its exception code takes
/// precedence over the HTTP status when deciding whether a retry can help.
fn classify_upstream_failure(status: reqwest::StatusCode, body: &str) -> QueryFailureKind {
    let code = body.trim_start().strip_prefix("Code: ").and_then(|rest| {
        let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
        rest.get(..digits)?.parse::<u32>().ok()
    });

    match code {
        // Execution, socket, query-slot, and memory-reservation timeouts.
        Some(159 | 209 | 1019 | 1020) => return QueryFailureKind::Timeout,
        // DNS errors, overloaded connections, and unavailable shards.
        Some(198 | 202 | 203 | 279 | 297 | 904) => return QueryFailureKind::Unavailable,
        // NETWORK_ERROR.
        Some(210) => return QueryFailureKind::BadGateway,
        _ => {}
    }

    if status == reqwest::StatusCode::REQUEST_TIMEOUT
        || status == reqwest::StatusCode::GATEWAY_TIMEOUT
    {
        return QueryFailureKind::Timeout;
    }
    if status == reqwest::StatusCode::SERVICE_UNAVAILABLE
        || status == reqwest::StatusCode::TOO_MANY_REQUESTS
    {
        return QueryFailureKind::Unavailable;
    }
    if status == reqwest::StatusCode::BAD_GATEWAY {
        return QueryFailureKind::BadGateway;
    }
    if code.is_some() || body.contains("DB::Exception") || body.contains("DB::NetException") {
        return QueryFailureKind::InvalidQuery;
    }
    if status.is_server_error()
        || status == reqwest::StatusCode::UNAUTHORIZED
        || status == reqwest::StatusCode::FORBIDDEN
    {
        return QueryFailureKind::BadGateway;
    }
    QueryFailureKind::InvalidQuery
}

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
    /// On instance-specific transport or availability failure, the engine
    /// tries the next instance. Query errors are returned immediately.
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
                        "ClickHouse instance failed, trying next"
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

        let resp_status = resp.status();
        if !resp_status.is_success() {
            let error_text = read_limited_response(resp).await?;
            return Err(anyhow::Error::new(
                QueryFailure::new(
                    classify_upstream_failure(resp_status, &error_text),
                    format!("ClickHouse query failed: {error_text}"),
                )
                .with_status(resp_status),
            ));
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

        let malformed = || {
            anyhow::Error::new(
                QueryFailure::new(
                    QueryFailureKind::BadGateway,
                    "Malformed ClickHouse JSON response",
                )
                .with_status(resp_status),
            )
        };
        let parsed: serde_json::Value =
            serde_json::from_str(&json_response).map_err(|_| malformed())?;
        let meta = parsed
            .get("meta")
            .and_then(|value| value.as_array())
            .ok_or_else(malformed)?;
        let data = parsed
            .get("data")
            .and_then(|value| value.as_array())
            .ok_or_else(malformed)?;
        let columns: Vec<String> = meta
            .iter()
            .map(|col| {
                col.get("name")
                    .and_then(|value| value.as_str())
                    .map(String::from)
                    .ok_or_else(malformed)
            })
            .collect::<Result<_>>()?;
        let column_types: Vec<String> = meta
            .iter()
            .map(|col| {
                col.get("type")
                    .and_then(|value| value.as_str())
                    .map(String::from)
                    .ok_or_else(malformed)
            })
            .collect::<Result<_>>()?;
        let rows: Vec<Vec<serde_json::Value>> = data
            .iter()
            .map(|row| {
                columns
                    .iter()
                    .map(|col| row.get(col).cloned().ok_or_else(malformed))
                    .collect::<Result<_>>()
            })
            .collect::<Result<_>>()?;

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
    query_failure(
        QueryFailureKind::Timeout,
        format!(
            "ClickHouse request timed out after {}ms",
            timeout.as_millis()
        ),
    )
}

/// Wrap a reqwest send failure, keeping the typed source so
/// [`is_connection_error`] can classify it precisely.
fn send_error(e: reqwest::Error, timeout: Option<std::time::Duration>) -> anyhow::Error {
    let (kind, msg) = if e.is_timeout() {
        (
            QueryFailureKind::Timeout,
            match timeout {
                Some(t) => format!("ClickHouse request timed out after {}ms", t.as_millis()),
                None => "ClickHouse request timed out".to_string(),
            },
        )
    } else if e.is_connect() {
        (
            QueryFailureKind::Unavailable,
            format!("ClickHouse HTTP request failed: {e}"),
        )
    } else {
        (
            QueryFailureKind::BadGateway,
            format!("ClickHouse HTTP request failed: {e}"),
        )
    };
    anyhow::Error::new(e).context(QueryFailure::new(kind, msg))
}

async fn read_limited_response(mut resp: reqwest::Response) -> Result<String> {
    let status = resp.status();
    let mut body = Vec::new();

    while let Some(chunk) = resp.chunk().await.map_err(|e| {
        let kind = if e.is_timeout() {
            QueryFailureKind::Timeout
        } else {
            QueryFailureKind::BadGateway
        };
        anyhow::Error::new(e).context(
            QueryFailure::new(kind, "Failed to read ClickHouse response").with_status(status),
        )
    })? {
        if body.len().saturating_add(chunk.len()) > MAX_QUERY_RESULT_BYTES {
            return Err(anyhow::Error::new(
                QueryFailure::new(
                    QueryFailureKind::InvalidQuery,
                    format!(
                        "ClickHouse response exceeded {} bytes",
                        MAX_QUERY_RESULT_BYTES
                    ),
                )
                .with_status(status),
            ));
        }
        body.extend_from_slice(&chunk);
    }

    String::from_utf8(body).map_err(|e| {
        anyhow::Error::new(
            QueryFailure::new(
                QueryFailureKind::BadGateway,
                format!("ClickHouse response was not valid UTF-8: {e}"),
            )
            .with_status(status),
        )
    })
}

/// Returns true for instance-specific transport or availability failures, but
/// not client timeouts or query errors that would recur on another instance.
pub(crate) fn is_connection_error(err: &anyhow::Error) -> bool {
    if let Some(failure) = err.downcast_ref::<QueryFailure>() {
        return matches!(
            failure.kind,
            QueryFailureKind::Unavailable | QueryFailureKind::BadGateway
        );
    }
    if let Some(e) = err.downcast_ref::<reqwest::Error>() {
        return e.is_connect() || !e.is_timeout();
    }
    let msg = err.to_string();
    msg.contains("connection refused")
        || msg.contains("Connection refused")
        || msg.contains("connect error")
        || msg.contains("dns error")
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn failure_kind(error: &anyhow::Error) -> Option<QueryFailureKind> {
        error
            .downcast_ref::<QueryFailure>()
            .map(|failure| failure.kind)
    }

    #[test]
    fn test_classify_upstream_failure() {
        use reqwest::StatusCode;

        let cases = [
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Code: 159. DB::Exception: Timeout exceeded (TIMEOUT_EXCEEDED)",
                QueryFailureKind::Timeout,
            ),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Code: 209. DB::Exception: Socket timeout (SOCKET_TIMEOUT)",
                QueryFailureKind::Timeout,
            ),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Code: 202. DB::Exception: Too many simultaneous queries",
                QueryFailureKind::Unavailable,
            ),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Code: 279. DB::Exception: All connection tries failed",
                QueryFailureKind::Unavailable,
            ),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Code: 1019. DB::Exception: Query slot acquisition timeout",
                QueryFailureKind::Timeout,
            ),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Code: 210. DB::NetException: Network error",
                QueryFailureKind::BadGateway,
            ),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Code: 62. DB::Exception: Syntax error",
                QueryFailureKind::InvalidQuery,
            ),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Code: 60. DB::Exception: Unknown table",
                QueryFailureKind::InvalidQuery,
            ),
            (
                StatusCode::BAD_GATEWAY,
                "upstream closed connection",
                QueryFailureKind::BadGateway,
            ),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "service unavailable",
                QueryFailureKind::Unavailable,
            ),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Code: 62. DB::Exception: Syntax error",
                QueryFailureKind::Unavailable,
            ),
            (
                StatusCode::GATEWAY_TIMEOUT,
                "gateway timeout",
                QueryFailureKind::Timeout,
            ),
            (
                StatusCode::REQUEST_TIMEOUT,
                "request timeout",
                QueryFailureKind::Timeout,
            ),
            (
                StatusCode::BAD_REQUEST,
                "Code: 62. DB::Exception: Syntax error",
                QueryFailureKind::InvalidQuery,
            ),
        ];

        for (status, body, kind) in cases {
            assert_eq!(
                classify_upstream_failure(status, body),
                kind,
                "{status}: {body}"
            );
        }
    }

    async fn read_request(stream: &mut tokio::net::TcpStream) {
        let mut request = Vec::new();
        let mut buf = [0; 1024];
        loop {
            let read = stream.read(&mut buf).await.unwrap();
            if read == 0 {
                return;
            }
            request.extend_from_slice(&buf[..read]);

            let Some(header_end) = request.windows(4).position(|w| w == b"\r\n\r\n") else {
                continue;
            };
            let headers = String::from_utf8_lossy(&request[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            if request.len() >= header_end + 4 + content_length {
                return;
            }
        }
    }

    async fn serve_once(listener: tokio::net::TcpListener, response: Vec<u8>) {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_request(&mut stream).await;
        stream.write_all(&response).await.unwrap();
        stream.shutdown().await.unwrap();
    }

    async fn query_user_error_response(status: u16, body: &str) -> anyhow::Error {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let response = format!(
            "HTTP/1.1 {status} Error\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let task = tokio::spawn(serve_once(listener, response.into_bytes()));
        let engine = ClickHouseEngine::new(
            &ClickHouseConfig {
                enabled: true,
                url,
                database: Some("default".to_string()),
                ..Default::default()
            },
            4217,
        )
        .unwrap();
        let error = engine
            .query_user("SELECT 1", &[], 1_000, 10)
            .await
            .expect_err("upstream error must fail the query");
        task.await.unwrap();
        error
    }

    #[tokio::test]
    async fn test_user_query_preserves_upstream_failure_kind() {
        let cases = [
            (502, "bad gateway", QueryFailureKind::BadGateway),
            (503, "unavailable", QueryFailureKind::Unavailable),
            (
                500,
                "Code: 159. DB::Exception: Timeout exceeded",
                QueryFailureKind::Timeout,
            ),
            (
                500,
                "Code: 62. DB::Exception: Syntax error",
                QueryFailureKind::InvalidQuery,
            ),
        ];

        for (status, body, kind) in cases {
            let error = query_user_error_response(status, body).await;
            assert_eq!(failure_kind(&error), Some(kind), "{status}: {body}");
            assert_eq!(
                error
                    .downcast_ref::<QueryFailure>()
                    .unwrap()
                    .upstream_status,
                Some(reqwest::StatusCode::from_u16(status).unwrap()),
            );
        }
    }

    #[tokio::test]
    async fn test_user_query_rejects_malformed_upstream_json() {
        for body in ["not json", "{}", r#"{"meta":"bad","data":[]}"#] {
            let error = query_user_error_response(200, body).await;
            assert_eq!(failure_kind(&error), Some(QueryFailureKind::BadGateway));
            assert_eq!(
                error
                    .downcast_ref::<QueryFailure>()
                    .unwrap()
                    .upstream_status,
                Some(reqwest::StatusCode::OK),
            );
        }
    }

    #[tokio::test]
    async fn test_user_query_preserves_status_on_truncated_response() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(serve_once(
            listener,
            b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 1024\r\nConnection: close\r\n\r\nshort"
                .to_vec(),
        ));
        let engine = ClickHouseEngine::new(
            &ClickHouseConfig {
                enabled: true,
                url,
                database: Some("default".to_string()),
                ..Default::default()
            },
            4217,
        )
        .unwrap();

        let error = engine
            .query_user("SELECT 1", &[], 1_000, 10)
            .await
            .expect_err("truncated upstream body must fail the query");
        assert_eq!(failure_kind(&error), Some(QueryFailureKind::BadGateway));
        assert_eq!(
            error
                .downcast_ref::<QueryFailure>()
                .unwrap()
                .upstream_status,
            Some(reqwest::StatusCode::BAD_GATEWAY),
        );
        task.await.unwrap();
    }

    #[tokio::test]
    async fn test_user_query_rejects_invalid_sql_without_dependency_failure() {
        let engine = ClickHouseEngine::new(
            &ClickHouseConfig {
                enabled: true,
                url: "http://127.0.0.1:1".to_string(),
                ..Default::default()
            },
            4217,
        )
        .unwrap();
        let error = engine
            .query_user("DELETE FROM blocks", &[], 1_000, 10)
            .await
            .expect_err("invalid SQL must be rejected locally");

        assert!(failure_kind(&error).is_none());
    }

    #[test]
    fn test_is_connection_error() {
        let conn_err = anyhow!("ClickHouse HTTP request failed: connection refused");
        assert!(is_connection_error(&conn_err));

        let query_err =
            anyhow!("ClickHouse query failed: Code: 60. DB::Exception: Table logs doesn't exist");
        assert!(!is_connection_error(&query_err));

        let timeout_err = timeout_error(std::time::Duration::from_secs(2));
        assert!(!is_connection_error(&timeout_err));
        assert_eq!(failure_kind(&timeout_err), Some(QueryFailureKind::Timeout));

        let query_err = query_failure(QueryFailureKind::InvalidQuery, "connection refused in SQL");
        assert!(!is_connection_error(&query_err));
    }

    #[tokio::test]
    async fn test_connect_failure_is_connection_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let e = reqwest::Client::new()
            .post(format!("http://{addr}/"))
            .timeout(std::time::Duration::from_secs(1))
            .send()
            .await
            .expect_err("connect must fail");
        let err = send_error(e, None);
        assert!(is_connection_error(&err), "got: {err:#}");
        assert_eq!(failure_kind(&err), Some(QueryFailureKind::Unavailable));
    }

    #[tokio::test]
    async fn test_reset_connection_is_connection_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            drop(stream);
        });

        let e = reqwest::Client::new()
            .post(format!("http://{addr}/"))
            .body("SELECT 1")
            .send()
            .await
            .expect_err("send must fail");
        let err = send_error(e, None);
        assert!(is_connection_error(&err), "got: {err:#}");
        assert_eq!(failure_kind(&err), Some(QueryFailureKind::BadGateway));
    }

    #[tokio::test]
    async fn test_malformed_response_fails_over() {
        let primary = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let primary_url = format!("http://{}", primary.local_addr().unwrap());
        let primary_task = tokio::spawn(serve_once(primary, b"HTTP/1.1 nope\r\n\r\n".to_vec()));

        let secondary = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let secondary_url = format!("http://{}", secondary.local_addr().unwrap());
        let body = r#"{"meta":[{"name":"n","type":"UInt8"}],"data":[{"n":1}],"rows":1}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let secondary_task = tokio::spawn(serve_once(secondary, response.into_bytes()));

        let config = ClickHouseConfig {
            enabled: true,
            url: primary_url,
            failover_urls: vec![secondary_url.clone()],
            database: Some("default".to_string()),
            ..Default::default()
        };
        let engine = ClickHouseEngine::new(&config, 4217).unwrap();
        let result = engine.query("SELECT 1 AS n", &[]).await.unwrap();

        assert_eq!(result.rows, vec![vec![serde_json::json!(1)]]);
        assert_eq!(engine.active_url(), secondary_url);
        primary_task.await.unwrap();
        secondary_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_service_unavailable_fails_over() {
        let primary = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let primary_url = format!("http://{}", primary.local_addr().unwrap());
        let primary_task = tokio::spawn(serve_once(
            primary,
            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                .to_vec(),
        ));

        let secondary = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let secondary_url = format!("http://{}", secondary.local_addr().unwrap());
        let body = r#"{"meta":[{"name":"n","type":"UInt8"}],"data":[{"n":1}],"rows":1}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let secondary_task = tokio::spawn(serve_once(secondary, response.into_bytes()));
        let engine = ClickHouseEngine::new(
            &ClickHouseConfig {
                enabled: true,
                url: primary_url,
                failover_urls: vec![secondary_url.clone()],
                database: Some("default".to_string()),
                ..Default::default()
            },
            4217,
        )
        .unwrap();

        let result = engine
            .query_user("SELECT 1 AS n", &[], 1_000, 10)
            .await
            .unwrap();
        assert_eq!(result.rows, vec![vec![serde_json::json!(1)]]);
        assert_eq!(engine.active_url(), secondary_url);
        primary_task.await.unwrap();
        secondary_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_truncated_response_body_fails_over() {
        let primary = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let primary_url = format!("http://{}", primary.local_addr().unwrap());
        let primary_task = tokio::spawn(async move {
            let (mut stream, _) = primary.accept().await.unwrap();
            read_request(&mut stream).await;
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                      Content-Length: 1024\r\nConnection: close\r\n\r\n{\"meta\":",
                )
                .await
                .unwrap();
            stream.shutdown().await.unwrap();
        });

        let secondary = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let secondary_url = format!("http://{}", secondary.local_addr().unwrap());
        let body = r#"{"meta":[{"name":"n","type":"UInt8"}],"data":[{"n":1}],"rows":1}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let secondary_task = tokio::spawn(serve_once(secondary, response.into_bytes()));

        let config = ClickHouseConfig {
            enabled: true,
            url: primary_url,
            failover_urls: vec![secondary_url.clone()],
            database: Some("default".to_string()),
            ..Default::default()
        };
        let engine = ClickHouseEngine::new(&config, 4217).unwrap();
        let result = engine.query("SELECT 1 AS n", &[]).await.unwrap();

        assert_eq!(result.rows, vec![vec![serde_json::json!(1)]]);
        assert_eq!(engine.active_url(), secondary_url);
        primary_task.await.unwrap();
        secondary_task.await.unwrap();
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
        assert_eq!(failure_kind(&err), Some(QueryFailureKind::Timeout));
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
