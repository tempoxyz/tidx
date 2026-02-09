mod rate_limit;

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use tokio::sync::RwLock;

use axum::{
    extract::{ConnectInfo, Query, State},
    http::{header, Method, StatusCode},
    middleware,
    response::{
        sse::{Event as SseEvent, KeepAlive, KeepAliveStream},
        IntoResponse, Response, Sse,
    },
    routing::get,
    Json, Router,
};
use futures::Stream;
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::broadcast::Broadcaster;
use crate::config::{HttpConfig, SharedHttpConfig};
use crate::db::Pool;
use crate::service::{QueryOptions, QueryResult, SyncStatus};

pub use rate_limit::{RateLimiter, SseConnectionGuard};

pub type SharedPools = Arc<RwLock<HashMap<u64, Pool>>>;

#[derive(Clone)]
pub struct AppState {
    /// Map of chain_id -> pool (hot-reloadable)
    pub pools: SharedPools,
    /// Default chain_id (first chain)
    pub default_chain_id: u64,
    pub broadcaster: Arc<Broadcaster>,
    /// Rate limiter for request throttling
    pub rate_limiter: RateLimiter,
    /// Parsed trusted CIDRs for admin operations
    pub trusted_cidrs: Arc<Vec<(IpAddr, u8)>>,
}

impl AppState {
    async fn get_pool(&self, chain_id: Option<u64>) -> Option<Pool> {
        let id = chain_id.unwrap_or(self.default_chain_id);
        self.pools.read().await.get(&id).cloned()
    }

    /// Check if an IP address is in the trusted CIDRs
    pub fn is_trusted_ip(&self, addr: &SocketAddr) -> bool {
        if self.trusted_cidrs.is_empty() {
            return true;
        }
        let ip = addr.ip();
        self.trusted_cidrs.iter().any(|(network, prefix)| ip_in_cidr(&ip, network, *prefix))
    }
}

/// Parse CIDR strings into (network, prefix_len) tuples
pub fn parse_cidrs(cidrs: &[String]) -> Vec<(IpAddr, u8)> {
    cidrs
        .iter()
        .filter_map(|cidr| {
            let parts: Vec<&str> = cidr.split('/').collect();
            if parts.len() != 2 {
                return None;
            }
            let ip: IpAddr = parts[0].parse().ok()?;
            let prefix: u8 = parts[1].parse().ok()?;
            Some((ip, prefix))
        })
        .collect()
}

/// Check if an IP is within a CIDR range
fn ip_in_cidr(ip: &IpAddr, network: &IpAddr, prefix_len: u8) -> bool {
    match (ip, network) {
        (IpAddr::V4(ip), IpAddr::V4(net)) => {
            if prefix_len > 32 {
                return false;
            }
            let mask = if prefix_len == 0 { 0 } else { u32::MAX << (32 - prefix_len) };
            (u32::from(*ip) & mask) == (u32::from(*net) & mask)
        }
        (IpAddr::V6(ip), IpAddr::V6(net)) => {
            if prefix_len > 128 {
                return false;
            }
            let ip_bits = u128::from(*ip);
            let net_bits = u128::from(*net);
            let mask = if prefix_len == 0 { 0 } else { u128::MAX << (128 - prefix_len) };
            (ip_bits & mask) == (net_bits & mask)
        }
        _ => false,
    }
}

pub fn router(pools: HashMap<u64, Pool>, default_chain_id: u64, broadcaster: Arc<Broadcaster>) -> Router<()> {
    router_with_options(pools, default_chain_id, broadcaster, &HttpConfig::default())
}

pub fn router_with_options(
    pools: HashMap<u64, Pool>,
    default_chain_id: u64,
    broadcaster: Arc<Broadcaster>,
    http_config: &HttpConfig,
) -> Router<()> {
    let rate_limiter = RateLimiter::new(
        http_config.rate_limit.clone(),
        http_config.api_keys.clone(),
    );

    rate_limit::spawn_cleanup_task(rate_limiter.clone());

    let trusted_cidrs = Arc::new(parse_cidrs(&http_config.trusted_cidrs));

    let state = AppState {
        pools: Arc::new(RwLock::new(pools)),
        default_chain_id,
        broadcaster,
        rate_limiter: rate_limiter.clone(),
        trusted_cidrs,
    };

    build_router(state, rate_limiter)
}

pub fn router_shared(
    pools: SharedPools,
    default_chain_id: u64,
    broadcaster: Arc<Broadcaster>,
    http_config: SharedHttpConfig,
    trusted_cidrs: Vec<String>,
) -> Router<()> {
    let rate_limiter = RateLimiter::new_shared(http_config);
    let trusted_cidrs = Arc::new(parse_cidrs(&trusted_cidrs));

    rate_limit::spawn_cleanup_task(rate_limiter.clone());

    let state = AppState {
        pools,
        default_chain_id,
        broadcaster,
        rate_limiter: rate_limiter.clone(),
        trusted_cidrs,
    };

    build_router(state, rate_limiter)
}

fn build_router(state: AppState, rate_limiter: RateLimiter) -> Router<()> {
    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::OPTIONS])
        .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION])
        .allow_origin(tower_http::cors::Any);

    Router::new()
        .route("/health", get(handle_health))
        .route("/status", get(handle_status))
        .route("/query", get(handle_query))
        .layer(middleware::from_fn_with_state(
            rate_limiter,
            rate_limit::rate_limit_middleware,
        ))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn handle_health() -> &'static str {
    "OK"
}

#[derive(Serialize)]
struct StatusResponse {
    ok: bool,
    chains: Vec<SyncStatus>,
}

async fn handle_status(State(state): State<AppState>) -> Result<Json<StatusResponse>, ApiError> {
    let mut all_chains = Vec::new();
    let pools = state.pools.read().await;
    for pool in pools.values() {
        if let Ok(chains) = crate::service::get_all_status(pool).await {
            all_chains.extend(chains);
        }
    }

    Ok(Json(StatusResponse {
        ok: true,
        chains: all_chains,
    }))
}

#[derive(Deserialize)]
pub struct QueryParams {
    /// SQL query (SELECT only)
    sql: String,
    /// Event signature to create a CTE
    #[serde(default)]
    signature: Option<String>,
    /// Chain ID to query (required)
    #[serde(alias = "chain_id")]
    #[serde(rename = "chainId")]
    chain_id: u64,
    /// Enable live streaming mode (SSE) - streams updates on new blocks
    #[serde(default)]
    live: bool,
    /// Query timeout in milliseconds
    #[serde(default = "default_timeout")]
    timeout_ms: u64,
    /// Maximum rows to return
    #[serde(default = "default_limit")]
    limit: i64,
}

fn default_timeout() -> u64 {
    5000
}
fn default_limit() -> i64 {
    10000
}

#[derive(Serialize)]
struct QueryResponse {
    #[serde(flatten)]
    result: QueryResult,
    ok: bool,
}

async fn handle_query(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: axum::http::HeaderMap,
    Query(params): Query<QueryParams>,
) -> Response {
    if params.live {
        handle_query_live(state, params, addr, headers).await.into_response()
    } else {
        handle_query_once(state, params).await.into_response()
    }
}

async fn handle_query_once(
    state: AppState,
    params: QueryParams,
) -> Result<Json<QueryResponse>, ApiError> {
    let pool = state
        .get_pool(Some(params.chain_id))
        .await
        .ok_or_else(|| ApiError::BadRequest(format!(
            "Unknown chain_id: {}",
            params.chain_id,
        )))?;

    let options = QueryOptions {
        timeout_ms: params.timeout_ms.clamp(100, 30000),
        limit: params.limit.clamp(1, 100000),
    };

    let result = crate::service::execute_query_postgres(&pool, &params.sql, params.signature.as_deref(), &options)
        .await
        .map_err(|e| {
            if e.to_string().contains("timeout") {
                ApiError::Timeout
            } else {
                ApiError::QueryError(e.to_string())
            }
        })?;

    Ok(Json(QueryResponse { result, ok: true }))
}

type SseStream = std::pin::Pin<Box<dyn Stream<Item = Result<SseEvent, Infallible>> + Send>>;

/// Maximum blocks to catch up in a single update (prevents query multiplication attack)
const MAX_CATCHUP_BLOCKS: u64 = 10;

async fn handle_query_live(
    state: AppState,
    params: QueryParams,
    addr: SocketAddr,
    headers: axum::http::HeaderMap,
) -> Sse<KeepAliveStream<SseStream>> {
    let ip = rate_limit::extract_client_ip(&headers, Some(&addr));
    let has_api_key = state.rate_limiter.has_valid_api_key(&headers).await;

    let connection_guard = if has_api_key {
        Some(SseConnectionGuard::new_unlimited())
    } else {
        match state.rate_limiter.acquire_sse_connection(ip).await {
            Ok(guard) => Some(guard),
            Err(()) => {
                let stream: SseStream = Box::pin(async_stream::stream! {
                    yield Ok(SseEvent::default()
                        .event("error")
                        .json_data(serde_json::json!({
                            "ok": false,
                            "error": "Too many SSE connections from this IP"
                        }))
                        .unwrap());
                });
                return Sse::new(stream).keep_alive(KeepAlive::default());
            }
        }
    };

    let pool = match state.get_pool(Some(params.chain_id)).await {
        Some(p) => p,
        None => {
            let stream: SseStream = Box::pin(async_stream::stream! {
                yield Ok(SseEvent::default()
                    .event("error")
                    .json_data(serde_json::json!({ "ok": false, "error": "Unknown chain_id" }))
                    .unwrap());
            });
            return Sse::new(stream).keep_alive(KeepAlive::default());
        }
    };

    let mut rx = state.broadcaster.subscribe();
    let sql = params.sql;
    let signature = params.signature;
    let options = QueryOptions {
        timeout_ms: params.timeout_ms.clamp(100, 30000),
        limit: params.limit.clamp(1, 100000),
    };

    // Detect if this is an OLAP query (aggregations, etc.)
    let is_olap = crate::query::route_query(&sql) == crate::query::QueryEngine::Olap;

    let stream = async_stream::stream! {
        // Keep guard alive for the lifetime of the stream
        let _guard = connection_guard;
        let mut last_block_num: u64 = 0;

        // Execute initial query (live streaming uses Postgres for realtime data)
        match crate::service::execute_query_postgres(&pool, &sql, signature.as_deref(), &options).await {
            Ok(result) => {
                yield Ok(SseEvent::default()
                    .event("result")
                    .json_data(QueryResponse { result, ok: true })
                    .unwrap());
            }
            Err(e) => {
                yield Ok(SseEvent::default()
                    .event("error")
                    .json_data(serde_json::json!({ "ok": false, "error": e.to_string() }))
                    .unwrap());
                return;
            }
        }

        // Get current head block
        if let Ok(statuses) = crate::service::get_all_status(&pool).await {
            if let Some(s) = statuses.first() {
                last_block_num = s.synced_num as u64;
            }
        }

        // Stream updates on each new block
        loop {
            match rx.recv().await {
                Ok(update) => {
                    if update.block_num <= last_block_num {
                        continue;
                    }

                    let start = last_block_num + 1;
                    let end = update.block_num;

                    // Limit catch-up to prevent query multiplication DoS
                    let blocks_behind = end - start + 1;
                    if blocks_behind > MAX_CATCHUP_BLOCKS {
                        yield Ok(SseEvent::default()
                            .event("lagged")
                            .json_data(serde_json::json!({
                                "skipped": blocks_behind - MAX_CATCHUP_BLOCKS,
                                "reason": "catch-up limit exceeded"
                            }))
                            .unwrap());
                        last_block_num = end - MAX_CATCHUP_BLOCKS;
                    }

                    // For OLAP queries, re-execute the full query once per update (not per-block)
                    // For OLTP queries, filter by each block
                    if is_olap {
                        match crate::service::execute_query_postgres(&pool, &sql, signature.as_deref(), &options).await {
                            Ok(result) => {
                                yield Ok(SseEvent::default()
                                    .event("result")
                                    .json_data(QueryResponse { result, ok: true })
                                    .unwrap());
                            }
                            Err(e) => {
                                yield Ok(SseEvent::default()
                                    .event("error")
                                    .json_data(serde_json::json!({ "ok": false, "error": e.to_string() }))
                                    .unwrap());
                            }
                        }
                        last_block_num = end;
                    } else {
                        let catch_up_start = last_block_num + 1;
                        for block_num in catch_up_start..=end {
                            let filtered_sql = inject_block_filter(&sql, block_num);
                            match crate::service::execute_query_postgres(&pool, &filtered_sql, signature.as_deref(), &options).await {
                                Ok(result) => {
                                    yield Ok(SseEvent::default()
                                        .event("result")
                                        .json_data(QueryResponse { result, ok: true })
                                        .unwrap());
                                }
                                Err(e) => {
                                    yield Ok(SseEvent::default()
                                        .event("error")
                                        .json_data(serde_json::json!({ "ok": false, "error": e.to_string() }))
                                        .unwrap());
                                }
                            }
                        }
                        last_block_num = end;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    yield Ok(SseEvent::default()
                        .event("lagged")
                        .json_data(serde_json::json!({ "skipped": n }))
                        .unwrap());
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    break;
                }
            }
        }
    };

    let stream: SseStream = Box::pin(stream);
    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Inject a block number filter into SQL query for live streaming.
/// Transforms queries to only return data for the specific block.
/// Uses 'num' for blocks table, 'block_num' for txs/logs tables.
#[doc(hidden)]
pub fn inject_block_filter(sql: &str, block_num: u64) -> String {
    let sql_upper = sql.to_uppercase();
    
    // Determine column name based on table being queried
    let col = if sql_upper.contains("FROM BLOCKS") || sql_upper.contains("FROM \"BLOCKS\"") {
        "num"
    } else {
        "block_num"
    };
    
    // Find WHERE clause position
    if let Some(where_pos) = sql_upper.find("WHERE") {
        // Insert after WHERE
        let insert_pos = where_pos + 5;
        format!(
            "{} {} = {} AND {}",
            &sql[..insert_pos],
            col,
            block_num,
            &sql[insert_pos..]
        )
    } else if let Some(order_pos) = sql_upper.find("ORDER BY") {
        // Insert WHERE before ORDER BY
        format!(
            "{} WHERE {} = {} {}",
            &sql[..order_pos],
            col,
            block_num,
            &sql[order_pos..]
        )
    } else if let Some(limit_pos) = sql_upper.find("LIMIT") {
        // Insert WHERE before LIMIT
        format!(
            "{} WHERE {} = {} {}",
            &sql[..limit_pos],
            col,
            block_num,
            &sql[limit_pos..]
        )
    } else {
        // Append WHERE at end
        format!("{sql} WHERE {col} = {block_num}")
    }
}

#[derive(Debug)]
pub enum ApiError {
    BadRequest(String),
    Timeout,
    QueryError(String),
    #[allow(dead_code)]
    Internal(String),
    Forbidden(String),
    NotFound(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match self {
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::Timeout => (StatusCode::REQUEST_TIMEOUT, "Query timeout".to_string()),
            ApiError::QueryError(msg) => (StatusCode::UNPROCESSABLE_ENTITY, msg),
            ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
            ApiError::Forbidden(msg) => (StatusCode::FORBIDDEN, msg),
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
        };

        let body = serde_json::json!({
            "ok": false,
            "error": message
        });

        (status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cidrs() {
        let cidrs = vec![
            "100.64.0.0/10".to_string(),
            "10.0.0.0/8".to_string(),
            "192.168.1.0/24".to_string(),
        ];
        let parsed = parse_cidrs(&cidrs);
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0], ("100.64.0.0".parse().unwrap(), 10));
        assert_eq!(parsed[1], ("10.0.0.0".parse().unwrap(), 8));
        assert_eq!(parsed[2], ("192.168.1.0".parse().unwrap(), 24));
    }

    #[test]
    fn test_parse_cidrs_invalid() {
        let cidrs = vec![
            "invalid".to_string(),
            "100.64.0.0".to_string(),  // Missing prefix
            "100.64.0.0/abc".to_string(),  // Invalid prefix
        ];
        let parsed = parse_cidrs(&cidrs);
        assert_eq!(parsed.len(), 0);
    }

    #[test]
    fn test_ip_in_cidr_v4() {
        let network: IpAddr = "100.64.0.0".parse().unwrap();
        
        // Inside 100.64.0.0/10
        assert!(ip_in_cidr(&"100.64.0.1".parse().unwrap(), &network, 10));
        assert!(ip_in_cidr(&"100.100.50.25".parse().unwrap(), &network, 10));
        assert!(ip_in_cidr(&"100.127.255.255".parse().unwrap(), &network, 10));
        
        // Outside 100.64.0.0/10
        assert!(!ip_in_cidr(&"100.0.0.1".parse().unwrap(), &network, 10));
        assert!(!ip_in_cidr(&"100.128.0.0".parse().unwrap(), &network, 10));
        assert!(!ip_in_cidr(&"192.168.1.1".parse().unwrap(), &network, 10));
    }

    #[test]
    fn test_ip_in_cidr_v6() {
        let network: IpAddr = "fd7a:115c:a1e0::".parse().unwrap();
        
        // Inside fd7a:115c:a1e0::/48
        assert!(ip_in_cidr(&"fd7a:115c:a1e0::1".parse().unwrap(), &network, 48));
        assert!(ip_in_cidr(&"fd7a:115c:a1e0:ffff::1".parse().unwrap(), &network, 48));
        
        // Outside fd7a:115c:a1e0::/48
        assert!(!ip_in_cidr(&"fd7a:115c:a1e1::1".parse().unwrap(), &network, 48));
        assert!(!ip_in_cidr(&"2001:db8::1".parse().unwrap(), &network, 48));
    }
}
