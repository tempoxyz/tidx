mod rate_limit;

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
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
use crate::db::{DuckDbPool, Pool};
use crate::service::{QueryOptions, QueryResult, SyncStatus};

pub use rate_limit::{RateLimiter, SseConnectionGuard};

pub type SharedPools = Arc<RwLock<HashMap<u64, Pool>>>;
pub type SharedDuckDbPools = Arc<RwLock<HashMap<u64, Arc<DuckDbPool>>>>;

#[derive(Clone)]
pub struct AppState {
    /// Map of chain_id -> pool (hot-reloadable)
    pub pools: SharedPools,
    /// Default chain_id (first chain)
    pub default_chain_id: u64,
    pub broadcaster: Arc<Broadcaster>,
    /// Per-chain DuckDB pools for analytical queries (hot-reloadable)
    pub duckdb_pools: SharedDuckDbPools,
    /// Rate limiter for request throttling
    pub rate_limiter: RateLimiter,
}

impl AppState {
    async fn get_pool(&self, chain_id: Option<u64>) -> Option<Pool> {
        let id = chain_id.unwrap_or(self.default_chain_id);
        self.pools.read().await.get(&id).cloned()
    }

    async fn get_duckdb_pool(&self, chain_id: Option<u64>) -> Option<Arc<DuckDbPool>> {
        let id = chain_id.unwrap_or(self.default_chain_id);
        self.duckdb_pools.read().await.get(&id).cloned()
    }
}

pub fn router(pools: HashMap<u64, Pool>, default_chain_id: u64, broadcaster: Arc<Broadcaster>) -> Router<()> {
    router_with_options(pools, default_chain_id, broadcaster, HashMap::new(), &HttpConfig::default())
}

pub fn router_with_options(
    pools: HashMap<u64, Pool>,
    default_chain_id: u64,
    broadcaster: Arc<Broadcaster>,
    duckdb_pools: HashMap<u64, Arc<DuckDbPool>>,
    http_config: &HttpConfig,
) -> Router<()> {
    let rate_limiter = RateLimiter::new(
        http_config.rate_limit.clone(),
        http_config.api_keys.clone(),
    );

    rate_limit::spawn_cleanup_task(rate_limiter.clone());

    let state = AppState {
        pools: Arc::new(RwLock::new(pools)),
        default_chain_id,
        broadcaster,
        duckdb_pools: Arc::new(RwLock::new(duckdb_pools)),
        rate_limiter: rate_limiter.clone(),
    };

    build_router(state, rate_limiter)
}

pub fn router_shared(
    pools: SharedPools,
    default_chain_id: u64,
    broadcaster: Arc<Broadcaster>,
    duckdb_pools: SharedDuckDbPools,
    http_config: SharedHttpConfig,
) -> Router<()> {
    let rate_limiter = RateLimiter::new_shared(http_config);

    rate_limit::spawn_cleanup_task(rate_limiter.clone());

    let state = AppState {
        pools,
        default_chain_id,
        broadcaster,
        duckdb_pools,
        rate_limiter: rate_limiter.clone(),
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
    drop(pools);

    let duckdb_pools = state.duckdb_pools.read().await;
    
    for chain in &mut all_chains {
        let chain_id = chain.chain_id as u64;
        
        if let Some(duckdb_pool) = duckdb_pools.get(&chain_id) {
            // Get DuckDB range
            if let Ok((duck_min, duck_max)) = duckdb_pool.block_range().await {
                let duck_min = duck_min.unwrap_or(0);
                let duck_max = duck_max.unwrap_or(0);
                
                chain.duckdb_min = Some(duck_min);
                chain.duckdb_max = Some(duck_max);
                chain.duckdb_tip_lag = Some(chain.tip_num - duck_max);
                
                // Backfill remaining = blocks below duck_min that PG has
                if duck_min > 0 {
                    // Get PG min for this chain
                    let pools = state.pools.read().await;
                    if let Some(pool) = pools.get(&chain_id) {
                        if let Ok(conn) = pool.get().await {
                            if let Ok(row) = conn.query_one("SELECT COALESCE(MIN(num), 0) FROM blocks", &[]).await {
                                let pg_min: i64 = row.get(0);
                                chain.duckdb_backfill_remaining = Some((duck_min - pg_min).max(0));
                            }
                        }
                    }
                }
            }
            
            // Get gap info
            if let Ok(duck_status) = crate::sync::get_sync_status(&duckdb_pool).await {
                chain.duckdb_internal_gaps = Some(duck_status.gap_blocks);
                chain.duckdb_gaps = duck_status.gaps;
                
                // Deprecated fields for backwards compatibility
                chain.duckdb_synced_num = Some(duck_status.latest_block);
                chain.duckdb_lag = Some(chain.tip_num - duck_status.latest_block);
                chain.duckdb_gap_blocks = Some(duck_status.gap_blocks);
            }
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
    /// Force a specific engine: "postgres" or "duckdb"
    #[serde(default)]
    engine: Option<String>,
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

    let duckdb_pool = state.get_duckdb_pool(Some(params.chain_id)).await;
    let result = crate::service::execute_query_with_engine(
        &pool,
        duckdb_pool.as_ref(),
        &params.sql,
        params.signature.as_deref(),
        &options,
        params.engine.as_deref(),
    )
    .await
    .map_err(|e| {
        let msg = e.to_string();
        if msg.contains("timeout") {
            ApiError::Timeout
        } else if msg.contains("forbidden") || msg.contains("Only SELECT") {
            ApiError::BadRequest(msg)
        } else {
            ApiError::QueryError(msg)
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

    let duckdb_pool = state.get_duckdb_pool(Some(params.chain_id)).await;

    let mut rx = state.broadcaster.subscribe();
    let sql = params.sql;
    let signature = params.signature;
    let engine = params.engine;
    let options = QueryOptions {
        timeout_ms: params.timeout_ms.clamp(100, 30000),
        limit: params.limit.clamp(1, 100000),
    };

    // Detect if this is an OLAP query (aggregations, etc.)
    let is_olap = crate::query::route_query(&sql) == crate::query::QueryEngine::DuckDb;

    let stream = async_stream::stream! {
        // Keep guard alive for the lifetime of the stream
        let _guard = connection_guard;
        let mut last_block_num: u64 = 0;

        // Execute initial query
        match crate::service::execute_query_with_engine(&pool, duckdb_pool.as_ref(), &sql, signature.as_deref(), &options, engine.as_deref()).await {
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
                        match crate::service::execute_query_with_engine(&pool, duckdb_pool.as_ref(), &sql, signature.as_deref(), &options, engine.as_deref()).await {
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
                            match crate::service::execute_query(&pool, duckdb_pool.as_ref(), &filtered_sql, signature.as_deref(), &options).await {
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
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match self {
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::Timeout => (StatusCode::REQUEST_TIMEOUT, "Query timeout".to_string()),
            ApiError::QueryError(msg) => (StatusCode::UNPROCESSABLE_ENTITY, msg),
            ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };

        let body = serde_json::json!({
            "ok": false,
            "error": message
        });

        (status, Json(body)).into_response()
    }
}
