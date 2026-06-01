mod views;

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, RwLock as StdRwLock};

use tokio::sync::RwLock;

use anyhow::{Result as AnyhowResult, anyhow};
use axum::{
    Json, Router,
    extract::{Query, State},
    http::{Method, StatusCode, header},
    response::{
        IntoResponse, Response, Sse,
        sse::{Event as SseEvent, KeepAlive, KeepAliveStream},
    },
    routing::get,
};
use chrono::Utc;
use futures::Stream;
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::broadcast::Broadcaster;
use crate::clickhouse::ClickHouseEngine;
use crate::config::HttpConfig;
use crate::db::Pool;
use crate::query::EventSignature;
use crate::service::{QueryOptions, QueryResult, SyncStatus};

pub type SharedPools = Arc<RwLock<HashMap<u64, Pool>>>;
pub type SharedClickHouseEngines = Arc<RwLock<HashMap<u64, Arc<ClickHouseEngine>>>>;

/// Per-chain ClickHouse configuration.
#[derive(Clone, Debug, Default)]
pub struct ChainClickHouseConfig {
    pub enabled: bool,
    pub url: String,
    pub failover_urls: Vec<String>,
}

pub type SharedClickHouseConfigs = Arc<RwLock<HashMap<u64, ChainClickHouseConfig>>>;
pub type SharedTrustedCidrs = Arc<StdRwLock<Vec<(IpAddr, u8)>>>;

#[derive(Clone)]
pub struct AppState {
    /// Map of chain_id -> pool (hot-reloadable)
    pub pools: SharedPools,
    /// Default chain_id (first chain)
    pub default_chain_id: u64,
    pub broadcaster: Arc<Broadcaster>,
    /// Per-chain ClickHouse configuration (hot-reloadable)
    pub clickhouse_configs: SharedClickHouseConfigs,
    /// ClickHouse engines for OLAP queries (per chain)
    pub clickhouse_engines: SharedClickHouseEngines,
    /// Parsed trusted CIDRs for admin operations
    pub trusted_cidrs: SharedTrustedCidrs,
}

impl AppState {
    async fn get_pool(&self, chain_id: Option<u64>) -> Option<Pool> {
        let id = chain_id.unwrap_or(self.default_chain_id);
        self.pools.read().await.get(&id).cloned()
    }

    async fn get_clickhouse(&self, chain_id: Option<u64>) -> Option<Arc<ClickHouseEngine>> {
        let id = chain_id.unwrap_or(self.default_chain_id);
        self.clickhouse_engines.read().await.get(&id).cloned()
    }

    /// Check if an IP address is in the trusted CIDRs
    pub fn is_trusted_ip(&self, addr: &SocketAddr) -> bool {
        let ip = addr.ip();
        self.trusted_cidrs
            .read()
            .map(|cidrs| {
                cidrs
                    .iter()
                    .any(|(network, prefix)| ip_in_cidr(&ip, network, *prefix))
            })
            .unwrap_or(false)
    }
}

/// Parse CIDR strings into (network, prefix_len) tuples
pub fn parse_cidrs(cidrs: &[String]) -> AnyhowResult<Vec<(IpAddr, u8)>> {
    cidrs
        .iter()
        .map(|cidr| {
            let (ip, prefix) = cidr
                .split_once('/')
                .ok_or_else(|| anyhow!("Invalid CIDR '{cidr}': missing prefix"))?;
            let ip: IpAddr = ip
                .parse()
                .map_err(|e| anyhow!("Invalid CIDR '{cidr}': invalid IP address: {e}"))?;
            let prefix: u8 = prefix
                .parse()
                .map_err(|e| anyhow!("Invalid CIDR '{cidr}': invalid prefix: {e}"))?;
            match ip {
                IpAddr::V4(_) if prefix > 32 => {
                    Err(anyhow!("Invalid CIDR '{cidr}': IPv4 prefix exceeds 32"))
                }
                IpAddr::V6(_) if prefix > 128 => {
                    Err(anyhow!("Invalid CIDR '{cidr}': IPv6 prefix exceeds 128"))
                }
                _ => Ok((ip, prefix)),
            }
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
            let mask = if prefix_len == 0 {
                0
            } else {
                u32::MAX << (32 - prefix_len)
            };
            (u32::from(*ip) & mask) == (u32::from(*net) & mask)
        }
        (IpAddr::V6(ip), IpAddr::V6(net)) => {
            if prefix_len > 128 {
                return false;
            }
            let ip_bits = u128::from(*ip);
            let net_bits = u128::from(*net);
            let mask = if prefix_len == 0 {
                0
            } else {
                u128::MAX << (128 - prefix_len)
            };
            (ip_bits & mask) == (net_bits & mask)
        }
        _ => false,
    }
}

pub fn router(
    pools: HashMap<u64, Pool>,
    default_chain_id: u64,
    broadcaster: Arc<Broadcaster>,
) -> AnyhowResult<Router<()>> {
    router_with_options(
        pools,
        default_chain_id,
        broadcaster,
        HashMap::new(),
        &HttpConfig::default(),
    )
}

pub fn router_with_options(
    pools: HashMap<u64, Pool>,
    default_chain_id: u64,
    broadcaster: Arc<Broadcaster>,
    clickhouse_configs: HashMap<u64, ChainClickHouseConfig>,
    http_config: &HttpConfig,
) -> AnyhowResult<Router<()>> {
    let trusted_cidrs = Arc::new(StdRwLock::new(parse_cidrs(&http_config.trusted_cidrs)?));

    let state = AppState {
        pools: Arc::new(RwLock::new(pools)),
        default_chain_id,
        broadcaster,
        clickhouse_configs: Arc::new(RwLock::new(clickhouse_configs)),
        clickhouse_engines: Arc::new(RwLock::new(HashMap::new())),
        trusted_cidrs,
    };

    Ok(build_router(state))
}

pub fn router_shared(
    pools: SharedPools,
    default_chain_id: u64,
    broadcaster: Arc<Broadcaster>,
    clickhouse_configs: SharedClickHouseConfigs,
    clickhouse_engines: SharedClickHouseEngines,
    trusted_cidrs: SharedTrustedCidrs,
) -> Router<()> {
    let state = AppState {
        pools,
        default_chain_id,
        broadcaster,
        clickhouse_configs,
        clickhouse_engines,
        trusted_cidrs,
    };

    build_router(state)
}

fn build_router(state: AppState) -> Router<()> {
    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::OPTIONS])
        .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION])
        .allow_origin(tower_http::cors::Any);

    Router::new()
        .route("/health", get(handle_health))
        .route("/status", get(handle_status))
        .route("/query", get(handle_query))
        .route("/policy-data", get(handle_policy_data))
        .route("/views", get(views::list_views).post(views::create_view))
        .route(
            "/views/{name}",
            get(views::get_view).delete(views::delete_view),
        )
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn handle_health() -> &'static str {
    "OK"
}

const TIP403_REGISTRY_ADDRESS: [u8; 20] = [
    0x40, 0x3c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00,
];

#[derive(Deserialize)]
struct PolicyDataParams {
    #[serde(alias = "chain_id")]
    #[serde(rename = "chainId")]
    chain_id: u64,
    #[serde(alias = "policy_id")]
    #[serde(rename = "policyId")]
    policy_id: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
enum Tip403PolicyType {
    Whitelist,
    Blacklist,
    Compound,
    Unknown,
}

impl Tip403PolicyType {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Whitelist,
            1 => Self::Blacklist,
            2 => Self::Compound,
            _ => Self::Unknown,
        }
    }

    fn member_event_signature(&self) -> Option<&'static str> {
        match self {
            Self::Whitelist => Some(
                "WhitelistUpdated(uint64 indexed policyId,address indexed updater,address indexed account,bool allowed)",
            ),
            Self::Blacklist => Some(
                "BlacklistUpdated(uint64 indexed policyId,address indexed updater,address indexed account,bool restricted)",
            ),
            Self::Compound | Self::Unknown => None,
        }
    }
}

#[derive(Serialize)]
struct PolicyMetadata {
    policy_id: u64,
    policy_type: Tip403PolicyType,
    created_by: Option<String>,
    created_block_num: Option<i64>,
    created_tx_idx: Option<i32>,
    created_log_idx: Option<i32>,
}

#[derive(Serialize)]
struct PolicyMember {
    account: String,
    updated_by: String,
    updated_block_num: i64,
    updated_tx_idx: i32,
    updated_log_idx: i32,
    updated_tx_hash: String,
}

#[derive(Serialize)]
struct PolicyDataResponse {
    ok: bool,
    chain_id: u64,
    policy_id: u64,
    registry: &'static str,
    metadata: PolicyMetadata,
    members: Vec<PolicyMember>,
}

async fn handle_policy_data(
    State(state): State<AppState>,
    Query(params): Query<PolicyDataParams>,
) -> Result<Json<PolicyDataResponse>, ApiError> {
    let pool = state
        .get_pool(Some(params.chain_id))
        .await
        .ok_or_else(|| ApiError::BadRequest(format!("Unknown chain_id: {}", params.chain_id)))?;

    let metadata = load_tip403_policy_metadata(&pool, params.policy_id)
        .await
        .map_err(|e| ApiError::QueryError(e.to_string()))?
        .ok_or_else(|| {
            ApiError::NotFound(format!("TIP-403 policy not found: {}", params.policy_id))
        })?;

    let members = if let Some(signature) = metadata.policy_type.member_event_signature() {
        load_tip403_policy_members(&pool, params.policy_id, signature)
            .await
            .map_err(|e| ApiError::QueryError(e.to_string()))?
    } else {
        Vec::new()
    };

    Ok(Json(PolicyDataResponse {
        ok: true,
        chain_id: params.chain_id,
        policy_id: params.policy_id,
        registry: "0x403c000000000000000000000000000000000000",
        metadata,
        members,
    }))
}

fn tip403_topic0(signature: &str) -> AnyhowResult<Vec<u8>> {
    Ok(EventSignature::parse(signature)?.topic0.to_vec())
}

fn tip403_policy_topic(policy_id: u64) -> Vec<u8> {
    let mut topic = vec![0u8; 32];
    topic[24..32].copy_from_slice(&policy_id.to_be_bytes());
    topic
}

fn hex_prefixed(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

fn topic_address(topic: &[u8]) -> String {
    if topic.len() >= 32 {
        hex_prefixed(&topic[12..32])
    } else {
        hex_prefixed(topic)
    }
}

fn abi_bool_word(data: &[u8]) -> bool {
    data.get(31).copied().unwrap_or_default() != 0
}

fn abi_u8_word(data: &[u8]) -> u8 {
    data.get(31).copied().unwrap_or_default()
}

async fn load_tip403_policy_metadata(
    pool: &Pool,
    policy_id: u64,
) -> AnyhowResult<Option<PolicyMetadata>> {
    let conn = pool.get().await?;
    let selector = tip403_topic0(
        "PolicyCreated(uint64 indexed policyId,address indexed updater,uint8 policyType)",
    )?;
    let policy_topic = tip403_policy_topic(policy_id);
    let registry = TIP403_REGISTRY_ADDRESS.to_vec();

    let row = conn
        .query_opt(
            r#"
            SELECT topic2, data, block_num, tx_idx, log_idx
            FROM logs
            WHERE address = $1 AND selector = $2 AND topic1 = $3
            ORDER BY block_num DESC, tx_idx DESC, log_idx DESC
            LIMIT 1
            "#,
            &[&registry, &selector, &policy_topic],
        )
        .await?;

    let Some(row) = row else { return Ok(None) };
    let updater_topic: Option<Vec<u8>> = row.get(0);
    let data: Vec<u8> = row.get(1);

    Ok(Some(PolicyMetadata {
        policy_id,
        policy_type: Tip403PolicyType::from_u8(abi_u8_word(&data)),
        created_by: updater_topic.as_deref().map(topic_address),
        created_block_num: Some(row.get(2)),
        created_tx_idx: Some(row.get(3)),
        created_log_idx: Some(row.get(4)),
    }))
}

async fn load_tip403_policy_members(
    pool: &Pool,
    policy_id: u64,
    signature: &str,
) -> AnyhowResult<Vec<PolicyMember>> {
    let conn = pool.get().await?;
    let selector = tip403_topic0(signature)?;
    let policy_topic = tip403_policy_topic(policy_id);
    let registry = TIP403_REGISTRY_ADDRESS.to_vec();

    let rows = conn
        .query(
            r#"
            SELECT DISTINCT ON (topic3)
                topic3, topic2, data, block_num, tx_idx, log_idx, tx_hash
            FROM logs
            WHERE address = $1 AND selector = $2 AND topic1 = $3 AND topic3 IS NOT NULL
            ORDER BY topic3, block_num DESC, tx_idx DESC, log_idx DESC
            "#,
            &[&registry, &selector, &policy_topic],
        )
        .await?;

    let mut members = Vec::new();
    for row in rows {
        let data: Vec<u8> = row.get(2);
        if !abi_bool_word(&data) {
            continue;
        }

        let account_topic: Vec<u8> = row.get(0);
        let updater_topic: Option<Vec<u8>> = row.get(1);
        let tx_hash: Vec<u8> = row.get(6);
        members.push(PolicyMember {
            account: topic_address(&account_topic),
            updated_by: updater_topic
                .as_deref()
                .map(topic_address)
                .unwrap_or_else(|| "0x".to_string()),
            updated_block_num: row.get(3),
            updated_tx_idx: row.get(4),
            updated_log_idx: row.get(5),
            updated_tx_hash: hex_prefixed(&tx_hash),
        });
    }

    members.sort_by(|a, b| a.account.cmp(&b.account));
    Ok(members)
}

#[derive(Serialize)]
struct StatusResponse {
    ok: bool,
    version: &'static str,
    rev: &'static str,
    chains: Vec<SyncStatus>,
}

const VERSION: &str = env!("CARGO_PKG_VERSION");
const GIT_REV: &str = if let Some(rev) = option_env!("GIT_REV") {
    rev
} else {
    "dev"
};

async fn handle_status(State(state): State<AppState>) -> Result<Json<StatusResponse>, ApiError> {
    let mut all_chains = Vec::new();
    let pools: Vec<(u64, Pool)> = state
        .pools
        .read()
        .await
        .iter()
        .map(|(chain_id, pool)| (*chain_id, pool.clone()))
        .collect();
    for (chain_id, pool) in pools {
        let chains = crate::service::get_all_status(&pool).await.map_err(|e| {
            ApiError::QueryError(format!("Failed to load status for chain {chain_id}: {e}"))
        })?;
        if chains.is_empty() {
            all_chains.push(empty_status(chain_id));
        } else {
            all_chains.extend(chains);
        }
    }
    all_chains.sort_by_key(|chain| chain.chain_id);

    // Populate per-table store status for each chain
    let ch_configs = state.clickhouse_configs.read().await;
    for chain in &mut all_chains {
        let chain_id = chain.chain_id as u64;

        // PostgreSQL per-table watermarks (from in-memory atomics, no table scans)
        let (pg_blocks, pg_txs, pg_logs, pg_receipts) =
            crate::metrics::get_sink_watermarks("postgres");
        let (pg_bc, pg_tc, pg_lc, pg_rc) = crate::metrics::get_sink_row_counts("postgres");
        if pg_blocks.is_some() || pg_txs.is_some() || pg_logs.is_some() || pg_receipts.is_some() {
            chain.postgres = Some(crate::service::StoreStatus {
                blocks: pg_blocks,
                txs: pg_txs,
                logs: pg_logs,
                receipts: pg_receipts,
                rate: crate::metrics::get_sink_block_rate("postgres"),
                blocks_count: Some(pg_bc),
                txs_count: Some(pg_tc),
                logs_count: Some(pg_lc),
                receipts_count: Some(pg_rc),
            });
        }

        // ClickHouse per-table watermarks (from in-memory atomics, no table scans)
        if ch_configs.get(&chain_id).is_some_and(|c| c.enabled) {
            let (ch_blocks, ch_txs, ch_logs, ch_receipts) =
                crate::metrics::get_sink_watermarks("clickhouse");
            let (ch_bc, ch_tc, ch_lc, ch_rc) = crate::metrics::get_sink_row_counts("clickhouse");
            if ch_blocks.is_some() || ch_txs.is_some() || ch_logs.is_some() || ch_receipts.is_some()
            {
                chain.clickhouse = Some(crate::service::StoreStatus {
                    blocks: ch_blocks,
                    txs: ch_txs,
                    logs: ch_logs,
                    receipts: ch_receipts,
                    rate: crate::metrics::get_sink_block_rate("clickhouse"),
                    blocks_count: Some(ch_bc),
                    txs_count: Some(ch_tc),
                    logs_count: Some(ch_lc),
                    receipts_count: Some(ch_rc),
                });
            }
        }
    }

    Ok(Json(StatusResponse {
        ok: true,
        version: VERSION,
        rev: GIT_REV,
        chains: all_chains,
    }))
}

fn empty_status(chain_id: u64) -> SyncStatus {
    SyncStatus {
        chain_id: chain_id as i64,
        head_num: 0,
        synced_num: 0,
        tip_num: 0,
        lag: 0,
        gap_blocks: 0,
        gaps: Vec::new(),
        backfill_num: None,
        backfill_remaining: 0,
        sync_rate: None,
        eta_secs: None,
        updated_at: Utc::now(),
        postgres: None,
        clickhouse: None,
    }
}

#[derive(Deserialize)]
pub struct QueryParams {
    /// SQL query (SELECT only)
    sql: String,
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
    /// Force a specific engine: "postgres" or "clickhouse"
    #[serde(default)]
    engine: Option<String>,
}

fn default_timeout() -> u64 {
    5000
}
fn default_limit() -> i64 {
    crate::query::HARD_LIMIT_MAX
}

/// Extract all `signature` query params from the raw query string.
/// Supports multiple params: `?signature=Transfer(...)&signature=Approval(...)`
fn extract_signatures(query_str: Option<&str>) -> Vec<String> {
    let Some(qs) = query_str else { return vec![] };
    form_urlencoded::parse(qs.as_bytes())
        .filter(|(key, _)| key == "signature")
        .map(|(_, value)| value.into_owned())
        .collect()
}

#[derive(Serialize)]
struct QueryResponse {
    #[serde(flatten)]
    result: QueryResult,
    ok: bool,
}

async fn handle_query(
    State(state): State<AppState>,
    uri: axum::http::Uri,
    Query(params): Query<QueryParams>,
) -> Response {
    let signatures = extract_signatures(uri.query());

    if params.live {
        if params.engine.as_deref() == Some("clickhouse") {
            return ApiError::BadRequest(
                "engine=clickhouse is not supported with live=true (use PostgreSQL for real-time streaming)".to_string()
            ).into_response();
        }
        handle_query_live(state, params, signatures)
            .await
            .into_response()
    } else {
        handle_query_once(state, params, signatures)
            .await
            .into_response()
    }
}

async fn handle_query_once(
    state: AppState,
    params: QueryParams,
    signatures: Vec<String>,
) -> Result<Json<QueryResponse>, ApiError> {
    let pool = state
        .get_pool(Some(params.chain_id))
        .await
        .ok_or_else(|| ApiError::BadRequest(format!("Unknown chain_id: {}", params.chain_id)))?;

    let options = QueryOptions {
        timeout_ms: params.timeout_ms.clamp(100, 30000),
        limit: params.limit.clamp(1, crate::query::HARD_LIMIT_MAX),
    };

    // Route to appropriate engine
    let use_clickhouse = matches!(params.engine.as_deref(), Some("clickhouse"));

    let sigs: Vec<&str> = signatures.iter().map(String::as_str).collect();

    let result = if use_clickhouse {
        // Use ClickHouse engine for OLAP queries
        let clickhouse = state
            .get_clickhouse(Some(params.chain_id))
            .await
            .ok_or_else(|| {
                ApiError::BadRequest(format!(
                    "ClickHouse not configured for chain_id: {}",
                    params.chain_id
                ))
            })?;

        clickhouse
            .query_user(&params.sql, &sigs, options.timeout_ms, options.limit)
            .await
            .map(|r| QueryResult {
                columns: r.columns,
                rows: r.rows,
                row_count: r.row_count,
                engine: r.engine,
                query_time_ms: r.query_time_ms,
            })
            .map_err(|e| ApiError::QueryError(e.to_string()))?
    } else {
        // Use PostgreSQL
        crate::service::execute_query_postgres(&pool, &params.sql, &sigs, &options)
            .await
            .map_err(|e| {
                if e.to_string().contains("timeout") {
                    ApiError::Timeout
                } else {
                    ApiError::QueryError(e.to_string())
                }
            })?
    };

    Ok(Json(QueryResponse { result, ok: true }))
}

type SseStream = std::pin::Pin<Box<dyn Stream<Item = Result<SseEvent, Infallible>> + Send>>;

/// Maximum blocks to catch up in a single update (prevents query multiplication attack)
const MAX_CATCHUP_BLOCKS: u64 = 10;
const MAX_LIVE_CONNECTIONS: usize = 20;

async fn handle_query_live(
    state: AppState,
    params: QueryParams,
    signatures: Vec<String>,
) -> Sse<KeepAliveStream<SseStream>> {
    if state.broadcaster.receiver_count() >= MAX_LIVE_CONNECTIONS {
        let stream: SseStream = Box::pin(async_stream::stream! {
            yield Ok(SseEvent::default()
                .event("error")
                .json_data(serde_json::json!({ "ok": false, "error": "Live stream capacity reached" }))
                .unwrap());
        });
        return Sse::new(stream).keep_alive(KeepAlive::default());
    }

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
    let options = QueryOptions {
        timeout_ms: params.timeout_ms.clamp(100, 30000),
        limit: params.limit.clamp(1, crate::query::HARD_LIMIT_MAX),
    };

    let stream = async_stream::stream! {
        let mut last_block_num: u64 = 0;
        let sigs: Vec<&str> = signatures.iter().map(String::as_str).collect();

        // Execute initial query (live streaming uses Postgres for realtime data)
        match crate::service::execute_query_postgres(&pool, &sql, &sigs, &options).await {
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

                    // Filter by each block for per-block streaming
                    let catch_up_start = last_block_num + 1;
                    for block_num in catch_up_start..=end {
                        let filtered_sql = match inject_block_filter(&sql, block_num) {
                            Ok(s) => s,
                            Err(e) => {
                                yield Ok(SseEvent::default()
                                    .event("error")
                                    .json_data(serde_json::json!({ "ok": false, "error": e.to_string() }))
                                    .unwrap());
                                return;
                            }
                        };
                        match crate::service::execute_query_postgres(&pool, &filtered_sql, &sigs, &options).await {
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
///
/// Uses sqlparser AST manipulation to safely add the filter condition,
/// avoiding SQL injection risks from string-based splicing.
#[doc(hidden)]
pub fn inject_block_filter(sql: &str, block_num: u64) -> Result<String, ApiError> {
    use sqlparser::ast::{BinaryOperator, Expr, Ident, SetExpr, Statement, Value};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| ApiError::BadRequest(format!("SQL parse error: {e}")))?;

    if statements.len() != 1 {
        return Err(ApiError::BadRequest(
            "Live mode requires exactly one SQL statement".to_string(),
        ));
    }

    let stmt = &mut statements[0];
    let query = match stmt {
        Statement::Query(q) => q,
        _ => {
            return Err(ApiError::BadRequest(
                "Live mode requires a SELECT query".to_string(),
            ));
        }
    };

    let select = match query.body.as_mut() {
        SetExpr::Select(s) => s,
        _ => {
            return Err(ApiError::BadRequest(
                "Live mode requires a simple SELECT query (UNION/INTERSECT not supported)"
                    .to_string(),
            ));
        }
    };

    let table_name: String = select
        .from
        .first()
        .and_then(|twj| match &twj.relation {
            sqlparser::ast::TableFactor::Table { name, .. } => name
                .0
                .last()
                .and_then(|part| part.as_ident())
                .map(|ident| ident.value.to_lowercase()),
            _ => None,
        })
        .ok_or_else(|| {
            ApiError::BadRequest("Live mode requires a query with a FROM table clause".to_string())
        })?;

    let col_name = if table_name == "blocks" {
        "num"
    } else {
        "block_num"
    };

    let col_expr = Expr::CompoundIdentifier(vec![Ident::new(&table_name), Ident::new(col_name)]);

    let block_filter = Expr::BinaryOp {
        left: Box::new(col_expr),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Value(
            Value::Number(block_num.to_string(), false).into(),
        )),
    };

    select.selection = Some(match select.selection.take() {
        Some(existing) => Expr::BinaryOp {
            left: Box::new(Expr::Nested(Box::new(existing))),
            op: BinaryOperator::And,
            right: Box::new(block_filter),
        },
        None => block_filter,
    });

    Ok(stmt.to_string())
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

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiError::BadRequest(msg) => write!(f, "{msg}"),
            ApiError::Timeout => write!(f, "Query timeout"),
            ApiError::QueryError(msg) => write!(f, "{msg}"),
            ApiError::Internal(msg) => write!(f, "{msg}"),
            ApiError::Forbidden(msg) => write!(f, "{msg}"),
            ApiError::NotFound(msg) => write!(f, "{msg}"),
        }
    }
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
        let parsed = parse_cidrs(&cidrs).unwrap();
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0], ("100.64.0.0".parse().unwrap(), 10));
        assert_eq!(parsed[1], ("10.0.0.0".parse().unwrap(), 8));
        assert_eq!(parsed[2], ("192.168.1.0".parse().unwrap(), 24));
    }

    #[test]
    fn test_parse_cidrs_invalid() {
        let cidrs = vec![
            "invalid".to_string(),
            "100.64.0.0".to_string(),     // Missing prefix
            "100.64.0.0/abc".to_string(), // Invalid prefix
        ];
        assert!(parse_cidrs(&cidrs).is_err());
        assert!(parse_cidrs(&["100.64.0.0/33".to_string()]).is_err());
        assert!(parse_cidrs(&["fd7a:115c:a1e0::/129".to_string()]).is_err());
    }

    #[test]
    fn test_router_with_options_rejects_invalid_trusted_cidr() {
        let http_config = HttpConfig {
            trusted_cidrs: vec!["100.64.0.0/33".to_string()],
            ..Default::default()
        };

        let result = router_with_options(
            HashMap::new(),
            0,
            Arc::new(Broadcaster::new()),
            HashMap::new(),
            &http_config,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_trusted_ip_fails_closed_when_empty() {
        let state = AppState {
            pools: Arc::new(RwLock::new(HashMap::new())),
            default_chain_id: 0,
            broadcaster: Arc::new(Broadcaster::new()),
            clickhouse_configs: Arc::new(RwLock::new(HashMap::new())),
            clickhouse_engines: Arc::new(RwLock::new(HashMap::new())),
            trusted_cidrs: Arc::new(std::sync::RwLock::new(Vec::new())),
        };
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        assert!(!state.is_trusted_ip(&addr));
    }

    #[test]
    fn test_http_config_default_trusts_only_loopback() {
        let parsed = parse_cidrs(&HttpConfig::default().trusted_cidrs).unwrap();
        assert!(parsed.contains(&("127.0.0.1".parse().unwrap(), 32)));
        assert!(parsed.contains(&("::1".parse().unwrap(), 128)));
    }

    #[test]
    fn test_ip_in_cidr_v4() {
        let network: IpAddr = "100.64.0.0".parse().unwrap();

        // Inside 100.64.0.0/10
        assert!(ip_in_cidr(&"100.64.0.1".parse().unwrap(), &network, 10));
        assert!(ip_in_cidr(&"100.100.50.25".parse().unwrap(), &network, 10));
        assert!(ip_in_cidr(
            &"100.127.255.255".parse().unwrap(),
            &network,
            10
        ));

        // Outside 100.64.0.0/10
        assert!(!ip_in_cidr(&"100.0.0.1".parse().unwrap(), &network, 10));
        assert!(!ip_in_cidr(&"100.128.0.0".parse().unwrap(), &network, 10));
        assert!(!ip_in_cidr(&"192.168.1.1".parse().unwrap(), &network, 10));
    }

    #[test]
    fn test_ip_in_cidr_v6() {
        let network: IpAddr = "fd7a:115c:a1e0::".parse().unwrap();

        // Inside fd7a:115c:a1e0::/48
        assert!(ip_in_cidr(
            &"fd7a:115c:a1e0::1".parse().unwrap(),
            &network,
            48
        ));
        assert!(ip_in_cidr(
            &"fd7a:115c:a1e0:ffff::1".parse().unwrap(),
            &network,
            48
        ));

        // Outside fd7a:115c:a1e0::/48
        assert!(!ip_in_cidr(
            &"fd7a:115c:a1e1::1".parse().unwrap(),
            &network,
            48
        ));
        assert!(!ip_in_cidr(&"2001:db8::1".parse().unwrap(), &network, 48));
    }
}
