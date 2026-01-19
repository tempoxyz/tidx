use std::convert::Infallible;
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{
        sse::{Event as SseEvent, KeepAlive},
        IntoResponse, Response, Sse,
    },
    routing::{get, post},
    Json, Router,
};
use futures::Stream;
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::broadcast::Broadcaster;
use crate::db::Pool;
use crate::service::{QueryOptions, QueryResult, SyncStatus};

#[derive(Clone)]
pub struct AppState {
    pub pool: Pool,
    pub broadcaster: Arc<Broadcaster>,
}

pub fn router(pool: Pool, broadcaster: Arc<Broadcaster>) -> Router {
    let state = AppState { pool, broadcaster };

    Router::new()
        .route("/health", get(handle_health))
        .route("/status", get(handle_status))
        .route("/query", post(handle_query).get(handle_query_live_get))
        .route("/logs/{signature}", get(handle_logs))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn handle_health() -> &'static str {
    "OK"
}

#[derive(Serialize)]
struct StatusResponse {
    #[serde(flatten)]
    status: Option<SyncStatus>,
    ok: bool,
}

async fn handle_status(State(state): State<AppState>) -> Result<Json<StatusResponse>, ApiError> {
    let status = crate::service::get_status(&state.pool)
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    Ok(Json(StatusResponse {
        ok: status.is_some(),
        status,
    }))
}

#[derive(Deserialize)]
pub struct QueryRequest {
    sql: String,
    #[serde(default)]
    signature: Option<String>,
    #[serde(default = "default_timeout")]
    timeout_ms: u64,
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

#[derive(Deserialize)]
pub struct QueryParams {
    #[serde(default)]
    live: bool,
}

async fn handle_query(
    State(state): State<AppState>,
    Query(params): Query<QueryParams>,
    Json(req): Json<QueryRequest>,
) -> Response {
    if params.live {
        handle_query_live(state, req).await.into_response()
    } else {
        handle_query_once(state, req).await.into_response()
    }
}

async fn handle_query_once(
    state: AppState,
    req: QueryRequest,
) -> Result<Json<QueryResponse>, ApiError> {
    let options = QueryOptions {
        timeout_ms: req.timeout_ms.clamp(100, 30000), // 100ms - 30s
        limit: req.limit.clamp(1, 100000),            // 1 - 100k rows
    };

    let result = crate::service::execute_query(
        &state.pool,
        &req.sql,
        req.signature.as_deref(),
        &options,
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

async fn handle_query_live(
    state: AppState,
    req: QueryRequest,
) -> Sse<impl Stream<Item = Result<SseEvent, Infallible>>> {
    let mut rx = state.broadcaster.subscribe();
    let pool = state.pool;
    let sql = req.sql;
    let signature = req.signature;
    let options = QueryOptions {
        timeout_ms: req.timeout_ms.clamp(100, 30000),
        limit: req.limit.clamp(1, 100000),
    };

    let stream = async_stream::stream! {
        let mut last_result_hash: Option<String> = None;

        // Execute initial query
        match crate::service::execute_query(&pool, &sql, signature.as_deref(), &options).await {
            Ok(result) => {
                let response = QueryResponse { result, ok: true };
                last_result_hash = Some(format!("{:?}", response.result.rows));
                yield Ok(SseEvent::default()
                    .event("result")
                    .json_data(response)
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

        // Stream updates on each new block
        loop {
            match rx.recv().await {
                Ok(_update) => {
                    match crate::service::execute_query(&pool, &sql, signature.as_deref(), &options).await {
                        Ok(result) => {
                            let response = QueryResponse { result, ok: true };
                            let result_hash = format!("{:?}", response.result.rows);
                            
                            // Skip if result is identical to last sent
                            if Some(&result_hash) == last_result_hash.as_ref() {
                                continue;
                            }
                            last_result_hash = Some(result_hash);

                            yield Ok(SseEvent::default()
                                .event("result")
                                .json_data(response)
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

    Sse::new(stream).keep_alive(KeepAlive::default())
}

#[derive(Deserialize)]
pub struct QueryLiveGetParams {
    sql: String,
    #[serde(default)]
    signature: Option<String>,
    #[serde(default = "default_timeout")]
    timeout_ms: u64,
    #[serde(default = "default_limit")]
    limit: i64,
}

async fn handle_query_live_get(
    State(state): State<AppState>,
    Query(params): Query<QueryLiveGetParams>,
) -> Sse<impl Stream<Item = Result<SseEvent, Infallible>>> {
    let req = QueryRequest {
        sql: params.sql,
        signature: params.signature,
        timeout_ms: params.timeout_ms,
        limit: params.limit,
    };
    handle_query_live(state, req).await
}

#[derive(Deserialize)]
pub struct LogsQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    #[serde(default)]
    after: Option<String>,
}

async fn handle_logs(
    State(state): State<AppState>,
    Path(signature): Path<String>,
    Query(params): Query<LogsQuery>,
) -> Result<Json<QueryResponse>, ApiError> {
    let time_filter = if let Some(ref after) = params.after {
        parse_time_filter(after)?
    } else {
        "1 = 1".to_string()
    };

    let event_name = extract_event_name(&signature)?;
    
    let sql = format!(
        "SELECT * FROM \"{}\" WHERE {} ORDER BY block_timestamp DESC LIMIT {}",
        event_name,
        time_filter,
        params.limit.clamp(1, 10000)
    );

    let options = QueryOptions {
        timeout_ms: 5000,
        limit: params.limit.clamp(1, 10000),
    };

    let result = crate::service::execute_query(&state.pool, &sql, Some(&signature), &options)
        .await
        .map_err(|e| ApiError::QueryError(e.to_string()))?;

    Ok(Json(QueryResponse { result, ok: true }))
}

fn extract_event_name(signature: &str) -> Result<String, ApiError> {
    let name = signature
        .split('(')
        .next()
        .unwrap_or("")
        .trim();
    
    if name.is_empty() {
        return Err(ApiError::BadRequest("Empty event name".into()));
    }
    
    if name.len() > 64 {
        return Err(ApiError::BadRequest("Event name too long".into()));
    }
    
    let is_valid = name.chars().next().map(|c| c.is_ascii_alphabetic() || c == '_').unwrap_or(false)
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    
    if !is_valid {
        return Err(ApiError::BadRequest("Invalid event name: must be alphanumeric".into()));
    }
    
    Ok(name.to_string())
}

fn parse_time_filter(after: &str) -> Result<String, ApiError> {
    if after.ends_with('h') {
        let hours: i64 = after
            .trim_end_matches('h')
            .parse()
            .map_err(|_| ApiError::BadRequest("Invalid time format".into()))?;
        if hours <= 0 || hours > 8760 {
            return Err(ApiError::BadRequest("Hours must be between 1 and 8760".into()));
        }
        Ok(format!(
            "block_timestamp > NOW() - INTERVAL '{} hours'",
            hours
        ))
    } else if after.ends_with('d') {
        let days: i64 = after
            .trim_end_matches('d')
            .parse()
            .map_err(|_| ApiError::BadRequest("Invalid time format".into()))?;
        if days <= 0 || days > 365 {
            return Err(ApiError::BadRequest("Days must be between 1 and 365".into()));
        }
        Ok(format!(
            "block_timestamp > NOW() - INTERVAL '{} days'",
            days
        ))
    } else {
        let parsed = chrono::DateTime::parse_from_rfc3339(after)
            .map_err(|_| ApiError::BadRequest("Invalid timestamp format. Use RFC3339 or relative time (e.g., '1h', '7d')".into()))?;
        Ok(format!("block_timestamp > '{}'", parsed.to_rfc3339()))
    }
}

#[derive(Debug)]
pub enum ApiError {
    BadRequest(String),
    Timeout,
    QueryError(String),
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
