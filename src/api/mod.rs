use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::db::Pool;
use crate::service::{QueryOptions, QueryResult, SyncStatus};

#[derive(Clone)]
pub struct AppState {
    pub pool: Pool,
}

pub fn router(pool: Pool) -> Router {
    let state = AppState { pool };

    Router::new()
        .route("/health", get(handle_health))
        .route("/status", get(handle_status))
        .route("/query", post(handle_query))
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

async fn handle_query(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, ApiError> {
    let options = QueryOptions {
        timeout_ms: req.timeout_ms.min(30000), // Cap at 30s
        limit: req.limit.min(100000),          // Cap at 100k rows
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

    let sql = format!(
        "SELECT * FROM {} WHERE {} ORDER BY block_timestamp DESC LIMIT {}",
        extract_event_name(&signature),
        time_filter,
        params.limit.min(10000)
    );

    let options = QueryOptions {
        timeout_ms: 5000,
        limit: params.limit.min(10000),
    };

    let result = crate::service::execute_query(&state.pool, &sql, Some(&signature), &options)
        .await
        .map_err(|e| ApiError::QueryError(e.to_string()))?;

    Ok(Json(QueryResponse { result, ok: true }))
}

fn extract_event_name(signature: &str) -> String {
    signature
        .split('(')
        .next()
        .unwrap_or("Event")
        .to_string()
}

fn parse_time_filter(after: &str) -> Result<String, ApiError> {
    if after.ends_with('h') {
        let hours: i64 = after
            .trim_end_matches('h')
            .parse()
            .map_err(|_| ApiError::BadRequest("Invalid time format".into()))?;
        Ok(format!(
            "block_timestamp > NOW() - INTERVAL '{} hours'",
            hours
        ))
    } else if after.ends_with('d') {
        let days: i64 = after
            .trim_end_matches('d')
            .parse()
            .map_err(|_| ApiError::BadRequest("Invalid time format".into()))?;
        Ok(format!(
            "block_timestamp > NOW() - INTERVAL '{} days'",
            days
        ))
    } else {
        Ok(format!("block_timestamp > '{}'", after))
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
