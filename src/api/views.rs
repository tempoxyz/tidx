//! Views API for managing ClickHouse materialized views

use axum::{
    extract::{ConnectInfo, Path, Query, State},
    Json,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

use super::{AppState, ApiError};

/// Check if request comes from Tailscale (100.x.x.x range)
fn is_tailscale_ip(addr: &SocketAddr) -> bool {
    match addr.ip() {
        std::net::IpAddr::V4(ip) => ip.octets()[0] == 100,
        _ => false,
    }
}

/// Validate view name (alphanumeric + underscore only)
fn is_valid_view_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name.chars().next().is_some_and(|c| c.is_ascii_alphabetic())
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[derive(Deserialize)]
pub struct ChainQuery {
    #[serde(alias = "chain_id")]
    #[serde(rename = "chainId")]
    chain_id: u64,
}

#[derive(Serialize)]
pub struct ViewInfo {
    name: String,
    engine: String,
    database: String,
}

#[derive(Serialize)]
pub struct ListViewsResponse {
    ok: bool,
    views: Vec<ViewInfo>,
}

/// GET /views?chainId=42431 - List all views
pub async fn list_views(
    State(state): State<AppState>,
    Query(params): Query<ChainQuery>,
) -> Result<Json<ListViewsResponse>, ApiError> {
    let clickhouse = state
        .get_clickhouse(Some(params.chain_id))
        .await
        .ok_or_else(|| ApiError::BadRequest(format!(
            "ClickHouse not configured for chain_id: {}",
            params.chain_id
        )))?;

    let database = format!("analytics_{}", params.chain_id);
    
    // Query system.tables for views in analytics database
    let sql = format!(
        "SELECT name, engine FROM system.tables WHERE database = '{}' AND engine IN ('View', 'MaterializedView') ORDER BY name",
        database
    );

    let result = clickhouse.query(&sql, None).await
        .map_err(|e| ApiError::QueryError(e.to_string()))?;

    let views: Vec<ViewInfo> = result.rows.iter().map(|row| {
        ViewInfo {
            name: row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string(),
            engine: row.get(1).and_then(|v| v.as_str()).unwrap_or("").to_string(),
            database: database.clone(),
        }
    }).collect();

    Ok(Json(ListViewsResponse { ok: true, views }))
}

#[derive(Deserialize)]
pub struct CreateViewRequest {
    #[serde(alias = "chain_id")]
    #[serde(rename = "chainId")]
    chain_id: u64,
    name: String,
    sql: String,
    #[serde(default = "default_engine")]
    engine: String,
    #[serde(rename = "orderBy")]
    order_by: Vec<String>,
}

fn default_engine() -> String {
    "SummingMergeTree()".to_string()
}

#[derive(Serialize)]
pub struct CreateViewResponse {
    ok: bool,
    view: ViewInfo,
    backfill_rows: u64,
}

/// POST /views - Create a materialized view (Tailscale only)
pub async fn create_view(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(req): Json<CreateViewRequest>,
) -> Result<Json<CreateViewResponse>, ApiError> {
    // Check Tailscale access
    if !is_tailscale_ip(&addr) {
        return Err(ApiError::Forbidden("Mutations only allowed via Tailscale".to_string()));
    }

    // Validate view name
    if !is_valid_view_name(&req.name) {
        return Err(ApiError::BadRequest("Invalid view name: must be alphanumeric with underscores".to_string()));
    }

    // Validate order_by
    if req.order_by.is_empty() {
        return Err(ApiError::BadRequest("orderBy is required".to_string()));
    }

    // Validate SQL is SELECT only
    let sql_upper = req.sql.trim().to_uppercase();
    if !sql_upper.starts_with("SELECT") {
        return Err(ApiError::BadRequest("SQL must be a SELECT statement".to_string()));
    }

    let clickhouse = state
        .get_clickhouse(Some(req.chain_id))
        .await
        .ok_or_else(|| ApiError::BadRequest(format!(
            "ClickHouse not configured for chain_id: {}",
            req.chain_id
        )))?;

    let database = format!("analytics_{}", req.chain_id);
    let table_name = &req.name;
    let mv_name = format!("{}_mv", req.name);
    let order_by = req.order_by.join(", ");

    // Rewrite table references in SQL to include database prefix
    let sql = super::rewrite_analytics_tables(&req.sql, req.chain_id);

    // 1. Ensure database exists
    let create_db = format!("CREATE DATABASE IF NOT EXISTS {}", database);
    clickhouse.query(&create_db, None).await
        .map_err(|e| ApiError::QueryError(format!("Failed to create database: {}", e)))?;

    // 2. Create target table (infer schema from SELECT ... LIMIT 0)
    let create_table = format!(
        "CREATE TABLE IF NOT EXISTS {}.{} ENGINE = {} ORDER BY ({}) AS {} LIMIT 0",
        database, table_name, req.engine, order_by, sql
    );
    clickhouse.query(&create_table, None).await
        .map_err(|e| ApiError::QueryError(format!("Failed to create table: {}", e)))?;

    // 3. Create materialized view
    let create_mv = format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {}.{} TO {}.{} AS {}",
        database, mv_name, database, table_name, sql
    );
    clickhouse.query(&create_mv, None).await
        .map_err(|e| ApiError::QueryError(format!("Failed to create materialized view: {}", e)))?;

    // 4. Backfill existing data
    let backfill = format!(
        "INSERT INTO {}.{} {}",
        database, table_name, sql
    );
    clickhouse.query(&backfill, None).await
        .map_err(|e| ApiError::QueryError(format!("Failed to backfill: {}", e)))?;

    // 5. Get row count
    let count_sql = format!("SELECT count() FROM {}.{}", database, table_name);
    let count_result = clickhouse.query(&count_sql, None).await
        .map_err(|e| ApiError::QueryError(format!("Failed to get count: {}", e)))?;
    
    let backfill_rows = count_result.rows
        .first()
        .and_then(|r| r.first())
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    Ok(Json(CreateViewResponse {
        ok: true,
        view: ViewInfo {
            name: table_name.clone(),
            engine: "MaterializedView".to_string(),
            database,
        },
        backfill_rows,
    }))
}

#[derive(Serialize)]
pub struct DeleteViewResponse {
    ok: bool,
    deleted: Vec<String>,
}

/// DELETE /views/{name}?chainId=42431 - Delete a view (Tailscale only)
pub async fn delete_view(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Path(name): Path<String>,
    Query(params): Query<ChainQuery>,
) -> Result<Json<DeleteViewResponse>, ApiError> {
    // Check Tailscale access
    if !is_tailscale_ip(&addr) {
        return Err(ApiError::Forbidden("Mutations only allowed via Tailscale".to_string()));
    }

    // Validate view name
    if !is_valid_view_name(&name) {
        return Err(ApiError::BadRequest("Invalid view name".to_string()));
    }

    let clickhouse = state
        .get_clickhouse(Some(params.chain_id))
        .await
        .ok_or_else(|| ApiError::BadRequest(format!(
            "ClickHouse not configured for chain_id: {}",
            params.chain_id
        )))?;

    let database = format!("analytics_{}", params.chain_id);
    let mv_name = format!("{}_mv", name);
    let mut deleted = Vec::new();

    // Drop MV first
    let drop_mv = format!("DROP VIEW IF EXISTS {}.{}", database, mv_name);
    if clickhouse.query(&drop_mv, None).await.is_ok() {
        deleted.push(mv_name);
    }

    // Drop target table
    let drop_table = format!("DROP TABLE IF EXISTS {}.{}", database, name);
    if clickhouse.query(&drop_table, None).await.is_ok() {
        deleted.push(name);
    }

    Ok(Json(DeleteViewResponse { ok: true, deleted }))
}

#[derive(Serialize)]
pub struct GetViewResponse {
    ok: bool,
    view: ViewInfo,
    definition: String,
    row_count: u64,
}

/// GET /views/{name}?chainId=42431 - Get view details
pub async fn get_view(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<ChainQuery>,
) -> Result<Json<GetViewResponse>, ApiError> {
    let clickhouse = state
        .get_clickhouse(Some(params.chain_id))
        .await
        .ok_or_else(|| ApiError::BadRequest(format!(
            "ClickHouse not configured for chain_id: {}",
            params.chain_id
        )))?;

    let database = format!("analytics_{}", params.chain_id);

    // Get view definition
    let sql = format!(
        "SELECT engine, create_table_query FROM system.tables WHERE database = '{}' AND name = '{}'",
        database, name
    );
    let result = clickhouse.query(&sql, None).await
        .map_err(|e| ApiError::QueryError(e.to_string()))?;

    if result.rows.is_empty() {
        return Err(ApiError::NotFound(format!("View '{}' not found", name)));
    }

    let row = &result.rows[0];
    let engine = row.get(0).and_then(|v| v.as_str()).unwrap_or("").to_string();
    let definition = row.get(1).and_then(|v| v.as_str()).unwrap_or("").to_string();

    // Get row count
    let count_sql = format!("SELECT count() FROM {}.{}", database, name);
    let count_result = clickhouse.query(&count_sql, None).await.ok();
    let row_count = count_result
        .and_then(|r| r.rows.first().cloned())
        .and_then(|r| r.first().cloned())
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    Ok(Json(GetViewResponse {
        ok: true,
        view: ViewInfo {
            name,
            engine,
            database,
        },
        definition,
        row_count,
    }))
}
