//! Views API for managing ClickHouse materialized views

use axum::{
    extract::{ConnectInfo, Path, Query, State},
    http::HeaderMap,
    Json,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

use super::{AppState, ApiError};

/// Check if request comes from Tailscale CGNAT range.
/// IPv4: 100.64.0.0/10 (100.64.0.0 - 100.127.255.255)
/// IPv6: fd7a:115c:a1e0::/48
fn is_tailscale_ip(addr: &SocketAddr) -> bool {
    match addr.ip() {
        std::net::IpAddr::V4(ip) => {
            let octets = ip.octets();
            // 100.64.0.0/10: first octet = 100, second octet high 2 bits = 01
            octets[0] == 100 && (octets[1] & 0b11000000) == 64
        }
        std::net::IpAddr::V6(ip) => {
            // fd7a:115c:a1e0::/48
            let segments = ip.segments();
            segments[0] == 0xfd7a && segments[1] == 0x115c && segments[2] == 0xa1e0
        }
    }
}

/// Check if request has a valid API key for admin operations.
async fn has_admin_api_key(state: &AppState, headers: &HeaderMap) -> bool {
    state.rate_limiter.has_valid_api_key(headers).await
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
    database: String,
    engine: String,
    name: String,
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
    #[serde(default = "default_engine")]
    engine: String,
    name: String,
    #[serde(rename = "orderBy")]
    order_by: Vec<String>,
    sql: String,
}

fn default_engine() -> String {
    "SummingMergeTree()".to_string()
}

#[derive(Serialize)]
pub struct CreateViewResponse {
    backfill_rows: u64,
    ok: bool,
    view: ViewInfo,
}

/// POST /views - Create a materialized view (Tailscale + API key required)
pub async fn create_view(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<CreateViewRequest>,
) -> Result<Json<CreateViewResponse>, ApiError> {
    // Check Tailscale access
    if !is_tailscale_ip(&addr) {
        return Err(ApiError::Forbidden("Mutations only allowed via Tailscale".to_string()));
    }

    // Check API key
    if !has_admin_api_key(&state, &headers).await {
        return Err(ApiError::Forbidden("API key required for mutations".to_string()));
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
    deleted: Vec<String>,
    ok: bool,
}

/// DELETE /views/{name}?chainId=42431 - Delete a view (Tailscale + API key required)
pub async fn delete_view(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(params): Query<ChainQuery>,
) -> Result<Json<DeleteViewResponse>, ApiError> {
    // Check Tailscale access
    if !is_tailscale_ip(&addr) {
        return Err(ApiError::Forbidden("Mutations only allowed via Tailscale".to_string()));
    }

    // Check API key
    if !has_admin_api_key(&state, &headers).await {
        return Err(ApiError::Forbidden("API key required for mutations".to_string()));
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
    definition: String,
    ok: bool,
    row_count: u64,
    view: ViewInfo,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    fn addr_v4(a: u8, b: u8, c: u8, d: u8) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(a, b, c, d)), 12345)
    }

    fn addr_v6(segments: [u16; 8]) -> SocketAddr {
        SocketAddr::new(IpAddr::V6(Ipv6Addr::new(
            segments[0], segments[1], segments[2], segments[3],
            segments[4], segments[5], segments[6], segments[7],
        )), 12345)
    }

    #[test]
    fn test_tailscale_ipv4_valid() {
        // 100.64.0.0/10 range: 100.64.0.0 - 100.127.255.255
        assert!(is_tailscale_ip(&addr_v4(100, 64, 0, 1)));
        assert!(is_tailscale_ip(&addr_v4(100, 100, 50, 25)));
        assert!(is_tailscale_ip(&addr_v4(100, 127, 255, 255)));
        assert!(is_tailscale_ip(&addr_v4(100, 127, 112, 88)));
    }

    #[test]
    fn test_tailscale_ipv4_invalid() {
        // Outside 100.64.0.0/10
        assert!(!is_tailscale_ip(&addr_v4(100, 0, 0, 1)));     // 100.0.x.x
        assert!(!is_tailscale_ip(&addr_v4(100, 63, 255, 255))); // Just below range
        assert!(!is_tailscale_ip(&addr_v4(100, 128, 0, 0)));   // Just above range
        assert!(!is_tailscale_ip(&addr_v4(192, 168, 1, 1)));   // Private
        assert!(!is_tailscale_ip(&addr_v4(10, 0, 0, 1)));      // Private
        assert!(!is_tailscale_ip(&addr_v4(8, 8, 8, 8)));       // Public
    }

    #[test]
    fn test_tailscale_ipv6_valid() {
        // fd7a:115c:a1e0::/48
        assert!(is_tailscale_ip(&addr_v6([0xfd7a, 0x115c, 0xa1e0, 0x0000, 0, 0, 0, 1])));
        assert!(is_tailscale_ip(&addr_v6([0xfd7a, 0x115c, 0xa1e0, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff])));
    }

    #[test]
    fn test_tailscale_ipv6_invalid() {
        // Different prefix
        assert!(!is_tailscale_ip(&addr_v6([0xfd7a, 0x115c, 0xa1e1, 0, 0, 0, 0, 1]))); // Wrong 3rd segment
        assert!(!is_tailscale_ip(&addr_v6([0xfd7a, 0x115d, 0xa1e0, 0, 0, 0, 0, 1]))); // Wrong 2nd segment
        assert!(!is_tailscale_ip(&addr_v6([0x2001, 0x0db8, 0x0000, 0, 0, 0, 0, 1]))); // Public IPv6
    }

    #[test]
    fn test_valid_view_name() {
        assert!(is_valid_view_name("token_holders"));
        assert!(is_valid_view_name("my_view_123"));
        assert!(is_valid_view_name("View1"));
        
        assert!(!is_valid_view_name(""));
        assert!(!is_valid_view_name("123view")); // Starts with number
        assert!(!is_valid_view_name("my-view")); // Has hyphen
        assert!(!is_valid_view_name("my view")); // Has space
    }
}
