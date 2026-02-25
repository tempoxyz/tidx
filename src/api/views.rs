//! Views API for managing ClickHouse materialized views

use axum::{
    extract::{ConnectInfo, Path, Query, State},
    Json,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

use super::{AppState, ApiError};
use crate::query::EventSignature;

/// Validate view name (alphanumeric + underscore only)
fn is_valid_view_name(name: &str) -> bool {
    is_valid_identifier(name)
}

/// Validate that a string is a safe SQL identifier.
/// Allows `[a-zA-Z_][a-zA-Z0-9_]{0,63}`.
fn is_valid_identifier(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name.chars().next().is_some_and(|c| c.is_ascii_alphabetic())
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Allowed ClickHouse table engines for materialized views.
const ALLOWED_ENGINES: &[&str] = &[
    "SummingMergeTree()",
    "AggregatingMergeTree()",
    "ReplacingMergeTree()",
    "MergeTree()",
];

#[derive(Deserialize)]
pub struct ChainQuery {
    #[serde(alias = "chain_id")]
    #[serde(rename = "chainId")]
    chain_id: u64,
}

#[derive(Serialize)]
pub struct ColumnInfo {
    name: String,
    #[serde(rename = "type")]
    col_type: String,
}

#[derive(Serialize)]
pub struct ViewInfo {
    database: String,
    engine: String,
    name: String,
    columns: Vec<ColumnInfo>,
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

    let result = clickhouse.query(&sql, &[]).await
        .map_err(|e| ApiError::QueryError(e.to_string()))?;

    let mut views = Vec::new();
    for row in &result.rows {
        let name = row.first().and_then(|v| v.as_str()).unwrap_or("").to_string();
        let engine = row.get(1).and_then(|v| v.as_str()).unwrap_or("").to_string();
        
        // Get columns for this view
        let columns_sql = format!(
            "SELECT name, type FROM system.columns WHERE database = '{}' AND table = '{}' ORDER BY position",
            database, name
        );
        let columns_result = clickhouse.query(&columns_sql, &[]).await
            .map_err(|e| ApiError::QueryError(e.to_string()))?;
        
        let columns: Vec<ColumnInfo> = columns_result.rows.iter().map(|col_row| {
            ColumnInfo {
                name: col_row.first().and_then(|v| v.as_str()).unwrap_or("").to_string(),
                col_type: col_row.get(1).and_then(|v| v.as_str()).unwrap_or("").to_string(),
            }
        }).collect();
        
        views.push(ViewInfo {
            name,
            engine,
            database: database.clone(),
            columns,
        });
    }

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
    /// Optional event signature for automatic CTE generation and decoding.
    /// E.g., "Transfer(address indexed from, address indexed to, uint256 value)"
    signature: Option<String>,
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

/// POST /views - Create a materialized view (trusted IP required)
pub async fn create_view(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(req): Json<CreateViewRequest>,
) -> Result<Json<CreateViewResponse>, ApiError> {
    // Check trusted IP access
    if !state.is_trusted_ip(&addr) {
        return Err(ApiError::Forbidden("Mutations only allowed from trusted IPs".to_string()));
    }

    // Validate view name
    if !is_valid_view_name(&req.name) {
        return Err(ApiError::BadRequest("Invalid view name: must be alphanumeric with underscores".to_string()));
    }

    // Validate order_by columns are safe identifiers
    if req.order_by.is_empty() {
        return Err(ApiError::BadRequest("orderBy is required".to_string()));
    }
    for col in &req.order_by {
        if !is_valid_identifier(col) {
            return Err(ApiError::BadRequest(format!(
                "Invalid orderBy column '{col}': must be alphanumeric with underscores"
            )));
        }
    }

    // Validate engine is an allowed ClickHouse engine
    if !ALLOWED_ENGINES.contains(&req.engine.as_str()) {
        return Err(ApiError::BadRequest(format!(
            "Invalid engine '{}': must be one of {:?}",
            req.engine, ALLOWED_ENGINES
        )));
    }

    // Validate SQL is SELECT only
    let sql_upper = req.sql.trim().to_uppercase();
    if !sql_upper.starts_with("SELECT") {
        return Err(ApiError::BadRequest("SQL must be a SELECT statement".to_string()));
    }

    // Parse signature if provided
    let signature = if let Some(ref sig_str) = req.signature {
        Some(EventSignature::parse(sig_str)
            .map_err(|e| ApiError::BadRequest(format!("Invalid signature: {}", e)))?)
    } else {
        None
    };

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

    // If signature provided, generate CTE with decoded columns and apply predicate pushdown
    let sql = if let Some(ref sig) = signature {
        let sql = sig.normalize_table_references(&req.sql);
        let sql = sig.rewrite_filters_for_pushdown(&sql);
        let cte = sig.to_cte_sql_clickhouse();
        format!("WITH {} {}", cte, sql)
    } else {
        req.sql.clone()
    };
    
    // 1. Ensure database exists
    let create_db = format!("CREATE DATABASE IF NOT EXISTS {}", database);
    clickhouse.query(&create_db, &[]).await
        .map_err(|e| ApiError::QueryError(format!("Failed to create database: {}", e)))?;

    // 2. Create target table (infer schema from SELECT ... LIMIT 0)
    let create_table = format!(
        "CREATE TABLE IF NOT EXISTS {}.{} ENGINE = {} ORDER BY ({}) AS {} LIMIT 0",
        database, table_name, req.engine, order_by, sql
    );
    clickhouse.query(&create_table, &[]).await
        .map_err(|e| ApiError::QueryError(format!("Failed to create table: {}", e)))?;

    // 3. Create materialized view
    let create_mv = format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {}.{} TO {}.{} AS {}",
        database, mv_name, database, table_name, sql
    );
    clickhouse.query(&create_mv, &[]).await
        .map_err(|e| ApiError::QueryError(format!("Failed to create materialized view: {}", e)))?;

    // 4. Backfill existing data
    let backfill = format!(
        "INSERT INTO {}.{} {}",
        database, table_name, sql
    );
    clickhouse.query(&backfill, &[]).await
        .map_err(|e| ApiError::QueryError(format!("Failed to backfill: {}", e)))?;

    // 5. Get row count
    let count_sql = format!("SELECT count() FROM {}.{}", database, table_name);
    let count_result = clickhouse.query(&count_sql, &[]).await
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
            columns: vec![],
        },
        backfill_rows,
    }))
}

#[derive(Serialize)]
pub struct DeleteViewResponse {
    deleted: Vec<String>,
    ok: bool,
}

/// DELETE /views/{name}?chainId=42431 - Delete a view (trusted IP required)
pub async fn delete_view(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Path(name): Path<String>,
    Query(params): Query<ChainQuery>,
) -> Result<Json<DeleteViewResponse>, ApiError> {
    // Check trusted IP access
    if !state.is_trusted_ip(&addr) {
        return Err(ApiError::Forbidden("Mutations only allowed from trusted IPs".to_string()));
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
    if clickhouse.query(&drop_mv, &[]).await.is_ok() {
        deleted.push(mv_name);
    }

    // Drop target table
    let drop_table = format!("DROP TABLE IF EXISTS {}.{}", database, name);
    if clickhouse.query(&drop_table, &[]).await.is_ok() {
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

    // Get view definition
    let sql = format!(
        "SELECT engine, create_table_query FROM system.tables WHERE database = '{}' AND name = '{}'",
        database, name
    );
    let result = clickhouse.query(&sql, &[]).await
        .map_err(|e| ApiError::QueryError(e.to_string()))?;

    if result.rows.is_empty() {
        return Err(ApiError::NotFound(format!("View '{}' not found", name)));
    }

    let row = &result.rows[0];
    let engine = row.first().and_then(|v| v.as_str()).unwrap_or("").to_string();
    let definition = row.get(1).and_then(|v| v.as_str()).unwrap_or("").to_string();

    // Get row count
    let count_sql = format!("SELECT count() FROM {}.{}", database, name);
    let count_result = clickhouse.query(&count_sql, &[]).await.ok();
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
            columns: vec![],
        },
        definition,
        row_count,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use insta::assert_snapshot;

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

    // ========================================================================
    // Helper to generate full SQL from signature + user query
    // ========================================================================
    
    fn generate_view_sql(signature: &str, user_sql: &str) -> String {
        let sig = EventSignature::parse(signature).unwrap();
        let sql = sig.rewrite_filters_for_pushdown(user_sql);
        let cte = sig.to_cte_sql_clickhouse();
        format!("WITH {} {}", cte, sql)
    }
    
    /// Generate runtime SQL (what actually gets executed against ClickHouse)
    fn generate_runtime_sql(signature: &str, user_sql: &str) -> String {
        generate_view_sql(signature, user_sql)
    }

    // ========================================================================
    // Snapshot Tests for CTE Generation
    // ========================================================================

    // ========================================================================
    // CTE Snapshot Tests
    // ========================================================================

    #[test]
    fn test_cte_transfer() {
        let sql = generate_view_sql(
            "Transfer(address indexed from, address indexed to, uint256 value)",
            r#"SELECT "to", SUM("value") as total FROM Transfer GROUP BY "to""#,
        );
        assert_snapshot!(sql);
    }

    #[test]
    fn test_cte_swap() {
        let sql = generate_view_sql(
            "Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)",
            r#"SELECT address, "sender", "amount0In" FROM Swap LIMIT 10"#,
        );
        assert_snapshot!(sql);
    }

    #[test]
    fn test_cte_bool_param() {
        let sig = EventSignature::parse("Paused(bool paused)").unwrap();
        assert_snapshot!(sig.to_cte_sql_clickhouse());
    }

    #[test]
    fn test_cte_bytes32_indexed() {
        let sig = EventSignature::parse(
            "RoleGranted(bytes32 indexed role, address indexed account, address indexed sender)"
        ).unwrap();
        assert_snapshot!(sig.to_cte_sql_clickhouse());
    }

    #[test]
    fn test_cte_int256() {
        let sig = EventSignature::parse("PriceUpdate(int256 price)").unwrap();
        assert_snapshot!(sig.to_cte_sql_clickhouse());
    }

    #[test]
    fn test_cte_approval() {
        let sig = EventSignature::parse(
            "Approval(address indexed owner, address indexed spender, uint256 value)"
        ).unwrap();
        assert_snapshot!(sig.to_cte_sql_clickhouse());
    }

    #[test]
    fn test_cte_unnamed_params() {
        let sig = EventSignature::parse(
            "Transfer(address indexed, address indexed, uint256)"
        ).unwrap();
        assert_snapshot!(sig.to_cte_sql_clickhouse());
    }

    #[test]
    fn test_cte_deposit() {
        let sig = EventSignature::parse(
            "Deposit(address indexed dst, uint256 wad)"
        ).unwrap();
        assert_snapshot!(sig.to_cte_sql_clickhouse());
    }

    // ========================================================================
    // Predicate Pushdown Snapshot Tests
    // ========================================================================

    #[test]
    fn test_pushdown_single_address() {
        let sql = generate_view_sql(
            "Transfer(address indexed from, address indexed to, uint256 value)",
            r#"SELECT "value" FROM Transfer WHERE "from" = '0xdAC17F958D2ee523a2206206994597C13D831ec7'"#,
        );
        assert_snapshot!(sql);
    }

    #[test]
    fn test_pushdown_multiple_addresses() {
        let sql = generate_view_sql(
            "Transfer(address indexed from, address indexed to, uint256 value)",
            r#"SELECT "value" FROM Transfer WHERE "from" = '0xdAC17F958D2ee523a2206206994597C13D831ec7' AND "to" = '0xa726a1CD723409074DF9108A2187cfA19899aCF8'"#,
        );
        assert_snapshot!(sql);
    }

    #[test]
    fn test_pushdown_non_indexed_unchanged() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)"
        ).unwrap();
        let user_sql = r#"SELECT * FROM Transfer WHERE "value" > 1000000"#;
        let rewritten = sig.rewrite_filters_for_pushdown(user_sql);
        assert_eq!(user_sql, rewritten); // Should be unchanged
    }

    #[test]
    fn test_pushdown_invalid_address_unchanged() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)"
        ).unwrap();
        let user_sql = r#"SELECT * FROM Transfer WHERE "from" = '0xabc'"#;
        let rewritten = sig.rewrite_filters_for_pushdown(user_sql);
        assert_eq!(user_sql, rewritten); // Should be unchanged
    }

    // ========================================================================
    // Runtime SQL Format Tests (verifies 0x -> \x conversion)
    // ========================================================================

    #[test]
    fn test_runtime_sql_with_pushdown() {
        let sql = generate_runtime_sql(
            "Transfer(address indexed from, address indexed to, uint256 value)",
            r#"SELECT "value" FROM Transfer WHERE "from" = '0xdAC17F958D2ee523a2206206994597C13D831ec7'"#,
        );
        assert_snapshot!(sql);
    }

    #[test]
    fn test_runtime_sql_simple_query() {
        let sql = generate_runtime_sql(
            "Transfer(address indexed from, address indexed to, uint256 value)",
            r#"SELECT * FROM Transfer LIMIT 1"#,
        );
        assert_snapshot!(sql);
    }

    // ========================================================================
    // Signature Parse Error Tests
    // ========================================================================

    #[test]
    fn test_signature_parse_errors() {
        assert!(EventSignature::parse("Transfer(address indexed from").is_err());
        assert!(EventSignature::parse("(address from)").is_err());
        assert!(EventSignature::parse("Transfer(invalid_type from)").is_err());
    }

    // ========================================================================
    // Complex Real-World View Snapshot Tests
    // ========================================================================

    #[test]
    fn test_view_token_holders() {
        let sql = generate_view_sql(
            "Transfer(address indexed from, address indexed to, uint256 value)",
            r#"SELECT 
                address as token,
                holder,
                SUM(delta) as balance
            FROM (
                SELECT address, "to" as holder, CAST("value" AS Int256) as delta FROM Transfer
                UNION ALL
                SELECT address, "from" as holder, -CAST("value" AS Int256) as delta FROM Transfer
            )
            GROUP BY token, holder
            HAVING balance > 0"#,
        );
        assert_snapshot!(sql);
    }

    #[test]
    fn test_view_token_supply() {
        let sql = generate_view_sql(
            "Transfer(address indexed from, address indexed to, uint256 value)",
            r#"SELECT 
                address as token,
                SUM(CASE 
                    WHEN "from" = '0x0000000000000000000000000000000000000000' THEN CAST("value" AS Int256)
                    WHEN "to" = '0x0000000000000000000000000000000000000000' THEN -CAST("value" AS Int256)
                    ELSE 0
                END) as supply
            FROM Transfer
            GROUP BY token"#,
        );
        assert_snapshot!(sql);
    }

    #[test]
    fn test_view_transfer_counts() {
        let sql = generate_view_sql(
            "Transfer(address indexed from, address indexed to, uint256 value)",
            r#"SELECT 
                addr,
                SUM(sent) as total_sent,
                SUM(received) as total_received
            FROM (
                SELECT "from" as addr, 1 as sent, 0 as received FROM Transfer
                UNION ALL
                SELECT "to" as addr, 0 as sent, 1 as received FROM Transfer
            )
            GROUP BY addr"#,
        );
        assert_snapshot!(sql);
    }

    #[test]
    fn test_view_uniswap_volume() {
        let sql = generate_view_sql(
            "Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)",
            r#"SELECT 
                address as pair,
                toStartOfHour(block_timestamp) as hour,
                COUNT(*) as swap_count,
                SUM("amount0In") + SUM("amount0Out") as volume0,
                SUM("amount1In") + SUM("amount1Out") as volume1
            FROM Swap
            GROUP BY pair, hour"#,
        );
        assert_snapshot!(sql);
    }

    #[test]
    fn test_view_approval_allowances() {
        let sql = generate_view_sql(
            "Approval(address indexed owner, address indexed spender, uint256 value)",
            r#"SELECT 
                address as token,
                "owner",
                "spender",
                argMax("value", block_num) as current_allowance
            FROM Approval
            GROUP BY token, "owner", "spender""#,
        );
        assert_snapshot!(sql);
    }

    #[test]
    fn test_view_daily_stats() {
        let sql = generate_view_sql(
            "Transfer(address indexed from, address indexed to, uint256 value)",
            r#"SELECT 
                toDate(block_timestamp) as day,
                address as token,
                COUNT(*) as transfer_count,
                COUNT(DISTINCT "from") as unique_senders,
                COUNT(DISTINCT "to") as unique_receivers,
                SUM("value") as total_volume
            FROM Transfer
            GROUP BY day, token"#,
        );
        assert_snapshot!(sql);
    }

    #[test]
    fn test_view_whale_transfers() {
        let sql = generate_view_sql(
            "Transfer(address indexed from, address indexed to, uint256 value)",
            r#"SELECT block_num, "from", "to", "value"
            FROM Transfer
            WHERE "value" > 1000000000000000000000
            ORDER BY block_num DESC"#,
        );
        assert_snapshot!(sql);
    }

    #[test]
    fn test_view_filtered_token_holders() {
        let sql = generate_view_sql(
            "Transfer(address indexed from, address indexed to, uint256 value)",
            r#"SELECT "to" as holder, SUM(CAST("value" AS Int256)) as balance
            FROM Transfer
            WHERE address = '0xdAC17F958D2ee523a2206206994597C13D831ec7'
            GROUP BY holder
            HAVING balance > 0"#,
        );
        assert_snapshot!(sql);
    }
}
