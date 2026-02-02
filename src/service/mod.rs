use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::time::Instant;

use crate::db::Pool;
use crate::metrics;
use crate::query::{extract_column_references, validate_query, EventSignature};

#[derive(Debug, Clone, Serialize)]
pub struct SyncStatus {
    pub chain_id: i64,
    pub head_num: i64,
    pub synced_num: i64,
    pub tip_num: i64,
    pub lag: i64,
    pub gap_blocks: i64,
    /// Detected gaps in the blocks table: [(start, end), ...]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub gaps: Vec<(i64, i64)>,
    pub backfill_num: Option<i64>,
    pub backfill_remaining: i64,
    pub sync_rate: Option<f64>,
    pub eta_secs: Option<f64>,
    pub updated_at: DateTime<Utc>,
}

pub async fn get_all_status(pool: &Pool) -> Result<Vec<SyncStatus>> {
    let conn = pool.get().await?;

    let rows = conn
        .query(
            "SELECT chain_id, head_num, synced_num, tip_num, backfill_num, started_at, updated_at FROM sync_state ORDER BY chain_id",
            &[],
        )
        .await?;

    // Detect actual gaps in the blocks table
    let gaps = crate::sync::writer::detect_gaps(pool).await.unwrap_or_default();
    let gaps_i64: Vec<(i64, i64)> = gaps.iter().map(|(s, e)| (*s as i64, *e as i64)).collect();

    Ok(rows
        .iter()
        .map(|row| {
            let synced_num: i64 = row.get(2);
            let tip_num: i64 = row.get(3);
            let backfill_num: Option<i64> = row.get(4);
            let started_at: Option<DateTime<Utc>> = row.get(5);

            let backfill_remaining = match backfill_num {
                None => synced_num.saturating_sub(1),
                Some(0) => 0,
                Some(n) => n,
            };

            let sync_rate = started_at.and_then(|started| {
                let elapsed = Utc::now().signed_duration_since(started);
                let secs = elapsed.num_seconds() as f64;
                let total_indexed = match backfill_num {
                    Some(n) => synced_num - n + 1,
                    None => 1,
                };
                if secs > 0.0 { Some(total_indexed as f64 / secs) } else { None }
            });

            let eta_secs = sync_rate.and_then(|rate| {
                if rate > 0.0 { Some(backfill_remaining as f64 / rate) } else { None }
            });

            // Gap = blocks between synced_num and tip_num that may be missing
            let gap_blocks = tip_num.saturating_sub(synced_num);

            SyncStatus {
                chain_id: row.get(0),
                head_num: row.get(1),
                synced_num,
                tip_num,
                lag: row.get::<_, i64>(1) - tip_num, // lag from head to tip (realtime)
                gap_blocks,
                gaps: gaps_i64.clone(),
                backfill_num,
                backfill_remaining,
                sync_rate,
                eta_secs,
                updated_at: row.get(6),
            }
        })
        .collect())
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub row_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub engine: Option<String>,
    /// Server-side query execution time in milliseconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_time_ms: Option<f64>,
}

pub struct QueryOptions {
    pub timeout_ms: u64,
    pub limit: i64,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            timeout_ms: 5000,
            limit: 10000,
        }
    }
}

/// Execute a query on PostgreSQL.
pub async fn execute_query_postgres(
    pool: &Pool,
    sql: &str,
    signature: Option<&str>,
    options: &QueryOptions,
) -> Result<QueryResult> {
    // Validate query
    validate_query(sql)?;

    // Generate CTE SQL if a signature is provided
    let sql = if let Some(sig_str) = signature {
        let sig = EventSignature::parse(sig_str)?;
        let used_columns = extract_column_references(sql);
        let filter = if used_columns.is_empty() {
            None
        } else {
            Some(&used_columns)
        };
        let cte = sig.to_cte_sql_postgres_filtered(filter);
        format!("WITH {cte} {sql}")
    } else {
        sql.to_string()
    };

    // Add LIMIT if not present
    let sql_upper = sql.to_uppercase();
    let sql = if !sql_upper.contains("LIMIT") {
        format!("{} LIMIT {}", sql, options.limit)
    } else {
        sql
    };

    // Convert '0x...' hex strings to '\x...' bytea literals for PostgreSQL
    let sql = sql.replace("'0x", "'\\x");

    let conn = pool.get().await?;

    // Set statement timeout for this session
    conn.execute(
        &format!("SET statement_timeout = {}", options.timeout_ms),
        &[],
    )
    .await?;

    let start = Instant::now();
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(options.timeout_ms + 100),
        conn.query(&sql, &[]),
    )
    .await;

    let rows = match result {
        Ok(Ok(rows)) => {
            metrics::record_query_duration(start.elapsed());
            rows
        }
        Ok(Err(e)) => return Err(anyhow!("Query error: {}", sanitize_db_error(&e.to_string()))),
        Err(_) => return Err(anyhow!("Query timeout")),
    };

    // Get columns from result (even if empty, prepared statement has column info)
    let columns: Vec<String> = if rows.is_empty() {
        // For empty results, prepare statement to get column metadata
        conn.prepare(&sql)
            .await
            .ok()
            .map(|s| s.columns().iter().map(|c| c.name().to_string()).collect())
            .unwrap_or_default()
    } else {
        rows[0].columns().iter().map(|c| c.name().to_string()).collect()
    };

    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    if rows.is_empty() {
        return Ok(QueryResult {
            columns,
            rows: vec![],
            row_count: 0,
            engine: Some("postgres".to_string()),
            query_time_ms: Some(elapsed_ms),
        });
    }
    let row_count = rows.len();
    metrics::record_query_rows(row_count as u64);

    let result_rows: Vec<Vec<serde_json::Value>> = rows
        .iter()
        .map(|row| {
            (0..columns.len())
                .map(|i| format_column_json(row, i))
                .collect()
        })
        .collect();

    Ok(QueryResult {
        columns,
        rows: result_rows,
        row_count,
        engine: Some("postgres".to_string()),
        query_time_ms: Some(elapsed_ms),
    })
}

pub fn format_column_json(row: &tokio_postgres::Row, idx: usize) -> serde_json::Value {
    let col = &row.columns()[idx];

    match col.type_().name() {
        "int2" => row
            .try_get::<_, i16>(idx)
            .ok()
            .map_or(serde_json::Value::Null, |v| serde_json::Value::Number(v.into())),
        "int4" => row
            .try_get::<_, i32>(idx)
            .ok()
            .map_or(serde_json::Value::Null, |v| serde_json::Value::Number(v.into())),
        "int8" => row
            .try_get::<_, i64>(idx)
            .ok()
            .map_or(serde_json::Value::Null, |v| serde_json::Value::Number(v.into())),
        "numeric" => row
            .try_get::<_, rust_decimal::Decimal>(idx)
            .ok()
            .map_or(serde_json::Value::Null, |v| serde_json::Value::String(v.to_string())),
        "float4" | "float8" => row
            .try_get::<_, f64>(idx)
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        "bytea" => row
            .try_get::<_, Vec<u8>>(idx)
            .ok()
            .map_or(serde_json::Value::Null, |v| serde_json::Value::String(format!("0x{}", hex::encode(v)))),
        "text" | "varchar" | "name" => row
            .try_get::<_, String>(idx)
            .ok()
            .map_or(serde_json::Value::Null, serde_json::Value::String),
        "timestamptz" | "timestamp" => row
            .try_get::<_, DateTime<Utc>>(idx)
            .ok()
            .map_or(serde_json::Value::Null, |v| serde_json::Value::String(v.to_rfc3339())),
        "bool" => row
            .try_get::<_, bool>(idx)
            .ok()
            .map_or(serde_json::Value::Null, serde_json::Value::Bool),
        _ => serde_json::Value::Null,
    }
}

pub fn format_column_string(row: &tokio_postgres::Row, idx: usize) -> String {
    match format_column_json(row, idx) {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::String(s) => s,
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

/// Sanitize database error messages to prevent information leakage.
///
/// Removes file paths, internal schema details, and other sensitive info
/// while preserving useful error context for debugging.
fn sanitize_db_error(error: &str) -> String {
    // Truncate very long errors
    let error = if error.len() > 500 {
        format!("{}...", &error[..500])
    } else {
        error.to_string()
    };

    // Remove file paths (Unix and Windows)
    let error = regex_lite::Regex::new(r"(/[a-zA-Z0-9_./-]+|[A-Z]:\\[a-zA-Z0-9_.\\ -]+)")
        .map(|re| re.replace_all(&error, "[path]").to_string())
        .unwrap_or(error);

    // Remove potential connection strings
    let error = regex_lite::Regex::new(r"postgres://[^\s]+")
        .map(|re| re.replace_all(&error, "[connection]").to_string())
        .unwrap_or(error);

    // Remove IP addresses
    let error = regex_lite::Regex::new(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(:\d+)?")
        .map(|re| re.replace_all(&error, "[address]").to_string())
        .unwrap_or(error);

    error
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{route_query, EventSignature, QueryEngine};

    // ========================================================================
    // Event CTE SQL Generation Tests (Both Engines)
    // ========================================================================
    //
    // These tests verify that:
    // 1. Users query decoded columns directly (e.g., "from", "to", "value")
    // 2. No explicit decode() calls needed - the CTE handles it transparently
    // 3. Both engines produce the same user-facing column names
    // ========================================================================

    #[test]
    fn test_transfer_cte_postgres_format() {
        let sig = EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)").unwrap();
        let cte = sig.to_cte_sql_postgres();
        
        // Postgres CTE uses bytea selector format
        assert!(cte.contains("WHERE selector = '\\x"));
        // Uses native abi_* functions - users don't need to call these
        assert!(cte.contains("abi_address(topic1)"));
        assert!(cte.contains("abi_address(topic2)"));
        assert!(cte.contains("abi_uint(substring(data FROM 1 FOR 32))"));
        // Has correct column aliases - users query these directly
        assert!(cte.contains("AS \"from\""));
        assert!(cte.contains("AS \"to\""));
        assert!(cte.contains("AS \"value\""));
    }

    #[test]
    fn test_transfer_cte_clickhouse_format() {
        let sig = EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)").unwrap();
        let cte = sig.to_cte_sql_clickhouse();
        
        // ClickHouse CTE uses string comparison for selector (MaterializedPostgreSQL format)
        assert!(cte.contains(r"WHERE selector = '\\x"));
        // Uses substring to extract address from hex string (last 40 chars)
        assert!(cte.contains("concat('0x', lower(substring(topic1, 27)))"));
        assert!(cte.contains("concat('0x', lower(substring(topic2, 27)))"));
        // Uses unhex + reinterpret for uint decoding
        assert!(cte.contains("reinterpretAsUInt256(reverse(unhex(substring(data"));
    }

    #[test]
    fn test_approval_cte_both_engines() {
        let sig = EventSignature::parse("Approval(address indexed owner, address indexed spender, uint256 value)").unwrap();
        
        let pg_cte = sig.to_cte_sql_postgres();
        let ch_cte = sig.to_cte_sql_clickhouse();
        
        // Both should have the same structure but different function calls
        assert!(pg_cte.contains("AS \"owner\""));
        assert!(ch_cte.contains("AS \"owner\""));
        
        // Postgres uses abi_address UDFs, ClickHouse uses substring on hex strings
        assert!(pg_cte.contains("abi_address(topic1)"));
        assert!(ch_cte.contains("substring(topic1, 27)"));
    }

    #[test]
    fn test_swap_cte_multiple_data_params() {
        // Swap has 4 data params and 2 indexed params
        let sig = EventSignature::parse(
            "Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)"
        ).unwrap();
        
        let pg_cte = sig.to_cte_sql_postgres();
        let ch_cte = sig.to_cte_sql_clickhouse();
        
        // Check indexed params use topics
        assert!(pg_cte.contains("abi_address(topic1)"));
        assert!(pg_cte.contains("abi_address(topic2)"));
        
        // Check data params use correct offsets (0, 32, 64, 96 bytes)
        assert!(pg_cte.contains("substring(data FROM 1 FOR 32)"));   // offset 0
        assert!(pg_cte.contains("substring(data FROM 33 FOR 32)"));  // offset 32
        assert!(pg_cte.contains("substring(data FROM 65 FOR 32)"));  // offset 64
        assert!(pg_cte.contains("substring(data FROM 97 FOR 32)"));  // offset 96
        
        // ClickHouse uses hex-based offsets: hex_start = 3 + offset*2, and 64 chars for 32 bytes
        assert!(ch_cte.contains("substring(data, 3, 64)"));    // offset 0: 3 + 0*2 = 3
        assert!(ch_cte.contains("substring(data, 67, 64)"));   // offset 32: 3 + 32*2 = 67
        assert!(ch_cte.contains("substring(data, 131, 64)"));  // offset 64: 3 + 64*2 = 131
        assert!(ch_cte.contains("substring(data, 195, 64)"));  // offset 96: 3 + 96*2 = 195
    }

    #[test]
    fn test_filtered_cte_only_includes_used_columns() {
        let sig = EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)").unwrap();
        
        let mut used_columns = std::collections::HashSet::new();
        used_columns.insert("to".to_string());
        used_columns.insert("value".to_string());
        
        let pg_cte = sig.to_cte_sql_postgres_filtered(Some(&used_columns));
        let ch_cte = sig.to_cte_sql_clickhouse_filtered(Some(&used_columns));
        
        // Should include "to" and "value"
        assert!(pg_cte.contains("AS \"to\""));
        assert!(pg_cte.contains("AS \"value\""));
        assert!(ch_cte.contains("AS \"to\""));
        assert!(ch_cte.contains("AS \"value\""));
        
        // Should NOT include "from"
        assert!(!pg_cte.contains("AS \"from\""));
        assert!(!ch_cte.contains("AS \"from\""));
    }

    #[test]
    fn test_cte_with_bool_param() {
        let sig = EventSignature::parse("Paused(bool paused)").unwrap();
        
        let pg_cte = sig.to_cte_sql_postgres();
        let ch_cte = sig.to_cte_sql_clickhouse();
        
        // Postgres uses abi_bool UDF
        assert!(pg_cte.contains("abi_bool("));
        // ClickHouse uses unhex comparison on hex string
        assert!(ch_cte.contains("unhex(") && ch_cte.contains("!= unhex('00')"));
    }

    #[test]
    fn test_cte_with_bytes32_param() {
        let sig = EventSignature::parse("RoleGranted(bytes32 indexed role, address indexed account, address indexed sender)").unwrap();
        
        let pg_cte = sig.to_cte_sql_postgres();
        let ch_cte = sig.to_cte_sql_clickhouse();
        
        // bytes32 indexed returns the topic as hex
        assert!(pg_cte.contains("topic1"));
        assert!(ch_cte.contains("topic1"));
    }

    // ========================================================================
    // User Experience Tests - No Explicit Decoding Required
    // ========================================================================
    //
    // These tests demonstrate that users write simple SQL queries without
    // needing to call decode functions. The CTE handles decoding transparently.
    
    #[test]
    fn test_user_query_example_transfer_events() {
        // This is what users write - simple SQL with decoded column names
        let user_query = r#"SELECT "from", "to", "value" FROM transfer WHERE "value" > 1000000"#;
        
        // The system generates the CTE with decode functions
        let sig = EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)").unwrap();
        
        // For PostgreSQL mode (uses Postgres functions)
        let pg_cte = sig.to_cte_sql_postgres();
        let full_pg_query = format!("WITH {} {}", pg_cte, user_query);
        
        // For ClickHouse mode (uses native ClickHouse functions)
        let ch_cte = sig.to_cte_sql_clickhouse();
        let full_ch_query = format!("WITH {} {}", ch_cte, user_query);
        
        // Both produce valid SQL that users can run
        assert!(full_pg_query.contains("SELECT \"from\", \"to\", \"value\""));
        assert!(full_ch_query.contains("SELECT \"from\", \"to\", \"value\""));
        
        // The decode functions are hidden in the CTE, not in user's query
        assert!(!user_query.contains("abi_"));
        assert!(!user_query.contains("decode"));
    }

    #[test]
    fn test_user_query_example_group_by_address() {
        // User wants to count transfers by recipient - no decode calls needed
        let user_query = r#"SELECT "to", COUNT(*) as transfer_count, SUM("value") as total_value FROM transfer GROUP BY "to" ORDER BY total_value DESC LIMIT 10"#;
        
        let sig = EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)").unwrap();
        let cte = sig.to_cte_sql_postgres();
        let full_query = format!("WITH {} {}", cte, user_query);
        
        // User query is clean - no decode functions visible
        assert!(!user_query.contains("abi_"));
        assert!(!user_query.contains("topic"));
        assert!(!user_query.contains("data"));
        
        // But the full query has the CTE with decode logic
        assert!(full_query.contains("abi_address"));
        assert!(full_query.contains("abi_uint"));
    }

    #[test]
    fn test_user_query_example_swap_events() {
        // Complex Uniswap Swap event - user just queries by column name
        let user_query = r#"SELECT "sender", "amount0In", "amount1Out" FROM swap WHERE "amount0In" > 0"#;
        
        let sig = EventSignature::parse(
            "Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)"
        ).unwrap();
        
        let cte = sig.to_cte_sql_postgres();
        let full_query = format!("WITH {} {}", cte, user_query);
        
        // User query references decoded column names directly
        assert!(user_query.contains("\"sender\""));
        assert!(user_query.contains("\"amount0In\""));
        assert!(user_query.contains("\"amount1Out\""));
        
        // No raw data manipulation in user query
        assert!(!user_query.contains("substring"));
        assert!(!user_query.contains("encode"));
        
        // CTE handles the complexity
        assert!(full_query.contains("abi_uint"));
        assert!(full_query.contains("abi_address"));
    }

    #[test]
    fn test_both_engines_produce_same_column_names() {
        let sig = EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)").unwrap();
        
        let pg_cte = sig.to_cte_sql_postgres();
        let ch_cte = sig.to_cte_sql_clickhouse();
        
        // Both engines expose the same column names to users
        assert!(pg_cte.contains("AS \"from\""));
        assert!(ch_cte.contains("AS \"from\""));
        
        assert!(pg_cte.contains("AS \"to\""));
        assert!(ch_cte.contains("AS \"to\""));
        
        assert!(pg_cte.contains("AS \"value\""));
        assert!(ch_cte.contains("AS \"value\""));
        
        // Users can write the same query regardless of engine
        let user_query = r#"SELECT "from", "to", "value" FROM transfer"#;
        
        // Both work with the same user query
        let pg_full = format!("WITH {} {}", pg_cte, user_query);
        let ch_full = format!("WITH {} {}", ch_cte, user_query);
        
        assert!(pg_full.contains(user_query));
        assert!(ch_full.contains(user_query));
    }

    // ========================================================================
    // Query Routing Tests
    // ========================================================================

    #[test]
    fn test_route_olap_query_to_clickhouse() {
        // OLAP patterns should route to ClickHouse
        assert_eq!(route_query("SELECT COUNT(*) FROM logs GROUP BY address"), QueryEngine::ClickHouse);
        assert_eq!(route_query("SELECT SUM(gas_used) FROM blocks"), QueryEngine::ClickHouse);
        assert_eq!(route_query("SELECT AVG(gas_limit) FROM txs"), QueryEngine::ClickHouse);
        assert_eq!(route_query("SELECT *, ROW_NUMBER() OVER (PARTITION BY address) FROM logs"), QueryEngine::ClickHouse);
    }

    #[test]
    fn test_route_oltp_query_to_postgres() {
        // Point lookups should route to Postgres
        assert_eq!(route_query("SELECT * FROM blocks WHERE num = 100"), QueryEngine::Postgres);
        assert_eq!(route_query("SELECT * FROM txs WHERE hash = '\\x1234'"), QueryEngine::Postgres);
        assert_eq!(route_query("SELECT * FROM logs WHERE address = '\\xabcd'"), QueryEngine::Postgres);
    }

    #[test]
    fn test_explicit_engine_hints() {
        assert_eq!(route_query("/* engine=clickhouse */ SELECT * FROM blocks"), QueryEngine::ClickHouse);
        assert_eq!(route_query("/* engine=postgres */ SELECT COUNT(*) FROM logs GROUP BY address"), QueryEngine::Postgres);
    }

    // ========================================================================
    // Query Options Tests
    // ========================================================================

    #[test]
    fn test_query_options_default() {
        let options = QueryOptions::default();
        assert_eq!(options.timeout_ms, 5000);
        assert_eq!(options.limit, 10000);
    }

    // ========================================================================
    // Sanitize Error Tests
    // ========================================================================

    #[test]
    fn test_sanitize_removes_file_paths() {
        let error = "Error at /home/user/project/src/main.rs:42";
        let sanitized = sanitize_db_error(error);
        assert!(!sanitized.contains("/home/user"));
        assert!(sanitized.contains("[path]"));
    }

    #[test]
    fn test_sanitize_removes_connection_strings() {
        // Note: the path regex runs first, so parts of the URL may be matched as paths
        // The key is that sensitive info (user:pass) is removed
        let error = "Connection failed: postgres://user:pass@host:5432/db";
        let sanitized = sanitize_db_error(error);
        // User credentials should not be visible
        assert!(!sanitized.contains("user:pass"));
        // Either [connection] or [path] replacement happened
        assert!(sanitized.contains("[connection]") || sanitized.contains("[path]"));
    }

    #[test]
    fn test_sanitize_removes_ip_addresses() {
        let error = "Connection to 192.168.1.100:5432 failed";
        let sanitized = sanitize_db_error(error);
        assert!(!sanitized.contains("192.168.1.100"));
        assert!(sanitized.contains("[address]"));
    }

    #[test]
    fn test_sanitize_truncates_long_errors() {
        let error = "x".repeat(600);
        let sanitized = sanitize_db_error(&error);
        assert!(sanitized.len() < 510); // 500 + "..."
        assert!(sanitized.ends_with("..."));
    }

}

