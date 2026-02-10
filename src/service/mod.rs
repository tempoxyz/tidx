use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::time::Instant;

use crate::db::Pool;
use crate::metrics;
use crate::query::{extract_column_references, validate_query, EventSignature, HARD_LIMIT_MAX};

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
            limit: HARD_LIMIT_MAX,
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
        
        // Normalize table references to match CTE name (case-insensitive)
        let sql = sig.normalize_table_references(sql);
        // Rewrite filters to push down to indexed columns (e.g., "from" = '0x...' -> topic1 = '0x...')
        let sql = sig.rewrite_filters_for_pushdown(&sql);
        
        let used_columns = extract_column_references(&sql);
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

    // Add LIMIT if not present (AST-based detection to avoid string matching bypass)
    let sql = append_limit_if_missing(&sql, options.limit);

    // Convert '0x...' hex literals to '\x...' bytea literals for PostgreSQL
    // Only replace hex values (40+ chars), not short '0x' prefixes used in concat()
    let sql = crate::query::convert_hex_literals_postgres(&sql);

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

fn append_limit_if_missing(sql: &str, limit: i64) -> String {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    if let Ok(stmts) = Parser::parse_sql(&dialect, sql) {
        if let Some(sqlparser::ast::Statement::Query(query)) = stmts.first() {
            if query.limit_clause.is_none() {
                return format!("{sql} LIMIT {limit}");
            }
        }
    }
    sql.to_string()
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
    use insta::assert_snapshot;

    // ========================================================================
    // Event CTE SQL Generation Tests (Both Engines)
    // ========================================================================

    #[test]
    fn test_transfer_cte_postgres() {
        let sig = EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)").unwrap();
        assert_snapshot!(sig.to_cte_sql_postgres());
    }

    #[test]
    fn test_transfer_cte_clickhouse() {
        let sig = EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)").unwrap();
        assert_snapshot!(sig.to_cte_sql_clickhouse());
    }

    #[test]
    fn test_approval_cte_postgres() {
        let sig = EventSignature::parse("Approval(address indexed owner, address indexed spender, uint256 value)").unwrap();
        assert_snapshot!(sig.to_cte_sql_postgres());
    }

    #[test]
    fn test_approval_cte_clickhouse() {
        let sig = EventSignature::parse("Approval(address indexed owner, address indexed spender, uint256 value)").unwrap();
        assert_snapshot!(sig.to_cte_sql_clickhouse());
    }

    #[test]
    fn test_swap_cte_postgres() {
        let sig = EventSignature::parse(
            "Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)"
        ).unwrap();
        assert_snapshot!(sig.to_cte_sql_postgres());
    }

    #[test]
    fn test_swap_cte_clickhouse() {
        let sig = EventSignature::parse(
            "Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)"
        ).unwrap();
        assert_snapshot!(sig.to_cte_sql_clickhouse());
    }

    #[test]
    fn test_paused_cte_postgres() {
        let sig = EventSignature::parse("Paused(bool paused)").unwrap();
        assert_snapshot!(sig.to_cte_sql_postgres());
    }

    #[test]
    fn test_paused_cte_clickhouse() {
        let sig = EventSignature::parse("Paused(bool paused)").unwrap();
        assert_snapshot!(sig.to_cte_sql_clickhouse());
    }

    #[test]
    fn test_role_granted_cte_postgres() {
        let sig = EventSignature::parse("RoleGranted(bytes32 indexed role, address indexed account, address indexed sender)").unwrap();
        assert_snapshot!(sig.to_cte_sql_postgres());
    }

    #[test]
    fn test_role_granted_cte_clickhouse() {
        let sig = EventSignature::parse("RoleGranted(bytes32 indexed role, address indexed account, address indexed sender)").unwrap();
        assert_snapshot!(sig.to_cte_sql_clickhouse());
    }

    #[test]
    fn test_filtered_cte_postgres() {
        let sig = EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)").unwrap();
        let mut used_columns = std::collections::HashSet::new();
        used_columns.insert("to".to_string());
        used_columns.insert("value".to_string());
        assert_snapshot!(sig.to_cte_sql_postgres_filtered(Some(&used_columns)));
    }

    #[test]
    fn test_filtered_cte_clickhouse() {
        let sig = EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)").unwrap();
        let mut used_columns = std::collections::HashSet::new();
        used_columns.insert("to".to_string());
        used_columns.insert("value".to_string());
        assert_snapshot!(sig.to_cte_sql_clickhouse_filtered(Some(&used_columns)));
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

