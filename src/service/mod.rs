use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::sync::Arc;
use std::time::Instant;

use crate::db::{execute_duckdb_query, DuckDbPool, Pool};
use crate::metrics;
use crate::query::{extract_column_references, route_query, validate_query, EventSignature, QueryEngine};

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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duckdb_min: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duckdb_max: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duckdb_tip_lag: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duckdb_backfill_remaining: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duckdb_internal_gaps: Option<i64>,
    // Deprecated: use duckdb_max instead
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duckdb_synced_num: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duckdb_lag: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duckdb_gap_blocks: Option<i64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub duckdb_gaps: Vec<(i64, i64)>,
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
                duckdb_min: None,
                duckdb_max: None,
                duckdb_tip_lag: None,
                duckdb_backfill_remaining: None,
                duckdb_internal_gaps: None,
                duckdb_synced_num: None,
                duckdb_lag: None,
                duckdb_gap_blocks: None,
                duckdb_gaps: Vec::new(),
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

/// Execute a query, automatically routing to DuckDB or PostgreSQL based on query patterns.
///
/// If `force_engine` is provided, it overrides the automatic routing decision.
/// Valid values: "postgres", "duckdb"
pub async fn execute_query(
    pg_pool: &Pool,
    duckdb_pool: Option<&Arc<DuckDbPool>>,
    sql: &str,
    signature: Option<&str>,
    options: &QueryOptions,
) -> Result<QueryResult> {
    execute_query_with_engine(pg_pool, duckdb_pool, sql, signature, options, None).await
}

/// Execute a query with optional engine override.
pub async fn execute_query_with_engine(
    pg_pool: &Pool,
    duckdb_pool: Option<&Arc<DuckDbPool>>,
    sql: &str,
    signature: Option<&str>,
    options: &QueryOptions,
    force_engine: Option<&str>,
) -> Result<QueryResult> {
    // Validate query using proper SQL parsing (prevents V1 comment bypass, V2/V13 dangerous functions)
    validate_query(sql)?;

    // Determine which engine to use (forced or auto-detected)
    let engine = match force_engine {
        Some("postgres") | Some("pg") => QueryEngine::Postgres,
        Some("duckdb") | Some("duck") => QueryEngine::DuckDb,
        _ => route_query(sql),
    };

    // For DuckDB queries without a DuckDB pool, fall back to Postgres
    let engine = if engine == QueryEngine::DuckDb && duckdb_pool.is_none() {
        QueryEngine::Postgres
    } else {
        engine
    };

    // Generate engine-specific CTE SQL if a signature is provided
    // Smart CTE: only decode columns that are actually used in the query
    let sql = if let Some(sig_str) = signature {
        let sig = EventSignature::parse(sig_str)?;
        let used_columns = extract_column_references(sql);
        // If empty (e.g., SELECT *), include all decoded columns
        let filter = if used_columns.is_empty() {
            None
        } else {
            Some(&used_columns)
        };
        let cte = match engine {
            QueryEngine::DuckDb => sig.to_cte_sql_duckdb_filtered(filter),
            QueryEngine::Postgres => sig.to_cte_sql_postgres_filtered(filter),
        };
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

    match engine {
        QueryEngine::DuckDb => {
            execute_query_duckdb(duckdb_pool.unwrap(), &sql, options).await
        }
        QueryEngine::Postgres => {
            execute_query_postgres(pg_pool, &sql, options).await
        }
    }
}

/// Execute a query on PostgreSQL.
async fn execute_query_postgres(
    pool: &Pool,
    sql: &str,
    options: &QueryOptions,
) -> Result<QueryResult> {
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

/// Execute a query on DuckDB.
///
/// Runs in a blocking thread pool to avoid blocking the async runtime.
/// Uses DuckDB's query timeout setting for actual cancellation.
async fn execute_query_duckdb(
    pool: &Arc<DuckDbPool>,
    sql: &str,
    options: &QueryOptions,
) -> Result<QueryResult> {
    let start = Instant::now();
    let pool = Arc::clone(pool);
    let sql = sql.to_string();
    let timeout_secs = (options.timeout_ms as f64 / 1000.0).max(1.0);

    // Run DuckDB query in blocking thread pool with timeout
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(options.timeout_ms + 500),
        tokio::task::spawn_blocking(move || {
            let conn = futures::executor::block_on(pool.conn());
            // Set query timeout at DuckDB level for actual cancellation
            let _ = conn.execute(
                &format!("SET query_timeout = '{}s'", timeout_secs as u64),
                [],
            );
            execute_duckdb_query(&conn, &sql)
        }),
    )
    .await;

    match result {
        Ok(Ok(Ok((columns, rows)))) => {
            let elapsed = start.elapsed();
            metrics::record_query_duration(elapsed);
            let row_count = rows.len();
            metrics::record_query_rows(row_count as u64);

            Ok(QueryResult {
                columns,
                rows,
                row_count,
                engine: Some("duckdb".to_string()),
                query_time_ms: Some(elapsed.as_secs_f64() * 1000.0),
            })
        }
        Ok(Ok(Err(e))) => Err(anyhow!("DuckDB query error: {}", sanitize_db_error(&e.to_string()))),
        Ok(Err(e)) => Err(anyhow!("DuckDB task error: {e}")),
        Err(_) => Err(anyhow!("Query timeout")),
    }
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

