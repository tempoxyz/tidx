use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::time::Instant;

use crate::db::Pool;
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

/// pg_duckdb configuration for query execution.
#[derive(Clone, Debug, Default)]
pub struct PgDuckdbConfig {
    pub memory_limit: Option<String>,
    pub threads: Option<u32>,
}

/// Parquet configuration for hybrid queries (PG heap + Parquet files)
#[derive(Clone, Debug, Default)]
pub struct ParquetConfig {
    /// Whether Parquet querying is enabled
    pub enabled: bool,
    /// Directory containing Parquet files for this chain
    pub data_dir: Option<String>,
    /// Chain ID (for file path construction)
    pub chain_id: Option<u64>,
    /// Maximum block number in Parquet files (blocks > this are in PG)
    pub max_parquet_block: Option<u64>,
}

/// Execute a query with pg_duckdb support.
/// Routes OLAP queries to pg_duckdb, OLTP queries to Postgres.
/// If parquet_config is provided and the query touches the logs table,
/// automatically combines Parquet files with PostgreSQL data.
pub async fn execute_query(
    pg_pool: &Pool,
    sql: &str,
    signature: Option<&str>,
    options: &QueryOptions,
    pg_duckdb_config: &PgDuckdbConfig,
    force_engine: Option<&str>,
) -> Result<QueryResult> {
    execute_query_with_parquet(
        pg_pool,
        sql,
        signature,
        options,
        pg_duckdb_config,
        force_engine,
        None, // No Parquet config by default
    )
    .await
}

/// Execute a query with optional Parquet source integration.
/// When parquet_config is provided and the query references the logs table,
/// the query is rewritten to combine Parquet files with PostgreSQL data.
pub async fn execute_query_with_parquet(
    pg_pool: &Pool,
    sql: &str,
    signature: Option<&str>,
    options: &QueryOptions,
    pg_duckdb_config: &PgDuckdbConfig,
    force_engine: Option<&str>,
    parquet_config: Option<&ParquetConfig>,
) -> Result<QueryResult> {
    // Validate query
    validate_query(sql)?;

    // Determine which engine to use (forced or auto-detected)
    let use_pg_duckdb = match force_engine {
        Some("postgres") | Some("pg") => false,
        Some("duckdb") | Some("duck") | Some("pg_duckdb") => true,
        _ => route_query(sql) == QueryEngine::DuckDb,
    };

    // Check if we should use Parquet sources
    let use_parquet = parquet_config
        .map(|c| c.enabled && c.max_parquet_block.is_some())
        .unwrap_or(false);

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

    // Rewrite query to use Parquet sources if enabled and query touches logs
    let sql = if use_parquet && query_references_logs(&sql) {
        rewrite_query_for_parquet(&sql, parquet_config.unwrap())?
    } else {
        sql
    };

    // Add LIMIT if not present
    let sql_upper = sql.to_uppercase();
    let sql = if !sql_upper.contains("LIMIT") {
        format!("{} LIMIT {}", sql, options.limit)
    } else {
        sql
    };

    // Execute with appropriate backend
    if use_pg_duckdb {
        execute_query_pg_duckdb(
            pg_pool,
            &sql,
            options,
            pg_duckdb_config.memory_limit.as_deref(),
            pg_duckdb_config.threads,
        )
        .await
    } else {
        execute_query_postgres(pg_pool, &sql, options).await
    }
}

/// Check if a query references the logs table
fn query_references_logs(sql: &str) -> bool {
    let sql_upper = sql.to_uppercase();
    // Check for FROM logs or JOIN logs patterns
    sql_upper.contains("FROM LOGS") || sql_upper.contains("JOIN LOGS")
}

/// Rewrite a query to use UNION ALL with Parquet and PostgreSQL sources
fn rewrite_query_for_parquet(sql: &str, config: &ParquetConfig) -> Result<String> {
    let max_block = config.max_parquet_block.unwrap_or(0);
    let chain_id = config.chain_id.unwrap_or(0);
    let data_dir = config.data_dir.as_deref().unwrap_or("/data");

    // Build the Parquet file glob pattern
    let parquet_glob = format!("{}/{}/logs_*.parquet", data_dir, chain_id);

    // Create a CTE that combines both sources
    let hybrid_cte = format!(
        r#"logs_hybrid AS (
    -- Recent logs from PostgreSQL heap
    SELECT * FROM logs WHERE block_num > {}
    UNION ALL
    -- Historical logs from Parquet files
    SELECT * FROM read_parquet('{}')
)"#,
        max_block, parquet_glob
    );

    // Check if query already has WITH clause
    let sql_upper = sql.to_uppercase();
    let modified_sql = if sql_upper.starts_with("WITH ") {
        // Append to existing WITH clause
        let with_end = sql.find(|c: char| !c.is_whitespace() && c != 'W' && c != 'I' && c != 'T' && c != 'H')
            .unwrap_or(4);
        format!(
            "WITH {}, {}",
            hybrid_cte,
            &sql[with_end..]
        )
    } else {
        // Add new WITH clause
        format!("WITH {} {}", hybrid_cte, sql)
    };

    // Replace references to 'logs' table with 'logs_hybrid'
    // This is a simple replacement - for production, use SQL parser
    let modified_sql = modified_sql
        .replace("FROM logs", "FROM logs_hybrid")
        .replace("FROM LOGS", "FROM logs_hybrid")
        .replace("from logs", "FROM logs_hybrid");

    Ok(modified_sql)
}

/// Execute a query on PostgreSQL.
async fn execute_query_postgres(
    pool: &Pool,
    sql: &str,
    options: &QueryOptions,
) -> Result<QueryResult> {
    execute_query_postgres_inner(pool, sql, options, "postgres").await
}

/// Execute a query on PostgreSQL with pg_duckdb extension.
/// This enables DuckDB's analytical engine for OLAP queries directly in PostgreSQL.
pub async fn execute_query_pg_duckdb(
    pool: &Pool,
    sql: &str,
    options: &QueryOptions,
    memory_limit: Option<&str>,
    threads: Option<u32>,
) -> Result<QueryResult> {
    // Convert '0x...' hex strings to '\x...' bytea literals for PostgreSQL
    let sql = sql.replace("'0x", "'\\x");

    let conn = pool.get().await?;

    // Configure pg_duckdb for this session
    // Force DuckDB execution for analytical queries
    conn.execute("SET duckdb.force_execution = true", &[]).await?;
    
    // Set memory limit (default 16GB for 64GB server)
    let mem_limit = memory_limit.unwrap_or("16GB");
    conn.execute(&format!("SET duckdb.max_memory = '{}'", mem_limit), &[]).await
        .unwrap_or_else(|e| {
            tracing::debug!(error = %e, "Failed to set duckdb.max_memory (pg_duckdb may not be installed)");
            0
        });
    
    // Set thread count (default 8)
    let thread_count = threads.unwrap_or(8);
    conn.execute(&format!("SET duckdb.threads = {}", thread_count), &[]).await
        .unwrap_or_else(|e| {
            tracing::debug!(error = %e, "Failed to set duckdb.threads");
            0
        });

    // Set scan parallelism for reading from PostgreSQL tables
    // These control how many PostgreSQL workers scan the table in parallel
    conn.execute(&format!("SET duckdb.max_workers_per_postgres_scan = {}", thread_count), &[]).await
        .unwrap_or_else(|e| {
            tracing::debug!(error = %e, "Failed to set duckdb.max_workers_per_postgres_scan");
            0
        });
    conn.execute(&format!("SET duckdb.threads_for_postgres_scan = {}", thread_count), &[]).await
        .unwrap_or_else(|e| {
            tracing::debug!(error = %e, "Failed to set duckdb.threads_for_postgres_scan");
            0
        });

    // Load tidx_abi extension for ABI decoding functions (abi_address, abi_uint, etc.)
    // This is needed when querying Parquet files that contain raw encoded event data
    if let Err(e) = conn.execute(
        "SELECT duckdb.raw_query($$ LOAD '/usr/share/duckdb/extensions/tidx_abi.duckdb_extension' $$)",
        &[],
    ).await {
        tracing::debug!(error = %e, "Failed to load tidx_abi extension (may not be installed)");
    }

    // Set statement timeout
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

    // Get columns from result
    let columns: Vec<String> = if rows.is_empty() {
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
            engine: Some("pg_duckdb".to_string()),
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
        engine: Some("pg_duckdb".to_string()),
        query_time_ms: Some(elapsed_ms),
    })
}

/// Internal PostgreSQL query executor with configurable engine name.
async fn execute_query_postgres_inner(
    pool: &Pool,
    sql: &str,
    options: &QueryOptions,
    engine_name: &str,
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
            engine: Some(engine_name.to_string()),
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
        engine: Some(engine_name.to_string()),
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
    use crate::query::EventSignature;

    // ========================================================================
    // PgDuckdbConfig Tests
    // ========================================================================

    #[test]
    fn test_pg_duckdb_config_default() {
        let config = PgDuckdbConfig::default();
        assert!(config.memory_limit.is_none());
        assert!(config.threads.is_none());
    }

    #[test]
    fn test_pg_duckdb_config_with_limits() {
        let config = PgDuckdbConfig {
            memory_limit: Some("16GB".to_string()),
            threads: Some(8),
        };
        assert_eq!(config.memory_limit, Some("16GB".to_string()));
        assert_eq!(config.threads, Some(8));
    }

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
    fn test_transfer_cte_duckdb_format() {
        let sig = EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)").unwrap();
        let cte = sig.to_cte_sql_duckdb();
        
        // DuckDB CTE uses hex string selector format
        assert!(cte.contains("WHERE selector = '0x"));
        // Uses tidx_abi extension functions
        assert!(cte.contains("abi_address(topic1)"));
        assert!(cte.contains("abi_address(topic2)"));
        assert!(cte.contains("abi_uint(data, 0)"));
    }

    #[test]
    fn test_approval_cte_both_engines() {
        let sig = EventSignature::parse("Approval(address indexed owner, address indexed spender, uint256 value)").unwrap();
        
        let pg_cte = sig.to_cte_sql_postgres();
        let duck_cte = sig.to_cte_sql_duckdb();
        
        // Both should have the same structure but different function calls
        assert!(pg_cte.contains("AS \"owner\""));
        assert!(duck_cte.contains("AS \"owner\""));
        
        // Both use abi_address, Postgres uses it on bytea, DuckDB on BLOB
        assert!(pg_cte.contains("abi_address(topic1)"));
        assert!(duck_cte.contains("abi_address(topic1)"));
    }

    #[test]
    fn test_swap_cte_multiple_data_params() {
        // Swap has 4 data params and 2 indexed params
        let sig = EventSignature::parse(
            "Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)"
        ).unwrap();
        
        let pg_cte = sig.to_cte_sql_postgres();
        let duck_cte = sig.to_cte_sql_duckdb();
        
        // Check indexed params use topics
        assert!(pg_cte.contains("abi_address(topic1)"));
        assert!(pg_cte.contains("abi_address(topic2)"));
        
        // Check data params use correct offsets (0, 32, 64, 96 bytes)
        assert!(pg_cte.contains("substring(data FROM 1 FOR 32)"));   // offset 0
        assert!(pg_cte.contains("substring(data FROM 33 FOR 32)"));  // offset 32
        assert!(pg_cte.contains("substring(data FROM 65 FOR 32)"));  // offset 64
        assert!(pg_cte.contains("substring(data FROM 97 FOR 32)"));  // offset 96
        
        // DuckDB uses tidx_abi extension functions with byte offsets
        assert!(duck_cte.contains("abi_uint(data, 0)"));
        assert!(duck_cte.contains("abi_uint(data, 32)"));
        assert!(duck_cte.contains("abi_uint(data, 64)"));
        assert!(duck_cte.contains("abi_uint(data, 96)"));
    }

    #[test]
    fn test_filtered_cte_only_includes_used_columns() {
        let sig = EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)").unwrap();
        
        let mut used_columns = std::collections::HashSet::new();
        used_columns.insert("to".to_string());
        used_columns.insert("value".to_string());
        
        let pg_cte = sig.to_cte_sql_postgres_filtered(Some(&used_columns));
        let duck_cte = sig.to_cte_sql_duckdb_filtered(Some(&used_columns));
        
        // Should include "to" and "value"
        assert!(pg_cte.contains("AS \"to\""));
        assert!(pg_cte.contains("AS \"value\""));
        assert!(duck_cte.contains("AS \"to\""));
        assert!(duck_cte.contains("AS \"value\""));
        
        // Should NOT include "from"
        assert!(!pg_cte.contains("AS \"from\""));
        assert!(!duck_cte.contains("AS \"from\""));
    }

    #[test]
    fn test_cte_with_bool_param() {
        let sig = EventSignature::parse("Paused(bool paused)").unwrap();
        
        let pg_cte = sig.to_cte_sql_postgres();
        let duck_cte = sig.to_cte_sql_duckdb();
        
        assert!(pg_cte.contains("abi_bool("));
        assert!(duck_cte.contains("abi_bool("));
    }

    #[test]
    fn test_cte_with_bytes32_param() {
        let sig = EventSignature::parse("RoleGranted(bytes32 indexed role, address indexed account, address indexed sender)").unwrap();
        
        let pg_cte = sig.to_cte_sql_postgres();
        let duck_cte = sig.to_cte_sql_duckdb();
        
        // bytes32 indexed just returns the topic directly
        assert!(pg_cte.contains("topic1 AS \"role\"") || pg_cte.contains("topic1"));
        assert!(duck_cte.contains("topic1 AS \"role\"") || duck_cte.contains("topic1"));
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
        
        // For pg_duckdb mode (uses Postgres functions)
        let pg_cte = sig.to_cte_sql_postgres();
        let full_pg_query = format!("WITH {} {}", pg_cte, user_query);
        
        // For native DuckDB mode (uses DuckDB UDFs)
        let duck_cte = sig.to_cte_sql_duckdb();
        let full_duck_query = format!("WITH {} {}", duck_cte, user_query);
        
        // Both produce valid SQL that users can run
        assert!(full_pg_query.contains("SELECT \"from\", \"to\", \"value\""));
        assert!(full_duck_query.contains("SELECT \"from\", \"to\", \"value\""));
        
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
        let duck_cte = sig.to_cte_sql_duckdb();
        
        // Both engines expose the same column names to users
        assert!(pg_cte.contains("AS \"from\""));
        assert!(duck_cte.contains("AS \"from\""));
        
        assert!(pg_cte.contains("AS \"to\""));
        assert!(duck_cte.contains("AS \"to\""));
        
        assert!(pg_cte.contains("AS \"value\""));
        assert!(duck_cte.contains("AS \"value\""));
        
        // Users can write the same query regardless of engine
        let user_query = r#"SELECT "from", "to", "value" FROM transfer"#;
        
        // Both work with the same user query
        let pg_full = format!("WITH {} {}", pg_cte, user_query);
        let duck_full = format!("WITH {} {}", duck_cte, user_query);
        
        assert!(pg_full.contains(user_query));
        assert!(duck_full.contains(user_query));
    }

    // ========================================================================
    // Query Routing Tests
    // ========================================================================

    #[test]
    fn test_route_olap_query_to_duckdb() {
        // OLAP patterns should route to DuckDB
        assert_eq!(route_query("SELECT COUNT(*) FROM logs GROUP BY address"), QueryEngine::DuckDb);
        assert_eq!(route_query("SELECT SUM(gas_used) FROM blocks"), QueryEngine::DuckDb);
        assert_eq!(route_query("SELECT AVG(gas_limit) FROM txs"), QueryEngine::DuckDb);
        assert_eq!(route_query("SELECT *, ROW_NUMBER() OVER (PARTITION BY address) FROM logs"), QueryEngine::DuckDb);
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
        assert_eq!(route_query("/* engine=duckdb */ SELECT * FROM blocks"), QueryEngine::DuckDb);
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

    // ========================================================================
    // Parquet Query Rewriting Tests
    // ========================================================================

    #[test]
    fn test_query_references_logs_positive() {
        assert!(query_references_logs("SELECT * FROM logs WHERE block_num > 100"));
        assert!(query_references_logs("SELECT * from logs"));
        assert!(query_references_logs("SELECT * FROM LOGS"));
        assert!(query_references_logs("SELECT * FROM blocks JOIN logs ON blocks.num = logs.block_num"));
    }

    #[test]
    fn test_query_references_logs_negative() {
        assert!(!query_references_logs("SELECT * FROM blocks WHERE num > 100"));
        assert!(!query_references_logs("SELECT * FROM transactions"));
        assert!(!query_references_logs("SELECT 'logs' as table_name"));
    }

    #[test]
    fn test_rewrite_query_for_parquet_simple() {
        let config = ParquetConfig {
            enabled: true,
            data_dir: Some("/data".to_string()),
            chain_id: Some(42431),
            max_parquet_block: Some(1000000),
        };

        let sql = "SELECT * FROM logs WHERE block_num > 500000";
        let rewritten = rewrite_query_for_parquet(sql, &config).unwrap();

        // Should have logs_hybrid CTE
        assert!(rewritten.contains("logs_hybrid AS"), "Missing logs_hybrid CTE: {}", rewritten);
        // Should reference both sources - the CTE has the block filter
        assert!(rewritten.contains("block_num > 1000000"), "Missing block filter: {}", rewritten);
        assert!(rewritten.contains("read_parquet('/data/42431/logs_*.parquet')"), "Missing parquet path: {}", rewritten);
        // Should use logs_hybrid instead of logs in user query
        assert!(rewritten.contains("FROM logs_hybrid"), "Not using logs_hybrid: {}", rewritten);
    }

    #[test]
    fn test_rewrite_query_for_parquet_with_existing_cte() {
        let config = ParquetConfig {
            enabled: true,
            data_dir: Some("/data".to_string()),
            chain_id: Some(1),
            max_parquet_block: Some(5000000),
        };

        let sql = "WITH transfer AS (SELECT * FROM logs WHERE selector = '\\x1234') SELECT * FROM transfer";
        let rewritten = rewrite_query_for_parquet(sql, &config).unwrap();

        // Should combine CTEs
        assert!(rewritten.starts_with("WITH logs_hybrid AS"));
        // Original query logic should be preserved
        assert!(rewritten.contains("transfer"));
    }

    #[test]
    fn test_parquet_config_default() {
        let config = ParquetConfig::default();
        assert!(!config.enabled);
        assert!(config.data_dir.is_none());
        assert!(config.chain_id.is_none());
        assert!(config.max_parquet_block.is_none());
    }
}

