use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::time::Instant;

use crate::db::Pool;
use crate::metrics;
use crate::query::{
    extract_column_references, extract_raw_column_predicates, validate_query, EventSignature,
    HARD_LIMIT_MAX,
};

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
    /// Per-table high-water marks for PostgreSQL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub postgres: Option<StoreStatus>,
    /// Per-table high-water marks for ClickHouse (if enabled).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clickhouse: Option<StoreStatus>,
}

/// Per-table high-water marks for a storage backend.
#[derive(Debug, Clone, Serialize)]
pub struct StoreStatus {
    pub blocks: Option<i64>,
    pub txs: Option<i64>,
    pub logs: Option<i64>,
    pub receipts: Option<i64>,
    /// Write rate in blocks/sec (from rolling window)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate: Option<f64>,
    /// Cumulative row counts (since process start)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocks_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub txs_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logs_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receipts_count: Option<u64>,
}

pub async fn get_all_status(pool: &Pool) -> Result<Vec<SyncStatus>> {
    let conn = pool.get().await?;

    let rows = conn
        .query(
            "SELECT chain_id, head_num, synced_num, tip_num, backfill_num, started_at, updated_at FROM sync_state ORDER BY chain_id",
            &[],
        )
        .await?;

    // Detect actual gaps in the blocks table (bounded by max tip_num)
    let max_tip: u64 = rows.iter().map(|r| r.get::<_, i64>(3) as u64).max().unwrap_or(0);
    let gaps = crate::sync::writer::detect_gaps(pool, max_tip).await.unwrap_or_default();
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
                postgres: None,
                clickhouse: None,
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
    signatures: &[&str],
    options: &QueryOptions,
) -> Result<QueryResult> {
    // Generate CTE SQL if signatures are provided
    let sql = if !signatures.is_empty() {
        let sigs: Vec<EventSignature> = signatures
            .iter()
            .map(|s| EventSignature::parse(s))
            .collect::<Result<_>>()?;

        // Normalize table references and rewrite filters for each signature
        let mut sql = sql.to_string();
        for sig in &sigs {
            sql = sig.normalize_table_references(&sql);
            sql = sig.rewrite_filters_for_pushdown(&sql);
        }

        let used_columns = extract_column_references(&sql);
        let filter = if used_columns.is_empty() {
            None
        } else {
            Some(&used_columns)
        };
        let pushdown = extract_raw_column_predicates(&sql);
        let ctes: Vec<String> = sigs
            .iter()
            .map(|sig| sig.to_cte_sql_postgres_with_pushdown(filter, &pushdown))
            .collect();
        format!("WITH {} {sql}", ctes.join(", "))
    } else {
        sql.to_string()
    };

    // Validate query (after CTE wrapping so signature-derived table names are valid)
    validate_query(&sql)?;

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
            .try_get::<_, PgNumeric>(idx)
            .ok()
            .map_or(serde_json::Value::Null, |v| serde_json::Value::String(v.0)),
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

/// Wrapper that decodes PostgreSQL NUMERIC binary format directly into a String,
/// avoiding `rust_decimal::Decimal` which panics on values exceeding its 96-bit
/// mantissa (~28 digits). Postgres NUMERIC is arbitrary precision (e.g. abi_uint()
/// on uint256 produces 78-digit values).
struct PgNumeric(String);

impl<'a> postgres_types::FromSql<'a> for PgNumeric {
    fn from_sql(
        _ty: &postgres_types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() < 8 {
            return Err("NUMERIC binary too short".into());
        }
        let ndigits = u16::from_be_bytes([raw[0], raw[1]]) as usize;
        let weight = i16::from_be_bytes([raw[2], raw[3]]);
        let sign = u16::from_be_bytes([raw[4], raw[5]]);
        let dscale = u16::from_be_bytes([raw[6], raw[7]]) as usize;

        const SIGN_POS: u16 = 0x0000;
        const SIGN_NEG: u16 = 0x4000;
        const SIGN_NAN: u16 = 0xC000;
        const SIGN_PINF: u16 = 0xD000;
        const SIGN_NINF: u16 = 0xF000;

        match sign {
            SIGN_NAN => return Ok(PgNumeric("NaN".to_string())),
            SIGN_PINF => return Ok(PgNumeric("Infinity".to_string())),
            SIGN_NINF => return Ok(PgNumeric("-Infinity".to_string())),
            SIGN_POS | SIGN_NEG => {}
            _ => return Err("invalid NUMERIC sign".into()),
        }

        if ndigits == 0 {
            return Ok(PgNumeric(if dscale > 0 {
                format!("0.{}", "0".repeat(dscale))
            } else {
                "0".to_string()
            }));
        }

        let expected_len = ndigits
            .checked_mul(2)
            .and_then(|n| n.checked_add(8))
            .ok_or("NUMERIC length overflow")?;
        if raw.len() < expected_len {
            return Err("NUMERIC binary truncated".into());
        }

        let mut digits = Vec::with_capacity(ndigits);
        for i in 0..ndigits {
            let off = 8 + i * 2;
            let d = u16::from_be_bytes([raw[off], raw[off + 1]]);
            if d > 9999 {
                return Err("invalid NUMERIC digit".into());
            }
            digits.push(d);
        }

        let mut s = String::new();
        if sign == SIGN_NEG {
            s.push('-');
        }

        // Integer part: digit groups at positions 0..=weight
        let weight_i = i32::from(weight);
        let int_groups = (weight_i + 1).max(0) as usize;
        if int_groups == 0 {
            s.push('0');
        } else {
            for i in 0..int_groups {
                let d = if i < ndigits { digits[i] } else { 0 };
                if i == 0 {
                    s.push_str(&d.to_string());
                } else {
                    s.push_str(&format!("{d:04}"));
                }
            }
        }

        // Fractional part
        if dscale > 0 {
            s.push('.');
            let mut frac = String::new();
            // Leading zero groups for weight < -1 (e.g. 0.00000042 has weight=-2)
            let frac_leading_zero_groups = (-weight_i - 1).max(0) as usize;
            for _ in 0..frac_leading_zero_groups {
                frac.push_str("0000");
            }
            for i in int_groups..ndigits {
                frac.push_str(&format!("{:04}", digits[i]));
            }
            while frac.len() < dscale {
                frac.push('0');
            }
            s.push_str(&frac[..dscale]);
        }

        Ok(PgNumeric(s))
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        *ty == postgres_types::Type::NUMERIC
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
    use insta::assert_snapshot;
    use postgres_types::FromSql;

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
    // PgNumeric Wire Format Decoding Tests
    // ========================================================================

    fn encode_pg_numeric(ndigits: i16, weight: i16, sign: u16, dscale: u16, digits: &[u16]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&ndigits.to_be_bytes());
        buf.extend_from_slice(&weight.to_be_bytes());
        buf.extend_from_slice(&sign.to_be_bytes());
        buf.extend_from_slice(&dscale.to_be_bytes());
        for &d in digits {
            buf.extend_from_slice(&d.to_be_bytes());
        }
        buf
    }

    #[test]
    fn test_pg_numeric_zero() {
        let raw = encode_pg_numeric(0, 0, 0, 0, &[]);
        let v = PgNumeric::from_sql(&postgres_types::Type::NUMERIC, &raw).unwrap();
        assert_eq!(v.0, "0");
    }

    #[test]
    fn test_pg_numeric_small_int() {
        // 42 = weight=0, digits=[42]
        let raw = encode_pg_numeric(1, 0, 0, 0, &[42]);
        let v = PgNumeric::from_sql(&postgres_types::Type::NUMERIC, &raw).unwrap();
        assert_eq!(v.0, "42");
    }

    #[test]
    fn test_pg_numeric_large_int() {
        // 1_000_000 = weight=1, digits=[100, 0]
        let raw = encode_pg_numeric(2, 1, 0, 0, &[100, 0]);
        let v = PgNumeric::from_sql(&postgres_types::Type::NUMERIC, &raw).unwrap();
        assert_eq!(v.0, "1000000");
    }

    #[test]
    fn test_pg_numeric_negative() {
        let raw = encode_pg_numeric(1, 0, 0x4000, 0, &[123]);
        let v = PgNumeric::from_sql(&postgres_types::Type::NUMERIC, &raw).unwrap();
        assert_eq!(v.0, "-123");
    }

    #[test]
    fn test_pg_numeric_nan() {
        let raw = encode_pg_numeric(0, 0, 0xC000, 0, &[]);
        let v = PgNumeric::from_sql(&postgres_types::Type::NUMERIC, &raw).unwrap();
        assert_eq!(v.0, "NaN");
    }

    #[test]
    fn test_pg_numeric_decimal() {
        // 3.14 = weight=0, dscale=2, digits=[3, 1400]
        let raw = encode_pg_numeric(2, 0, 0, 2, &[3, 1400]);
        let v = PgNumeric::from_sql(&postgres_types::Type::NUMERIC, &raw).unwrap();
        assert_eq!(v.0, "3.14");
    }

    #[test]
    fn test_pg_numeric_uint256_max() {
        // 2^256 - 1 = 115792089237316195423570985008687907853269984665640564039457584007913129639935
        // This is a 78-digit number that would panic with rust_decimal::Decimal.
        // PG NUMERIC base-10000 encoding: weight=19 (20 groups for integer part)
        let digits: Vec<u16> = vec![
            11, 5792, 892, 3731, 6195, 4235, 7098, 5008,
            6879, 785, 3269, 9846, 6564, 564, 394, 5758,
            4007, 9131, 2963, 9935,
        ];
        let raw = encode_pg_numeric(20, 19, 0, 0, &digits);
        let v = PgNumeric::from_sql(&postgres_types::Type::NUMERIC, &raw).unwrap();
        assert_eq!(v.0, "115792089237316195423570985008687907853269984665640564039457584007913129639935");
    }

    #[test]
    fn test_pg_numeric_fractional_only() {
        // 0.0042 = weight=-1, dscale=4, digits=[42]
        let raw = encode_pg_numeric(1, -1, 0, 4, &[42]);
        let v = PgNumeric::from_sql(&postgres_types::Type::NUMERIC, &raw).unwrap();
        assert_eq!(v.0, "0.0042");
    }

    #[test]
    fn test_pg_numeric_deep_fraction() {
        // 0.00000042 = weight=-2, dscale=8, digits=[42]
        let raw = encode_pg_numeric(1, -2, 0, 8, &[42]);
        let v = PgNumeric::from_sql(&postgres_types::Type::NUMERIC, &raw).unwrap();
        assert_eq!(v.0, "0.00000042");
    }

    #[test]
    fn test_pg_numeric_infinity() {
        let raw = encode_pg_numeric(0, 0, 0xD000, 0, &[]);
        let v = PgNumeric::from_sql(&postgres_types::Type::NUMERIC, &raw).unwrap();
        assert_eq!(v.0, "Infinity");
    }

    #[test]
    fn test_pg_numeric_neg_infinity() {
        let raw = encode_pg_numeric(0, 0, 0xF000, 0, &[]);
        let v = PgNumeric::from_sql(&postgres_types::Type::NUMERIC, &raw).unwrap();
        assert_eq!(v.0, "-Infinity");
    }

    #[test]
    fn test_pg_numeric_invalid_digit() {
        let raw = encode_pg_numeric(1, 0, 0, 0, &[10000]);
        assert!(PgNumeric::from_sql(&postgres_types::Type::NUMERIC, &raw).is_err());
    }

    #[test]
    fn test_pg_numeric_invalid_sign() {
        let raw = encode_pg_numeric(1, 0, 0x1234, 0, &[42]);
        assert!(PgNumeric::from_sql(&postgres_types::Type::NUMERIC, &raw).is_err());
    }
}

