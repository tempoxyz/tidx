use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::time::Instant;

use crate::db::Pool;
use crate::metrics;
use crate::query::EventSignature;

#[derive(Debug, Clone, Serialize)]
pub struct SyncStatus {
    pub chain_id: i64,
    pub head_num: i64,
    pub synced_num: i64,
    pub lag: i64,
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
            "SELECT chain_id, head_num, synced_num, backfill_num, started_at, updated_at FROM sync_state ORDER BY chain_id",
            &[],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|row| {
            let synced_num: i64 = row.get(2);
            let backfill_num: Option<i64> = row.get(3);
            let started_at: Option<DateTime<Utc>> = row.get(4);

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

            SyncStatus {
                chain_id: row.get(0),
                head_num: row.get(1),
                synced_num,
                lag: row.get::<_, i64>(1) - synced_num,
                backfill_num,
                backfill_remaining,
                sync_rate,
                eta_secs,
                updated_at: row.get(5),
            }
        })
        .collect())
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub row_count: usize,
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

pub async fn execute_query(
    pool: &Pool,
    sql: &str,
    signature: Option<&str>,
    options: &QueryOptions,
) -> Result<QueryResult> {
    let normalized = sql.trim().to_uppercase();

    if !normalized.starts_with("SELECT") && !normalized.starts_with("WITH") {
        return Err(anyhow!("Only SELECT queries are allowed"));
    }

    let forbidden = [
        "INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "ALTER", "CREATE", "GRANT", "REVOKE",
    ];
    for word in &forbidden {
        if normalized.contains(word) {
            return Err(anyhow!("Query contains forbidden keyword: {word}"));
        }
    }

    let sql = if let Some(sig_str) = signature {
        let sig = EventSignature::parse(sig_str)?;
        let cte = sig.to_cte_sql();
        format!("WITH {cte} {sql}")
    } else {
        sql.to_string()
    };

    // Convert '0x...' hex strings to '\x...' bytea literals
    let sql = sql.replace("'0x", "'\\x");

    let sql = if !normalized.contains("LIMIT") {
        format!("{} LIMIT {}", sql, options.limit)
    } else {
        sql
    };

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
        Ok(Err(e)) => return Err(anyhow!("Query error: {e:?}")),
        Err(_) => return Err(anyhow!("Query timeout")),
    };

    if rows.is_empty() {
        return Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            row_count: 0,
        });
    }

    let columns: Vec<String> = rows[0].columns().iter().map(|c| c.name().to_string()).collect();
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
