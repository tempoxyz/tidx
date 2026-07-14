use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use serde::Serialize;
use std::time::Instant;
use tokio_postgres::types::ToSql;

use crate::db::Pool;
use crate::metrics;
use crate::query::{
    EventSignature, HARD_LIMIT_MAX, apply_event_signature_ctes_postgres,
    apply_event_signature_ctes_tiered, plan_tiered_split, validate_query,
};

#[derive(Debug, Clone, Serialize)]
pub struct SyncStatus {
    pub chain_id: i64,
    pub head_num: i64,
    pub synced_num: i64,
    pub tip_num: i64,
    pub lag: i64,
    pub gap_blocks: i64,
    /// Latest exact gaps reported by the sync loop.
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
            "SELECT chain_id, head_num, synced_num, tip_num, backfill_num, started_at, updated_at, pruned_below FROM sync_state ORDER BY chain_id",
            &[],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|row| {
            let synced_num: i64 = row.get(2);
            let tip_num: i64 = row.get(3);
            let backfill_num: Option<i64> = row.get(4);
            let started_at: Option<DateTime<Utc>> = row.get(5);
            let pruned_below: i64 = row.get(7);

            // Blocks at or below the prune floor were intentionally dropped.
            let backfill_remaining = match backfill_num {
                None => synced_num.saturating_sub(1 + pruned_below).max(0),
                Some(n) => n.saturating_sub(pruned_below).max(0),
            };

            let sync_rate = started_at.and_then(|started| {
                let elapsed = Utc::now().signed_duration_since(started);
                let secs = elapsed.num_seconds() as f64;
                let total_indexed = match backfill_num {
                    Some(n) => synced_num - n + 1,
                    None => 1,
                };
                if secs > 0.0 {
                    Some(total_indexed as f64 / secs)
                } else {
                    None
                }
            });

            let eta_secs = sync_rate.and_then(|rate| {
                if rate > 0.0 {
                    Some(backfill_remaining as f64 / rate)
                } else {
                    None
                }
            });

            // Gap = blocks between synced_num and tip_num that may be missing
            let gap_blocks = tip_num.saturating_sub(synced_num);
            let gaps = metrics::get_gap_status(row.get::<_, i64>(0) as u64, "postgres")
                .map(|status| {
                    status
                        .ranges
                        .into_iter()
                        .map(|(start, end)| (start as i64, end as i64))
                        .collect()
                })
                .unwrap_or_default();

            SyncStatus {
                chain_id: row.get(0),
                head_num: row.get(1),
                synced_num,
                tip_num,
                lag: row.get::<_, i64>(1) - tip_num, // lag from head to tip (realtime)
                gap_blocks,
                gaps,
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

impl From<crate::clickhouse::QueryResult> for QueryResult {
    fn from(r: crate::clickhouse::QueryResult) -> Self {
        Self {
            columns: r.columns,
            rows: r.rows,
            row_count: r.row_count,
            engine: r.engine,
            query_time_ms: r.query_time_ms,
        }
    }
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

const MAX_QUERY_RESULT_BYTES: usize = 10 * 1024 * 1024;
const MAX_CELL_BYTES: usize = 1024 * 1024;

/// Execute a query on PostgreSQL.
pub async fn execute_query_postgres(
    pool: &Pool,
    sql: &str,
    signatures: &[&str],
    options: &QueryOptions,
) -> Result<QueryResult> {
    let sql = apply_event_signature_ctes_postgres(sql, signatures)?;

    // Validate query (after CTE wrapping so signature-derived table names are valid)
    validate_query(&sql)?;

    // Add LIMIT if not present (AST-based detection to avoid string matching bypass)
    let sql = append_limit_if_missing(&sql, options.limit);

    // Convert '0x...' hex literals to '\x...' bytea literals for PostgreSQL
    // Only replace hex values (40+ chars), not short '0x' prefixes used in concat()
    let sql = crate::query::convert_hex_literals_postgres(&sql);

    run_pg_query(pool, &sql, options, &[], "postgres").await
}

/// ClickHouse settings for the tiered cold arm. 64-bit+ integers keep
/// ClickHouse's default quoting (exact strings; unquoted UInt256 would parse
/// lossily as f64) — [`normalize_cold_result`] then converts per column type.
const TIERED_COLD_CH_SETTINGS: &[(&str, &str)] = &[
    ("date_time_output_format", "iso"),
    // No `final = 1`: reads match the native ClickHouse engine's semantics
    // (unmerged ReplacingMergeTree duplicates are possible but rare, and the
    // split cold arm only reads long-merged history below the prune boundary).
];

/// Rewrite ClickHouse JSON values to the hot (PostgreSQL) arm's
/// representations, per column type:
///
/// - `Int64`/`UInt64`: quoted string → JSON number (PG int8 is a number);
/// - `DateTime*`: ISO string → chrono RFC 3339 (PG timestamptz formatting);
/// - `(U)Int128`/`(U)Int256`: PG NUMERIC parity — decimal string when the
///   value fits [`rust_decimal::Decimal`], else NULL (PG's formatter nulls
///   values past Decimal's 96-bit mantissa, see [`try_format_column_json`]);
/// - `selector_null_cols` (projection indexes): '' → NULL, undoing the
///   ClickHouse sink's empty-string encoding of PG NULL selectors.
fn normalize_cold_result(
    result: &mut crate::clickhouse::QueryResult,
    selector_null_cols: &[usize],
) {
    for &i in selector_null_cols {
        for row in &mut result.rows {
            if let Some(cell) = row.get_mut(i)
                && matches!(&*cell, serde_json::Value::String(s) if s.is_empty())
            {
                *cell = serde_json::Value::Null;
            }
        }
    }
    for (i, ty) in result.column_types.iter().enumerate() {
        let base = ty
            .strip_prefix("Nullable(")
            .and_then(|t| t.strip_suffix(')'))
            .unwrap_or(ty);
        enum Kind {
            Int64,
            UInt64,
            BigNum,
            DateTime,
        }
        let kind = match base {
            "Int64" => Kind::Int64,
            "UInt64" => Kind::UInt64,
            "Int128" | "UInt128" | "Int256" | "UInt256" => Kind::BigNum,
            t if t.starts_with("DateTime") => Kind::DateTime,
            _ => continue,
        };
        for row in &mut result.rows {
            let Some(cell) = row.get_mut(i) else { continue };
            let serde_json::Value::String(s) = &*cell else {
                continue;
            };
            match kind {
                Kind::Int64 => {
                    if let Ok(v) = s.parse::<i64>() {
                        *cell = serde_json::Value::Number(v.into());
                    }
                }
                Kind::UInt64 => {
                    if let Ok(v) = s.parse::<u64>() {
                        *cell = serde_json::Value::Number(v.into());
                    }
                }
                Kind::BigNum => {
                    *cell = match s.parse::<rust_decimal::Decimal>() {
                        Ok(v) => serde_json::Value::String(v.to_string()),
                        Err(_) => serde_json::Value::Null,
                    };
                }
                Kind::DateTime => {
                    if let Ok(v) = DateTime::parse_from_rfc3339(s) {
                        *cell = serde_json::Value::String(v.with_timezone(&Utc).to_rfc3339());
                    }
                }
            }
        }
    }
}

/// Floor for the cold arm's remaining timeout budget.
const TIERED_COLD_MIN_TIMEOUT_MS: u64 = 250;

/// Extra headroom on `statement_timeout` for FDW sessions: pg_clickhouse
/// 0.3.x double-frees (and restarts PostgreSQL) when a statement-timeout
/// cancel lands mid ClickHouse scan, so ClickHouse's own deadline
/// (`max_execution_time`) must always fire first.
const FDW_STATEMENT_TIMEOUT_MARGIN_MS: u64 = 5_000;

/// Hot-confined aggregates whose proven scan span is at least this many
/// blocks route to ClickHouse: columnar scans beat PostgreSQL index scans
/// on wide-window rollups, while narrow shapes (page-enrichment IN-lists
/// near the tip) stay on PostgreSQL.
const TIERED_HOT_AGG_CH_MIN_SPAN: i64 = 50_000;

/// Whether a hot-window-confined query should still prefer the ClickHouse
/// archive: an aggregation shape scanning a wide (or unprovable) block span.
fn hot_query_prefers_clickhouse(hw: &crate::query::HotWindow, tip: i64) -> bool {
    hw.aggregate
        && hw
            .floor
            .is_none_or(|floor| tip.saturating_sub(floor) >= TIERED_HOT_AGG_CH_MIN_SPAN)
}

/// Execute a query on the tiered engine.
///
/// Eligible shapes (see [`plan_tiered_split`]) are split at the prune
/// boundary and served natively — hot arm on PostgreSQL `public.*`, cold arm
/// on ClickHouse — then stitched. Split-ineligible queries provably confined
/// to the hot window run on plain PostgreSQL, except wide-span aggregates,
/// which ClickHouse serves faster. Everything else runs natively on the
/// ClickHouse archive (it holds full history), falling back on failure:
/// hot-confined queries to plain PostgreSQL, the rest to the pg_clickhouse
/// FDW (`ch.*`).
pub async fn execute_query_tiered(
    pool: &Pool,
    clickhouse: Option<&crate::clickhouse::ClickHouseEngine>,
    chain_id: u64,
    sql: &str,
    signatures: &[&str],
    options: &QueryOptions,
) -> Result<QueryResult> {
    let crate::db::tiered::PruneBoundary {
        boundary,
        boundary_ts,
        tip,
    } = crate::db::tiered::fetch_prune_boundary(pool, chain_id).await?;
    if boundary <= 0 {
        // Nothing pruned yet: full history is hot in PostgreSQL.
        let mut result = execute_query_postgres(pool, sql, signatures, options).await?;
        result.engine = Some("tiered".to_string());
        return Ok(result);
    }
    match try_execute_tiered_split(pool, clickhouse, boundary, sql, signatures, options).await? {
        Some(result) => Ok(result),
        None => {
            let hot = crate::query::hot_window_confinement(sql, boundary, boundary_ts);
            let hot_on_pg = match &hot {
                Some(hw) => clickhouse.is_none() || !hot_query_prefers_clickhouse(hw, tip),
                None => false,
            };
            if hot_on_pg {
                // Every scan is provably above the prune boundary and
                // index-friendly: plain PostgreSQL serves it whole.
                let mut result = execute_query_postgres(pool, sql, signatures, options).await?;
                result.engine = Some("tiered".to_string());
                return Ok(result);
            }
            // Cold/unbounded history, or a hot wide-span aggregate: run
            // against the full ClickHouse archive — natively first (columnar
            // sorts/aggregates, no FDW row transfer), with fallback when
            // ClickHouse fails (e.g. bare UNION, timestamp literals).
            let start = Instant::now();
            let native = match clickhouse {
                Some(ch) => Some(
                    ch.query_user_with_settings(
                        sql,
                        signatures,
                        options.timeout_ms,
                        options.limit,
                        TIERED_COLD_CH_SETTINGS,
                    )
                    .await,
                ),
                None => None,
            };
            match native {
                Some(Ok(mut raw)) => {
                    normalize_cold_result(&mut raw, &[]);
                    let mut result: QueryResult = raw.into();
                    result.engine = Some("tiered".to_string());
                    result.query_time_ms = Some(start.elapsed().as_secs_f64() * 1000.0);
                    Ok(result)
                }
                // Unreachable ClickHouse: pg_clickhouse shares the fate, but
                // plain PostgreSQL is complete for hot-confined queries.
                Some(Err(e)) if hot.is_none() && crate::clickhouse::is_connection_error(&e) => {
                    Err(e)
                }
                Some(Err(e)) => {
                    let remaining = options
                        .timeout_ms
                        .saturating_sub(start.elapsed().as_millis() as u64);
                    if remaining < TIERED_COLD_MIN_TIMEOUT_MS {
                        return Err(e);
                    }
                    let opts = QueryOptions {
                        timeout_ms: remaining,
                        limit: options.limit,
                    };
                    let mut result = if hot.is_some() {
                        tracing::debug!(target: "tidx::query", error = %e, "tiered: clickhouse failed hot aggregate; falling back to postgres");
                        execute_query_postgres(pool, sql, signatures, &opts).await?
                    } else {
                        tracing::debug!(target: "tidx::query", error = %e, "tiered: clickhouse rejected query; falling back to pg_clickhouse");
                        execute_query_postgres_via_clickhouse(pool, sql, signatures, &opts).await?
                    };
                    result.engine = Some("tiered".to_string());
                    Ok(result)
                }
                None => {
                    let mut result =
                        execute_query_postgres_via_clickhouse(pool, sql, signatures, options)
                            .await?;
                    result.engine = Some("tiered".to_string());
                    Ok(result)
                }
            }
        }
    }
}

/// Attempt the tiered fast path. `Ok(None)` = ineligible, use the FDW path.
async fn try_execute_tiered_split(
    pool: &Pool,
    clickhouse: Option<&crate::clickhouse::ClickHouseEngine>,
    boundary: i64,
    sql: &str,
    signatures: &[&str],
    options: &QueryOptions,
) -> Result<Option<QueryResult>> {
    let start = Instant::now();

    let mut events = Vec::with_capacity(signatures.len());
    for sig in signatures {
        // Let the fallback path surface signature parse errors.
        let Ok(parsed) = EventSignature::parse(sig) else {
            return Ok(None);
        };
        events.push(parsed);
    }
    let Some(plan) = plan_tiered_split(sql, &events) else {
        return Ok(None);
    };
    // Mirror the fallback's raw-SQL gates (LIMIT ALL, length, …): the arms
    // re-emit SQL from the AST, which would otherwise erase them.
    if apply_event_signature_ctes_tiered(sql, signatures)
        .and_then(|s| validate_query(&s))
        .is_err()
    {
        return Ok(None);
    }
    // Plain PG errors when an explicit SQL LIMIT streams more rows than the
    // API cap; the split would silently cap. Let the fallback decide.
    if plan.sql_limit.is_some_and(|l| l > options.limit) {
        return Ok(None);
    }

    let eff_limit = plan
        .sql_limit
        .map_or(options.limit, |l| l.min(options.limit));
    let budget = |start: &Instant| {
        options
            .timeout_ms
            .saturating_sub(start.elapsed().as_millis() as u64)
            .max(TIERED_COLD_MIN_TIMEOUT_MS)
    };

    // The hot arm always runs: it carries plain-PostgreSQL semantics, so any
    // error PostgreSQL would raise for this query surfaces even when cold
    // rows alone could fill the page.
    let (hot, cold) = if plan.cold_leads {
        // Ascending order: cold (ClickHouse) rows sort first. Run both arms
        // concurrently; cold may fill the page, hot back-fills the rest.
        let Some(ch) = clickhouse else {
            return Ok(None);
        };
        let cold_sql = plan.arm_sql(false, boundary, Some(eff_limit));
        let hot_sql = plan.arm_sql(true, boundary, Some(eff_limit));
        let timeout_ms = budget(&start);
        let hot_options = QueryOptions {
            timeout_ms,
            limit: options.limit,
        };
        let (mut cold_raw, hot) = tokio::try_join!(
            ch.query_user_with_settings(
                &cold_sql,
                signatures,
                timeout_ms,
                eff_limit.max(1),
                TIERED_COLD_CH_SETTINGS,
            ),
            execute_query_postgres(pool, &hot_sql, signatures, &hot_options),
        )?;
        normalize_cold_result(&mut cold_raw, &plan.selector_null_cols);
        (hot, cold_raw.into())
    } else {
        // Descending or unordered: hot (PostgreSQL) rows first.
        let hot_sql = plan.arm_sql(true, boundary, Some(eff_limit.max(0)));
        let hot = execute_query_postgres(pool, &hot_sql, signatures, options).await?;
        if hot.row_count as i64 >= eff_limit {
            return Ok(Some(finish_tiered(hot, None, false, eff_limit, start)));
        }
        // Hot under-filled: fill the remainder from the ClickHouse archive.
        let Some(ch) = clickhouse else {
            return Ok(None);
        };
        let remaining = eff_limit - hot.row_count as i64;
        let cold_sql = plan.arm_sql(false, boundary, Some(remaining));
        let mut cold_raw = ch
            .query_user_with_settings(
                &cold_sql,
                signatures,
                budget(&start),
                remaining,
                TIERED_COLD_CH_SETTINGS,
            )
            .await?;
        normalize_cold_result(&mut cold_raw, &plan.selector_null_cols);
        (hot, cold_raw.into())
    };
    let cold: QueryResult = cold;

    // Column sets must line up to concatenate rows; a mismatch (shouldn't
    // happen for planned shapes — e.g. ClickHouse renaming an output column)
    // falls back to the FDW path for correctness. Exact comparison: the
    // planner already normalizes identifier case.
    let names_match = hot.columns.len() == cold.columns.len()
        && hot.columns.iter().zip(&cold.columns).all(|(a, b)| a == b);
    if !names_match && !hot.columns.is_empty() && !cold.columns.is_empty() {
        tracing::warn!(
            hot_cols = ?hot.columns,
            cold_cols = ?cold.columns,
            "tiered split arms returned mismatched columns; falling back to FDW path"
        );
        return Ok(None);
    }

    Ok(Some(finish_tiered(
        hot,
        Some(cold),
        plan.cold_leads,
        eff_limit,
        start,
    )))
}

/// Assemble the stitched tiered result. Hot (PostgreSQL) column names win:
/// they are byte-identical to the plain-postgres engine's output.
fn finish_tiered(
    hot: QueryResult,
    cold: Option<QueryResult>,
    cold_leads: bool,
    limit: i64,
    start: Instant,
) -> QueryResult {
    let mut result = hot;
    if let Some(mut cold) = cold {
        if result.columns.is_empty() {
            result.columns = std::mem::take(&mut cold.columns);
        }
        if cold_leads {
            let hot_rows = std::mem::take(&mut result.rows);
            result.rows = cold.rows;
            result.rows.extend(hot_rows);
        } else {
            result.rows.extend(cold.rows);
        }
        result.rows.truncate(limit.max(0) as usize);
        result.row_count = result.rows.len();
    }
    result.engine = Some("tiered".to_string());
    result.query_time_ms = Some(start.elapsed().as_secs_f64() * 1000.0);
    result
}

/// PostgreSQL executing entirely over the `ch.*` pg_clickhouse foreign
/// tables: full ClickHouse archive through the PostgreSQL planner, no hot
/// PostgreSQL arm. Benchmarking reference against `tiered`.
pub async fn execute_query_postgres_via_clickhouse(
    pool: &Pool,
    sql: &str,
    signatures: &[&str],
    options: &QueryOptions,
) -> Result<QueryResult> {
    // Without the ch.* schema, bare table names would silently resolve to
    // public.* (plain PostgreSQL) — refuse instead.
    if !crate::db::tiered::is_bootstrapped(pool).await? {
        return Err(anyhow!(
            "postgres-via-clickhouse requires tiered storage (ch.* foreign tables); enable tiered mode for this chain"
        ));
    }

    // ch.* exposes the same ClickHouse text representation as tiered.*.
    let sql = apply_event_signature_ctes_tiered(sql, signatures)?;
    validate_query(&sql)?;
    let sql = append_limit_if_missing(&sql, options.limit);

    // ClickHouse aborts its scans strictly before PostgreSQL's
    // statement_timeout: a cancel mid pg_clickhouse scan double-frees and
    // restarts the server (upstream 0.3.x bug), so the FDW must never be
    // what PostgreSQL cancels.
    let ch_deadline_secs = (options.timeout_ms / 1000).max(1);
    let session_settings = format!(
        "SET LOCAL pg_clickhouse.session_settings = 'join_use_nulls 1, \
         group_by_use_nulls 1, final 0, max_execution_time {ch_deadline_secs}'"
    );
    let statement_timeout = format!(
        "SET LOCAL statement_timeout = {}",
        options.timeout_ms + FDW_STATEMENT_TIMEOUT_MARGIN_MS
    );
    run_pg_query(
        pool,
        &sql,
        options,
        &[
            // Core tables resolve to ch.* FDW; public supplies abi_string().
            "SET LOCAL search_path = ch, public",
            // ch.* carries prune-boundary CHECKs for the tiered views; the
            // archive holds full history, so never let them prune scans here.
            "SET LOCAL constraint_exclusion = off",
            // `final 0` drops pg_clickhouse's default FINAL merge (costly
            // reads; native ClickHouse engine semantics instead).
            &session_settings,
            // Overrides run_pg_query's base statement_timeout (margin above).
            &statement_timeout,
        ],
        "postgres-via-clickhouse",
    )
    .await
}

/// Run prepared SQL on PostgreSQL and stream the result rows into a
/// [`QueryResult`]. `session_setup` statements (e.g. `SET LOCAL …`) run
/// inside the transaction before the query.
async fn run_pg_query(
    pool: &Pool,
    sql: &str,
    options: &QueryOptions,
    session_setup: &[&str],
    engine: &str,
) -> Result<QueryResult> {
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;

    tx.execute(
        &format!("SET LOCAL statement_timeout = {}", options.timeout_ms),
        &[],
    )
    .await?;

    for stmt in session_setup {
        tx.execute(*stmt, &[]).await?;
    }

    let start = Instant::now();
    let timeout = std::time::Duration::from_millis(options.timeout_ms + 100);
    let limit = options.limit as usize;
    let result = tokio::time::timeout(timeout, async {
        let params = std::iter::empty::<&(dyn ToSql + Sync)>();
        let stream = tx.query_raw(sql, params).await?;
        futures::pin_mut!(stream);
        let mut columns: Option<Vec<String>> = None;
        let mut rows = Vec::new();
        let mut result_bytes = 0usize;

        while let Some(row) = stream.try_next().await? {
            if columns.is_none() {
                columns = Some(row.columns().iter().map(|c| c.name().to_string()).collect());
            }
            if rows.len() >= limit {
                return Err(anyhow!("Query returned more than {limit} rows"));
            }
            let cols = columns
                .as_ref()
                .expect("columns initialized from first row");
            let row_values = (0..cols.len())
                .map(|i| try_format_column_json(&row, i))
                .collect::<Result<Vec<_>>>()?;
            result_bytes = result_bytes.saturating_add(
                row_values
                    .iter()
                    .map(estimated_json_value_bytes)
                    .sum::<usize>(),
            );
            if result_bytes > MAX_QUERY_RESULT_BYTES {
                return Err(anyhow!(
                    "Query result exceeded {} bytes",
                    MAX_QUERY_RESULT_BYTES
                ));
            }
            rows.push(row_values);
        }

        Ok::<_, anyhow::Error>((columns.unwrap_or_default(), rows))
    })
    .await;

    let (mut columns, result_rows) = match result {
        Ok(Ok(result)) => {
            metrics::record_query_duration(start.elapsed());
            result
        }
        Ok(Err(e)) => {
            return Err(anyhow!(
                "Query error: {}",
                sanitize_db_error(&e.to_string())
            ));
        }
        Err(_) => return Err(anyhow!("Query timeout")),
    };

    tx.commit().await?;

    if columns.is_empty() {
        columns = conn
            .prepare(sql)
            .await
            .ok()
            .map(|s| s.columns().iter().map(|c| c.name().to_string()).collect())
            .unwrap_or_default();
    }

    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let row_count = result_rows.len();
    metrics::record_query_rows(row_count as u64);

    Ok(QueryResult {
        columns,
        rows: result_rows,
        row_count,
        engine: Some(engine.to_string()),
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
                return format!("{sql}\nLIMIT {limit}");
            }
        }
    }
    sql.to_string()
}

pub fn format_column_json(row: &tokio_postgres::Row, idx: usize) -> serde_json::Value {
    try_format_column_json(row, idx).unwrap_or(serde_json::Value::Null)
}

fn try_format_column_json(row: &tokio_postgres::Row, idx: usize) -> Result<serde_json::Value> {
    let col = &row.columns()[idx];

    let value = match col.type_().name() {
        "int2" => row
            .try_get::<_, i16>(idx)
            .ok()
            .map_or(serde_json::Value::Null, |v| {
                serde_json::Value::Number(v.into())
            }),
        "int4" => row
            .try_get::<_, i32>(idx)
            .ok()
            .map_or(serde_json::Value::Null, |v| {
                serde_json::Value::Number(v.into())
            }),
        "int8" => row
            .try_get::<_, i64>(idx)
            .ok()
            .map_or(serde_json::Value::Null, |v| {
                serde_json::Value::Number(v.into())
            }),
        "numeric" => {
            // rust_decimal::Decimal panics (not errors) for values exceeding its
            // 96-bit mantissa (~28 digits). Postgres NUMERIC is arbitrary precision
            // (e.g. abi_uint() on uint256 = 78 digits), so catch the panic.
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                row.try_get::<_, rust_decimal::Decimal>(idx)
            })) {
                Ok(Ok(v)) => serde_json::Value::String(v.to_string()),
                _ => serde_json::Value::Null,
            }
        }
        "float4" | "float8" => row
            .try_get::<_, f64>(idx)
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        "bytea" => match row.try_get::<_, &[u8]>(idx) {
            Ok(v) if v.len() > MAX_CELL_BYTES => {
                return Err(anyhow!(
                    "Query result cell exceeded {} bytes",
                    MAX_CELL_BYTES
                ));
            }
            Ok(v) => serde_json::Value::String(format!("0x{}", hex::encode(v))),
            Err(_) => serde_json::Value::Null,
        },
        "text" | "varchar" | "name" => match row.try_get::<_, &str>(idx) {
            Ok(v) if v.len() > MAX_CELL_BYTES => {
                return Err(anyhow!(
                    "Query result cell exceeded {} bytes",
                    MAX_CELL_BYTES
                ));
            }
            Ok(v) => serde_json::Value::String(v.to_string()),
            Err(_) => serde_json::Value::Null,
        },
        "timestamptz" | "timestamp" => row
            .try_get::<_, DateTime<Utc>>(idx)
            .ok()
            .map_or(serde_json::Value::Null, |v| {
                serde_json::Value::String(v.to_rfc3339())
            }),
        "bool" => row
            .try_get::<_, bool>(idx)
            .ok()
            .map_or(serde_json::Value::Null, serde_json::Value::Bool),
        _ => serde_json::Value::Null,
    };

    Ok(value)
}

fn estimated_json_value_bytes(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Null => 4,
        serde_json::Value::Bool(_) => 5,
        serde_json::Value::Number(n) => n.to_string().len(),
        serde_json::Value::String(s) => s.len(),
        serde_json::Value::Array(values) => values.iter().map(estimated_json_value_bytes).sum(),
        serde_json::Value::Object(values) => values
            .iter()
            .map(|(key, value)| key.len() + estimated_json_value_bytes(value))
            .sum(),
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

/// Upper bound on characters retained from a database error message.
const MAX_DB_ERROR_CHARS: usize = 500;

/// Sanitize database error messages to prevent information leakage.
///
/// Removes file paths, internal schema details, and other sensitive info
/// while preserving useful error context for debugging.
fn sanitize_db_error(error: &str) -> String {
    // Truncate very long errors. PostgreSQL echoes offending user literals
    // verbatim, so `error` is attacker-controlled UTF-8: slice on a char
    // boundary, never a raw byte index (which panics mid-codepoint).
    let error = match error.char_indices().nth(MAX_DB_ERROR_CHARS) {
        Some((boundary, _)) => format!("{}...", &error[..boundary]),
        None => error.to_string(),
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
    use serde_json::{Value as J, json};

    // ========================================================================
    // Tiered cold-arm normalization
    // ========================================================================

    fn ch_result(column_types: &[&str], rows: Vec<Vec<J>>) -> crate::clickhouse::QueryResult {
        crate::clickhouse::QueryResult {
            columns: (0..column_types.len()).map(|i| format!("c{i}")).collect(),
            column_types: column_types.iter().map(ToString::to_string).collect(),
            row_count: rows.len(),
            rows,
            engine: None,
            query_time_ms: None,
        }
    }

    #[test]
    fn normalize_cold_converts_int64_and_datetime() {
        let mut r = ch_result(
            &[
                "Int64",
                "Nullable(Int64)",
                "UInt64",
                "DateTime64(3, 'UTC')",
                "UInt256",
                "UInt256",
            ],
            vec![vec![
                json!("42"),
                J::Null,
                json!("18446744073709551615"),
                // CH `date_time_output_format=iso` emits Z-suffixed strings.
                json!("2025-06-01T12:30:45.123Z"),
                json!("1000000000000000000"),
                json!(
                    "115792089237316195423570985008687907853269984665640564039457584007913129639935"
                ),
            ]],
        );
        normalize_cold_result(&mut r, &[]);
        assert_eq!(r.rows[0][0], json!(42));
        assert_eq!(r.rows[0][1], J::Null);
        assert_eq!(r.rows[0][2], json!(18_446_744_073_709_551_615_u64));
        // RFC 3339 in UTC, matching PG timestamptz serialization.
        assert_eq!(r.rows[0][3], json!("2025-06-01T12:30:45.123+00:00"));
        // Fits rust_decimal: decimal string, like PG NUMERIC.
        assert_eq!(r.rows[0][4], json!("1000000000000000000"));
        // Exceeds Decimal's 96-bit mantissa: NULL, matching PG's formatter.
        assert_eq!(r.rows[0][5], J::Null);
    }

    #[test]
    fn normalize_cold_rewrites_empty_selector_to_null() {
        let mut r = ch_result(
            &["String", "String"],
            vec![
                vec![json!(""), json!("")],
                vec![json!("0xddf252ad"), json!("keep")],
            ],
        );
        // Only column 0 is a projected `logs.selector`.
        normalize_cold_result(&mut r, &[0]);
        assert_eq!(r.rows[0][0], J::Null);
        assert_eq!(r.rows[0][1], json!("")); // non-selector '' kept
        assert_eq!(r.rows[1][0], json!("0xddf252ad"));
        assert_eq!(r.rows[1][1], json!("keep"));
    }

    // ========================================================================
    // Tiered hot-engine choice
    // ========================================================================

    #[test]
    fn hot_aggregates_prefer_clickhouse_by_span() {
        use crate::query::HotWindow;
        let tip = 29_218_717;
        let wide = HotWindow {
            floor: Some(tip - TIERED_HOT_AGG_CH_MIN_SPAN),
            aggregate: true,
        };
        assert!(hot_query_prefers_clickhouse(&wide, tip));
        // Narrow aggregate (page-enrichment IN-list): stays on PostgreSQL.
        let narrow = HotWindow {
            floor: Some(tip - 100),
            aggregate: true,
        };
        assert!(!hot_query_prefers_clickhouse(&narrow, tip));
        // Unprovable span (timestamp bound): treated as wide.
        let unknown = HotWindow {
            floor: None,
            aggregate: true,
        };
        assert!(hot_query_prefers_clickhouse(&unknown, tip));
        // Non-aggregates always stay on PostgreSQL.
        let page = HotWindow {
            floor: None,
            aggregate: false,
        };
        assert!(!hot_query_prefers_clickhouse(&page, tip));
    }

    // ========================================================================
    // Event CTE SQL Generation Tests (Both Engines)
    // ========================================================================

    #[test]
    fn test_transfer_cte_postgres() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
        assert_snapshot!(sig.to_cte_sql_postgres());
    }

    #[test]
    fn test_transfer_cte_clickhouse() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
        assert_snapshot!(sig.to_cte_sql_clickhouse());
    }

    #[test]
    fn test_transfer_cte_tiered() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
        assert_snapshot!(sig.to_cte_sql_tiered());
    }

    #[test]
    fn test_apply_ctes_tiered_pushdown() {
        let sql = "SELECT \"from\", \"to\", value FROM Transfer WHERE address = '0x20c54c5f742f123abb49a982ffe9ba3d82fd8a86' AND block_num > 100";
        let sigs = ["Transfer(address indexed from, address indexed to, uint256 value)"];
        assert_snapshot!(apply_event_signature_ctes_tiered(sql, &sigs).unwrap());
    }

    #[test]
    fn test_approval_cte_postgres() {
        let sig = EventSignature::parse(
            "Approval(address indexed owner, address indexed spender, uint256 value)",
        )
        .unwrap();
        assert_snapshot!(sig.to_cte_sql_postgres());
    }

    #[test]
    fn test_approval_cte_clickhouse() {
        let sig = EventSignature::parse(
            "Approval(address indexed owner, address indexed spender, uint256 value)",
        )
        .unwrap();
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
        let sig = EventSignature::parse(
            "RoleGranted(bytes32 indexed role, address indexed account, address indexed sender)",
        )
        .unwrap();
        assert_snapshot!(sig.to_cte_sql_postgres());
    }

    #[test]
    fn test_role_granted_cte_clickhouse() {
        let sig = EventSignature::parse(
            "RoleGranted(bytes32 indexed role, address indexed account, address indexed sender)",
        )
        .unwrap();
        assert_snapshot!(sig.to_cte_sql_clickhouse());
    }

    #[test]
    fn test_filtered_cte_postgres() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
        let mut used_columns = std::collections::HashSet::new();
        used_columns.insert("to".to_string());
        used_columns.insert("value".to_string());
        assert_snapshot!(sig.to_cte_sql_postgres_filtered(Some(&used_columns)));
    }

    #[test]
    fn test_filtered_cte_clickhouse() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
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

    #[test]
    fn test_append_limit_uses_newline_after_line_comment() {
        let sql = "SELECT * FROM blocks -- trailing comment";
        let limited = append_limit_if_missing(sql, 100);
        assert_eq!(
            limited,
            "SELECT * FROM blocks -- trailing comment\nLIMIT 100"
        );
    }

    #[test]
    fn test_append_limit_preserves_existing_limit() {
        let sql = "SELECT * FROM blocks LIMIT 10";
        assert_eq!(append_limit_if_missing(sql, 100), sql);
    }

    #[test]
    fn test_estimated_json_value_bytes_counts_nested_strings() {
        let value = serde_json::json!({
            "rows": [["abc"], ["defg"]],
            "ok": true
        });

        assert!(estimated_json_value_bytes(&value) >= 10);
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
        assert!(sanitized.chars().count() <= MAX_DB_ERROR_CHARS + 3); // 500 chars + "..."
        assert!(sanitized.ends_with("..."));
    }

    #[test]
    fn test_sanitize_truncates_multibyte_without_panic() {
        // A multi-byte char straddling the old 500-BYTE cut point used to panic
        // ("byte index 500 is not a char boundary"). The message body is
        // attacker-controlled (PostgreSQL echoes bad literals), so this must
        // never panic and must cut on a char boundary.
        for pad in 495..505 {
            let error = format!("{}{}", "a".repeat(pad), "é".repeat(20));
            let sanitized = sanitize_db_error(&error);
            assert!(sanitized.ends_with("..."));
            assert!(sanitized.chars().count() <= MAX_DB_ERROR_CHARS + 3);
        }

        // Also exercise a wide (4-byte) codepoint right at the boundary.
        let error = format!("{}{}", "a".repeat(499), "😀".repeat(10));
        let sanitized = sanitize_db_error(&error);
        assert!(sanitized.chars().count() <= MAX_DB_ERROR_CHARS + 3);
    }
}
