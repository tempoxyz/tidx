use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use serde::Serialize;
use std::task::Poll;
use std::time::Instant;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::{FromSql, ToSql, Type};

use crate::db::Pool;
use crate::metrics;
use crate::query::{
    EventSignature, HARD_LIMIT_MAX, TieredSplit, apply_event_signature_ctes_postgres,
    apply_event_signature_ctes_tiered, plan_tiered_split, prepare_blocking, validate_query,
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
    /// Lowest block in the contiguous ClickHouse archive interval.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archive_backfill_num: Option<i64>,
    /// Highest block in the contiguous ClickHouse archive interval.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archive_tip_num: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archive_backfill_remaining: Option<i64>,
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
            "SELECT chain_id, head_num, synced_num, tip_num, backfill_num, started_at, updated_at, pruned_below, archive_tip_num, archive_backfill_num FROM sync_state ORDER BY chain_id",
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
            let archive_tip_num: i64 = row.get(8);
            let archive_backfill_num: Option<i64> = row.get(9);

            // Blocks at or below the prune floor were intentionally dropped.
            let backfill_remaining = match backfill_num {
                None => synced_num.saturating_sub(1 + pruned_below).max(0),
                Some(n) => n.saturating_sub(1 + pruned_below).max(0),
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
                archive_backfill_num,
                archive_tip_num: archive_backfill_num.map(|_| archive_tip_num),
                archive_backfill_remaining: archive_backfill_num.map(|n| n.saturating_sub(1)),
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

#[derive(Clone)]
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

/// Most bytes PostgreSQL may send for one result, counted by wire length. It
/// bounds the bytes read for cells the response may drop, such as arrays and
/// `jsonb`, which format as null. It is larger than [`MAX_QUERY_RESULT_BYTES`]
/// because such cells are not returned.
const MAX_QUERY_WIRE_BYTES: usize = 4 * MAX_QUERY_RESULT_BYTES;
const MAX_CELL_BYTES: usize = 1024 * 1024;

/// Most cells (rows times columns) one result may hold: [`HARD_LIMIT_MAX`] rows
/// of 100 columns, above the widest core table. Even an empty cell keeps a
/// `serde_json::Value` (up to 72 bytes) alive for the whole request, so the
/// byte budget alone would let a result hold millions of them.
const MAX_QUERY_CELLS: usize = 1_000_000;

#[derive(Debug, thiserror::Error)]
enum PostgresQueryFailure {
    #[error("Query timeout")]
    Timeout,
    #[error("{message}")]
    Database { code: SqlState, message: String },
    #[error("{message}")]
    Unavailable { message: String },
    #[error("{message}")]
    Processing { message: String },
}

/// Execute a query on PostgreSQL.
pub async fn execute_query_postgres(
    pool: &Pool,
    sql: &str,
    signatures: &[&str],
    options: &QueryOptions,
) -> Result<QueryResult> {
    PostgresQuery::prepare(sql, signatures, options)
        .await?
        .execute(pool, &[], options)
        .await
}

/// A public query rewritten and validated for PostgreSQL once, so that it can
/// run repeatedly with different parameters, e.g. once per block for a live
/// stream.
#[derive(Clone, Debug)]
pub struct PostgresQuery {
    sql: String,
}

impl PostgresQuery {
    pub fn new(sql: &str, signatures: &[&str], options: &QueryOptions) -> Result<Self> {
        let sql = apply_event_signature_ctes_postgres(sql, signatures)?;

        // Validate query (after CTE wrapping so signature-derived table names are valid)
        validate_query(&sql)?;

        // Add LIMIT if not present (AST-based detection to avoid string matching bypass)
        let sql = append_limit_if_missing(&sql, options.limit);

        // Convert '0x...' hex literals to '\x...' bytea literals for PostgreSQL
        // Only replace hex values (40+ chars), not short '0x' prefixes used in concat()
        let sql = crate::query::convert_hex_literals_postgres(&sql);

        Ok(Self { sql })
    }

    /// [`Self::new`] on the blocking pool, for async callers.
    pub async fn prepare(sql: &str, signatures: &[&str], options: &QueryOptions) -> Result<Self> {
        let options = options.clone();
        prepare_blocking(sql, signatures, move |sql, signatures| {
            Self::new(sql, signatures, &options)
        })
        .await
    }

    /// The rewritten SQL that runs on PostgreSQL.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Run the query, binding `params` to its `$n` placeholders.
    pub async fn execute(
        &self,
        pool: &Pool,
        params: &[(&(dyn ToSql + Sync), Type)],
        options: &QueryOptions,
    ) -> Result<QueryResult> {
        run_pg_query(pool, &self.sql, params, options, &[], &[], "postgres").await
    }
}

/// ClickHouse settings for public queries (native `engine=clickhouse` and the
/// tiered cold arm): ISO timestamps for [`normalize_datetime_columns`].
/// 64-bit+ integers arrive as exact quoted strings (every ClickHouse request
/// pins `output_format_json_quote_64bit_integers=1`); 64-bit ones are then
/// converted to JSON numbers by [`normalize_int64_columns`].
const CH_QUERY_SETTINGS: &[(&str, &str)] = &[
    ("date_time_output_format", "iso"),
    // No `final = 1`: reads match the native ClickHouse engine's semantics
    // (unmerged ReplacingMergeTree duplicates are possible but rare, and the
    // split cold arm only reads long-merged history below the prune boundary).
];

/// Execute a public query directly on ClickHouse.
pub async fn execute_query_clickhouse(
    clickhouse: &crate::clickhouse::ClickHouseEngine,
    sql: &str,
    signatures: &[&str],
    options: &QueryOptions,
) -> Result<QueryResult> {
    let mut result = clickhouse
        .query_user_with_settings(
            sql,
            signatures,
            options.timeout_ms,
            options.limit,
            CH_QUERY_SETTINGS,
        )
        .await?;
    normalize_datetime_columns(&mut result);
    normalize_int64_columns(&mut result);
    Ok(result.into())
}

/// Strip `LowCardinality(...)` and `Nullable(...)` from a ClickHouse column
/// type, e.g. `LowCardinality(Nullable(DateTime('UTC')))` → `DateTime('UTC')`.
fn ch_base_type(mut ty: &str) -> &str {
    while let Some(inner) = ty
        .strip_prefix("Nullable(")
        .or_else(|| ty.strip_prefix("LowCardinality("))
        .and_then(|t| t.strip_suffix(')'))
    {
        ty = inner;
    }
    ty
}

/// Rewrite `DateTime*` columns (fetched with `date_time_output_format=iso`)
/// from ClickHouse ISO strings to chrono RFC 3339, the formatting
/// [`try_format_column_json`] gives PostgreSQL `timestamptz`.
fn normalize_datetime_columns(result: &mut crate::clickhouse::QueryResult) {
    for (i, ty) in result.column_types.iter().enumerate() {
        if !ch_base_type(ty).starts_with("DateTime") {
            continue;
        }
        for row in &mut result.rows {
            if let Some(serde_json::Value::String(s)) = row.get_mut(i)
                && let Ok(v) = DateTime::parse_from_rfc3339(s)
            {
                *s = v.with_timezone(&Utc).to_rfc3339();
            }
        }
    }
}

/// Rewrite ClickHouse JSON values to the hot (PostgreSQL) arm's
/// representations, per column type:
///
/// - `Int64`/`UInt64`: see [`normalize_int64_columns`];
/// - `DateTime*`: see [`normalize_datetime_columns`];
/// - `(U)Int128`/`(U)Int256`: kept as ClickHouse's exact quoted decimal
///   string, matching PG NUMERIC's formatting in [`try_format_column_json`];
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
    normalize_datetime_columns(result);
    normalize_int64_columns(result);
}

/// Rewrite `Int64`/`UInt64` values from ClickHouse's quoted strings to JSON
/// numbers, as PostgreSQL formats int8, including those nested in `Array`,
/// `Tuple` and `Map` values; wider integers stay exact strings.
pub fn normalize_int64_columns(result: &mut crate::clickhouse::QueryResult) {
    for (i, ty) in result.column_types.iter().enumerate() {
        // Parsed once per column, so the walk costs O(values), not
        // O(values × type length).
        let Some(shape) = Int64Shape::parse(ty) else {
            continue;
        };
        for row in &mut result.rows {
            if let Some(cell) = row.get_mut(i) {
                shape.apply(cell);
            }
        }
    }
}

/// Where a ClickHouse type holds `Int64`/`UInt64` values. Types without any
/// parse to `None`, so their values are never walked. In JSON output, arrays
/// and unnamed tuples are JSON arrays; maps and named tuples are objects.
enum Int64Shape {
    Int64,
    UInt64,
    Array(Box<Int64Shape>),
    Map(Box<Int64Shape>),
    /// Element position, name (for named tuples) and shape.
    Tuple(Vec<(usize, Option<String>, Int64Shape)>),
}

impl Int64Shape {
    /// Deeper types are left as they are: serde_json already refuses JSON
    /// nested past 128 levels.
    const MAX_DEPTH: usize = 128;

    /// Parse a column type in one pass, so the cost is linear in its length.
    /// Malformed or unknown types parse to `None` and are left as they are.
    fn parse(ty: &str) -> Option<Self> {
        if !ty.contains("Int64") {
            return None;
        }
        let mut c = TypeCursor {
            s: ty.as_bytes(),
            pos: 0,
        };
        let shape = Self::parse_type(&mut c, 0).ok()??;
        c.skip_ws();
        (c.pos == c.s.len()).then_some(shape)
    }

    /// One type at the cursor, if it holds 64-bit integers.
    fn parse_type(c: &mut TypeCursor, depth: usize) -> Result<Option<Self>, Malformed> {
        c.skip_ws();
        let name = c.ident();
        if name.is_empty() {
            return Err(Malformed);
        }
        Self::parse_after_name(c, name, depth)
    }

    fn parse_after_name(
        c: &mut TypeCursor,
        name: &str,
        depth: usize,
    ) -> Result<Option<Self>, Malformed> {
        if c.peek() != Some(b'(') {
            return Ok(match name {
                "Int64" => Some(Self::Int64),
                "UInt64" => Some(Self::UInt64),
                _ => None,
            });
        }
        c.pos += 1;
        if depth >= Self::MAX_DEPTH {
            c.skip_args().ok_or(Malformed)?;
            return Ok(None);
        }
        let shape = match name {
            "Nullable" | "LowCardinality" => Self::parse_type(c, depth + 1)?,
            "Array" => Self::parse_type(c, depth + 1)?.map(|s| Self::Array(Box::new(s))),
            "Map" => {
                Self::parse_type(c, depth + 1)?;
                if !c.eat(b',') {
                    return Err(Malformed);
                }
                Self::parse_type(c, depth + 1)?.map(|s| Self::Map(Box::new(s)))
            }
            "Tuple" => {
                let mut fields = Vec::new();
                for i in 0.. {
                    let (field, shape) = Self::parse_tuple_element(c, depth + 1)?;
                    if let Some(shape) = shape {
                        fields.push((i, field, shape));
                    }
                    if !c.eat(b',') {
                        break;
                    }
                }
                (!fields.is_empty()).then_some(Self::Tuple(fields))
            }
            // Arguments of other types, such as `DateTime64(3, 'UTC')`.
            _ => {
                c.skip_args().ok_or(Malformed)?;
                return Ok(None);
            }
        };
        if c.eat(b')') {
            Ok(shape)
        } else {
            Err(Malformed)
        }
    }

    /// A tuple element: `n UInt64`, `` `a b` UInt64 `` or `UInt64`. ClickHouse
    /// escapes `` ` `` and `\` in backquoted names with a backslash, and JSON
    /// output keys named tuples by the decoded name.
    fn parse_tuple_element(
        c: &mut TypeCursor,
        depth: usize,
    ) -> Result<(Option<String>, Option<Self>), Malformed> {
        c.skip_ws();
        if c.peek() == Some(b'`') {
            let name = c.quoted().and_then(|n| String::from_utf8(n).ok());
            let name = name.ok_or(Malformed)?;
            return Ok((Some(name), Self::parse_type(c, depth)?));
        }
        let first = c.ident();
        if first.is_empty() {
            return Err(Malformed);
        }
        let after = c.pos;
        c.skip_ws();
        if c.pos > after
            && c.peek()
                .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        {
            return Ok((Some(first.to_string()), Self::parse_type(c, depth)?));
        }
        c.pos = after;
        Ok((None, Self::parse_after_name(c, first, depth)?))
    }

    fn apply(&self, value: &mut serde_json::Value) {
        use serde_json::Value;
        match (self, value) {
            (Self::Int64, value @ Value::String(_)) => {
                if let Some(v) = value.as_str().and_then(|s| s.parse::<i64>().ok()) {
                    *value = Value::Number(v.into());
                }
            }
            (Self::UInt64, value @ Value::String(_)) => {
                if let Some(v) = value.as_str().and_then(|s| s.parse::<u64>().ok()) {
                    *value = Value::Number(v.into());
                }
            }
            (Self::Array(element), Value::Array(items)) => {
                for v in items {
                    element.apply(v);
                }
            }
            (Self::Map(v_shape), Value::Object(entries)) => {
                for v in entries.values_mut() {
                    v_shape.apply(v);
                }
            }
            (Self::Tuple(fields), Value::Array(items)) => {
                for (i, _, shape) in fields {
                    if let Some(v) = items.get_mut(*i) {
                        shape.apply(v);
                    }
                }
            }
            (Self::Tuple(fields), Value::Object(entries)) => {
                for (_, name, shape) in fields {
                    if let Some(v) = name.as_deref().and_then(|n| entries.get_mut(n)) {
                        shape.apply(v);
                    }
                }
            }
            _ => {}
        }
    }
}

/// A ClickHouse type string [`Int64Shape::parse`] cannot read.
struct Malformed;

/// A position in a ClickHouse type string, for [`Int64Shape::parse`].
struct TypeCursor<'a> {
    s: &'a [u8],
    pos: usize,
}

impl<'a> TypeCursor<'a> {
    fn peek(&self) -> Option<u8> {
        self.s.get(self.pos).copied()
    }

    fn skip_ws(&mut self) {
        while self.peek().is_some_and(|b| b.is_ascii_whitespace()) {
            self.pos += 1;
        }
    }

    /// Consume `b` after any whitespace.
    fn eat(&mut self, b: u8) -> bool {
        self.skip_ws();
        let found = self.peek() == Some(b);
        if found {
            self.pos += 1;
        }
        found
    }

    fn ident(&mut self) -> &'a str {
        let start = self.pos;
        while self
            .peek()
            .is_some_and(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            self.pos += 1;
        }
        // ASCII only, so always valid UTF-8.
        std::str::from_utf8(&self.s[start..self.pos]).unwrap_or_default()
    }

    /// The decoded contents of the quoted string at the cursor (`'…'`,
    /// `` `…` `` or `"…"`), with backslash escapes resolved.
    fn quoted(&mut self) -> Option<Vec<u8>> {
        let quote = self.peek()?;
        self.pos += 1;
        let mut out = Vec::new();
        loop {
            let b = self.peek()?;
            self.pos += 1;
            match b {
                b'\\' => {
                    let escaped = self.peek()?;
                    self.pos += 1;
                    out.push(match escaped {
                        b'n' => b'\n',
                        b't' => b'\t',
                        b'r' => b'\r',
                        b'0' => 0,
                        other => other,
                    });
                }
                b if b == quote => return Some(out),
                b => out.push(b),
            }
        }
    }

    /// Skip to just past the `)` closing the argument list the cursor is in,
    /// ignoring parentheses inside quotes.
    fn skip_args(&mut self) -> Option<()> {
        let mut depth = 1usize;
        loop {
            match self.peek()? {
                b'\'' | b'`' | b'"' => {
                    self.quoted()?;
                    continue;
                }
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        self.pos += 1;
                        return Some(());
                    }
                }
                _ => {}
            }
            self.pos += 1;
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
/// on ClickHouse — then stitched. A failing hot arm degrades the split to
/// the native ClickHouse archive. Split-ineligible queries provably confined
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
    // The keyword check, the split planner and the hot-window check parse the
    // raw input, partly on this worker: bound it before any of them runs.
    crate::query::check_event_signature_limits(sql, signatures)?;
    // Parts of this query may run natively on ClickHouse, which only applies
    // the ClickHouse validator: keep PostgreSQL's session-keyword rule.
    crate::query::reject_postgres_session_keywords(sql)?;
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
                        CH_QUERY_SETTINGS,
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

    let Some(plan) = prepare_blocking(sql, signatures, |sql, signatures| {
        Ok(plan_validated_tiered_split(sql, signatures))
    })
    .await?
    else {
        return Ok(None);
    };
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
        let (cold, hot) = tokio::join!(
            ch.query_user_with_settings(
                &cold_sql,
                signatures,
                timeout_ms,
                eff_limit.max(1),
                CH_QUERY_SETTINGS,
            ),
            execute_query_postgres(pool, &hot_sql, signatures, &hot_options),
        );
        let hot = match hot {
            Ok(hot) => hot,
            Err(e) if is_retryable_hot_postgres_failure(&e) => {
                return degrade_split_to_clickhouse(
                    ch,
                    sql,
                    signatures,
                    &plan.selector_null_cols,
                    e,
                    options,
                    start,
                )
                .await
                .map(Some);
            }
            Err(e) => return Err(e),
        };
        let mut cold_raw = cold?;
        normalize_cold_result(&mut cold_raw, &plan.selector_null_cols);
        (hot, cold_raw.into())
    } else {
        // Descending or unordered: hot (PostgreSQL) rows first.
        let hot_sql = plan.arm_sql(true, boundary, Some(eff_limit.max(0)));
        let hot = match execute_query_postgres(pool, &hot_sql, signatures, options).await {
            Ok(hot) => hot,
            Err(e) if is_retryable_hot_postgres_failure(&e) => {
                let Some(ch) = clickhouse else {
                    return Err(e);
                };
                return degrade_split_to_clickhouse(
                    ch,
                    sql,
                    signatures,
                    &plan.selector_null_cols,
                    e,
                    options,
                    start,
                )
                .await
                .map(Some);
            }
            Err(e) => return Err(e),
        };
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
                CH_QUERY_SETTINGS,
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

/// Plan the tiered split of `sql`, or `None` when it is ineligible or the
/// fallback path would reject it.
fn plan_validated_tiered_split(sql: &str, signatures: &[&str]) -> Option<TieredSplit> {
    let mut events = Vec::with_capacity(signatures.len());
    for sig in signatures {
        // Let the fallback path surface signature parse errors.
        let Ok(parsed) = EventSignature::parse(sig) else {
            return None;
        };
        events.push(parsed);
    }
    let plan = plan_tiered_split(sql, &events)?;
    // Mirror the fallback's raw-SQL gates (LIMIT ALL, length, …): the arms
    // re-emit SQL from the AST, which would otherwise erase them.
    if apply_event_signature_ctes_tiered(sql, signatures)
        .and_then(|s| validate_query(&s))
        .is_err()
    {
        return None;
    }
    Some(plan)
}

/// Degraded split path: the hot PostgreSQL arm failed, so serve the original
/// query natively on the ClickHouse archive (dual-written, full history).
async fn degrade_split_to_clickhouse(
    ch: &crate::clickhouse::ClickHouseEngine,
    sql: &str,
    signatures: &[&str],
    selector_null_cols: &[usize],
    hot_err: anyhow::Error,
    options: &QueryOptions,
    start: Instant,
) -> Result<QueryResult> {
    tracing::warn!(target: "tidx::query", error = %hot_err, "tiered: hot arm failed; degrading split query to the clickhouse archive");
    let timeout_ms = options
        .timeout_ms
        .saturating_sub(start.elapsed().as_millis() as u64)
        .max(TIERED_COLD_MIN_TIMEOUT_MS);
    let mut raw = ch
        .query_user_with_settings(
            sql,
            signatures,
            timeout_ms,
            options.limit,
            CH_QUERY_SETTINGS,
        )
        .await
        .map_err(|ch_err| {
            anyhow!("tiered split failed on both arms: hot (postgres): {hot_err}; cold (clickhouse): {ch_err}")
        })?;
    normalize_cold_result(&mut raw, selector_null_cols);
    let mut result: QueryResult = raw.into();
    result.engine = Some("tiered".to_string());
    result.query_time_ms = Some(start.elapsed().as_secs_f64() * 1000.0);
    Ok(result)
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
    let limit = options.limit;
    let sql = prepare_blocking(sql, signatures, move |sql, signatures| {
        let sql = apply_event_signature_ctes_tiered(sql, signatures)?;
        validate_query(&sql)?;
        Ok(append_limit_if_missing(&sql, limit))
    })
    .await?;

    // ClickHouse aborts its scans strictly before PostgreSQL's
    // statement_timeout: a cancel mid pg_clickhouse scan double-frees and
    // restarts the server (upstream 0.3.x bug), so the FDW must never be
    // what PostgreSQL cancels.
    let ch_deadline_secs = (options.timeout_ms / 1000).max(1);
    // The request's tag reaches ClickHouse's query_log through the FDW; its
    // characters need no quoting in the settings list. Bound as a parameter
    // (set_config below) so the per-request value never enters statement
    // text, which would give pg_stat_statements an entry per request.
    let log_comment = crate::query::QueryTag::current()
        .map(|tag| format!(", log_comment {}", tag.comment()))
        .unwrap_or_default();
    let session_settings = format!(
        "join_use_nulls 1, group_by_use_nulls 1, final 0, \
         max_execution_time {ch_deadline_secs}{log_comment}"
    );
    let statement_timeout = format!(
        "SET LOCAL statement_timeout = {}",
        options.timeout_ms + FDW_STATEMENT_TIMEOUT_MARGIN_MS
    );
    run_pg_query(
        pool,
        &sql,
        &[],
        options,
        &[
            // Core tables resolve to ch.* FDW; public supplies abi_string().
            "SET LOCAL search_path = ch, public",
            // ch.* carries prune-boundary CHECKs for the tiered views; the
            // archive holds full history, so never let them prune scans here.
            "SET LOCAL constraint_exclusion = off",
            // Overrides run_pg_query's base statement_timeout (margin above).
            &statement_timeout,
        ],
        // `final 0` drops pg_clickhouse's default FINAL merge (costly
        // reads; native ClickHouse engine semantics instead).
        &[("pg_clickhouse.session_settings", &session_settings)],
        "postgres-via-clickhouse",
    )
    .await
}

/// `SELECT set_config('<name>', $1, true), …` for transaction-local settings
/// whose values are bound. Names come from code, never from requests.
fn set_config_statement(settings: &[(&str, &str)]) -> String {
    let calls: Vec<String> = settings
        .iter()
        .enumerate()
        .map(|(i, (name, _))| format!("set_config('{name}', ${}, true)", i + 1))
        .collect();
    format!("SELECT {}", calls.join(", "))
}

/// Run prepared SQL on PostgreSQL and stream the result rows into a
/// [`QueryResult`]. `params` are bound to the query's `$n` placeholders, and
/// `session_setup` statements (e.g. `SET LOCAL …`) run inside the
/// transaction before the query.
async fn run_pg_query(
    pool: &Pool,
    sql: &str,
    params: &[(&(dyn ToSql + Sync), Type)],
    options: &QueryOptions,
    session_setup: &[&str],
    bound_settings: &[(&str, &str)],
    engine: &str,
) -> Result<QueryResult> {
    let mut conn = pool.get().await.map_err(|e| {
        anyhow::Error::new(PostgresQueryFailure::Unavailable {
            message: e.to_string(),
        })
    })?;
    let tx = conn.transaction().await.map_err(classify_postgres_error)?;

    let mut setup = format!("SET LOCAL statement_timeout = {}", options.timeout_ms);
    // Transaction-local settings whose values vary per request, such as the
    // request's tag (application_name, shown in pg_stat_activity and logs as
    // %a). Their values are bound parameters of one set_config statement, so
    // its text stays the same and pg_stat_statements keeps one entry for it.
    let application_name = crate::query::QueryTag::current().map(|tag| tag.application_name());
    let mut bound: Vec<(&str, &str)> = Vec::with_capacity(bound_settings.len() + 1);
    if let Some(name) = &application_name {
        bound.push(("application_name", name));
    }
    bound.extend_from_slice(bound_settings);
    let set_config_sql = (!bound.is_empty()).then(|| set_config_statement(&bound));
    let set_config_params: Vec<(&(dyn ToSql + Sync), Type)> = bound
        .iter()
        .map(|(_, value)| (value as &(dyn ToSql + Sync), Type::TEXT))
        .collect();
    for stmt in session_setup {
        setup.push_str(";\n");
        setup.push_str(stmt);
    }

    let start = Instant::now();
    let timeout = std::time::Duration::from_millis(options.timeout_ms + 100);
    let limit = options.limit as usize;
    let result = tokio::time::timeout(timeout, async {
        // The settings and the prepare go out together instead of one round
        // trip each. A request is sent on its first poll and PostgreSQL runs
        // requests in arrival order, so the settings apply before the query
        // is parsed. Resolve result types before executing: query_typed_raw
        // can deadlock discovering a composite type behind unread data rows.
        let setup = tx.batch_execute(&setup);
        let set_config = async {
            match &set_config_sql {
                Some(sql) => tx.query_typed(sql, &set_config_params).await.map(drop),
                None => Ok(()),
            }
        };
        let param_types: Vec<Type> = params.iter().map(|(_, ty)| ty.clone()).collect();
        let prepare = tx.prepare_typed(sql, &param_types);
        futures::pin_mut!(setup, set_config, prepare);
        let setup_sent = futures::poll!(setup.as_mut());
        let set_config_sent = futures::poll!(set_config.as_mut());
        let prepare_sent = futures::poll!(prepare.as_mut());
        match setup_sent {
            Poll::Ready(result) => result?,
            Poll::Pending => setup.await?,
        }
        match set_config_sent {
            Poll::Ready(result) => result?,
            Poll::Pending => set_config.await?,
        }
        let statement = match prepare_sent {
            Poll::Ready(result) => result?,
            Poll::Pending => prepare.await?,
        };
        let columns: Vec<String> = statement
            .columns()
            .iter()
            .map(|column| column.name().to_string())
            .collect();
        let stream = tx
            .query_raw(&statement, params.iter().map(|(value, _)| *value))
            .await?;
        futures::pin_mut!(stream);
        let mut rows = Vec::new();
        let mut result_bytes = 0usize;
        let mut result_cells = 0usize;
        let mut wire_bytes = 0usize;

        while let Some(row) = stream.try_next().await? {
            if rows.len() >= limit {
                return Err(anyhow!("Query returned more than {limit} rows"));
            }
            result_cells = add_result_cells(result_cells, columns.len())?;
            // The driver already buffered the row, so its wire size is known
            // before any cell is formatted. Every cell counts against the wire
            // budget, including those of types that format as null.
            let mut row_wire_bytes = 0usize;
            for i in 0..columns.len() {
                row_wire_bytes = row_wire_bytes.saturating_add(raw_cell_len(&row, i)?);
            }
            wire_bytes = add_wire_bytes(wire_bytes, row_wire_bytes)?;
            // Sized exactly, unlike a `collect` through `Result`, so the row
            // holds no spare capacity for the rest of the request.
            // Each cell is charged as soon as it is formatted: a compact wire
            // value can expand far past its wire size, such as a NUMERIC
            // `1e131068` (a few bytes) into 131,069 digits.
            let mut row_values = Vec::with_capacity(columns.len());
            result_bytes = result_bytes.saturating_add(row_structure_bytes(columns.len()));
            for i in 0..columns.len() {
                if result_bytes > MAX_QUERY_RESULT_BYTES {
                    return Err(result_too_large());
                }
                let value = try_format_column_json(&row, i)?;
                result_bytes = result_bytes.saturating_add(estimated_json_value_bytes(&value));
                row_values.push(value);
            }
            if result_bytes > MAX_QUERY_RESULT_BYTES {
                return Err(result_too_large());
            }
            rows.push(row_values);
        }

        Ok::<_, anyhow::Error>((columns, rows))
    })
    .await;

    let (columns, result_rows) = match result {
        Ok(Ok(result)) => {
            metrics::record_query_duration(start.elapsed());
            result
        }
        Ok(Err(e)) => {
            return Err(classify_query_execution_error(e));
        }
        Err(_) => return Err(PostgresQueryFailure::Timeout.into()),
    };

    tx.commit().await.map_err(classify_postgres_error)?;

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

/// Cells decode as `Option<T>` so NULLs don't allocate a `WasNull` error.
/// Dispatching on the built-in [`Type`] rather than the type name is
/// equivalent: none of these decoders accept a custom type that merely shares
/// a built-in's name, so such columns were already NULL.
fn try_format_column_json(row: &tokio_postgres::Row, idx: usize) -> Result<serde_json::Value> {
    let value = match *row.columns()[idx].type_() {
        Type::INT2 => row
            .try_get::<_, Option<i16>>(idx)
            .ok()
            .flatten()
            .map_or(serde_json::Value::Null, |v| {
                serde_json::Value::Number(v.into())
            }),
        Type::INT4 => row
            .try_get::<_, Option<i32>>(idx)
            .ok()
            .flatten()
            .map_or(serde_json::Value::Null, |v| {
                serde_json::Value::Number(v.into())
            }),
        Type::INT8 => row
            .try_get::<_, Option<i64>>(idx)
            .ok()
            .flatten()
            .map_or(serde_json::Value::Null, |v| {
                serde_json::Value::Number(v.into())
            }),
        // Arbitrary precision (abi_uint() on uint256 is 78 digits), so decoded
        // straight to its exact decimal text rather than a fixed-width type.
        Type::NUMERIC => row
            .try_get::<_, Option<PgNumericText>>(idx)
            .ok()
            .flatten()
            .and_then(|v| v.0)
            .map_or(serde_json::Value::Null, serde_json::Value::String),
        // Widen via the shortest decimal so 1.1::float4 is 1.1, as PostgreSQL
        // prints it, rather than 1.100000023841858.
        Type::FLOAT4 => row
            .try_get::<_, Option<f32>>(idx)
            .ok()
            .flatten()
            .and_then(|v| v.to_string().parse::<f64>().ok())
            .and_then(serde_json::Number::from_f64)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        Type::FLOAT8 => row
            .try_get::<_, Option<f64>>(idx)
            .ok()
            .flatten()
            .and_then(serde_json::Number::from_f64)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        Type::BYTEA => match row.try_get::<_, Option<&[u8]>>(idx) {
            Ok(Some(v)) if v.len() > MAX_CELL_BYTES => {
                return Err(anyhow!(
                    "Query result cell exceeded {} bytes",
                    MAX_CELL_BYTES
                ));
            }
            Ok(Some(v)) => serde_json::Value::String(alloy::hex::encode_prefixed(v)),
            _ => serde_json::Value::Null,
        },
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
            match row.try_get::<_, Option<&str>>(idx) {
                Ok(Some(v)) if v.len() > MAX_CELL_BYTES => {
                    return Err(anyhow!(
                        "Query result cell exceeded {} bytes",
                        MAX_CELL_BYTES
                    ));
                }
                Ok(Some(v)) => serde_json::Value::String(v.to_string()),
                _ => serde_json::Value::Null,
            }
        }
        Type::TIMESTAMPTZ => row
            .try_get::<_, Option<DateTime<Utc>>>(idx)
            .ok()
            .flatten()
            .map_or(serde_json::Value::Null, |v| {
                serde_json::Value::String(v.to_rfc3339())
            }),
        // Zoneless; read as UTC so it formats like timestamptz and ClickHouse
        // DateTime columns (e.g. `block_timestamp AT TIME ZONE 'UTC'`).
        Type::TIMESTAMP => row
            .try_get::<_, Option<chrono::NaiveDateTime>>(idx)
            .ok()
            .flatten()
            .map_or(serde_json::Value::Null, |v| {
                serde_json::Value::String(v.and_utc().to_rfc3339())
            }),
        Type::BOOL => row
            .try_get::<_, Option<bool>>(idx)
            .ok()
            .flatten()
            .map_or(serde_json::Value::Null, serde_json::Value::Bool),
        _ => serde_json::Value::Null,
    };

    Ok(value)
}

/// Exact size of `value` as `serde_json` serializes it, the encoder of API
/// responses: quotes, escapes and separators included.
fn estimated_json_value_bytes(value: &serde_json::Value) -> usize {
    let mut count = ByteCount(0);
    // Serializing a `Value` into a counter cannot fail; if it ever did, charge
    // everything so the result is rejected.
    match serde_json::to_writer(&mut count, value) {
        Ok(()) => count.0,
        Err(_) => usize::MAX,
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

fn classify_query_execution_error(error: anyhow::Error) -> anyhow::Error {
    match error.downcast::<tokio_postgres::Error>() {
        Ok(error) => classify_postgres_error(error),
        Err(error) => PostgresQueryFailure::Processing {
            message: format!("Query error: {}", sanitize_db_error(&format!("{error:#}"))),
        }
        .into(),
    }
}

fn classify_postgres_error(error: tokio_postgres::Error) -> anyhow::Error {
    let Some(db) = error.as_db_error() else {
        return PostgresQueryFailure::Unavailable {
            message: error.to_string(),
        }
        .into();
    };
    if db.code() == &SqlState::QUERY_CANCELED {
        return PostgresQueryFailure::Timeout.into();
    }
    PostgresQueryFailure::Database {
        code: db.code().clone(),
        message: format!(
            "Query error: {}",
            sanitize_db_error(&format!("{} ({})", db.message(), db.code().code()))
        ),
    }
    .into()
}

fn is_retryable_hot_postgres_failure(error: &anyhow::Error) -> bool {
    match error.downcast_ref::<PostgresQueryFailure>() {
        Some(PostgresQueryFailure::Unavailable { .. }) => true,
        Some(PostgresQueryFailure::Database { code, .. }) => {
            let code = code.code();
            code.starts_with("08")
                || matches!(code, "42P01" | "57P01" | "57P02" | "57P03" | "57P04")
        }
        _ => false,
    }
}

/// Sanitize database error messages to prevent information leakage.
///
/// Removes file paths, internal schema details, and other sensitive info
/// while preserving useful error context for debugging.
fn sanitize_db_error(error: &str) -> String {
    // Truncate very long errors on a char boundary (byte 500 may split a
    // multi-byte character).
    let error = if error.len() > 500 {
        let mut end = 500;
        while !error.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}...", &error[..end])
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

/// Adds one row of `columns` cells to the `total` cells of a result, failing
/// once the result would hold more than [`MAX_QUERY_CELLS`].
fn add_result_cells(total: usize, columns: usize) -> Result<usize> {
    total
        .checked_add(columns)
        .filter(|cells| *cells <= MAX_QUERY_CELLS)
        .ok_or_else(|| anyhow!("Query result exceeded {} cells", MAX_QUERY_CELLS))
}

/// Adds one row's `row_wire_bytes` to the `total` read from the database so
/// far, failing once the result would exceed [`MAX_QUERY_WIRE_BYTES`].
fn add_wire_bytes(total: usize, row_wire_bytes: usize) -> Result<usize> {
    total
        .checked_add(row_wire_bytes)
        .filter(|bytes| *bytes <= MAX_QUERY_WIRE_BYTES)
        .ok_or_else(|| {
            anyhow!(
                "Query read more than {} bytes from the database",
                MAX_QUERY_WIRE_BYTES
            )
        })
}

/// Wire length of the cell at `idx`, failing if it exceeds [`MAX_CELL_BYTES`].
/// Unlike [`try_format_column_json`], this bounds every column type.
fn raw_cell_len(row: &tokio_postgres::Row, idx: usize) -> Result<usize> {
    let RawCell(len) = row.try_get(idx)?;
    check_cell_len(len)
}

fn check_cell_len(len: usize) -> Result<usize> {
    if len > MAX_CELL_BYTES {
        return Err(anyhow!(
            "Query result cell exceeded {} bytes",
            MAX_CELL_BYTES
        ));
    }
    Ok(len)
}

/// Bytes charged against [`MAX_QUERY_RESULT_BYTES`] for a row of `cells`
/// cells besides the cells themselves, which are each charged their exact
/// JSON size ([`estimated_json_value_bytes`]), so an escape-heavy string is
/// charged what it serializes to and a cell formatted as null only its
/// `null`. The row's brackets, the commas between its cells and the comma
/// after the row are counted, so the charges of all rows add up to the size
/// of the response's `rows` array, give or take the array's own two brackets.
fn row_structure_bytes(cells: usize) -> usize {
    2 + cells.saturating_sub(1) + 1
}

fn result_too_large() -> anyhow::Error {
    anyhow!("Query result exceeded {} bytes", MAX_QUERY_RESULT_BYTES)
}

/// A cell's wire length, read from any column type and from NULLs.
struct RawCell(usize);

impl FromSql<'_> for RawCell {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(raw.len()))
    }

    fn from_sql_null(_: &Type) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(0))
    }

    fn accepts(_: &Type) -> bool {
        true
    }
}

/// A PostgreSQL NUMERIC as PostgreSQL's own decimal text (`numeric_out`),
/// exact at any precision; `None` for NaN and ±Infinity, which format as
/// NULL like non-finite floats.
struct PgNumericText(Option<String>);

impl FromSql<'_> for PgNumericText {
    /// Binary NUMERIC: `ndigits`, `weight`, `sign`, `dscale` (16 bits each),
    /// then `ndigits` base-10000 digits; digit `i` is worth
    /// 10000^(weight - i) and `dscale` is the number of fraction digits shown.
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        use std::fmt::Write;
        let word = |i: usize| -> Result<u16, Box<dyn std::error::Error + Sync + Send>> {
            raw.get(2 * i..2 * i + 2)
                .map(|b| u16::from_be_bytes([b[0], b[1]]))
                .ok_or_else(|| "truncated NUMERIC".into())
        };
        let ndigits = usize::from(word(0)?);
        let weight = i64::from(word(1)? as i16);
        let sign = word(2)?;
        let dscale = usize::from(word(3)?);
        if raw.len() != 8 + 2 * ndigits {
            return Err("malformed NUMERIC".into());
        }
        let negative = match sign {
            0x0000 => false,
            0x4000 => true,
            // NaN, +Infinity, -Infinity.
            0xC000 | 0xD000 | 0xF000 => return Ok(Self(None)),
            _ => return Err("invalid NUMERIC sign".into()),
        };
        // The base-10000 digit worth 10000^exp; positions past the stored
        // digits are zero.
        let digit = |exp: i64| -> Result<u16, Box<dyn std::error::Error + Sync + Send>> {
            match usize::try_from(weight - exp) {
                Ok(i) if i < ndigits => word(4 + i),
                _ => Ok(0),
            }
        };

        let mut out = String::new();
        if negative {
            out.push('-');
        }
        if weight < 0 {
            out.push('0');
        } else {
            write!(out, "{}", digit(weight)?)?;
            for exp in (0..weight).rev() {
                write!(out, "{:04}", digit(exp)?)?;
            }
        }
        if dscale > 0 {
            out.push('.');
            let mut fraction = String::with_capacity(dscale + 3);
            let mut exp = -1;
            while fraction.len() < dscale {
                write!(fraction, "{:04}", digit(exp)?)?;
                exp -= 1;
            }
            out.push_str(&fraction[..dscale]);
        }
        Ok(Self(Some(out)))
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::NUMERIC
    }
}

/// Counts the bytes written to it.
struct ByteCount(usize);

impl std::io::Write for ByteCount {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(buf.len());
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
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
        // Exact decimal strings at any width, like PG NUMERIC.
        assert_eq!(r.rows[0][4], json!("1000000000000000000"));
        assert_eq!(
            r.rows[0][5],
            json!("115792089237316195423570985008687907853269984665640564039457584007913129639935")
        );
    }

    #[test]
    fn normalize_int64_converts_nested_values() {
        // Types and cells as ClickHouse 26.10 writes them in JSONCompact
        // with output_format_json_quote_64bit_integers=1.
        let mut r = ch_result(
            &[
                "Array(UInt64)",
                "Tuple(Int64, String)",
                "Tuple(n UInt64, s String)",
                "Map(String, UInt64)",
                "Array(Map(String, Array(Int64)))",
                "Enum8('a,(b' = 1)",
                "Nullable(UInt64)",
                "Array(UInt256)",
                "Array(Nullable(Int64))",
                "Tuple(`a b` Int64, e Enum8('x,y' = 1), arr Array(String))",
                "Tuple(`a\\`b` UInt64, `c\\\\d` UInt64, `e\\`f` UInt64)",
                "Array(Tuple(DateTime64(3, 'U,T(C'), UInt64))",
            ],
            vec![vec![
                json!(["1", "18446744073709551615"]),
                json!(["-3", "x"]),
                json!({"n": "4", "s": "y"}),
                json!({"k": "5"}),
                json!([{"a": ["6"]}]),
                json!("a,(b"),
                json!("7"),
                json!([
                    "115792089237316195423570985008687907853269984665640564039457584007913129639935"
                ]),
                json!([null, "9"]),
                json!({"a b": "10", "e": "x,y", "arr": ["11"]}),
                json!({"a`b": "5", "c\\d": "6", "e`f": "7"}),
                json!([["2026-01-01T00:00:00.000Z", "12"]]),
            ]],
        );
        normalize_int64_columns(&mut r);
        assert_eq!(
            r.rows[0],
            [
                json!([1, 18_446_744_073_709_551_615_u64]),
                json!([-3, "x"]),
                json!({"n": 4, "s": "y"}),
                json!({"k": 5}),
                json!([{"a": [6]}]),
                json!("a,(b"),
                json!(7),
                json!([
                    "115792089237316195423570985008687907853269984665640564039457584007913129639935"
                ]),
                json!([null, 9]),
                json!({"a b": 10, "e": "x,y", "arr": ["11"]}),
                json!({"a`b": 5, "c\\d": 6, "e`f": 7}),
                json!([["2026-01-01T00:00:00.000Z", 12]]),
            ]
        );
    }

    #[test]
    fn normalize_datetime_unwraps_low_cardinality() {
        let mut r = ch_result(
            &[
                "LowCardinality(DateTime('UTC'))",
                "LowCardinality(Nullable(DateTime('UTC')))",
            ],
            vec![vec![json!("2026-09-11T22:38:08Z"), J::Null]],
        );
        normalize_datetime_columns(&mut r);
        assert_eq!(r.rows[0], [json!("2026-09-11T22:38:08+00:00"), J::Null]);
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
    fn hot_split_retries_only_postgres_availability_failures() {
        let unavailable = anyhow::Error::new(PostgresQueryFailure::Unavailable {
            message: "connection closed".to_string(),
        });
        assert!(is_retryable_hot_postgres_failure(&unavailable));

        for code in [
            SqlState::UNDEFINED_TABLE,
            SqlState::CONNECTION_FAILURE,
            SqlState::ADMIN_SHUTDOWN,
            SqlState::CRASH_SHUTDOWN,
            SqlState::CANNOT_CONNECT_NOW,
            SqlState::DATABASE_DROPPED,
        ] {
            let database = anyhow::Error::new(PostgresQueryFailure::Database {
                code,
                message: "availability error".to_string(),
            });
            assert!(is_retryable_hot_postgres_failure(&database));
        }

        for code in [
            SqlState::from_code("22021"),
            SqlState::QUERY_CANCELED,
            SqlState::SYNTAX_ERROR,
        ] {
            let database = anyhow::Error::new(PostgresQueryFailure::Database {
                code,
                message: "query error".to_string(),
            });
            assert!(!is_retryable_hot_postgres_failure(&database));
        }

        let timeout = anyhow::Error::new(PostgresQueryFailure::Timeout);
        assert!(!is_retryable_hot_postgres_failure(&timeout));

        let processing = anyhow::Error::new(PostgresQueryFailure::Processing {
            message: "result limit".to_string(),
        });
        assert!(!is_retryable_hot_postgres_failure(&processing));
        assert!(!is_retryable_hot_postgres_failure(&anyhow!(
            "validation error"
        )));
    }

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

        assert_eq!(
            estimated_json_value_bytes(&value),
            serde_json::to_vec(&value).unwrap().len()
        );
    }

    #[test]
    fn test_estimated_json_value_bytes_counts_escapes_and_quotes() {
        for s in [
            String::new(),
            "\"".repeat(1024),
            "\u{1}".repeat(1024),
            "a\\b\n\t\"\u{1f}\u{7f}".repeat(100),
            "h\u{e9}llo \u{1f600}".repeat(100),
        ] {
            let value = J::String(s);
            assert_eq!(
                estimated_json_value_bytes(&value),
                serde_json::to_vec(&value).unwrap().len()
            );
        }

        // Quotes double and control characters take six bytes, plus the quotes.
        assert_eq!(estimated_json_value_bytes(&json!("\"".repeat(1024))), 2050);
        assert_eq!(
            estimated_json_value_bytes(&json!("\u{1}".repeat(1024))),
            6146
        );
    }

    /// What `run_pg_query` charges for a row of `values`.
    fn row_result_bytes(values: &[serde_json::Value]) -> usize {
        values
            .iter()
            .map(estimated_json_value_bytes)
            .fold(row_structure_bytes(values.len()), usize::saturating_add)
    }

    #[test]
    fn test_row_result_bytes_charges_empty_cells() {
        let values = vec![json!(""); 512];
        let row = row_result_bytes(&values);

        // 512 times `""`, 511 commas, the brackets and the comma after the row.
        assert_eq!(row, 512 * 2 + 511 + 2 + 1);
        assert_eq!(row, serde_json::to_vec(&values).unwrap().len() + 1);
        // 10,000 such rows far exceed the budget they used to stay under.
        assert!(row * 10_000 > MAX_QUERY_RESULT_BYTES);
    }

    #[test]
    fn test_row_result_bytes_matches_serialized_row() {
        let values = vec![
            json!("\"quoted\" \u{1}"),
            json!(null),
            json!(-42),
            json!(true),
            json!("0xdeadbeef"),
        ];
        assert_eq!(
            row_result_bytes(&values),
            serde_json::to_vec(&values).unwrap().len() + 1
        );
        assert_eq!(row_result_bytes(&[]), 3);
    }

    #[test]
    fn test_row_result_bytes_charges_null_cells_only_their_json_size() {
        // A `jsonb` column formats as null (4 bytes) however large its wire
        // length, so it costs the response budget only `null`.
        let values = vec![J::Null, json!("a")];
        assert_eq!(row_result_bytes(&values), 4 + 3 + 1 + 2 + 1);

        // Its wire length counts against the larger wire budget instead: 10,000
        // such rows stay under the response budget but not under the wire one.
        assert!(row_result_bytes(&values) * 10_000 < MAX_QUERY_RESULT_BYTES);
        let mut wire = 0;
        let mut rows = 0;
        while let Ok(total) = add_wire_bytes(wire, 500_000) {
            wire = total;
            rows += 1;
        }
        assert!(rows < 10_000);
        assert!(wire > MAX_QUERY_RESULT_BYTES);
        assert_eq!(
            add_wire_bytes(wire, MAX_QUERY_WIRE_BYTES)
                .unwrap_err()
                .to_string(),
            "Query read more than 41943040 bytes from the database"
        );
        assert_eq!(
            add_wire_bytes(0, MAX_QUERY_WIRE_BYTES).unwrap(),
            MAX_QUERY_WIRE_BYTES
        );
    }

    #[test]
    fn test_raw_cell_reads_wire_length_of_any_type() {
        for ty in [
            Type::INT4_ARRAY,
            Type::TEXT_ARRAY,
            Type::JSONB,
            Type::UUID,
            Type::INT8,
            Type::TEXT,
        ] {
            assert!(RawCell::accepts(&ty), "{ty}");
            let RawCell(len) = RawCell::from_sql_nullable(&ty, Some(&[0; 4096])).unwrap();
            assert_eq!(len, 4096, "{ty}");
            let RawCell(len) = RawCell::from_sql_nullable(&ty, None).unwrap();
            assert_eq!(len, 0, "{ty}");
        }
    }

    #[test]
    fn test_check_cell_len_boundary() {
        assert_eq!(check_cell_len(MAX_CELL_BYTES).unwrap(), MAX_CELL_BYTES);
        assert_eq!(
            check_cell_len(MAX_CELL_BYTES + 1).unwrap_err().to_string(),
            "Query result cell exceeded 1048576 bytes"
        );
    }

    #[test]
    fn test_add_result_cells_boundary() {
        assert_eq!(add_result_cells(0, 512).unwrap(), 512);
        assert_eq!(
            add_result_cells(MAX_QUERY_CELLS - 512, 512).unwrap(),
            MAX_QUERY_CELLS
        );
        assert_eq!(
            add_result_cells(MAX_QUERY_CELLS - 511, 512)
                .unwrap_err()
                .to_string(),
            "Query result exceeded 1000000 cells"
        );
        assert!(add_result_cells(usize::MAX, 1).is_err());
    }

    #[test]
    fn test_estimated_json_value_bytes_numbers_match_json_text() {
        for n in [
            json!(0),
            json!(-42),
            json!(u64::MAX),
            json!(i64::MIN),
            json!(1.5),
            json!(1e-7),
        ] {
            assert_eq!(estimated_json_value_bytes(&n), n.to_string().len(), "{n}");
        }
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

    #[test]
    fn test_sanitize_truncates_multibyte_on_char_boundary() {
        // 3-byte chars; byte 500 falls mid-character.
        let error = "語".repeat(400);
        let sanitized = sanitize_db_error(&error);
        assert!(sanitized.ends_with("..."));
        assert!(sanitized.trim_end_matches("...").chars().all(|c| c == '語'));
    }
}
