//! Tiered-storage query objects: pg_clickhouse foreign tables over the
//! ClickHouse archive plus `tiered.*` views that stitch the hot PostgreSQL
//! window and the cold archive into one relation per table.
//!
//! Layout (per chain database):
//! - schema `ch`: foreign tables (`ch.blocks`, `ch.txs`, `ch.logs`,
//!   `ch.receipts`) over the ClickHouse archive via the `clickhouse_fdw`
//!   wrapper from the pg_clickhouse extension. Columns keep ClickHouse's
//!   representation: `0x…` hex text, `bigint`, `timestamptz`.
//! - schema `tiered`: `UNION ALL` views. The hot arm reads `public.*` and
//!   normalizes PG bytea columns to `0x…` hex text (`'0x' || encode(…)`);
//!   the cold arm reads `ch.*` unchanged. Normalizing on the small hot side
//!   keeps predicates (`address = '0x…'`) pushable to ClickHouse's bloom
//!   indexes on the big cold side.
//!
//! Boundary `B` = `sync_state.pruned_below` (highest block dropped from PG):
//! - cold arm: `WHERE block_num <= B` (enforces no duplicates)
//! - hot arm: `WHERE block_num > B` (hardens the crash window where the
//!   watermark advanced but partitions are not yet dropped)
//! - foreign tables carry `CHECK (block_num <= B)` and, when known,
//!   `CHECK (block_timestamp < T_B)` so constraint exclusion (default
//!   `constraint_exclusion = partition` covers UNION ALL arms) plans away
//!   the ClickHouse round trip for hot-window-bounded queries.
//!
//! The pruner refreshes the boundary right after advancing `pruned_below`
//! and **before** dropping partitions, so the views never expose a hole.
//!
//! All cold reads (native and FDW) run without `final`: unmerged
//! ReplacingMergeTree duplicates are possible but rare (crash replay only;
//! reorgs delete synchronously), matching the native ClickHouse engine.

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use tracing::{debug, info};

use super::Pool;

/// FDW server name (one ClickHouse archive per chain database).
const SERVER: &str = "tidx_clickhouse";

/// How a hot (PostgreSQL-native) column is normalized to the cold
/// (ClickHouse text) representation exposed by the tiered views.
#[derive(Clone, Copy)]
enum HotConv {
    /// Same representation on both sides (ints, timestamps, plain text).
    Direct,
    /// bytea -> '0x…' hex text (NULL propagates).
    HexText,
    /// bytea -> '0x…' hex text, NULL -> '' (CH stores `String DEFAULT ''`).
    HexTextEmpty,
    /// boolean -> 0/1 smallint (CH stores UInt8).
    IntFromBool,
    /// jsonb -> JSON text (CH stores `Nullable(String)`).
    TextFromJsonb,
}

/// One column of a tiered table: name, FDW-side PostgreSQL type, conversion.
struct Col {
    name: &'static str,
    fdw_type: &'static str,
    conv: HotConv,
}

const fn col(name: &'static str, fdw_type: &'static str, conv: HotConv) -> Col {
    Col {
        name,
        fdw_type,
        conv,
    }
}

/// A tiered table: PG/CH table name, boundary + timestamp columns, columns.
struct Table {
    name: &'static str,
    boundary_col: &'static str,
    timestamp_col: &'static str,
    cols: &'static [Col],
}

const BLOCKS: Table = Table {
    name: "blocks",
    boundary_col: "num",
    timestamp_col: "timestamp",
    cols: &[
        col("num", "bigint", HotConv::Direct),
        col("hash", "text", HotConv::HexText),
        col("parent_hash", "text", HotConv::HexText),
        col("timestamp", "timestamptz", HotConv::Direct),
        col("timestamp_ms", "bigint", HotConv::Direct),
        col("gas_limit", "bigint", HotConv::Direct),
        col("gas_used", "bigint", HotConv::Direct),
        col("miner", "text", HotConv::HexText),
        col("extra_data", "text", HotConv::HexText),
        col("consensus_proposer", "text", HotConv::HexText),
    ],
};

const TXS: Table = Table {
    name: "txs",
    boundary_col: "block_num",
    timestamp_col: "block_timestamp",
    cols: &[
        col("block_num", "bigint", HotConv::Direct),
        col("block_timestamp", "timestamptz", HotConv::Direct),
        col("idx", "integer", HotConv::Direct),
        col("hash", "text", HotConv::HexText),
        col("type", "smallint", HotConv::Direct),
        col("from", "text", HotConv::HexText),
        col("to", "text", HotConv::HexText),
        col("value", "text", HotConv::Direct),
        col("input", "text", HotConv::HexText),
        col("gas_limit", "bigint", HotConv::Direct),
        col("max_fee_per_gas", "text", HotConv::Direct),
        col("max_priority_fee_per_gas", "text", HotConv::Direct),
        col("gas_used", "bigint", HotConv::Direct),
        col("nonce_key", "text", HotConv::HexText),
        col("nonce", "bigint", HotConv::Direct),
        col("fee_token", "text", HotConv::HexText),
        col("fee_payer", "text", HotConv::HexText),
        col("calls", "text", HotConv::TextFromJsonb),
        col("call_count", "smallint", HotConv::Direct),
        col("valid_before", "bigint", HotConv::Direct),
        col("valid_after", "bigint", HotConv::Direct),
        col("signature_type", "smallint", HotConv::Direct),
    ],
};

const LOGS: Table = Table {
    name: "logs",
    boundary_col: "block_num",
    timestamp_col: "block_timestamp",
    cols: &[
        col("block_num", "bigint", HotConv::Direct),
        col("block_timestamp", "timestamptz", HotConv::Direct),
        col("log_idx", "integer", HotConv::Direct),
        col("tx_idx", "integer", HotConv::Direct),
        col("tx_hash", "text", HotConv::HexText),
        col("address", "text", HotConv::HexText),
        col("selector", "text", HotConv::HexTextEmpty),
        col("topic0", "text", HotConv::HexText),
        col("topic1", "text", HotConv::HexText),
        col("topic2", "text", HotConv::HexText),
        col("topic3", "text", HotConv::HexText),
        col("data", "text", HotConv::HexText),
        col("is_virtual_forward", "smallint", HotConv::IntFromBool),
    ],
};

const RECEIPTS: Table = Table {
    name: "receipts",
    boundary_col: "block_num",
    timestamp_col: "block_timestamp",
    cols: &[
        col("block_num", "bigint", HotConv::Direct),
        col("block_timestamp", "timestamptz", HotConv::Direct),
        col("tx_idx", "integer", HotConv::Direct),
        col("tx_hash", "text", HotConv::HexText),
        col("from", "text", HotConv::HexText),
        col("to", "text", HotConv::HexText),
        col("contract_address", "text", HotConv::HexText),
        col("gas_used", "bigint", HotConv::Direct),
        col("cumulative_gas_used", "bigint", HotConv::Direct),
        col("effective_gas_price", "text", HotConv::Direct),
        col("status", "smallint", HotConv::Direct),
        col("fee_payer", "text", HotConv::HexText),
    ],
};

const TABLES: &[&Table] = &[&BLOCKS, &TXS, &LOGS, &RECEIPTS];

/// ClickHouse connection target for the FDW, as seen from PostgreSQL.
#[derive(Debug, Clone)]
pub struct FdwTarget {
    pub host: String,
    pub port: u16,
    pub secure: bool,
    pub database: String,
    pub user: String,
    pub password: String,
}

impl FdwTarget {
    /// Parse an HTTP(S) ClickHouse URL into FDW server options.
    pub fn new(
        url: &str,
        database: String,
        user: Option<String>,
        password: Option<String>,
    ) -> Result<Self> {
        let parsed = url::Url::parse(url).with_context(|| format!("invalid FDW URL '{url}'"))?;
        let secure = match parsed.scheme() {
            "http" => false,
            "https" => true,
            other => return Err(anyhow!("unsupported FDW URL scheme '{other}'")),
        };
        let host = parsed
            .host_str()
            .ok_or_else(|| anyhow!("FDW URL '{url}' has no host"))?
            .to_string();
        let port = parsed.port().unwrap_or(if secure { 8443 } else { 8123 });
        Ok(Self {
            host,
            port,
            secure,
            database,
            user: user.unwrap_or_else(|| "default".to_string()),
            password: password.unwrap_or_default(),
        })
    }
}

/// Escape a string for use inside a single-quoted SQL literal.
fn sql_literal(s: &str) -> String {
    s.replace('\'', "''")
}

/// The hot-arm select expression normalizing a PG column to CH representation.
fn hot_expr(c: &Col) -> String {
    let q = format!("\"{}\"", c.name);
    match c.conv {
        HotConv::Direct => q,
        HotConv::HexText => format!("'0x' || encode({q}, 'hex')"),
        HotConv::HexTextEmpty => format!("COALESCE('0x' || encode({q}, 'hex'), '')"),
        HotConv::IntFromBool => format!("({q}::int)::smallint"),
        HotConv::TextFromJsonb => format!("{q}::text"),
    }
}

/// Foreign table DDL for one archive table.
fn foreign_table_sql(t: &Table) -> String {
    let cols: Vec<String> = t
        .cols
        .iter()
        .map(|c| format!("\"{}\" {}", c.name, c.fdw_type))
        .collect();
    format!(
        "CREATE FOREIGN TABLE ch.{name} (\n    {cols}\n) SERVER {SERVER} OPTIONS (table_name '{name}')",
        name = t.name,
        cols = cols.join(",\n    "),
    )
}

/// CHECK-constraint DDL bounding a foreign table at the prune boundary.
///
/// Declarative only (PG assumes, never verifies): correct because the cold
/// view arm also filters `block_num <= boundary`. Present so constraint
/// exclusion can plan away the ClickHouse arm for hot-bounded queries.
fn boundary_check_sql(t: &Table, boundary: i64, boundary_ts: Option<DateTime<Utc>>) -> String {
    let mut actions = vec![
        format!(
            "DROP CONSTRAINT IF EXISTS {name}_boundary_num",
            name = t.name
        ),
        format!(
            "ADD CONSTRAINT {name}_boundary_num CHECK (\"{col}\" <= {boundary})",
            name = t.name,
            col = t.boundary_col,
        ),
        format!(
            "DROP CONSTRAINT IF EXISTS {name}_boundary_ts",
            name = t.name
        ),
    ];
    if let Some(ts) = boundary_ts {
        actions.push(format!(
            "ADD CONSTRAINT {name}_boundary_ts CHECK (\"{col}\" < '{ts}')",
            name = t.name,
            col = t.timestamp_col,
            ts = ts.to_rfc3339(),
        ));
    }
    format!(
        "ALTER FOREIGN TABLE ch.{name} {actions}",
        name = t.name,
        actions = actions.join(", "),
    )
}

/// Tiered view DDL: normalized hot PG arm above the boundary, cold FDW arm
/// at/below it. With `boundary <= 0` (nothing pruned) the view is hot-only.
fn tiered_view_sql(t: &Table, boundary: i64) -> String {
    let hot_cols: Vec<String> = t.cols.iter().map(hot_expr).collect();
    let hot_select = t
        .cols
        .iter()
        .zip(&hot_cols)
        .map(|(c, expr)| format!("{expr} AS \"{}\"", c.name))
        .collect::<Vec<_>>()
        .join(", ");

    if boundary <= 0 {
        return format!(
            "CREATE OR REPLACE VIEW tiered.{name} AS\nSELECT {hot_select}\nFROM public.{name}",
            name = t.name,
        );
    }

    let cold_cols: Vec<String> = t.cols.iter().map(|c| format!("\"{}\"", c.name)).collect();
    format!(
        "CREATE OR REPLACE VIEW tiered.{name} AS\n\
         SELECT {hot_select}\nFROM public.{name}\nWHERE \"{bcol}\" > {boundary}\n\
         UNION ALL\n\
         SELECT {cold_cols}\nFROM ch.{name}\nWHERE \"{bcol}\" <= {boundary}",
        name = t.name,
        bcol = t.boundary_col,
        cold_cols = cold_cols.join(", "),
    )
}

/// Prune watermark plus the sync tip, read together from `sync_state`.
#[derive(Debug, Clone, Copy, Default)]
pub struct PruneBoundary {
    /// Highest block pruned from PostgreSQL (0 = nothing pruned).
    pub boundary: i64,
    /// Exclusive upper timestamp bound of pruned rows.
    pub boundary_ts: Option<DateTime<Utc>>,
    /// Highest block synced near chain head (`sync_state.tip_num`).
    pub tip: i64,
}

/// Read the prune watermark, its partition-boundary timestamp, and the sync
/// tip. Returns the default (all zero/None) when the chain has no state row.
pub async fn fetch_prune_boundary(pool: &Pool, chain_id: u64) -> Result<PruneBoundary> {
    let conn = pool.get().await?;
    let row = conn
        .query_opt(
            "SELECT pruned_below, pruned_below_ts, tip_num FROM sync_state WHERE chain_id = $1",
            &[&(chain_id as i64)],
        )
        .await?;
    Ok(row
        .map(|r| PruneBoundary {
            boundary: r.get(0),
            boundary_ts: r.get(1),
            tip: r.get(2),
        })
        .unwrap_or_default())
}

/// Whether the tiered FDW server exists in this database.
pub async fn is_bootstrapped(pool: &Pool) -> Result<bool> {
    let conn = pool.get().await?;
    let row = conn
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = $1)",
            &[&SERVER],
        )
        .await?;
    Ok(row.get(0))
}

/// Create/replace all tiered objects: extension, FDW server, user mapping,
/// foreign tables, boundary constraints, and views at the current boundary.
///
/// Idempotent; recreates the server (CASCADE drops dependents) to pick up
/// config changes. Requires the pg_clickhouse extension to be installable
/// on the PostgreSQL server. Returns the boundary baked into the views.
pub async fn bootstrap(pool: &Pool, target: &FdwTarget, chain_id: u64) -> Result<i64> {
    let mut ddl = vec![
        "CREATE EXTENSION IF NOT EXISTS pg_clickhouse".to_string(),
        "CREATE SCHEMA IF NOT EXISTS ch".to_string(),
        "CREATE SCHEMA IF NOT EXISTS tiered".to_string(),
        // CASCADE drops old foreign tables and dependent views.
        format!("DROP SERVER IF EXISTS {SERVER} CASCADE"),
        format!(
            "CREATE SERVER {SERVER} FOREIGN DATA WRAPPER clickhouse_fdw OPTIONS (\
             driver 'http', host '{host}', port '{port}', dbname '{db}', secure '{secure}')",
            host = sql_literal(&target.host),
            port = target.port,
            db = sql_literal(&target.database),
            secure = if target.secure { "on" } else { "off" },
        ),
        format!(
            "CREATE USER MAPPING FOR CURRENT_USER SERVER {SERVER} OPTIONS (user '{user}', password '{password}')",
            user = sql_literal(&target.user),
            password = sql_literal(&target.password),
        ),
    ];
    ddl.extend(TABLES.iter().map(|t| foreign_table_sql(t)));

    let conn = pool.get().await?;
    conn.batch_execute(&ddl.join(";\n"))
        .await
        .context("tiered bootstrap DDL failed (is the pg_clickhouse extension available?)")?;
    drop(conn);

    let boundary = refresh_boundary(pool, chain_id).await?;
    info!(
        chain_id,
        boundary,
        host = %target.host,
        database = %target.database,
        "Tiered storage bootstrapped (ch.* foreign tables + tiered.* views)"
    );
    Ok(boundary)
}

/// Rebake the boundary constraints and tiered views at the current
/// `pruned_below` watermark. No-op (returns -1) when not bootstrapped.
pub async fn refresh_boundary(pool: &Pool, chain_id: u64) -> Result<i64> {
    if !is_bootstrapped(pool).await? {
        debug!(
            chain_id,
            "Tiered storage not bootstrapped; skipping refresh"
        );
        return Ok(-1);
    }

    let PruneBoundary {
        boundary,
        boundary_ts,
        ..
    } = fetch_prune_boundary(pool, chain_id).await?;
    let mut ddl: Vec<String> = TABLES
        .iter()
        .map(|t| boundary_check_sql(t, boundary, boundary_ts))
        .collect();
    ddl.extend(TABLES.iter().map(|t| tiered_view_sql(t, boundary)));

    let conn = pool.get().await?;
    conn.batch_execute(&ddl.join(";\n")).await?;
    debug!(chain_id, boundary, "Tiered boundary refreshed");
    Ok(boundary)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fdw_target_parses_http_url() {
        let t = FdwTarget::new("http://clickhouse:8123", "db".into(), None, None).unwrap();
        assert_eq!(t.host, "clickhouse");
        assert_eq!(t.port, 8123);
        assert!(!t.secure);
        assert_eq!(t.user, "default");
        assert_eq!(t.password, "");
    }

    #[test]
    fn test_fdw_target_parses_https_url_with_defaults() {
        let t = FdwTarget::new(
            "https://ch.example.com",
            "db".into(),
            Some("reader".into()),
            Some("s3cret".into()),
        )
        .unwrap();
        assert_eq!(t.host, "ch.example.com");
        assert_eq!(t.port, 8443);
        assert!(t.secure);
        assert_eq!(t.user, "reader");
        assert_eq!(t.password, "s3cret");
    }

    #[test]
    fn test_fdw_target_rejects_non_http_scheme() {
        assert!(FdwTarget::new("tcp://ch:9000", "db".into(), None, None).is_err());
    }

    #[test]
    fn test_tiered_view_hot_only_when_nothing_pruned() {
        let sql = tiered_view_sql(&BLOCKS, 0);
        assert!(!sql.contains("UNION ALL"));
        assert!(sql.contains("FROM public.blocks"));
        assert!(sql.contains("'0x' || encode(\"hash\", 'hex') AS \"hash\""));
    }

    #[test]
    fn test_tiered_view_splits_at_boundary() {
        let sql = tiered_view_sql(&LOGS, 12345);
        assert!(sql.contains("FROM public.logs\nWHERE \"block_num\" > 12345"));
        assert!(sql.contains("FROM ch.logs\nWHERE \"block_num\" <= 12345"));
        // Hot arm normalizes to CH text representation.
        assert!(sql.contains("COALESCE('0x' || encode(\"selector\", 'hex'), '') AS \"selector\""));
        assert!(sql.contains("(\"is_virtual_forward\"::int)::smallint AS \"is_virtual_forward\""));
        // Cold arm passes FDW columns through unchanged.
        assert!(sql.contains("SELECT \"block_num\", \"block_timestamp\""));
    }

    #[test]
    fn test_blocks_view_uses_num_boundary() {
        let sql = tiered_view_sql(&BLOCKS, 77);
        assert!(sql.contains("WHERE \"num\" > 77"));
        assert!(sql.contains("WHERE \"num\" <= 77"));
    }

    #[test]
    fn test_txs_view_normalizes_jsonb_and_reserved_names() {
        let sql = tiered_view_sql(&TXS, 10);
        assert!(sql.contains("\"calls\"::text AS \"calls\""));
        assert!(sql.contains("'0x' || encode(\"from\", 'hex') AS \"from\""));
    }

    #[test]
    fn test_boundary_check_includes_num_and_timestamp() {
        let ts = DateTime::parse_from_rfc3339("2026-01-05T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let sql = boundary_check_sql(&LOGS, 500, Some(ts));
        assert!(sql.starts_with("ALTER FOREIGN TABLE ch.logs "));
        assert!(sql.contains("ADD CONSTRAINT logs_boundary_num CHECK (\"block_num\" <= 500)"));
        assert!(
            sql.contains("ADD CONSTRAINT logs_boundary_ts CHECK (\"block_timestamp\" < '2026-01-05T00:00:00+00:00')")
        );
    }

    #[test]
    fn test_boundary_check_without_timestamp() {
        let sql = boundary_check_sql(&BLOCKS, 500, None);
        assert!(sql.contains("ADD CONSTRAINT blocks_boundary_num CHECK (\"num\" <= 500)"));
        assert!(sql.contains("DROP CONSTRAINT IF EXISTS blocks_boundary_ts"));
        assert!(!sql.contains("ADD CONSTRAINT blocks_boundary_ts"));
    }

    #[test]
    fn test_foreign_table_quotes_reserved_names() {
        let sql = foreign_table_sql(&TXS);
        assert!(sql.contains("\"from\" text"));
        assert!(sql.contains("\"to\" text"));
        assert!(sql.contains("OPTIONS (table_name 'txs')"));
    }

    #[test]
    fn test_sql_literal_escapes_quotes() {
        assert_eq!(sql_literal("pa'ss"), "pa''ss");
    }
}
