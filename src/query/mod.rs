mod parser;
mod router;
mod tiered_split;
mod validator;

pub use parser::{
    AbiParam, AbiType, EventSignature, apply_event_signature_ctes_clickhouse,
    apply_event_signature_ctes_postgres, apply_event_signature_ctes_tiered,
    extract_column_references, extract_equality_filters, extract_group_by_columns,
    extract_order_by_columns, extract_raw_column_predicates,
};
pub use router::QueryRoute;
pub use tiered_split::{
    HotWindow, TieredSplit, hot_window_confined, hot_window_confinement, plan_tiered_split,
};
pub use validator::{HARD_LIMIT_MAX, validate_clickhouse_query, validate_query};

use regex_lite::Regex;
use sqlparser::ast::{SetExpr, Statement, TableFactor};
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser;
use std::sync::LazyLock;

/// Regex to match hex literals: '0x' followed by 40+ hex characters (addresses, topics, hashes)
/// This avoids matching short '0x' prefixes used in concat() expressions
static HEX_LITERAL_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"'0x([0-9a-fA-F]{40,})'").unwrap());

/// Convert '0x...' hex literals to '\x...' for PostgreSQL bytea comparison.
/// Only replaces hex values of 40+ chars (addresses, topics, hashes), not short '0x' prefixes.
pub fn convert_hex_literals_postgres(sql: &str) -> String {
    HEX_LITERAL_RE.replace_all(sql, r"'\x$1'").into_owned()
}

/// Whether `sql` contains a 40+ digit '0x…' literal with uppercase hex.
/// PostgreSQL decodes these case-insensitively as bytea; ClickHouse compares
/// its lowercase hex strings case-sensitively.
pub(crate) fn has_mixed_case_hex_literal(sql: &str) -> bool {
    HEX_LITERAL_RE
        .captures_iter(sql)
        .any(|c| c[1].bytes().any(|b| b.is_ascii_uppercase()))
}

/// Regex to match RFC 3339 timestamp string literals with an explicit UTC
/// offset ('Z' or ±HH:MM). ClickHouse cannot parse these into DateTime64.
static TS_LITERAL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"'(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?)(Z|z|[+-]\d{2}:\d{2})'")
        .unwrap()
});

/// Rewrite RFC 3339 timestamp literals (offset-suffixed, e.g.
/// '2026-07-09T03:30:42+00:00') to ClickHouse-parseable UTC form
/// ('2026-07-09 03:30:42'). PostgreSQL accepts both; ClickHouse rejects
/// offsets when comparing against DateTime64 columns (stored as UTC).
pub fn convert_timestamp_literals_clickhouse(sql: &str) -> String {
    use chrono::{DateTime, SecondsFormat, Utc};
    TS_LITERAL_RE
        .replace_all(sql, |c: &regex_lite::Captures<'_>| {
            let rfc3339 = format!("{}T{}{}", &c[1], &c[2], &c[3]);
            match DateTime::parse_from_rfc3339(&rfc3339) {
                Ok(dt) => {
                    let utc = dt
                        .with_timezone(&Utc)
                        .to_rfc3339_opts(SecondsFormat::AutoSi, true);
                    // '2026-07-09T03:30:42Z' → '2026-07-09 03:30:42'
                    format!("'{}'", utc.trim_end_matches('Z').replace('T', " "))
                }
                Err(_) => c[0].to_string(),
            }
        })
        .into_owned()
}

/// Hoist a set operation's trailing `ORDER BY`/`LIMIT`/`OFFSET` into a
/// derived-table wrapper: `SELECT * FROM (<set operation>) AS tidx_set_query
/// ORDER BY ... LIMIT ...`. The trailing form is valid PostgreSQL, but
/// ClickHouse grammar only accepts another set operator after a parenthesized
/// arm. Non-matching queries are returned unchanged.
pub fn hoist_set_operation_order_by_clickhouse(sql: &str) -> String {
    let Ok(mut statements) = Parser::parse_sql(&ClickHouseDialect {}, sql) else {
        return sql.to_string();
    };
    let [Statement::Query(query)] = statements.as_mut_slice() else {
        return sql.to_string();
    };
    if !matches!(query.body.as_ref(), SetExpr::SetOperation { .. })
        || (query.order_by.is_none() && query.limit_clause.is_none() && query.fetch.is_none())
    {
        return sql.to_string();
    }

    // Splice the set operation into a parsed wrapper; every other query-level
    // clause stays put and now attaches to the wrapper's plain SELECT.
    let wrapper = "SELECT * FROM (SELECT 1) AS tidx_set_query";
    let Some(Statement::Query(mut wrapper_query)) =
        Parser::parse_sql(&ClickHouseDialect {}, wrapper)
            .expect("valid wrapper template")
            .pop()
    else {
        unreachable!("wrapper template is a single query");
    };
    let SetExpr::Select(select) = wrapper_query.body.as_mut() else {
        unreachable!("wrapper template body is a select");
    };
    let Some(TableFactor::Derived { subquery, .. }) =
        select.from.first_mut().map(|t| &mut t.relation)
    else {
        unreachable!("wrapper template selects from a derived table");
    };
    std::mem::swap(&mut subquery.body, &mut query.body);
    query.body = wrapper_query.body;
    statements[0].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_literals_utc_offset() {
        assert_eq!(
            convert_timestamp_literals_clickhouse("ts >= '2026-07-09T03:30:42+00:00'"),
            "ts >= '2026-07-09 03:30:42'"
        );
        assert_eq!(
            convert_timestamp_literals_clickhouse("ts >= '2026-07-09T03:30:42Z'"),
            "ts >= '2026-07-09 03:30:42'"
        );
    }

    #[test]
    fn timestamp_literals_nonzero_offset_converts_to_utc() {
        assert_eq!(
            convert_timestamp_literals_clickhouse("ts < '2026-07-09T05:30:42+02:00'"),
            "ts < '2026-07-09 03:30:42'"
        );
    }

    #[test]
    fn timestamp_literals_fractional_seconds() {
        assert_eq!(
            convert_timestamp_literals_clickhouse("ts = '2026-07-09T03:30:42.123+00:00'"),
            "ts = '2026-07-09 03:30:42.123'"
        );
    }

    #[test]
    fn timestamp_literals_space_separator() {
        assert_eq!(
            convert_timestamp_literals_clickhouse("ts >= '2026-07-09 03:30:42+00:00'"),
            "ts >= '2026-07-09 03:30:42'"
        );
    }

    #[test]
    fn hoist_union_trailing_order_by_limit() {
        assert_eq!(
            hoist_set_operation_order_by_clickhouse(
                "(SELECT num FROM blocks ORDER BY num DESC LIMIT 2) \
                 UNION (SELECT num FROM blocks ORDER BY num DESC LIMIT 2) \
                 ORDER BY num DESC LIMIT 2"
            ),
            "SELECT * FROM ((SELECT num FROM blocks ORDER BY num DESC LIMIT 2) \
             UNION (SELECT num FROM blocks ORDER BY num DESC LIMIT 2)) AS tidx_set_query \
             ORDER BY num DESC LIMIT 2"
        );
    }

    #[test]
    fn hoist_union_trailing_offset() {
        assert_eq!(
            hoist_set_operation_order_by_clickhouse(
                "SELECT num FROM blocks UNION ALL SELECT num FROM txs ORDER BY num LIMIT 5 OFFSET 1"
            ),
            "SELECT * FROM (SELECT num FROM blocks UNION ALL SELECT num FROM txs) \
             AS tidx_set_query ORDER BY num LIMIT 5 OFFSET 1"
        );
    }

    #[test]
    fn hoist_preserves_leading_cte() {
        assert_eq!(
            hoist_set_operation_order_by_clickhouse(
                "WITH b AS (SELECT num FROM blocks) \
                 (SELECT num FROM b) UNION (SELECT num FROM b) ORDER BY num"
            ),
            "WITH b AS (SELECT num FROM blocks) \
             SELECT * FROM ((SELECT num FROM b) UNION (SELECT num FROM b)) \
             AS tidx_set_query ORDER BY num"
        );
    }

    #[test]
    fn hoist_untouched() {
        // Plain selects and bare set operations pass through unchanged.
        for sql in [
            "SELECT num FROM blocks ORDER BY num DESC LIMIT 2",
            "(SELECT num FROM blocks LIMIT 1) UNION (SELECT num FROM txs LIMIT 1)",
            "SELECT * FROM (SELECT num FROM blocks UNION SELECT num FROM txs) AS t ORDER BY num",
        ] {
            assert_eq!(hoist_set_operation_order_by_clickhouse(sql), sql);
        }
    }

    #[test]
    fn timestamp_literals_untouched() {
        // No offset: already ClickHouse-parseable.
        for sql in [
            "ts >= '2026-07-09 03:30:42'",
            "ts >= '2026-07-09T03:30:42'",
            "name = 'not a timestamp'",
            "v = '0x2026'",
        ] {
            assert_eq!(convert_timestamp_literals_clickhouse(sql), sql);
        }
    }
}
