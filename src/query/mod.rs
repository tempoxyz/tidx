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
