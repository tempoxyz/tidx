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
pub use tiered_split::{TieredSplit, hot_window_confined, plan_tiered_split};
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
