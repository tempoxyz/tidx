mod parser;
mod router;
mod validator;

pub use parser::{
    extract_column_references, extract_equality_filters, extract_group_by_columns,
    extract_order_by_columns, AbiParam, AbiType, EventSignature,
};
pub use router::{route_query, QueryEngine};
pub use validator::{validate_query, HARD_LIMIT_MAX};

use regex_lite::Regex;
use std::sync::LazyLock;

/// Regex to match hex literals: '0x' followed by 40+ hex characters (addresses, topics, hashes)
/// This avoids matching short '0x' prefixes used in concat() expressions
static HEX_LITERAL_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"'0x([0-9a-fA-F]{40,})'").unwrap());

/// Convert '0x...' hex literals to ClickHouse-compatible format for MaterializedPostgreSQL.
/// 
/// MaterializedPostgreSQL stores PostgreSQL bytea as '\x'-prefixed hex strings.
/// ClickHouse interprets '\x' as an escape sequence, so we use concat(char(92), 'x...')
/// to build a literal backslash-x prefix.
/// 
/// Only replaces hex values of 40+ chars (addresses, topics, hashes), not short '0x' prefixes.
pub fn convert_hex_literals_clickhouse(sql: &str) -> String {
    HEX_LITERAL_RE
        .replace_all(sql, r"concat(char(92), 'x$1')")
        .into_owned()
}

/// Convert '0x...' hex literals to '\x...' for PostgreSQL bytea comparison.
/// Only replaces hex values of 40+ chars (addresses, topics, hashes), not short '0x' prefixes.
pub fn convert_hex_literals_postgres(sql: &str) -> String {
    HEX_LITERAL_RE
        .replace_all(sql, r"'\x$1'")
        .into_owned()
}
