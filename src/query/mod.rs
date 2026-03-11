mod parser;
mod router;
mod validator;

pub use parser::{
    extract_column_references, extract_equality_filters, extract_group_by_columns,
    extract_order_by_columns, extract_raw_column_predicates, AbiParam, AbiType, EventSignature,
};
pub use router::QueryEngine;
pub use validator::{validate_query, HARD_LIMIT_MAX};

use regex_lite::Regex;
use std::sync::LazyLock;

/// Regex to match hex literals: '0x' followed by 40+ hex characters (addresses, topics, hashes)
/// This avoids matching short '0x' prefixes used in concat() expressions
static HEX_LITERAL_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"'0x([0-9a-fA-F]{40,})'").unwrap());

/// Convert '0x...' hex literals to '\x...' for PostgreSQL bytea comparison.
/// Only replaces hex values of 40+ chars (addresses, topics, hashes), not short '0x' prefixes.
pub fn convert_hex_literals_postgres(sql: &str) -> String {
    HEX_LITERAL_RE
        .replace_all(sql, r"'\x$1'")
        .into_owned()
}
