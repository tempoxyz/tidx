mod parser;
mod router;
mod validator;

pub use parser::{
    extract_column_references, extract_equality_filters, extract_group_by_columns,
    extract_order_by_columns, AbiParam, AbiType, EventSignature,
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

/// Merge generated CTE definitions into a user query.
///
/// If query already starts with a WITH clause, generated CTEs are prepended
/// to the existing CTE list. Otherwise, a new WITH clause is added.
pub fn merge_ctes_into_query(sql: &str, generated_ctes: &[String]) -> String {
    if generated_ctes.is_empty() {
        return sql.to_string();
    }

    let ctes = generated_ctes.join(", ");
    let trimmed = sql.trim_start();
    let leading_ws = &sql[..sql.len() - trimmed.len()];

    if starts_with_keyword(trimmed, "WITH RECURSIVE") {
        let rest = trimmed["WITH RECURSIVE".len()..].trim_start();
        return format!("{leading_ws}WITH RECURSIVE {ctes}, {rest}");
    }

    if starts_with_keyword(trimmed, "WITH") {
        let rest = trimmed["WITH".len()..].trim_start();
        return format!("{leading_ws}WITH {ctes}, {rest}");
    }

    format!("{leading_ws}WITH {ctes} {trimmed}")
}

fn starts_with_keyword(input: &str, keyword: &str) -> bool {
    input.len() >= keyword.len()
        && input[..keyword.len()].eq_ignore_ascii_case(keyword)
        && input[keyword.len()..]
            .chars()
            .next()
            .is_none_or(|c| c.is_ascii_whitespace())
}

#[cfg(test)]
mod tests {
    use super::merge_ctes_into_query;

    #[test]
    fn test_merge_ctes_into_plain_select() {
        let merged = merge_ctes_into_query(
            "SELECT n FROM numbers",
            &["Transfer AS (SELECT 1)".to_string()],
        );
        assert_eq!(merged, "WITH Transfer AS (SELECT 1) SELECT n FROM numbers");
    }

    #[test]
    fn test_merge_ctes_into_existing_with() {
        let merged = merge_ctes_into_query(
            "WITH numbers AS (SELECT 1 AS n) SELECT n FROM numbers",
            &["Transfer AS (SELECT 1)".to_string()],
        );
        assert_eq!(
            merged,
            "WITH Transfer AS (SELECT 1), numbers AS (SELECT 1 AS n) SELECT n FROM numbers"
        );
    }

    #[test]
    fn test_merge_ctes_into_existing_with_recursive() {
        let merged = merge_ctes_into_query(
            "WITH RECURSIVE r AS (SELECT 1) SELECT * FROM r",
            &["Transfer AS (SELECT 1)".to_string()],
        );
        assert_eq!(
            merged,
            "WITH RECURSIVE Transfer AS (SELECT 1), r AS (SELECT 1) SELECT * FROM r"
        );
    }

}
