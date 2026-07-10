//! ABI decode SQL lambda UDFs (`abi_uint`, `abi_address`, …).
//!
//! Applied with `CREATE OR REPLACE` on every startup, before tables and
//! views, so definitions in `db/clickhouse/functions.sql` are the single
//! source of truth and edits take effect on restart.

const FUNCTIONS_SQL: &str = include_str!("../../db/clickhouse/functions.sql");

/// Individual `CREATE OR REPLACE FUNCTION` statements, in file order.
/// ClickHouse executes one statement per query, so the file is split on `;`
/// after dropping `--` comment lines (which may themselves contain `;`).
pub fn function_statements() -> impl Iterator<Item = String> {
    let sql: String = FUNCTIONS_SQL
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    sql.split(';')
        .map(str::trim)
        .filter(|stmt| !stmt.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>()
        .into_iter()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statements_split_cleanly() {
        let stmts: Vec<_> = function_statements().collect();
        assert!(!stmts.is_empty());
        for stmt in stmts {
            assert!(
                stmt.contains("CREATE OR REPLACE FUNCTION"),
                "statement missing CREATE OR REPLACE FUNCTION: {stmt}"
            );
        }
    }

    #[test]
    fn defines_expected_functions() {
        let names: Vec<String> = function_statements()
            .map(|s| s.split_whitespace().nth(4).unwrap().to_owned())
            .collect();
        assert_eq!(
            names,
            [
                "abi_address",
                "abi_uint",
                "abi_int",
                "abi_bool",
                "abi_bytes32",
                "abi_word",
                "abi_word_uint",
                "abi_bytes",
                "abi_string",
            ]
        );
    }
}
