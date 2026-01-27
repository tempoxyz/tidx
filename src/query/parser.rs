use anyhow::{anyhow, Result};
use sha3::{Digest, Keccak256};
use sqlparser::ast::visit_expressions;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashSet;
use std::ops::ControlFlow;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventSignature {
    pub name: String,
    pub params: Vec<AbiParam>,
    pub topic0: [u8; 32],
}

fn is_valid_identifier(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.chars().next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

impl EventSignature {
    pub fn parse(sig: &str) -> Result<Self> {
        let sig = sig.trim();

        let open_paren = sig
            .find('(')
            .ok_or_else(|| anyhow!("Invalid signature: missing '('"))?;
        let close_paren = sig
            .rfind(')')
            .ok_or_else(|| anyhow!("Invalid signature: missing ')'"))?;

        if close_paren <= open_paren {
            return Err(anyhow!("Invalid signature: malformed parentheses"));
        }

        let name = sig[..open_paren].trim().to_string();
        if name.is_empty() {
            return Err(anyhow!("Invalid signature: empty event name"));
        }
        
        if !is_valid_identifier(&name) {
            return Err(anyhow!("Invalid signature: event name must be alphanumeric"));
        }

        let params_str = &sig[open_paren + 1..close_paren];
        let params = parse_params(params_str)?;

        let canonical = Self::canonical_signature(&name, &params);
        let hash = Keccak256::digest(canonical.as_bytes());
        let topic0: [u8; 32] = hash.into();

        Ok(Self {
            name,
            params,
            topic0,
        })
    }

    pub fn topic0_hex(&self) -> String {
        hex::encode(self.topic0)
    }

    fn canonical_signature(name: &str, params: &[AbiParam]) -> String {
        let param_types: Vec<String> = params.iter().map(|p| p.ty.canonical()).collect();
        format!("{}({})", name, param_types.join(","))
    }

    /// Generate PostgreSQL-compatible CTE SQL (includes all decoded columns)
    pub fn to_cte_sql(&self) -> String {
        self.to_cte_sql_postgres_filtered(None)
    }

    /// Generate PostgreSQL-compatible CTE SQL (uses bytea and abi_* functions)
    pub fn to_cte_sql_postgres(&self) -> String {
        self.to_cte_sql_postgres_filtered(None)
    }

    /// Generate PostgreSQL-compatible CTE SQL, only including columns used in the query.
    /// If `used_columns` is None, includes all decoded columns.
    pub fn to_cte_sql_postgres_filtered(&self, used_columns: Option<&HashSet<String>>) -> String {
        let selects = self.build_select_expressions(used_columns, false);

        let select_clause = if selects.is_empty() {
            String::new()
        } else {
            format!(", {}", selects.join(", "))
        };

        format!(
            r#"{name} AS (
    SELECT block_num, block_timestamp, log_idx, tx_idx, tx_hash, address{select_clause}
    FROM logs
    WHERE selector = '\x{topic0}'
)"#,
            name = self.name.to_lowercase(),
            select_clause = select_clause,
            topic0 = self.topic0_hex(),
        )
    }

    /// Generate DuckDB-compatible CTE SQL (includes all decoded columns)
    pub fn to_cte_sql_duckdb(&self) -> String {
        self.to_cte_sql_duckdb_filtered(None)
    }

    /// Generate DuckDB-compatible CTE SQL, only including columns used in the query.
    /// If `used_columns` is None, includes all decoded columns.
    pub fn to_cte_sql_duckdb_filtered(&self, used_columns: Option<&HashSet<String>>) -> String {
        let selects = self.build_select_expressions(used_columns, true);

        let select_clause = if selects.is_empty() {
            String::new()
        } else {
            format!(", {}", selects.join(", "))
        };

        format!(
            r#"{name} AS (
    SELECT block_num, block_timestamp, log_idx, tx_idx, tx_hash, address{select_clause}
    FROM logs
    WHERE selector = '0x{topic0}'
)"#,
            name = self.name.to_lowercase(),
            select_clause = select_clause,
            topic0 = self.topic0_hex(),
        )
    }

    /// Build SELECT expressions for decoded columns, optionally filtering by used columns.
    fn build_select_expressions(
        &self,
        used_columns: Option<&HashSet<String>>,
        is_duckdb: bool,
    ) -> Vec<String> {
        let mut selects = Vec::new();
        let mut topic_idx = 2;
        let mut data_offset = 0;

        for (i, param) in self.params.iter().enumerate() {
            let col_name = param
                .name
                .as_deref()
                .map_or_else(|| format!("arg{i}"), |n| n.to_string());

            // Calculate offsets even for skipped columns to maintain correct positions
            let (decode_expr, new_topic_idx, new_data_offset) = if param.indexed {
                let expr = if is_duckdb {
                    param.ty.topic_decode_sql_duckdb(topic_idx)
                } else {
                    param.ty.topic_decode_sql_postgres(topic_idx)
                };
                (expr, topic_idx + 1, data_offset)
            } else {
                let expr = if is_duckdb {
                    param.ty.data_decode_sql_duckdb(data_offset)
                } else {
                    param.ty.data_decode_sql_postgres(data_offset)
                };
                (expr, topic_idx, data_offset + 32)
            };

            topic_idx = new_topic_idx;
            data_offset = new_data_offset;

            // Only include column if it's used (or if no filter is specified)
            let include = match used_columns {
                None => true,
                Some(cols) => cols.contains(&col_name) || cols.contains(&col_name.to_lowercase()),
            };

            if include {
                selects.push(format!("{decode_expr} AS \"{col_name}\""));
            }
        }

        selects
    }

    /// Returns the list of decoded column names from this signature.
    pub fn decoded_column_names(&self) -> Vec<String> {
        self.params
            .iter()
            .enumerate()
            .map(|(i, param)| {
                param
                    .name
                    .as_deref()
                    .map_or_else(|| format!("arg{i}"), |n| n.to_string())
            })
            .collect()
    }
}

/// Extract all column/identifier references from a SQL query.
/// Returns a set of lowercase column names referenced in the query.
pub fn extract_column_references(sql: &str) -> HashSet<String> {
    let mut columns = HashSet::new();

    let dialect = GenericDialect {};
    let Ok(statements) = Parser::parse_sql(&dialect, sql) else {
        return columns;
    };

    for stmt in &statements {
        let _ = visit_expressions(stmt, |expr| {
            extract_idents_from_expr(expr, &mut columns);
            ControlFlow::<()>::Continue(())
        });
    }

    columns
}

/// Recursively extract identifiers from an expression.
fn extract_idents_from_expr(expr: &sqlparser::ast::Expr, columns: &mut HashSet<String>) {
    match expr {
        sqlparser::ast::Expr::Identifier(ident) => {
            columns.insert(ident.value.to_lowercase());
        }
        sqlparser::ast::Expr::CompoundIdentifier(idents) => {
            // For table.column, we want the column part (last identifier)
            if let Some(last) = idents.last() {
                columns.insert(last.value.to_lowercase());
            }
            // Also add without qualification in case it's used both ways
            for ident in idents {
                columns.insert(ident.value.to_lowercase());
            }
        }
        _ => {}
    }
}

fn parse_params(params_str: &str) -> Result<Vec<AbiParam>> {
    if params_str.trim().is_empty() {
        return Ok(Vec::new());
    }

    let mut params = Vec::new();
    let mut depth = 0;
    let mut current = String::new();

    for c in params_str.chars() {
        match c {
            '(' => {
                depth += 1;
                current.push(c);
            }
            ')' => {
                depth -= 1;
                current.push(c);
            }
            ',' if depth == 0 => {
                if !current.trim().is_empty() {
                    params.push(AbiParam::parse(current.trim())?);
                }
                current.clear();
            }
            _ => current.push(c),
        }
    }

    if !current.trim().is_empty() {
        params.push(AbiParam::parse(current.trim())?);
    }

    Ok(params)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbiParam {
    pub name: Option<String>,
    pub ty: AbiType,
    pub indexed: bool,
}

impl AbiParam {
    pub fn parse(s: &str) -> Result<Self> {
        let parts: Vec<&str> = s.split_whitespace().collect();

        if parts.is_empty() {
            return Err(anyhow!("Empty parameter"));
        }

        let (ty_str, indexed, name) = match parts.len() {
            1 => (parts[0], false, None),
            2 => {
                if parts[1] == "indexed" {
                    (parts[0], true, None)
                } else {
                    (parts[0], false, Some(parts[1].to_string()))
                }
            }
            3 => {
                if parts[1] == "indexed" {
                    (parts[0], true, Some(parts[2].to_string()))
                } else {
                    return Err(anyhow!("Invalid parameter format: {s}"));
                }
            }
            _ => return Err(anyhow!("Invalid parameter format: {s}")),
        };

        if let Some(ref n) = name
            && !is_valid_identifier(n)
        {
            return Err(anyhow!("Invalid parameter name: must be alphanumeric"));
        }

        let ty = AbiType::parse(ty_str)?;

        Ok(Self { name, ty, indexed })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AbiType {
    Address,
    Bool,
    Uint(u16),
    Int(u16),
    Bytes(Option<u8>),
    String,
    FixedArray(Box<AbiType>, usize),
    DynamicArray(Box<AbiType>),
    Tuple(Vec<AbiType>),
}

impl AbiType {
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();

        if s == "address" {
            return Ok(AbiType::Address);
        }
        if s == "bool" {
            return Ok(AbiType::Bool);
        }
        if s == "string" {
            return Ok(AbiType::String);
        }
        if s == "bytes" {
            return Ok(AbiType::Bytes(None));
        }

        if let Some(rest) = s.strip_prefix("uint") {
            let bits: u16 = if rest.is_empty() {
                256
            } else {
                rest.parse().map_err(|_| anyhow!("Invalid uint size"))?
            };
            return Ok(AbiType::Uint(bits));
        }

        if let Some(rest) = s.strip_prefix("int") {
            let bits: u16 = if rest.is_empty() {
                256
            } else {
                rest.parse().map_err(|_| anyhow!("Invalid int size"))?
            };
            return Ok(AbiType::Int(bits));
        }

        if let Some(rest) = s.strip_prefix("bytes") {
            if rest.is_empty() {
                return Ok(AbiType::Bytes(None));
            }
            let size: u8 = rest
                .parse()
                .map_err(|_| anyhow!("Invalid bytes size: {rest}"))?;
            return Ok(AbiType::Bytes(Some(size)));
        }

        if let Some(inner_str) = s.strip_suffix("[]") {
            let inner = AbiType::parse(inner_str)?;
            return Ok(AbiType::DynamicArray(Box::new(inner)));
        }

        if let Some(bracket_pos) = s.rfind('[')
            && let Some(inner_with_bracket) = s.strip_suffix(']')
        {
            let inner = AbiType::parse(&s[..bracket_pos])?;
            let size_str = &inner_with_bracket[bracket_pos + 1..];
            let size: usize = size_str
                .parse()
                .map_err(|_| anyhow!("Invalid array size: {size_str}"))?;
            return Ok(AbiType::FixedArray(Box::new(inner), size));
        }

        Err(anyhow!("Unknown ABI type: {s}"))
    }

    pub fn canonical(&self) -> String {
        match self {
            AbiType::Address => "address".to_string(),
            AbiType::Bool => "bool".to_string(),
            AbiType::Uint(bits) => format!("uint{bits}"),
            AbiType::Int(bits) => format!("int{bits}"),
            AbiType::Bytes(None) => "bytes".to_string(),
            AbiType::Bytes(Some(n)) => format!("bytes{n}"),
            AbiType::String => "string".to_string(),
            AbiType::FixedArray(inner, size) => format!("{}[{}]", inner.canonical(), size),
            AbiType::DynamicArray(inner) => format!("{}[]", inner.canonical()),
            AbiType::Tuple(types) => {
                let inner: Vec<String> = types.iter().map(|t| t.canonical()).collect();
                format!("({})", inner.join(","))
            }
        }
    }

    // PostgreSQL decode functions (bytea-based)

    pub fn topic_decode_sql_postgres(&self, topic_idx: usize) -> String {
        // topic_idx is 1-based from the signature parser, maps to topic0, topic1, etc.
        let col = format!("topic{}", topic_idx.saturating_sub(1));
        match self {
            AbiType::Address => format!("abi_address({col})"),
            AbiType::Uint(_) | AbiType::Int(_) => format!("abi_uint({col})"),
            AbiType::Bool => format!("abi_bool({col})"),
            AbiType::Bytes(Some(_) | None) => col,
            _ => col,
        }
    }

    pub fn data_decode_sql_postgres(&self, offset: usize) -> String {
        let start = offset + 1;
        match self {
            AbiType::Address => format!("abi_address(substring(data FROM {start} FOR 32))"),
            AbiType::Uint(_) => {
                format!("abi_uint(substring(data FROM {start} FOR 32))")
            }
            AbiType::Int(_) => {
                format!("abi_int(substring(data FROM {start} FOR 32))")
            }
            AbiType::Bool => format!("abi_bool(substring(data FROM {start} FOR 32))"),
            AbiType::Bytes(Some(_) | None) => {
                format!("substring(data FROM {start} FOR 32)")
            }
            AbiType::String => format!("abi_string(data, {offset})"),
            _ => format!("substring(data FROM {start} FOR 32)"),
        }
    }

    // DuckDB decode functions (use native Rust UDFs for performance)

    pub fn topic_decode_sql_duckdb(&self, topic_idx: usize) -> String {
        // topic_idx is 1-based from the signature parser, maps to topic0, topic1, etc.
        let col = format!("topic{}", topic_idx.saturating_sub(1));
        match self {
            AbiType::Address => format!("topic_address_native({col})"),
            AbiType::Uint(_) | AbiType::Int(_) => format!("topic_uint_native({col})"),
            AbiType::Bool => format!("{col} != '0x0000000000000000000000000000000000000000000000000000000000000000'"),
            AbiType::Bytes(Some(_) | None) => col,
            _ => col,
        }
    }

    pub fn data_decode_sql_duckdb(&self, offset: usize) -> String {
        match self {
            AbiType::Address => format!("abi_address_native(data, {offset})"),
            AbiType::Uint(_) => format!("abi_uint_native(data, {offset})"),
            AbiType::Int(_) => format!("abi_uint_native(data, {offset})"), // TODO: proper signed handling
            AbiType::Bool => format!("abi_bool(data, {offset})"),
            AbiType::Bytes(Some(_) | None) => format!("abi_bytes32(data, {offset})"),
            AbiType::String => format!("abi_bytes32(data, {offset})"), // TODO: dynamic string support
            _ => format!("abi_bytes32(data, {offset})"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // EventSignature Parsing Tests
    // ========================================================================

    #[test]
    fn test_parse_transfer_signature() {
        let sig = EventSignature::parse("Transfer(address,address,uint256)").unwrap();
        assert_eq!(sig.name, "Transfer");
        assert_eq!(sig.params.len(), 3);
        assert_eq!(sig.params[0].ty, AbiType::Address);
        assert_eq!(sig.params[1].ty, AbiType::Address);
        assert_eq!(sig.params[2].ty, AbiType::Uint(256));
        assert!(sig.topic0_hex().starts_with("ddf252ad"));
    }

    #[test]
    fn test_parse_approval_signature() {
        let sig = EventSignature::parse("Approval(address,address,uint256)").unwrap();
        assert_eq!(sig.name, "Approval");
        assert!(sig.topic0_hex().starts_with("8c5be1e5"));
    }

    #[test]
    fn test_parse_indexed_params() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
        assert_eq!(sig.name, "Transfer");
        assert_eq!(sig.params.len(), 3);
        assert!(sig.params[0].indexed);
        assert_eq!(sig.params[0].name.as_deref(), Some("from"));
        assert!(sig.params[1].indexed);
        assert_eq!(sig.params[1].name.as_deref(), Some("to"));
        assert!(!sig.params[2].indexed);
        assert_eq!(sig.params[2].name.as_deref(), Some("value"));
        assert!(sig.topic0_hex().starts_with("ddf252ad"));
    }

    #[test]
    fn test_parse_empty_params() {
        let sig = EventSignature::parse("Paused()").unwrap();
        assert_eq!(sig.name, "Paused");
        assert!(sig.params.is_empty());
    }

    #[test]
    fn test_cte_generation() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
        let cte = sig.to_cte_sql();
        assert!(cte.contains("transfer AS"));
        assert!(cte.contains("abi_address(topic1)"));
        assert!(cte.contains("abi_address(topic2)"));
        assert!(cte.contains("abi_uint(substring(data FROM 1 FOR 32))"));
        assert!(cte.contains("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"));
    }

    #[test]
    fn test_parse_bytes32() {
        let sig = EventSignature::parse("SomeEvent(bytes32)").unwrap();
        assert_eq!(sig.params[0].ty, AbiType::Bytes(Some(32)));
    }

    #[test]
    fn test_parse_dynamic_bytes() {
        let sig = EventSignature::parse("SomeEvent(bytes)").unwrap();
        assert_eq!(sig.params[0].ty, AbiType::Bytes(None));
    }

    #[test]
    fn test_canonical_signature() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
        let canonical = EventSignature::canonical_signature(&sig.name, &sig.params);
        assert_eq!(canonical, "Transfer(address,address,uint256)");
    }

    #[test]
    fn test_cte_generation_duckdb() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
        let cte = sig.to_cte_sql_duckdb();
        assert!(cte.contains("transfer AS"));
        assert!(cte.contains("topic_address_native(topic1)"));
        assert!(cte.contains("topic_address_native(topic2)"));
        assert!(cte.contains("abi_uint_native(data, 0)"));
        // DuckDB uses '0x...' format and filters by full 32-byte selector (topic0)
        assert!(cte.contains("selector = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'"));
    }

    #[test]
    fn test_extract_column_references() {
        let cols = extract_column_references("SELECT \"to\", COUNT(*) FROM transfer GROUP BY \"to\"");
        assert!(cols.contains("to"));
        // COUNT is a function, not a column reference

        let cols = extract_column_references("SELECT value, SUM(amount) FROM t WHERE x > 5");
        assert!(cols.contains("value"));
        assert!(cols.contains("amount"));
        assert!(cols.contains("x"));
    }

    #[test]
    fn test_extract_column_references_compound() {
        let cols = extract_column_references("SELECT t.from, t.to FROM transfer t");
        assert!(cols.contains("from"));
        assert!(cols.contains("to"));
    }

    #[test]
    fn test_cte_filtered_duckdb_only_to() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
        
        let used_cols: HashSet<String> = ["to"].iter().map(|s| s.to_string()).collect();
        let cte = sig.to_cte_sql_duckdb_filtered(Some(&used_cols));
        
        // Should include "to" but not "from" or "value"
        assert!(cte.contains("topic_address_native(topic2) AS \"to\""));
        assert!(!cte.contains("\"from\""));
        assert!(!cte.contains("\"value\""));
    }

    #[test]
    fn test_cte_filtered_duckdb_from_and_value() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
        
        let used_cols: HashSet<String> = ["from", "value"].iter().map(|s| s.to_string()).collect();
        let cte = sig.to_cte_sql_duckdb_filtered(Some(&used_cols));
        
        // Should include "from" and "value" but not "to"
        assert!(cte.contains("topic_address_native(topic1) AS \"from\""));
        assert!(cte.contains("abi_uint_native(data, 0) AS \"value\""));
        assert!(!cte.contains("\"to\""));
    }

    #[test]
    fn test_cte_filtered_postgres_only_value() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
        
        let used_cols: HashSet<String> = ["value"].iter().map(|s| s.to_string()).collect();
        let cte = sig.to_cte_sql_postgres_filtered(Some(&used_cols));
        
        // Should include "value" but not "from" or "to"
        assert!(cte.contains("abi_uint(substring(data FROM 1 FOR 32)) AS \"value\""));
        assert!(!cte.contains("\"from\""));
        assert!(!cte.contains("\"to\""));
    }

    #[test]
    fn test_cte_filtered_none_includes_all() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
        
        let cte = sig.to_cte_sql_duckdb_filtered(None);
        
        // Should include all columns
        assert!(cte.contains("\"from\""));
        assert!(cte.contains("\"to\""));
        assert!(cte.contains("\"value\""));
    }

    #[test]
    fn test_cte_filtered_preserves_offsets() {
        // When skipping "from", "to" should still use topic2 and "value" should still use data offset 0
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
        
        let used_cols: HashSet<String> = ["to", "value"].iter().map(|s| s.to_string()).collect();
        let cte = sig.to_cte_sql_duckdb_filtered(Some(&used_cols));
        
        // "to" is the second indexed param, so it should still be topic2
        assert!(cte.contains("topic_address_native(topic2) AS \"to\""));
        // "value" is the first data param, so it should still be offset 0
        assert!(cte.contains("abi_uint_native(data, 0) AS \"value\""));
    }

    // ========================================================================
    // Event CTE Execution Tests (DuckDB)
    // ========================================================================
    // These tests verify that generated CTEs work correctly with real data

    mod cte_execution_tests {
        use super::*;
        use crate::db::{execute_duckdb_query, DuckDbPool};

        fn insert_transfer_log(
            conn: &duckdb::Connection,
            block_num: i64,
            log_idx: i32,
            from_addr: &str,
            to_addr: &str,
            value: u128,
        ) {
            // selector stores full 32-byte topic0 hash
            let selector = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
            let topic0 = selector;
            let topic1 = format!("0x000000000000000000000000{}", from_addr);
            let topic2 = format!("0x000000000000000000000000{}", to_addr);
            let data = format!("0x{:064x}", value);

            conn.execute(
                &format!(
                    "INSERT INTO logs (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topic0, topic1, topic2, topic3, data)
                     VALUES ({block_num}, '2024-01-01 00:00:00+00', {log_idx}, 0, '0xabc{block_num:03x}', '0xtoken', '{selector}', 
                             '{topic0}', '{topic1}', '{topic2}', NULL, '{data}')"
                ),
                [],
            ).unwrap();
        }

        fn insert_approval_log(
            conn: &duckdb::Connection,
            block_num: i64,
            log_idx: i32,
            owner_addr: &str,
            spender_addr: &str,
            value: u128,
        ) {
            // selector stores full 32-byte topic0 hash
            let selector = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925";
            let topic0 = selector;
            let topic1 = format!("0x000000000000000000000000{}", owner_addr);
            let topic2 = format!("0x000000000000000000000000{}", spender_addr);
            let data = format!("0x{:064x}", value);

            conn.execute(
                &format!(
                    "INSERT INTO logs (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topic0, topic1, topic2, topic3, data)
                     VALUES ({block_num}, '2024-01-01 00:00:00+00', {log_idx}, 0, '0xabc{block_num:03x}', '0xtoken', '{selector}', 
                             '{topic0}', '{topic1}', '{topic2}', NULL, '{data}')"
                ),
                [],
            ).unwrap();
        }

        #[test]
        fn test_transfer_cte_decodes_addresses() {
            let pool = DuckDbPool::in_memory().unwrap();
            let conn = futures::executor::block_on(pool.conn());

            insert_transfer_log(&conn, 1, 0, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 1_000_000_000_000_000_000);

            let sig = EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)").unwrap();
            let cte = sig.to_cte_sql_duckdb();
            let sql = format!("WITH {cte} SELECT \"from\", \"to\", \"value\" FROM transfer");

            let (cols, rows) = execute_duckdb_query(&conn, &sql).unwrap();

            assert_eq!(cols, vec!["from", "to", "value"]);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], serde_json::json!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
            assert_eq!(rows[0][1], serde_json::json!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
            assert_eq!(rows[0][2], serde_json::json!(1_000_000_000_000_000_000_i64));
        }

        #[test]
        fn test_transfer_cte_group_by_to() {
            let pool = DuckDbPool::in_memory().unwrap();
            let conn = futures::executor::block_on(pool.conn());

            // Insert 10 transfers to 2 different addresses
            for i in 0..10 {
                let to_addr = if i % 2 == 0 { "1111111111111111111111111111111111111111" } else { "2222222222222222222222222222222222222222" };
                insert_transfer_log(&conn, i, 0, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", to_addr, 1_000_000_000_000_000_000);
            }

            let sig = EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)").unwrap();
            let cte = sig.to_cte_sql_duckdb();
            let sql = format!("WITH {cte} SELECT \"to\", COUNT(*) as cnt FROM transfer GROUP BY \"to\" ORDER BY cnt DESC");

            let (cols, rows) = execute_duckdb_query(&conn, &sql).unwrap();

            assert_eq!(cols, vec!["to", "cnt"]);
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][1], serde_json::json!(5));
            assert_eq!(rows[1][1], serde_json::json!(5));
        }

        #[test]
        fn test_transfer_cte_sum_value() {
            let pool = DuckDbPool::in_memory().unwrap();
            let conn = futures::executor::block_on(pool.conn());

            // Insert 5 transfers with values 1-5 Gwei (smaller to avoid i64 overflow in sum)
            for i in 1..=5 {
                insert_transfer_log(&conn, i, 0, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", i as u128 * 1_000_000_000);
            }

            let sig = EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)").unwrap();
            let cte = sig.to_cte_sql_duckdb();
            let sql = format!("WITH {cte} SELECT SUM(\"value\") as total FROM transfer");

            let (cols, rows) = execute_duckdb_query(&conn, &sql).unwrap();

            assert_eq!(cols, vec!["total"]);
            assert_eq!(rows.len(), 1);
            // Sum of 1+2+3+4+5 = 15 Gwei = 15_000_000_000
            assert_eq!(rows[0][0], serde_json::json!(15_000_000_000_i64));
        }

        #[test]
        fn test_transfer_cte_filter_by_from() {
            let pool = DuckDbPool::in_memory().unwrap();
            let conn = futures::executor::block_on(pool.conn());

            // Insert transfers from 2 different addresses
            for i in 0..10 {
                let from_addr = if i < 6 { "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" } else { "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" };
                insert_transfer_log(&conn, i, 0, from_addr, "cccccccccccccccccccccccccccccccccccccccc", 1_000_000_000);
            }

            let sig = EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)").unwrap();
            let cte = sig.to_cte_sql_duckdb();
            let sql = format!("WITH {cte} SELECT COUNT(*) as cnt FROM transfer WHERE \"from\" = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'");

            let (_, rows) = execute_duckdb_query(&conn, &sql).unwrap();

            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], serde_json::json!(6));
        }

        #[test]
        fn test_approval_cte_decodes_correctly() {
            let pool = DuckDbPool::in_memory().unwrap();
            let conn = futures::executor::block_on(pool.conn());

            insert_approval_log(&conn, 1, 0, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", u128::MAX >> 1);

            let sig = EventSignature::parse("Approval(address indexed owner, address indexed spender, uint256 value)").unwrap();
            let cte = sig.to_cte_sql_duckdb();
            let sql = format!("WITH {cte} SELECT \"owner\", \"spender\", \"value\" FROM approval");

            let (cols, rows) = execute_duckdb_query(&conn, &sql).unwrap();

            assert_eq!(cols, vec!["owner", "spender", "value"]);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], serde_json::json!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
            assert_eq!(rows[0][1], serde_json::json!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
        }

        #[test]
        fn test_filtered_cte_only_decodes_used_columns() {
            let pool = DuckDbPool::in_memory().unwrap();
            let conn = futures::executor::block_on(pool.conn());

            insert_transfer_log(&conn, 1, 0, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 1_000_000_000);

            let sig = EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)").unwrap();
            let used_cols: HashSet<String> = ["to"].iter().map(|s| s.to_string()).collect();
            let cte = sig.to_cte_sql_duckdb_filtered(Some(&used_cols));
            let sql = format!("WITH {cte} SELECT \"to\" FROM transfer");

            let (cols, rows) = execute_duckdb_query(&conn, &sql).unwrap();

            assert_eq!(cols, vec!["to"]);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], serde_json::json!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
        }

        #[test]
        fn test_cte_with_multiple_data_params() {
            let pool = DuckDbPool::in_memory().unwrap();
            let conn = futures::executor::block_on(pool.conn());

            // Swap event: Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)
            // selector stores full 32-byte topic0 hash
            let selector = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";
            let topic0 = selector;
            let topic1 = "0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"; // sender
            let topic2 = "0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"; // to
            // data: 4 uint256 values (amount0In, amount1In, amount0Out, amount1Out)
            let data = format!(
                "0x{:064x}{:064x}{:064x}{:064x}",
                100_u128, 0_u128, 0_u128, 200_u128
            );

            conn.execute(
                &format!(
                    "INSERT INTO logs (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topic0, topic1, topic2, topic3, data)
                     VALUES (1, '2024-01-01 00:00:00+00', 0, 0, '0xabc', '0xpair', '{selector}', 
                             '{topic0}', '{topic1}', '{topic2}', NULL, '{data}')"
                ),
                [],
            ).unwrap();

            let sig = EventSignature::parse("Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)").unwrap();
            let cte = sig.to_cte_sql_duckdb();
            let sql = format!("WITH {cte} SELECT \"sender\", \"amount0In\", \"amount1Out\", \"to\" FROM swap");

            let (cols, rows) = execute_duckdb_query(&conn, &sql).unwrap();

            assert_eq!(cols, vec!["sender", "amount0In", "amount1Out", "to"]);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], serde_json::json!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
            assert_eq!(rows[0][1], serde_json::json!(100));
            assert_eq!(rows[0][2], serde_json::json!(200));
            assert_eq!(rows[0][3], serde_json::json!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
        }

        #[test]
        fn test_cte_handles_empty_results() {
            let pool = DuckDbPool::in_memory().unwrap();
            let conn = futures::executor::block_on(pool.conn());

            // Don't insert any logs
            let sig = EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)").unwrap();
            let cte = sig.to_cte_sql_duckdb();
            let sql = format!("WITH {cte} SELECT \"from\", \"to\", \"value\" FROM transfer");

            let (cols, rows) = execute_duckdb_query(&conn, &sql).unwrap();

            assert_eq!(cols, vec!["from", "to", "value"]);
            assert_eq!(rows.len(), 0);
        }

        #[test]
        fn test_cte_with_null_topics() {
            let pool = DuckDbPool::in_memory().unwrap();
            let conn = futures::executor::block_on(pool.conn());

            // Insert a log with fewer topics than expected (simulating edge case)
            // selector stores full 32-byte topic0 hash
            let selector = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
            let topic0 = selector;
            let topic1 = "0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
            let data = "0x0000000000000000000000000000000000000000000000000000000000000064"; // 100

            conn.execute(
                &format!(
                    "INSERT INTO logs (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topic0, topic1, topic2, topic3, data)
                     VALUES (1, '2024-01-01 00:00:00+00', 0, 0, '0xabc', '0xtoken', '{selector}', 
                             '{topic0}', '{topic1}', NULL, NULL, '{data}')"
                ),
                [],
            ).unwrap();

            let sig = EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)").unwrap();
            let cte = sig.to_cte_sql_duckdb();
            let sql = format!("WITH {cte} SELECT \"from\", \"to\", \"value\" FROM transfer");

            let (cols, rows) = execute_duckdb_query(&conn, &sql).unwrap();

            assert_eq!(cols, vec!["from", "to", "value"]);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], serde_json::json!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
            assert_eq!(rows[0][1], serde_json::Value::Null); // topic2 is NULL
            assert_eq!(rows[0][2], serde_json::json!(100));
        }
    }
}
