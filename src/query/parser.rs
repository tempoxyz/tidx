use anyhow::{anyhow, Result};
use sha3::{Digest, Keccak256};
use sqlparser::ast::{visit_expressions, BinaryOperator, Expr, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::{HashMap, HashSet};
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
        let selects = self.build_select_expressions_postgres(used_columns);

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

    /// Generate ClickHouse-compatible CTE SQL (includes all decoded columns)
    pub fn to_cte_sql_clickhouse(&self) -> String {
        self.to_cte_sql_clickhouse_filtered(None)
    }

    /// Generate ClickHouse-compatible CTE SQL, only including columns used in the query.
    /// 
    /// Note: MaterializedPostgreSQL stores PostgreSQL bytea as '\x'-prefixed hex strings.
    /// We use substring(..., 3) to strip the '\x' prefix before unhex().
    pub fn to_cte_sql_clickhouse_filtered(&self, used_columns: Option<&HashSet<String>>) -> String {
        let selects = self.build_select_expressions_clickhouse(used_columns);

        let select_clause = if selects.is_empty() {
            String::new()
        } else {
            format!(", {}", selects.join(", "))
        };

        // Convert tx_hash/address from '\x...' format to '0x...' format
        let include_tx_hash = used_columns
            .map(|cols| cols.contains("tx_hash"))
            .unwrap_or(true);
        let include_address = used_columns
            .map(|cols| cols.contains("address"))
            .unwrap_or(true);

        // tx_hash/address are stored as '\xABCD...' - replace '\x' with '0x'
        let tx_hash_col = if include_tx_hash {
            "concat('0x', lower(substring(tx_hash, 3))) AS tx_hash"
        } else {
            "tx_hash"
        };
        let address_col = if include_address {
            "concat('0x', lower(substring(address, 3))) AS address"
        } else {
            "address"
        };

        // selector is stored as '\xABCD...' string, compare directly
        format!(
            r#"{name} AS (
    SELECT block_num, block_timestamp, log_idx, tx_idx, 
           {tx_hash_col},
           {address_col}{select_clause}
    FROM logs
    WHERE selector = '\\x{topic0}'
)"#,
            name = self.name.to_lowercase(),
            tx_hash_col = tx_hash_col,
            address_col = address_col,
            select_clause = select_clause,
            topic0 = self.topic0_hex(),
        )
    }

    /// Build SELECT expressions for ClickHouse decoded columns.
    fn build_select_expressions_clickhouse(
        &self,
        used_columns: Option<&HashSet<String>>,
    ) -> Vec<String> {
        let mut selects = Vec::new();
        let mut topic_idx = 2;
        let mut data_offset = 0;

        for (i, param) in self.params.iter().enumerate() {
            let col_name = param
                .name
                .as_deref()
                .map_or_else(|| format!("arg{i}"), |n| n.to_string());

            let (decode_expr, new_topic_idx, new_data_offset) = if param.indexed {
                let expr = param.ty.topic_decode_sql_clickhouse(topic_idx);
                (expr, topic_idx + 1, data_offset)
            } else {
                let expr = param.ty.data_decode_sql_clickhouse(data_offset);
                (expr, topic_idx, data_offset + 32)
            };

            topic_idx = new_topic_idx;
            data_offset = new_data_offset;

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

    /// Build SELECT expressions for PostgreSQL decoded columns.
    fn build_select_expressions_postgres(
        &self,
        used_columns: Option<&HashSet<String>>,
    ) -> Vec<String> {
        let mut selects = Vec::new();
        let mut topic_idx = 2;
        let mut data_offset = 0;

        for (i, param) in self.params.iter().enumerate() {
            let col_name = param
                .name
                .as_deref()
                .map_or_else(|| format!("arg{i}"), |n| n.to_string());

            let (decode_expr, new_topic_idx, new_data_offset) = if param.indexed {
                let expr = param.ty.topic_decode_sql_postgres(topic_idx);
                (expr, topic_idx + 1, data_offset)
            } else {
                let expr = param.ty.data_decode_sql_postgres(data_offset);
                (expr, topic_idx, data_offset + 32)
            };

            topic_idx = new_topic_idx;
            data_offset = new_data_offset;

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

    /// Encode a filter value based on the ABI type.
    fn encode_value_for_pushdown(&self, ty: &AbiType, value: &str) -> Option<String> {
        match ty {
            AbiType::Address => encode_address_for_topic(value),
            AbiType::Uint(_) | AbiType::Int(_) => encode_uint256_for_topic(value),
            AbiType::Bool => {
                let b = value.to_lowercase();
                if b == "true" || b == "1" {
                    Some("0000000000000000000000000000000000000000000000000000000000000001".to_string())
                } else if b == "false" || b == "0" {
                    Some("0000000000000000000000000000000000000000000000000000000000000000".to_string())
                } else {
                    None
                }
            }
            AbiType::Bytes(Some(32)) => {
                // bytes32 - expect 0x-prefixed hex
                let hex = value.strip_prefix("0x").unwrap_or(value);
                if hex.len() == 64 && hex.chars().all(|c| c.is_ascii_hexdigit()) {
                    Some(hex.to_lowercase())
                } else {
                    None
                }
            }
            _ => None, // Dynamic types not supported for pushdown
        }
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

/// Extract equality filters from a SQL query.
/// Returns a map of column name -> value for simple equality comparisons.
pub fn extract_equality_filters(sql: &str) -> HashMap<String, String> {
    let mut filters = HashMap::new();

    let dialect = GenericDialect {};
    let Ok(statements) = Parser::parse_sql(&dialect, sql) else {
        return filters;
    };

    for stmt in &statements {
        let _ = visit_expressions(stmt, |expr| {
            extract_eq_from_expr(expr, &mut filters);
            ControlFlow::<()>::Continue(())
        });
    }

    filters
}

/// Extract equality comparisons from an expression.
fn extract_eq_from_expr(expr: &Expr, filters: &mut HashMap<String, String>) {
    if let Expr::BinaryOp { left, op: BinaryOperator::Eq, right } = expr {
        // Check for column = literal patterns
        if let Some((col, val)) = extract_col_eq_literal(left, right) {
            filters.insert(col.to_lowercase(), val);
        } else if let Some((col, val)) = extract_col_eq_literal(right, left) {
            filters.insert(col.to_lowercase(), val);
        }
    }
}

/// Try to extract column = 'literal' pattern
fn extract_col_eq_literal(col_expr: &Expr, val_expr: &Expr) -> Option<(String, String)> {
    let col_name = match col_expr {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(idents) => idents.last()?.value.clone(),
        _ => return None,
    };

    let value = match val_expr {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) => s.clone(),
            Value::Number(n, _) => n.clone(),
            _ => return None,
        },
        _ => return None,
    };

    Some((col_name, value))
}

/// Encode a hex address string to 32-byte ABI-encoded format for topic comparison.
/// Input: "0xdAC17F958D2ee523a2206206994597C13D831ec7" (40 hex chars)
/// Output: "000000000000000000000000dac17f958d2ee523a2206206994597c13d831ec7" (64 hex chars)
pub fn encode_address_for_topic(addr: &str) -> Option<String> {
    let addr = addr.strip_prefix("0x").unwrap_or(addr);
    if addr.len() != 40 {
        return None;
    }
    // Validate it's valid hex
    if !addr.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    // Left-pad with 24 zeros (12 bytes) to make 32 bytes total
    Some(format!("000000000000000000000000{}", addr.to_lowercase()))
}

/// Encode a uint256 value to 32-byte ABI-encoded format for topic/data comparison.
/// Input: "1000000" or decimal number
/// Output: 64 hex chars, big-endian
pub fn encode_uint256_for_topic(value: &str) -> Option<String> {
    let n: u128 = value.parse().ok()?;
    Some(format!("{:064x}", n))
}

/// Extract GROUP BY column names from a SQL query.
pub fn extract_group_by_columns(sql: &str) -> HashSet<String> {
    let mut columns = HashSet::new();

    let dialect = GenericDialect {};
    let Ok(statements) = Parser::parse_sql(&dialect, sql) else {
        return columns;
    };

    for stmt in &statements {
        if let sqlparser::ast::Statement::Query(query) = stmt {
            extract_group_by_from_query(query, &mut columns);
        }
    }

    columns
}

/// Extract GROUP BY columns from a query.
fn extract_group_by_from_query(query: &sqlparser::ast::Query, columns: &mut HashSet<String>) {
    if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
        if let sqlparser::ast::GroupByExpr::Expressions(exprs, _) = &select.group_by {
            for expr in exprs {
                extract_ident_from_expr(expr, columns);
            }
        }
    }
}

/// Extract ORDER BY column names from a SQL query.
pub fn extract_order_by_columns(sql: &str) -> HashSet<String> {
    let mut columns = HashSet::new();

    let dialect = GenericDialect {};
    let Ok(statements) = Parser::parse_sql(&dialect, sql) else {
        return columns;
    };

    for stmt in &statements {
        if let sqlparser::ast::Statement::Query(query) = stmt {
            if let Some(order_by) = &query.order_by {
                if let sqlparser::ast::OrderByKind::Expressions(exprs) = &order_by.kind {
                    for expr in exprs {
                        extract_ident_from_expr(&expr.expr, &mut columns);
                    }
                }
            }
        }
    }

    columns
}

/// Extract identifier from an expression.
fn extract_ident_from_expr(expr: &Expr, columns: &mut HashSet<String>) {
    match expr {
        Expr::Identifier(ident) => {
            columns.insert(ident.value.to_lowercase());
        }
        Expr::CompoundIdentifier(idents) => {
            if let Some(last) = idents.last() {
                columns.insert(last.value.to_lowercase());
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

    // ClickHouse decode functions for MaterializedPostgreSQL data
    // 
    // IMPORTANT: PostgreSQL bytea columns are stored as '\x'-prefixed hex strings.
    // e.g., topic1 = '\x000000000000000000000000a975ba910c2ee169956f3df99ee2ece79d3887cf'
    // We need to:
    // 1. Strip the '\x' prefix with substring(..., 3)
    // 2. Work with hex string (64 chars = 32 bytes) using substring on the hex
    // 3. unhex() only when we need actual bytes for reinterpret functions

    pub fn topic_decode_sql_clickhouse(&self, topic_idx: usize) -> String {
        // topic_idx is 1-based from the signature parser, maps to topic0, topic1, etc.
        let col = format!("topic{}", topic_idx.saturating_sub(1));
        // Strip '\x' prefix: substring(col, 3) gives us the 64-char hex string
        match self {
            // Address: last 20 bytes = last 40 hex chars, add 0x prefix
            AbiType::Address => format!("concat('0x', lower(substring({col}, 27)))"),
            // Uint: unhex the full 64 chars, reverse for big-endian, reinterpret
            AbiType::Uint(_) | AbiType::Int(_) => {
                format!("reinterpretAsUInt256(reverse(unhex(substring({col}, 3))))")
            }
            // Bool: check last byte (last 2 hex chars)
            AbiType::Bool => format!("unhex(substring({col}, 67, 2)) != unhex('00')"),
            // Bytes32: just format as 0x-prefixed, already lowercase
            AbiType::Bytes(Some(_) | None) => format!("concat('0x', substring({col}, 3))"),
            _ => format!("concat('0x', substring({col}, 3))"),
        }
    }

    pub fn data_decode_sql_clickhouse(&self, offset: usize) -> String {
        // data is stored as '\x' + hex string
        // offset is in bytes, but we're working with hex (2 chars per byte)
        // +3 to skip '\x' prefix, then offset*2 for hex position
        let hex_start = 3 + offset * 2;
        match self {
            // Address: skip first 12 bytes (24 hex chars) of 32-byte word, take 20 bytes (40 hex chars)
            AbiType::Address => {
                format!("concat('0x', lower(substring(data, {}, 40)))", hex_start + 24)
            }
            // Uint: unhex 32 bytes (64 hex chars), reverse, reinterpret
            AbiType::Uint(_) => {
                format!("reinterpretAsUInt256(reverse(unhex(substring(data, {hex_start}, 64))))")
            }
            AbiType::Int(_) => {
                format!("reinterpretAsInt256(reverse(unhex(substring(data, {hex_start}, 64))))")
            }
            // Bool: check last byte of 32-byte word (last 2 hex chars of 64)
            AbiType::Bool => {
                format!("unhex(substring(data, {}, 2)) != unhex('00')", hex_start + 62)
            }
            // Bytes32: take 64 hex chars, format with 0x prefix
            AbiType::Bytes(Some(_) | None) => {
                format!("concat('0x', lower(substring(data, {hex_start}, 64)))")
            }
            // String: dynamic, not fully supported yet
            AbiType::String => {
                format!("concat('0x', lower(substring(data, {hex_start}, 64)))")
            }
            _ => format!("concat('0x', lower(substring(data, {hex_start}, 64)))"),
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
    fn test_cte_generation_clickhouse() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
        let cte = sig.to_cte_sql_clickhouse();
        assert!(cte.contains("transfer AS"));
        // ClickHouse: addresses from topics use substring to get last 40 hex chars
        // topic1 = '\x000...address' -> substring(topic1, 27) gets last 40 chars
        assert!(cte.contains("concat('0x', lower(substring(topic1, 27)))"));
        assert!(cte.contains("concat('0x', lower(substring(topic2, 27)))"));
        // ClickHouse: uint256 from data uses unhex then reinterpret
        assert!(cte.contains("reinterpretAsUInt256(reverse(unhex(substring(data"));
        // ClickHouse: selector comparison uses string match with '\x' prefix
        // Note: \\x in the output because we're generating SQL with escaped backslash
        assert!(cte.contains(r"selector = '\\x"));
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
    fn test_cte_filtered_clickhouse_only_to() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
        
        let used_cols: HashSet<String> = ["to"].iter().map(|s| s.to_string()).collect();
        let cte = sig.to_cte_sql_clickhouse_filtered(Some(&used_cols));
        
        // Should include "to" but not "from" or "value"
        assert!(cte.contains("AS \"to\""));
        assert!(!cte.contains("AS \"from\""));
        assert!(!cte.contains("AS \"value\""));
    }

    #[test]
    fn test_cte_filtered_clickhouse_from_and_value() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
        
        let used_cols: HashSet<String> = ["from", "value"].iter().map(|s| s.to_string()).collect();
        let cte = sig.to_cte_sql_clickhouse_filtered(Some(&used_cols));
        
        // Should include "from" and "value" but not "to"
        assert!(cte.contains("AS \"from\""));
        assert!(cte.contains("AS \"value\""));
        assert!(!cte.contains("AS \"to\""));
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
        
        let cte = sig.to_cte_sql_clickhouse_filtered(None);
        
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
        let cte = sig.to_cte_sql_clickhouse_filtered(Some(&used_cols));
        
        // "to" is the second indexed param, so it should still be topic2
        assert!(cte.contains("topic2"));
        assert!(cte.contains("AS \"to\""));
        // "value" is the first data param: hex_start = 3 + 0*2 = 3, so substring(data, 3, 64)
        assert!(cte.contains("substring(data, 3, 64)"));
        assert!(cte.contains("AS \"value\""));
    }

    // ========================================================================
    // Filter Pushdown Tests
    // ========================================================================

    #[test]
    fn test_extract_equality_filters() {
        let filters = extract_equality_filters(
            "SELECT * FROM transfer WHERE \"from\" = '0xABC123' AND value > 100"
        );
        assert_eq!(filters.get("from"), Some(&"0xABC123".to_string()));
        // value > 100 is not an equality filter
        assert!(!filters.contains_key("value"));
    }

    #[test]
    fn test_extract_equality_filters_multiple() {
        let filters = extract_equality_filters(
            "SELECT * FROM transfer WHERE \"from\" = '0xABC' AND \"to\" = '0xDEF'"
        );
        assert_eq!(filters.get("from"), Some(&"0xABC".to_string()));
        assert_eq!(filters.get("to"), Some(&"0xDEF".to_string()));
    }

    #[test]
    fn test_encode_address_for_topic() {
        let encoded = encode_address_for_topic("0xdAC17F958D2ee523a2206206994597C13D831ec7");
        assert_eq!(
            encoded,
            Some("000000000000000000000000dac17f958d2ee523a2206206994597c13d831ec7".to_string())
        );
    }

    #[test]
    fn test_encode_uint256_for_topic() {
        let encoded = encode_uint256_for_topic("1000000");
        assert_eq!(
            encoded,
            Some("00000000000000000000000000000000000000000000000000000000000f4240".to_string())
        );
    }

    // ========================================================================
    // Late Decode (GROUP BY / ORDER BY) Tests
    // ========================================================================

    #[test]
    fn test_extract_group_by_columns() {
        let cols = extract_group_by_columns("SELECT \"to\", COUNT(*) FROM transfer GROUP BY \"to\"");
        assert!(cols.contains("to"));
    }

    #[test]
    fn test_extract_order_by_columns() {
        let cols = extract_order_by_columns("SELECT * FROM transfer ORDER BY \"from\" DESC");
        assert!(cols.contains("from"));
    }

}
