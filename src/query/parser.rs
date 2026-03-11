use anyhow::{anyhow, Result};
use sha3::{Digest, Keccak256};
use sqlparser::ast::{visit_expressions, BinaryOperator, Expr, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

/// Raw columns on the `logs` table that can be safely pushed down into CTEs.
const RAW_PUSHDOWN_COLUMNS: &[&str] = &[
    "block_num",
    "block_timestamp",
    "address",
    "tx_hash",
    "log_idx",
    "tx_idx",
];

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
        self.to_cte_sql_postgres_with_pushdown(used_columns, &[])
    }

    /// Generate PostgreSQL-compatible CTE SQL with raw column predicates pushed
    /// into the inner WHERE clause for index utilization.
    pub fn to_cte_sql_postgres_with_pushdown(
        &self,
        used_columns: Option<&HashSet<String>>,
        pushdown_predicates: &[String],
    ) -> String {
        let selects = self.build_select_expressions_postgres(used_columns);

        let select_clause = if selects.is_empty() {
            String::new()
        } else {
            format!(", {}", selects.join(", "))
        };

        let extra_where = if pushdown_predicates.is_empty() {
            String::new()
        } else {
            format!(" AND {}", pushdown_predicates.join(" AND "))
        };

        format!(
            r#"{name} AS (
    SELECT block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topic1, topic2, topic3, data{select_clause}
    FROM logs
    WHERE selector = '\x{topic0}'{extra_where}
)"#,
            name = self.name,
            select_clause = select_clause,
            topic0 = self.topic0_hex(),
            extra_where = extra_where,
        )
    }

    /// Generate ClickHouse-compatible CTE SQL (includes all decoded columns)
    pub fn to_cte_sql_clickhouse(&self) -> String {
        self.to_cte_sql_clickhouse_filtered(None)
    }

    /// Generate ClickHouse-compatible CTE SQL, only including columns used in the query.
    /// 
    /// Data is stored as '0x'-prefixed hex strings via ClickHouseSink direct-write.
    /// We use substring(..., 3) to strip the '0x' prefix before unhex().
    /// 
    /// For output columns, we convert to '0x...' format for standard Ethereum hex representation.
    pub fn to_cte_sql_clickhouse_filtered(&self, used_columns: Option<&HashSet<String>>) -> String {
        self.to_cte_sql_clickhouse_with_pushdown(used_columns, &[])
    }

    /// Generate ClickHouse-compatible CTE SQL with raw column predicates pushed
    /// into the inner WHERE clause for index utilization.
    pub fn to_cte_sql_clickhouse_with_pushdown(
        &self,
        used_columns: Option<&HashSet<String>>,
        pushdown_predicates: &[String],
    ) -> String {
        let selects = self.build_select_expressions_clickhouse(used_columns);

        let select_clause = if selects.is_empty() {
            String::new()
        } else {
            format!(", {}", selects.join(", "))
        };

        // Output tx_hash/address in '0x...' format for standard Ethereum representation
        let tx_hash_col = r"concat('0x', lower(substring(tx_hash, 3))) AS tx_hash";
        let address_col = r"concat('0x', lower(substring(address, 3))) AS address";

        let extra_where = if pushdown_predicates.is_empty() {
            String::new()
        } else {
            format!(" AND {}", pushdown_predicates.join(" AND "))
        };

        // selector is stored as '0xABCD...' string via direct-write
        format!(
            r#"{name} AS (
    SELECT block_num, block_timestamp, log_idx, tx_idx, 
           {tx_hash_col},
           {address_col}, selector, topic1, topic2, topic3, data{select_clause}
    FROM logs
    WHERE selector = '0x{topic0}'{extra_where}
)"#,
            name = self.name,
            tx_hash_col = tx_hash_col,
            address_col = address_col,
            select_clause = select_clause,
            topic0 = self.topic0_hex(),
            extra_where = extra_where,
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

    /// Returns a mapping of decoded column name -> (raw_column, AbiType, is_indexed).
    /// For indexed params, raw_column is "topic1", "topic2", etc.
    /// For non-indexed params, raw_column is "data" with offset info encoded.
    pub fn column_mapping(&self) -> HashMap<String, (String, AbiType, bool)> {
        let mut mapping = HashMap::new();
        let mut topic_idx = 1; // topic0 is selector, indexed params start at topic1

        for (i, param) in self.params.iter().enumerate() {
            let col_name = param
                .name
                .as_deref()
                .map_or_else(|| format!("arg{i}"), |n| n.to_string());

            if param.indexed {
                mapping.insert(
                    col_name.to_lowercase(),
                    (format!("topic{}", topic_idx), param.ty.clone(), true),
                );
                topic_idx += 1;
            } else {
                // Non-indexed params come from data - pushdown not supported yet
                mapping.insert(
                    col_name.to_lowercase(),
                    ("data".to_string(), param.ty.clone(), false),
                );
            }
        }

        mapping
    }

    /// Normalize table references in SQL to match the CTE name (case-insensitive).
    /// E.g., if event name is "Transfer", converts "FROM transfer" or "FROM TRANSFER" to "FROM Transfer"
    pub fn normalize_table_references(&self, sql: &str) -> String {
        // Build a case-insensitive regex to match the event name as a table reference
        let pattern = format!(r"(?i)\b{}\b", regex_lite::escape(&self.name));
        let re = regex_lite::Regex::new(&pattern).unwrap();
        re.replace_all(sql, self.name.as_str()).into_owned()
    }

    /// Rewrite a SQL query to push down filters on decoded columns to use indexed raw columns.
    /// E.g., WHERE "from" = '0xabc...' becomes WHERE topic1 = '\x000...abc...'
    pub fn rewrite_filters_for_pushdown(&self, sql: &str) -> String {
        let mapping = self.column_mapping();
        let filters = extract_equality_filters(sql);

        let mut result = sql.to_string();

        for (col, value) in filters {
            let col_lower = col.to_lowercase();
            if let Some((raw_col, ty, is_indexed)) = mapping.get(&col_lower) {
                // Only push down indexed columns (topics)
                if !is_indexed {
                    continue;
                }

                if let Some(encoded) = Self::encode_value_for_pushdown(ty, &value) {
                    // Build the replacement patterns
                    // Match: "col" = 'value' or "col" = '0xvalue'
                    let patterns = [
                        format!(r#""{}" = '{}'"#, col, value),
                        format!(r#""{}" = '0x{}'"#, col, value.strip_prefix("0x").unwrap_or(&value)),
                        format!(r#""{}"='{}'"#, col, value),
                        format!(r#""{}"='0x{}'"#, col, value.strip_prefix("0x").unwrap_or(&value)),
                    ];

                    let replacement = format!("{} = '0x{}'", raw_col, encoded);

                    for pattern in &patterns {
                        if result.contains(pattern) {
                            result = result.replace(pattern, &replacement);
                            break;
                        }
                    }
                }
            }
        }

        result
    }

    /// Encode a filter value based on the ABI type.
    fn encode_value_for_pushdown(ty: &AbiType, value: &str) -> Option<String> {
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

/// Extract WHERE predicates on raw `logs` columns that can be pushed into the CTE.
///
/// Returns SQL fragments like `block_num >= 100`, `address = '0x...'` etc.
/// Only extracts simple comparisons (=, >=, <=, >, <) and IN lists on known
/// raw columns. Decoded event columns are NOT extracted.
pub fn extract_raw_column_predicates(sql: &str) -> Vec<String> {
    let mut predicates = Vec::new();

    let dialect = GenericDialect {};
    let Ok(statements) = Parser::parse_sql(&dialect, sql) else {
        return predicates;
    };

    for stmt in &statements {
        let _ = visit_expressions(stmt, |expr| {
            extract_raw_predicate(expr, &mut predicates);
            ControlFlow::<()>::Continue(())
        });
    }

    predicates
}

/// Extract a single raw-column predicate from an expression.
fn extract_raw_predicate(expr: &Expr, predicates: &mut Vec<String>) {
    match expr {
        // col op literal  or  literal op col
        Expr::BinaryOp { left, op, right } => {
            let valid_ops = [
                BinaryOperator::Eq,
                BinaryOperator::GtEq,
                BinaryOperator::LtEq,
                BinaryOperator::Gt,
                BinaryOperator::Lt,
            ];
            if !valid_ops.contains(op) {
                return;
            }

            if let Some(pred) = try_raw_comparison(left, op, right) {
                predicates.push(pred);
            } else if let Some(pred) = try_raw_comparison_reversed(left, op, right) {
                predicates.push(pred);
            }
        }
        // col IN (literal, literal, ...)
        Expr::InList {
            expr: list_expr,
            list,
            negated,
        } => {
            if *negated {
                return;
            }
            let col_name = match list_expr.as_ref() {
                Expr::Identifier(ident) => ident.value.to_lowercase(),
                _ => return,
            };
            if !RAW_PUSHDOWN_COLUMNS.contains(&col_name.as_str()) {
                return;
            }
            let mut values = Vec::new();
            for item in list {
                if let Expr::Value(v) = item {
                    match &v.value {
                        Value::SingleQuotedString(s) => values.push(format!("'{s}'")),
                        Value::Number(n, _) => values.push(n.clone()),
                        _ => return,
                    }
                } else {
                    return;
                }
            }
            if !values.is_empty() {
                predicates.push(format!("{col_name} IN ({})", values.join(", ")));
            }
        }
        _ => {}
    }
}

/// Try to extract `column op literal` where column is a raw logs column.
fn try_raw_comparison(left: &Expr, op: &BinaryOperator, right: &Expr) -> Option<String> {
    let col_name = match left {
        Expr::Identifier(ident) => ident.value.to_lowercase(),
        _ => return None,
    };
    if !RAW_PUSHDOWN_COLUMNS.contains(&col_name.as_str()) {
        return None;
    }
    let value = expr_to_sql_literal(right)?;
    let op_str = binary_op_to_str(op)?;
    Some(format!("{col_name} {op_str} {value}"))
}

/// Try to extract `literal op column` (reversed) where column is a raw logs column.
fn try_raw_comparison_reversed(left: &Expr, op: &BinaryOperator, right: &Expr) -> Option<String> {
    let col_name = match right {
        Expr::Identifier(ident) => ident.value.to_lowercase(),
        _ => return None,
    };
    if !RAW_PUSHDOWN_COLUMNS.contains(&col_name.as_str()) {
        return None;
    }
    let value = expr_to_sql_literal(left)?;
    // Flip the operator: 5 >= col  →  col <= 5
    let flipped_op = match op {
        BinaryOperator::Gt => BinaryOperator::Lt,
        BinaryOperator::Lt => BinaryOperator::Gt,
        BinaryOperator::GtEq => BinaryOperator::LtEq,
        BinaryOperator::LtEq => BinaryOperator::GtEq,
        BinaryOperator::Eq => BinaryOperator::Eq,
        _ => return None,
    };
    let op_str = binary_op_to_str(&flipped_op)?;
    Some(format!("{col_name} {op_str} {value}"))
}

fn expr_to_sql_literal(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) => Some(format!("'{s}'")),
            Value::Number(n, _) => Some(n.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn binary_op_to_str(op: &BinaryOperator) -> Option<&'static str> {
    match op {
        BinaryOperator::Eq => Some("="),
        BinaryOperator::Gt => Some(">"),
        BinaryOperator::Lt => Some("<"),
        BinaryOperator::GtEq => Some(">="),
        BinaryOperator::LtEq => Some("<="),
        _ => None,
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

        // Check array types first, before scalar parsing eats the suffix
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

    // ClickHouse decode functions for 0x-prefixed hex data (direct-write)
    // 
    // Columns are stored as '0x'-prefixed hex strings via ClickHouseSink.
    // e.g., topic1 = '0x000000000000000000000000a975ba910c2ee169956f3df99ee2ece79d3887cf'
    // We need to:
    // 1. Strip the '0x' prefix with substring(..., 3)
    // 2. Work with hex string (64 chars = 32 bytes) using substring on the hex
    // 3. unhex() only when we need actual bytes for reinterpret functions

    pub fn topic_decode_sql_clickhouse(&self, topic_idx: usize) -> String {
        // topic_idx is 1-based from the signature parser, maps to topic0, topic1, etc.
        let col = format!("topic{}", topic_idx.saturating_sub(1));
        // Strip '0x' prefix: substring(col, 3) gives us the 64-char hex string
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
        // data is stored as '0x' + hex string
        // offset is in bytes, but we're working with hex (2 chars per byte)
        // +3 to skip '0x' prefix, then offset*2 for hex position
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
    use insta::assert_snapshot;

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
    fn test_cte_generation_postgres() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
        assert_snapshot!(sig.to_cte_sql_postgres());
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
    fn test_parse_array_types() {
        let sig = EventSignature::parse(
            "TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)",
        )
        .unwrap();
        assert_eq!(sig.name, "TransferBatch");
        assert_eq!(sig.params.len(), 5);
        assert_eq!(sig.params[3].ty, AbiType::DynamicArray(Box::new(AbiType::Uint(256))));
        assert_eq!(sig.params[3].name.as_deref(), Some("ids"));
        assert_eq!(sig.params[4].ty, AbiType::DynamicArray(Box::new(AbiType::Uint(256))));
        // ERC-1155 TransferBatch topic0
        assert!(sig.topic0_hex().starts_with("4a39dc06"));
    }

    #[test]
    fn test_parse_fixed_array_type() {
        let sig = EventSignature::parse("SomeEvent(uint256[3])").unwrap();
        assert_eq!(sig.params[0].ty, AbiType::FixedArray(Box::new(AbiType::Uint(256)), 3));
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
        assert_snapshot!(sig.to_cte_sql_clickhouse());
    }

    #[test]
    fn test_extract_column_references() {
        let cols = extract_column_references("SELECT \"to\", COUNT(*) FROM transfer GROUP BY \"to\"");
        assert!(cols.contains("to"));

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
    fn test_normalize_table_references() {
        let sig = EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)").unwrap();
        
        // lowercase -> original case
        assert_eq!(
            sig.normalize_table_references("SELECT * FROM transfer"),
            "SELECT * FROM Transfer"
        );
        
        // uppercase -> original case
        assert_eq!(
            sig.normalize_table_references("SELECT * FROM TRANSFER"),
            "SELECT * FROM Transfer"
        );
        
        // mixed case -> original case
        assert_eq!(
            sig.normalize_table_references("SELECT * FROM TrAnSfEr"),
            "SELECT * FROM Transfer"
        );
        
        // multiple occurrences
        assert_eq!(
            sig.normalize_table_references("SELECT * FROM transfer UNION SELECT * FROM TRANSFER"),
            "SELECT * FROM Transfer UNION SELECT * FROM Transfer"
        );
        
        // shouldn't match partial words
        assert_eq!(
            sig.normalize_table_references("SELECT transfers FROM transfer"),
            "SELECT transfers FROM Transfer"
        );
    }

    #[test]
    fn test_normalize_table_references_query_patterns() {
        let sig = EventSignature::parse("Transfer(address indexed from, address indexed to, uint256 value)").unwrap();
        
        // Simple SELECT
        assert_eq!(
            sig.normalize_table_references("SELECT \"to\", \"value\" FROM transfer WHERE \"from\" = '0x123'"),
            "SELECT \"to\", \"value\" FROM Transfer WHERE \"from\" = '0x123'"
        );
        
        // SELECT with alias
        assert_eq!(
            sig.normalize_table_references("SELECT t.\"to\" FROM transfer t"),
            "SELECT t.\"to\" FROM Transfer t"
        );
        
        // SELECT with AS alias
        assert_eq!(
            sig.normalize_table_references("SELECT t.\"to\" FROM transfer AS t"),
            "SELECT t.\"to\" FROM Transfer AS t"
        );
        
        // JOIN (self-join pattern)
        assert_eq!(
            sig.normalize_table_references("SELECT a.\"to\", b.\"from\" FROM transfer a JOIN transfer b ON a.\"to\" = b.\"from\""),
            "SELECT a.\"to\", b.\"from\" FROM Transfer a JOIN Transfer b ON a.\"to\" = b.\"from\""
        );
        
        // Subquery
        assert_eq!(
            sig.normalize_table_references("SELECT * FROM (SELECT \"to\", SUM(\"value\") FROM transfer GROUP BY \"to\") sub"),
            "SELECT * FROM (SELECT \"to\", SUM(\"value\") FROM Transfer GROUP BY \"to\") sub"
        );
        
        // UNION ALL
        assert_eq!(
            sig.normalize_table_references("SELECT \"to\" as addr, \"value\" FROM transfer UNION ALL SELECT \"from\" as addr, -\"value\" FROM transfer"),
            "SELECT \"to\" as addr, \"value\" FROM Transfer UNION ALL SELECT \"from\" as addr, -\"value\" FROM Transfer"
        );
        
        // CTE (WITH clause) - user might write their own CTE referencing the event table
        assert_eq!(
            sig.normalize_table_references("WITH filtered AS (SELECT * FROM transfer WHERE \"value\" > 1000) SELECT * FROM filtered"),
            "WITH filtered AS (SELECT * FROM Transfer WHERE \"value\" > 1000) SELECT * FROM filtered"
        );
        
        // GROUP BY with aggregates
        assert_eq!(
            sig.normalize_table_references("SELECT \"to\", COUNT(*), SUM(\"value\") FROM transfer GROUP BY \"to\" HAVING COUNT(*) > 10"),
            "SELECT \"to\", COUNT(*), SUM(\"value\") FROM Transfer GROUP BY \"to\" HAVING COUNT(*) > 10"
        );
        
        // ORDER BY and LIMIT
        assert_eq!(
            sig.normalize_table_references("SELECT * FROM transfer ORDER BY block_num DESC LIMIT 100"),
            "SELECT * FROM Transfer ORDER BY block_num DESC LIMIT 100"
        );
        
        // Window functions
        assert_eq!(
            sig.normalize_table_references("SELECT \"to\", \"value\", ROW_NUMBER() OVER (PARTITION BY \"to\" ORDER BY block_num) FROM transfer"),
            "SELECT \"to\", \"value\", ROW_NUMBER() OVER (PARTITION BY \"to\" ORDER BY block_num) FROM Transfer"
        );
        
        // IN subquery
        assert_eq!(
            sig.normalize_table_references("SELECT * FROM transfer WHERE \"to\" IN (SELECT \"from\" FROM transfer WHERE \"value\" > 1000000)"),
            "SELECT * FROM Transfer WHERE \"to\" IN (SELECT \"from\" FROM Transfer WHERE \"value\" > 1000000)"
        );
        
        // EXISTS subquery
        assert_eq!(
            sig.normalize_table_references("SELECT * FROM transfer t1 WHERE EXISTS (SELECT 1 FROM transfer t2 WHERE t2.\"to\" = t1.\"from\")"),
            "SELECT * FROM Transfer t1 WHERE EXISTS (SELECT 1 FROM Transfer t2 WHERE t2.\"to\" = t1.\"from\")"
        );
    }

    #[test]
    fn test_normalize_different_event_names() {
        // Test with Approval event
        let sig = EventSignature::parse("Approval(address indexed owner, address indexed spender, uint256 value)").unwrap();
        assert_eq!(
            sig.normalize_table_references("SELECT * FROM approval WHERE \"owner\" = '0x123'"),
            "SELECT * FROM Approval WHERE \"owner\" = '0x123'"
        );
        assert_eq!(
            sig.normalize_table_references("SELECT * FROM APPROVAL"),
            "SELECT * FROM Approval"
        );
        
        // Test with Swap event (common in DEX)
        let sig = EventSignature::parse("Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)").unwrap();
        assert_eq!(
            sig.normalize_table_references("SELECT address, SUM(\"amount0In\") FROM swap GROUP BY address"),
            "SELECT address, SUM(\"amount0In\") FROM Swap GROUP BY address"
        );
        
        // Test with lowercase event name in signature
        let sig = EventSignature::parse("mint(address indexed to, uint256 amount)").unwrap();
        assert_eq!(
            sig.normalize_table_references("SELECT * FROM MINT"),
            "SELECT * FROM mint"
        );
        assert_eq!(
            sig.normalize_table_references("SELECT * FROM Mint"),
            "SELECT * FROM mint"
        );
    }

    #[test]
    fn test_cte_filtered_clickhouse_only_to() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
        
        let used_cols: HashSet<String> = ["to"].iter().map(|s| s.to_string()).collect();
        assert_snapshot!(sig.to_cte_sql_clickhouse_filtered(Some(&used_cols)));
    }

    #[test]
    fn test_cte_filtered_clickhouse_from_and_value() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
        
        let used_cols: HashSet<String> = ["from", "value"].iter().map(|s| s.to_string()).collect();
        assert_snapshot!(sig.to_cte_sql_clickhouse_filtered(Some(&used_cols)));
    }

    #[test]
    fn test_cte_filtered_postgres_only_value() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
        
        let used_cols: HashSet<String> = ["value"].iter().map(|s| s.to_string()).collect();
        assert_snapshot!(sig.to_cte_sql_postgres_filtered(Some(&used_cols)));
    }

    #[test]
    fn test_cte_filtered_none_includes_all() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
        assert_snapshot!(sig.to_cte_sql_clickhouse_filtered(None));
    }

    #[test]
    fn test_cte_filtered_preserves_offsets() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();
        
        let used_cols: HashSet<String> = ["to", "value"].iter().map(|s| s.to_string()).collect();
        assert_snapshot!(sig.to_cte_sql_clickhouse_filtered(Some(&used_cols)));
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

    // ========================================================================
    // Predicate Pushdown Tests
    // ========================================================================

    #[test]
    fn test_column_mapping() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();

        let mapping = sig.column_mapping();
        
        // "from" is first indexed param -> topic1
        let (col, ty, indexed) = mapping.get("from").unwrap();
        assert_eq!(col, "topic1");
        assert!(matches!(ty, AbiType::Address));
        assert!(indexed);
        
        // "to" is second indexed param -> topic2
        let (col, ty, indexed) = mapping.get("to").unwrap();
        assert_eq!(col, "topic2");
        assert!(matches!(ty, AbiType::Address));
        assert!(indexed);
        
        // "value" is not indexed -> data
        let (col, _, indexed) = mapping.get("value").unwrap();
        assert_eq!(col, "data");
        assert!(!indexed);
    }

    #[test]
    fn test_rewrite_filters_address() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();

        let sql = r#"SELECT * FROM Transfer WHERE "from" = '0xdAC17F958D2ee523a2206206994597C13D831ec7'"#;
        assert_snapshot!(sig.rewrite_filters_for_pushdown(sql));
    }

    #[test]
    fn test_rewrite_filters_multiple() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();

        let sql = r#"SELECT * FROM Transfer WHERE "from" = '0xdAC17F958D2ee523a2206206994597C13D831ec7' AND "to" = '0xa726a1CD723409074DF9108A2187cfA19899aCF8'"#;
        assert_snapshot!(sig.rewrite_filters_for_pushdown(sql));
    }

    #[test]
    fn test_rewrite_filters_non_indexed_unchanged() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        )
        .unwrap();

        let sql = r#"SELECT * FROM Transfer WHERE "value" = '1000000'"#;
        let rewritten = sig.rewrite_filters_for_pushdown(sql);
        assert_eq!(sql, rewritten);
    }

    // ========================================================================
    // Raw Column Predicate Pushdown Tests
    // ========================================================================

    #[test]
    fn test_extract_raw_predicates_block_num_range() {
        let preds = extract_raw_column_predicates(
            "SELECT * FROM OrderFilled WHERE address = '0xABC' AND block_num >= 100 AND block_num <= 200"
        );
        assert!(preds.contains(&"address = '0xABC'".to_string()));
        assert!(preds.contains(&"block_num >= 100".to_string()));
        assert!(preds.contains(&"block_num <= 200".to_string()));
        assert_eq!(preds.len(), 3);
    }

    #[test]
    fn test_extract_raw_predicates_ignores_decoded_columns() {
        let preds = extract_raw_column_predicates(
            r#"SELECT * FROM Transfer WHERE "from" = '0xABC' AND block_num > 50"#
        );
        // "from" is not a raw column, should not be extracted
        assert!(!preds.iter().any(|p| p.contains("from")));
        assert!(preds.contains(&"block_num > 50".to_string()));
    }

    #[test]
    fn test_extract_raw_predicates_in_list() {
        let preds = extract_raw_column_predicates(
            "SELECT * FROM txs WHERE tx_hash IN ('0xabc', '0xdef')"
        );
        assert_eq!(preds.len(), 1);
        assert!(preds[0].contains("tx_hash IN"));
    }

    #[test]
    fn test_extract_raw_predicates_reversed_comparison() {
        let preds = extract_raw_column_predicates(
            "SELECT * FROM OrderFilled WHERE 100 <= block_num"
        );
        assert!(preds.contains(&"block_num >= 100".to_string()));
    }

    #[test]
    fn test_extract_raw_predicates_empty_for_no_raw_columns() {
        let preds = extract_raw_column_predicates(
            r#"SELECT * FROM Transfer WHERE "to" = '0xABC' AND "value" > 1000"#
        );
        assert!(preds.is_empty());
    }

    #[test]
    fn test_cte_postgres_with_pushdown() {
        let sig = EventSignature::parse(
            "OrderFilled(uint128 indexed orderId, address indexed maker, address indexed taker, uint128 amountFilled, bool partialFill)",
        ).unwrap();

        let pushdown = vec![
            "block_num >= 100".to_string(),
            "block_num <= 200".to_string(),
            "address = '0xABC'".to_string(),
        ];
        assert_snapshot!(sig.to_cte_sql_postgres_with_pushdown(None, &pushdown));
    }

    #[test]
    fn test_cte_clickhouse_with_pushdown() {
        let sig = EventSignature::parse(
            "OrderFilled(uint128 indexed orderId, address indexed maker, address indexed taker, uint128 amountFilled, bool partialFill)",
        ).unwrap();

        let pushdown = vec![
            "block_num >= 100".to_string(),
            "block_num <= 200".to_string(),
            "address = '0xABC'".to_string(),
        ];
        assert_snapshot!(sig.to_cte_sql_clickhouse_with_pushdown(None, &pushdown));
    }

    #[test]
    fn test_cte_postgres_no_pushdown() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        ).unwrap();
        // Empty pushdown should produce identical output to _filtered
        let without = sig.to_cte_sql_postgres_filtered(None);
        let with = sig.to_cte_sql_postgres_with_pushdown(None, &[]);
        assert_eq!(without, with);
    }

    #[test]
    fn test_cte_clickhouse_no_pushdown() {
        let sig = EventSignature::parse(
            "Transfer(address indexed from, address indexed to, uint256 value)",
        ).unwrap();
        let without = sig.to_cte_sql_clickhouse_filtered(None);
        let with = sig.to_cte_sql_clickhouse_with_pushdown(None, &[]);
        assert_eq!(without, with);
    }

}
