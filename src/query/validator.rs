use std::collections::HashSet;

use anyhow::{anyhow, Result};
use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectName, Query, SetExpr,
    Statement, TableFactor, TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

const ALLOWED_TABLES: &[&str] = &[
    "blocks",
    "txs",
    "logs",
    "receipts",
    "token_holders",
    "token_balances",
];

const MAX_QUERY_LENGTH: usize = 65_536;
const MAX_SUBQUERY_DEPTH: usize = 4;
pub const HARD_LIMIT_MAX: i64 = 10_000;

/// Validates that a SQL query is safe to execute.
///
/// Uses a reject-by-default approach: only explicitly allowed tables,
/// functions, and expression types are permitted. Everything else is rejected.
pub fn validate_query(sql: &str) -> Result<()> {
    if sql.len() > MAX_QUERY_LENGTH {
        return Err(anyhow!(
            "Query too large ({} bytes, max {})",
            sql.len(),
            MAX_QUERY_LENGTH
        ));
    }

    let dialect = GenericDialect {};
    let statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| anyhow!("SQL parse error: {e}"))?;

    if statements.is_empty() {
        return Err(anyhow!("Empty query"));
    }

    if statements.len() > 1 {
        return Err(anyhow!("Multiple statements not allowed"));
    }

    let stmt = &statements[0];

    match stmt {
        Statement::Query(query) => {
            let cte_names = extract_cte_names(query);
            validate_query_ast(query, &cte_names, 0)
        }
        _ => Err(anyhow!("Only SELECT queries are allowed")),
    }
}

fn extract_cte_names(query: &Query) -> HashSet<String> {
    let mut names = HashSet::new();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            names.insert(cte.alias.name.value.to_lowercase());
        }
    }
    names
}

fn validate_query_ast(query: &Query, cte_names: &HashSet<String>, depth: usize) -> Result<()> {
    if depth > MAX_SUBQUERY_DEPTH {
        return Err(anyhow!(
            "Subquery nesting too deep (max {} levels)",
            MAX_SUBQUERY_DEPTH
        ));
    }

    // Block recursive CTEs (can cause endless loops / resource exhaustion)
    if let Some(with) = &query.with {
        if with.recursive {
            return Err(anyhow!("Recursive CTEs are not allowed"));
        }
    }

    // Block FOR UPDATE / FOR SHARE locking clauses
    if !query.locks.is_empty() {
        return Err(anyhow!(
            "Locking clauses (FOR UPDATE/SHARE) are not allowed"
        ));
    }

    // Block FETCH clause (alternative to LIMIT, could bypass cap)
    if query.fetch.is_some() {
        return Err(anyhow!("FETCH clause is not allowed, use LIMIT instead"));
    }

    let mut all_cte_names = cte_names.clone();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            all_cte_names.insert(cte.alias.name.value.to_lowercase());
        }
    }

    for cte in &query
        .with
        .as_ref()
        .map_or(vec![], |w| w.cte_tables.clone())
    {
        validate_query_ast(&cte.query, &all_cte_names, depth + 1)?;
    }

    validate_set_expr(&query.body, &all_cte_names, depth)?;

    // Validate ORDER BY expressions
    if let Some(order_by) = &query.order_by {
        match &order_by.kind {
            sqlparser::ast::OrderByKind::Expressions(exprs) => {
                for order_expr in exprs {
                    validate_expr(&order_expr.expr, &all_cte_names, depth)?;
                }
            }
            sqlparser::ast::OrderByKind::All(_) => {}
        }
    }

    // Validate LIMIT / OFFSET: only allow numeric literals
    if let Some(limit_clause) = &query.limit_clause {
        match limit_clause {
            sqlparser::ast::LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            } => {
                if let Some(limit_expr) = limit {
                    validate_limit_expr(limit_expr, "LIMIT")?;
                }
                if let Some(offset) = offset {
                    validate_limit_expr(&offset.value, "OFFSET")?;
                }
                if !limit_by.is_empty() {
                    return Err(anyhow!("LIMIT BY is not allowed"));
                }
            }
            sqlparser::ast::LimitClause::OffsetCommaLimit { offset, limit } => {
                validate_limit_expr(offset, "OFFSET")?;
                validate_limit_expr(limit, "LIMIT")?;
            }
        }
    }

    Ok(())
}

fn validate_limit_expr(expr: &Expr, context: &str) -> Result<()> {
    match expr {
        Expr::Value(v) => {
            let val = &v.value;
            match val {
                sqlparser::ast::Value::Number(n, _) => {
                    if let Ok(num) = n.parse::<i64>() {
                        if num < 0 {
                            return Err(anyhow!("{context} must not be negative"));
                        }
                        if num > HARD_LIMIT_MAX {
                            return Err(anyhow!(
                                "{context} value {num} exceeds maximum ({HARD_LIMIT_MAX})"
                            ));
                        }
                        Ok(())
                    } else {
                        Err(anyhow!("{context} must be a valid integer"))
                    }
                }
                sqlparser::ast::Value::Null => {
                    Err(anyhow!("{context} NULL is not allowed"))
                }
                _ => Err(anyhow!("{context} must be a numeric literal")),
            }
        }
        _ => Err(anyhow!("{context} must be a numeric literal")),
    }
}

fn validate_set_expr(
    set_expr: &SetExpr,
    cte_names: &HashSet<String>,
    depth: usize,
) -> Result<()> {
    match set_expr {
        SetExpr::Select(select) => {
            // Reject SELECT INTO (creates objects)
            if select.into.is_some() {
                return Err(anyhow!("SELECT INTO is not allowed"));
            }

            for table in &select.from {
                validate_table_with_joins(table, cte_names, depth)?;
            }

            for item in &select.projection {
                if let sqlparser::ast::SelectItem::UnnamedExpr(expr)
                | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } = item
                {
                    validate_expr(expr, cte_names, depth)?;
                }
            }

            if let Some(selection) = &select.selection {
                validate_expr(selection, cte_names, depth)?;
            }

            // Validate GROUP BY expressions
            if let sqlparser::ast::GroupByExpr::Expressions(exprs, _) = &select.group_by {
                for expr in exprs {
                    validate_expr(expr, cte_names, depth)?;
                }
            }

            // Validate HAVING
            if let Some(having) = &select.having {
                validate_expr(having, cte_names, depth)?;
            }

            Ok(())
        }
        SetExpr::Query(q) => validate_query_ast(q, cte_names, depth),
        SetExpr::SetOperation { left, right, .. } => {
            validate_set_expr(left, cte_names, depth)?;
            validate_set_expr(right, cte_names, depth)
        }
        SetExpr::Values(values) => {
            for row in &values.rows {
                for expr in row {
                    validate_expr(expr, cte_names, depth)?;
                }
            }
            Ok(())
        }
        SetExpr::Insert(_) => Err(anyhow!("INSERT not allowed")),
        SetExpr::Update(_) => Err(anyhow!("UPDATE not allowed")),
        SetExpr::Delete(_) => Err(anyhow!("DELETE not allowed")),
        SetExpr::Merge(_) => Err(anyhow!("MERGE not allowed")),
        SetExpr::Table(_) => Err(anyhow!("TABLE statement is not allowed")),
    }
}

fn validate_table_with_joins(
    table: &TableWithJoins,
    cte_names: &HashSet<String>,
    depth: usize,
) -> Result<()> {
    validate_table_factor(&table.relation, cte_names, depth)?;
    for join in &table.joins {
        validate_table_factor(&join.relation, cte_names, depth)?;
        let constraint = match &join.join_operator {
            sqlparser::ast::JoinOperator::Join(c)
            | sqlparser::ast::JoinOperator::Inner(c)
            | sqlparser::ast::JoinOperator::Left(c)
            | sqlparser::ast::JoinOperator::LeftOuter(c)
            | sqlparser::ast::JoinOperator::Right(c)
            | sqlparser::ast::JoinOperator::RightOuter(c)
            | sqlparser::ast::JoinOperator::FullOuter(c)
            | sqlparser::ast::JoinOperator::CrossJoin(c)
            | sqlparser::ast::JoinOperator::Semi(c)
            | sqlparser::ast::JoinOperator::LeftSemi(c)
            | sqlparser::ast::JoinOperator::RightSemi(c)
            | sqlparser::ast::JoinOperator::Anti(c)
            | sqlparser::ast::JoinOperator::LeftAnti(c)
            | sqlparser::ast::JoinOperator::RightAnti(c) => Some(c),
            _ => None,
        };
        if let Some(sqlparser::ast::JoinConstraint::On(expr)) = constraint {
            validate_expr(expr, cte_names, depth)?;
        }
    }
    Ok(())
}

fn validate_table_factor(
    factor: &TableFactor,
    cte_names: &HashSet<String>,
    depth: usize,
) -> Result<()> {
    match factor {
        TableFactor::Table { name, args, .. } => {
            if args.is_some() {
                return Err(anyhow!("Table functions are not allowed"));
            }
            validate_table_name(name, cte_names)
        }
        TableFactor::Derived { subquery, .. } => {
            validate_query_ast(subquery, cte_names, depth + 1)
        }
        TableFactor::TableFunction { .. } => Err(anyhow!("Table functions are not allowed")),
        TableFactor::Function { .. } => Err(anyhow!("Table functions are not allowed")),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => validate_table_with_joins(table_with_joins, cte_names, depth),
        _ => Err(anyhow!("Unsupported FROM clause type")),
    }
}

fn validate_table_name(name: &ObjectName, cte_names: &HashSet<String>) -> Result<()> {
    let full_name = name.to_string().to_lowercase();

    const BLOCKED_SCHEMAS: &[&str] = &[
        "pg_catalog",
        "information_schema",
        "pg_temp",
        "pg_toast",
    ];

    for schema in BLOCKED_SCHEMAS {
        if full_name.starts_with(schema) {
            return Err(anyhow!(
                "Access to system catalog '{schema}' is not allowed"
            ));
        }
    }

    const BLOCKED_TABLES: &[&str] = &[
        "pg_stat_activity",
        "pg_settings",
        "pg_user",
        "pg_shadow",
        "pg_authid",
        "pg_roles",
    ];

    for table in BLOCKED_TABLES {
        if full_name.contains(table) {
            return Err(anyhow!("Access to table '{table}' is not allowed"));
        }
    }

    let bare_name = name
        .0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.to_lowercase())
        .unwrap_or_default();

    if ALLOWED_TABLES.contains(&bare_name.as_str()) {
        return Ok(());
    }

    if cte_names.contains(&bare_name) {
        return Ok(());
    }

    Err(anyhow!("Access to table '{bare_name}' is not allowed"))
}

/// Reject-by-default expression validation.
/// Only explicitly allowed expression types are permitted.
fn validate_expr(expr: &Expr, cte_names: &HashSet<String>, depth: usize) -> Result<()> {
    match expr {
        // Safe leaf nodes
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => Ok(()),
        Expr::Value(_) => Ok(()),
        Expr::TypedString(_) => Ok(()),
        Expr::Wildcard(_) | Expr::QualifiedWildcard(_, _) => Ok(()),

        // Function calls (validated against allowlist)
        Expr::Function(func) => validate_function(func, cte_names, depth),

        // Subqueries (increment depth)
        Expr::Subquery(q) => validate_query_ast(q, cte_names, depth + 1),
        Expr::InSubquery {
            expr, subquery, ..
        } => {
            validate_expr(expr, cte_names, depth)?;
            validate_query_ast(subquery, cte_names, depth + 1)
        }
        Expr::Exists { subquery, .. } => validate_query_ast(subquery, cte_names, depth + 1),

        // Binary / unary operations
        Expr::BinaryOp { left, right, .. } => {
            validate_expr(left, cte_names, depth)?;
            validate_expr(right, cte_names, depth)
        }
        Expr::UnaryOp { expr, .. } => validate_expr(expr, cte_names, depth),

        // Range expressions
        Expr::Between {
            expr, low, high, ..
        } => {
            validate_expr(expr, cte_names, depth)?;
            validate_expr(low, cte_names, depth)?;
            validate_expr(high, cte_names, depth)
        }

        // CASE WHEN
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                validate_expr(op, cte_names, depth)?;
            }
            for case_when in conditions {
                validate_expr(&case_when.condition, cte_names, depth)?;
                validate_expr(&case_when.result, cte_names, depth)?;
            }
            if let Some(else_r) = else_result {
                validate_expr(else_r, cte_names, depth)?;
            }
            Ok(())
        }

        // Type casting
        Expr::Cast { expr, .. } => validate_expr(expr, cte_names, depth),
        Expr::Nested(e) => validate_expr(e, cte_names, depth),

        // IN list
        Expr::InList { expr, list, .. } => {
            validate_expr(expr, cte_names, depth)?;
            for item in list {
                validate_expr(item, cte_names, depth)?;
            }
            Ok(())
        }

        // Boolean tests
        Expr::IsNull(e)
        | Expr::IsNotNull(e)
        | Expr::IsTrue(e)
        | Expr::IsFalse(e)
        | Expr::IsNotTrue(e)
        | Expr::IsNotFalse(e)
        | Expr::IsUnknown(e)
        | Expr::IsNotUnknown(e) => validate_expr(e, cte_names, depth),

        // Pattern matching
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            validate_expr(expr, cte_names, depth)?;
            validate_expr(pattern, cte_names, depth)
        }
        Expr::SimilarTo { expr, pattern, .. } => {
            validate_expr(expr, cte_names, depth)?;
            validate_expr(pattern, cte_names, depth)
        }

        // ANY/ALL operators
        Expr::AnyOp { right, .. } | Expr::AllOp { right, .. } => {
            validate_expr(right, cte_names, depth)
        }

        // IS DISTINCT FROM
        Expr::IsDistinctFrom(a, b) | Expr::IsNotDistinctFrom(a, b) => {
            validate_expr(a, cte_names, depth)?;
            validate_expr(b, cte_names, depth)
        }

        // SQL builtins parsed as dedicated Expr variants (not Function)
        Expr::Extract { expr, .. } => validate_expr(expr, cte_names, depth),
        Expr::Substring { expr, substring_from, substring_for, .. } => {
            validate_expr(expr, cte_names, depth)?;
            if let Some(from) = substring_from {
                validate_expr(from, cte_names, depth)?;
            }
            if let Some(for_expr) = substring_for {
                validate_expr(for_expr, cte_names, depth)?;
            }
            Ok(())
        }
        Expr::Trim { expr, trim_what, .. } => {
            validate_expr(expr, cte_names, depth)?;
            if let Some(what) = trim_what {
                validate_expr(what, cte_names, depth)?;
            }
            Ok(())
        }
        Expr::Ceil { expr, .. } | Expr::Floor { expr, .. } => {
            validate_expr(expr, cte_names, depth)
        }
        Expr::Position { expr, r#in, .. } => {
            validate_expr(expr, cte_names, depth)?;
            validate_expr(r#in, cte_names, depth)
        }
        Expr::Overlay { expr, overlay_what, overlay_from, overlay_for, .. } => {
            validate_expr(expr, cte_names, depth)?;
            validate_expr(overlay_what, cte_names, depth)?;
            validate_expr(overlay_from, cte_names, depth)?;
            if let Some(for_expr) = overlay_for {
                validate_expr(for_expr, cte_names, depth)?;
            }
            Ok(())
        }
        Expr::Collate { expr, .. } => validate_expr(expr, cte_names, depth),
        Expr::AtTimeZone { timestamp, time_zone, .. } => {
            validate_expr(timestamp, cte_names, depth)?;
            validate_expr(time_zone, cte_names, depth)
        }

        // Tuple / row constructors
        Expr::Tuple(exprs) => {
            for e in exprs {
                validate_expr(e, cte_names, depth)?;
            }
            Ok(())
        }

        // Array literal
        Expr::Array(arr) => {
            for e in &arr.elem {
                validate_expr(e, cte_names, depth)?;
            }
            Ok(())
        }

        // Interval literal
        Expr::Interval(_) => Ok(()),

        // Reject everything else (reject-by-default)
        _ => Err(anyhow!("Unsupported expression type")),
    }
}

const ALLOWED_FUNCTIONS: &[&str] = &[
    // ABI decode helpers (custom PostgreSQL functions)
    "abi_uint",
    "abi_int",
    "abi_address",
    "abi_bool",
    "abi_bytes",
    "abi_string",
    "format_address",
    "format_uint",
    // Aggregates
    "count",
    "sum",
    "avg",
    "min",
    "max",
    // Scalar / null handling
    "coalesce",
    "nullif",
    "greatest",
    "least",
    // Numeric
    "abs",
    "round",
    "floor",
    "ceil",
    "ceiling",
    "trunc",
    "pow",
    "power",
    // String
    "lower",
    "upper",
    "length",
    "substring",
    "substr",
    "trim",
    "ltrim",
    "rtrim",
    "replace",
    "concat",
    "left",
    "right",
    "lpad",
    "rpad",
    // Bytea / hex
    "encode",
    "decode",
    "octet_length",
    // Time
    "date_trunc",
    "extract",
    "to_timestamp",
    "now",
    // Window functions
    "row_number",
    "rank",
    "dense_rank",
    "lag",
    "lead",
    "first_value",
    "last_value",
    "ntile",
    "percent_rank",
    "cume_dist",
    // Type casting helpers
    "cast",
];

fn is_allowed_function(name: &str) -> bool {
    let bare_name = name.rsplit('.').next().unwrap_or(name);
    ALLOWED_FUNCTIONS.contains(&bare_name)
}

fn validate_function(func: &Function, cte_names: &HashSet<String>, depth: usize) -> Result<()> {
    let func_name = func.name.to_string().to_lowercase();

    if !is_allowed_function(&func_name) {
        return Err(anyhow!("Function '{}' is not allowed", func_name));
    }

    if let FunctionArguments::List(arg_list) = &func.args {
        for arg in &arg_list.args {
            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
            | FunctionArg::Named {
                arg: FunctionArgExpr::Expr(expr),
                ..
            } = arg
            {
                validate_expr(expr, cte_names, depth)?;
            }
        }
    }

    // Validate FILTER (WHERE ...) clause
    if let Some(filter) = &func.filter {
        validate_expr(filter, cte_names, depth)?;
    }

    // Validate WITHIN GROUP (ORDER BY ...) clause
    for order_expr in &func.within_group {
        validate_expr(&order_expr.expr, cte_names, depth)?;
    }

    // Validate window function OVER clause
    if let Some(window_type) = &func.over {
        if let sqlparser::ast::WindowType::WindowSpec(spec) = window_type {
            for expr in &spec.partition_by {
                validate_expr(expr, cte_names, depth)?;
            }
            for order_expr in &spec.order_by {
                validate_expr(&order_expr.expr, cte_names, depth)?;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        assert!(validate_query("SELECT * FROM blocks").is_ok());
        assert!(validate_query("SELECT num, hash FROM blocks WHERE num > 100").is_ok());
    }

    #[test]
    fn test_select_with_cte() {
        assert!(validate_query("WITH t AS (SELECT * FROM blocks) SELECT * FROM t").is_ok());
    }

    #[test]
    fn test_rejects_multiple_statements() {
        assert!(validate_query("SELECT 1; SELECT 2").is_err());
    }

    #[test]
    fn test_rejects_insert() {
        assert!(validate_query("INSERT INTO blocks VALUES (1)").is_err());
    }

    #[test]
    fn test_rejects_update() {
        assert!(validate_query("UPDATE blocks SET num = 1").is_err());
    }

    #[test]
    fn test_rejects_delete() {
        assert!(validate_query("DELETE FROM blocks").is_err());
    }

    #[test]
    fn test_rejects_data_modifying_cte() {
        let result = validate_query(
            "WITH x AS (UPDATE blocks SET miner = 'pwn' RETURNING 1) SELECT * FROM x",
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_rejects_comment_bypass() {
        let result = validate_query(
            "WITH x AS (UPDA/**/TE blocks SET miner = 'pwn' RETURNING 1) SELECT * FROM x",
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_rejects_pg_sleep() {
        assert!(validate_query("SELECT pg_sleep(5)").is_err());
    }

    #[test]
    fn test_rejects_pg_read_file() {
        assert!(validate_query("SELECT pg_read_file('/etc/passwd')").is_err());
    }

    #[test]
    fn test_rejects_file_table_function() {
        assert!(validate_query("SELECT * FROM file('/etc/passwd')").is_err());
    }

    #[test]
    fn test_rejects_url_table_function() {
        assert!(validate_query("SELECT * FROM url('http://example.com/data.csv')").is_err());
    }

    #[test]
    fn test_rejects_s3_table_function() {
        assert!(validate_query("SELECT * FROM s3('s3://bucket/file.parquet')").is_err());
    }

    #[test]
    fn test_rejects_system_catalog() {
        assert!(validate_query("SELECT * FROM pg_catalog.pg_tables").is_err());
        assert!(validate_query("SELECT * FROM information_schema.tables").is_err());
    }

    #[test]
    fn test_allows_aggregate_functions() {
        assert!(validate_query("SELECT COUNT(*), SUM(gas_used) FROM blocks").is_ok());
    }

    #[test]
    fn test_allows_window_functions() {
        assert!(
            validate_query("SELECT num, ROW_NUMBER() OVER (ORDER BY num) FROM blocks").is_ok()
        );
    }

    #[test]
    fn test_allows_subquery() {
        assert!(
            validate_query("SELECT * FROM blocks WHERE num IN (SELECT block_num FROM txs)").is_ok()
        );
    }

    #[test]
    fn test_rejects_nested_dangerous_function() {
        assert!(validate_query("SELECT COALESCE(pg_sleep(1), 0)").is_err());
    }

    #[test]
    fn test_rejects_sync_state() {
        assert!(validate_query("SELECT * FROM sync_state").is_err());
    }

    #[test]
    fn test_rejects_pg_tables() {
        assert!(validate_query("SELECT * FROM pg_tables").is_err());
    }

    #[test]
    fn test_rejects_unknown_table() {
        assert!(validate_query("SELECT * FROM some_random_table").is_err());
    }

    #[test]
    fn test_allows_cte_defined_table() {
        assert!(
            validate_query("WITH my_cte AS (SELECT * FROM blocks) SELECT * FROM my_cte").is_ok()
        );
    }

    #[test]
    fn test_rejects_dblink() {
        assert!(validate_query(
            "SELECT * FROM dblink('host=evil dbname=secrets', 'SELECT * FROM passwords')"
        )
        .is_err());
        assert!(validate_query("SELECT dblink_connect('myconn', 'host=evil')").is_err());
        assert!(validate_query("SELECT dblink_exec('myconn', 'DROP TABLE blocks')").is_err());
    }

    #[test]
    fn test_allows_analytics_tables() {
        assert!(validate_query("SELECT * FROM token_holders").is_ok());
        assert!(validate_query("SELECT * FROM token_balances").is_ok());
        assert!(validate_query("SELECT * FROM public.blocks").is_ok());
    }

    #[test]
    fn test_rejects_recursive_cte() {
        assert!(validate_query(
            "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM r) SELECT * FROM r"
        )
        .is_err());
    }

    #[test]
    fn test_rejects_generate_series() {
        assert!(validate_query("SELECT generate_series(1, 1000000000)").is_err());
        assert!(validate_query(
            "SELECT * FROM blocks WHERE num IN (SELECT generate_series(1, 1000000))"
        )
        .is_err());
    }

    #[test]
    fn test_rejects_values_function_bypass() {
        assert!(validate_query("VALUES (pg_sleep(10))").is_err());
        assert!(validate_query("VALUES (pg_read_file('/etc/passwd'))").is_err());
    }

    #[test]
    fn test_rejects_table_statement() {
        assert!(validate_query("TABLE blocks").is_err());
        assert!(validate_query("TABLE pg_shadow").is_err());
    }

    #[test]
    fn test_rejects_select_into() {
        assert!(validate_query("SELECT * INTO newtable FROM blocks").is_err());
    }

    #[test]
    fn test_rejects_lo_functions() {
        assert!(validate_query("SELECT lo_get(12345)").is_err());
        assert!(validate_query("SELECT lo_open(12345, 262144)").is_err());
    }

    #[test]
    fn test_rejects_admin_file_functions() {
        assert!(validate_query("SELECT pg_file_read('/etc/passwd', 0, 1000)").is_err());
        assert!(validate_query("SELECT pg_file_write('/tmp/evil', 'data', false)").is_err());
    }

    #[test]
    fn test_rejects_dangerous_function_in_having() {
        assert!(validate_query(
            "SELECT COUNT(*) FROM blocks GROUP BY num HAVING pg_sleep(1) IS NOT NULL"
        )
        .is_err());
    }

    #[test]
    fn test_rejects_dangerous_function_in_join_on() {
        assert!(
            validate_query("SELECT * FROM blocks JOIN txs ON pg_sleep(1) IS NOT NULL").is_err()
        );
    }

    #[test]
    fn test_allows_simple_values() {
        assert!(validate_query("VALUES (1, 'hello'), (2, 'world')").is_ok());
    }

    #[test]
    fn test_rejects_unknown_function() {
        assert!(validate_query("SELECT md5('test') FROM blocks").is_err());
        assert!(validate_query("SELECT regexp_replace(hash, 'a', 'b') FROM blocks").is_err());
    }

    #[test]
    fn test_allows_abi_helpers() {
        assert!(validate_query("SELECT abi_uint(input) FROM txs").is_ok());
        assert!(validate_query("SELECT abi_address(input) FROM txs").is_ok());
        assert!(validate_query("SELECT format_address(miner) FROM blocks").is_ok());
    }

    #[test]
    fn test_allows_common_functions() {
        assert!(validate_query("SELECT COALESCE(gas_used, 0) FROM blocks").is_ok());
        assert!(validate_query("SELECT ABS(gas_used) FROM blocks").is_ok());
        assert!(validate_query("SELECT LOWER('test') FROM blocks").is_ok());
        assert!(
            validate_query("SELECT date_trunc('hour', to_timestamp(ts)) FROM blocks").is_ok()
        );
    }

    #[test]
    fn test_rejects_all_table_functions() {
        assert!(validate_query("SELECT * FROM generate_series(1, 100)").is_err());
        assert!(validate_query("SELECT * FROM unnest(ARRAY[1,2,3])").is_err());
    }

    #[test]
    fn test_rejects_unsupported_table_factor() {
        assert!(validate_query("SELECT * FROM UNNEST(ARRAY[1,2,3])").is_err());
    }

    // === New tests for this commit ===

    #[test]
    fn test_rejects_for_update() {
        assert!(validate_query("SELECT * FROM blocks FOR UPDATE").is_err());
        assert!(validate_query("SELECT * FROM blocks FOR SHARE").is_err());
    }

    #[test]
    fn test_rejects_dangerous_function_in_order_by() {
        assert!(
            validate_query("SELECT * FROM blocks ORDER BY pg_sleep(1)").is_err()
        );
    }

    #[test]
    fn test_rejects_excessive_limit() {
        assert!(validate_query("SELECT * FROM blocks LIMIT 100000000").is_err());
        assert!(validate_query("SELECT * FROM blocks LIMIT 10001").is_err());
    }

    #[test]
    fn test_allows_reasonable_limit() {
        assert!(validate_query("SELECT * FROM blocks LIMIT 100").is_ok());
        assert!(validate_query("SELECT * FROM blocks LIMIT 10000").is_ok());
        assert!(validate_query("SELECT * FROM blocks LIMIT 1 OFFSET 5").is_ok());
    }

    #[test]
    fn test_rejects_subquery_in_limit() {
        assert!(validate_query("SELECT * FROM blocks LIMIT (SELECT 1)").is_err());
    }

    #[test]
    fn test_rejects_deep_subquery_nesting() {
        // 5 levels of derived table nesting exceeds MAX_SUBQUERY_DEPTH (4)
        let deep = "SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM blocks) a) b) c) d) e";
        assert!(validate_query(deep).is_err());
    }

    #[test]
    fn test_allows_moderate_subquery_nesting() {
        // 3 levels of nesting is within limits
        let moderate = "SELECT * FROM (SELECT * FROM (SELECT * FROM blocks) a) b";
        assert!(validate_query(moderate).is_ok());
    }

    #[test]
    fn test_rejects_query_too_large() {
        let huge = format!("SELECT * FROM blocks WHERE num IN ({})", "1,".repeat(70_000));
        assert!(validate_query(&huge).is_err());
    }

    #[test]
    fn test_allows_order_by_column() {
        assert!(validate_query("SELECT * FROM blocks ORDER BY num DESC").is_ok());
        assert!(
            validate_query("SELECT * FROM blocks ORDER BY num DESC, hash ASC").is_ok()
        );
    }

    #[test]
    fn test_allows_cast_expression() {
        assert!(validate_query("SELECT CAST(num AS TEXT) FROM blocks").is_ok());
    }

    #[test]
    fn test_allows_between() {
        assert!(validate_query("SELECT * FROM blocks WHERE num BETWEEN 1 AND 100").is_ok());
    }

    #[test]
    fn test_allows_like() {
        assert!(validate_query("SELECT * FROM txs WHERE hash LIKE '%abc%'").is_ok());
    }

    #[test]
    fn test_allows_is_null() {
        assert!(validate_query("SELECT * FROM blocks WHERE miner IS NOT NULL").is_ok());
    }

    #[test]
    fn test_allows_case_when() {
        assert!(validate_query(
            "SELECT CASE WHEN num > 100 THEN 'big' ELSE 'small' END FROM blocks"
        )
        .is_ok());
    }

    #[test]
    fn test_allows_array_literal() {
        assert!(validate_query("SELECT * FROM blocks WHERE num = ANY(ARRAY[1,2,3])").is_ok());
    }

    #[test]
    fn test_rejects_filter_clause_bypass() {
        assert!(validate_query(
            "SELECT COUNT(*) FILTER (WHERE pg_sleep(1) IS NOT NULL) FROM blocks"
        )
        .is_err());
    }

    #[test]
    fn test_rejects_limit_null() {
        assert!(validate_query("SELECT * FROM blocks LIMIT NULL").is_err());
    }

    #[test]
    fn test_rejects_negative_limit() {
        assert!(validate_query("SELECT * FROM blocks LIMIT -1").is_err());
    }

    #[test]
    fn test_rejects_fetch_clause() {
        assert!(
            validate_query("SELECT * FROM blocks FETCH FIRST 10 ROWS ONLY").is_err()
        );
    }
}
