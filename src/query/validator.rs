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

/// Validates that a SQL query is safe to execute.
///
/// Rejects:
/// - Multiple statements
/// - Non-SELECT statements (INSERT, UPDATE, DELETE, etc.)
/// - Data-modifying CTEs
/// - Dangerous functions (pg_sleep, read_csv, pg_read_file, etc.)
/// - System catalog access
pub fn validate_query(sql: &str) -> Result<()> {
    const MAX_QUERY_LENGTH: usize = 16_384;
    if sql.len() > MAX_QUERY_LENGTH {
        return Err(anyhow!("Query too long (max {MAX_QUERY_LENGTH} bytes)"));
    }

    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| anyhow!("SQL parse error: {e}"))?;

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
            validate_query_ast(query, &cte_names)
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

fn validate_query_ast(query: &Query, cte_names: &HashSet<String>) -> Result<()> {
    // Block recursive CTEs (can cause endless loops / resource exhaustion)
    if let Some(with) = &query.with {
        if with.recursive {
            return Err(anyhow!("Recursive CTEs are not allowed"));
        }
    }

    let mut all_cte_names = cte_names.clone();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            all_cte_names.insert(cte.alias.name.value.to_lowercase());
        }
    }

    for cte in &query.with.as_ref().map_or(vec![], |w| w.cte_tables.clone()) {
        validate_query_ast(&cte.query, &all_cte_names)?;
    }

    validate_set_expr(&query.body, &all_cte_names)?;

    if let Some(order_by) = &query.order_by {
        if let sqlparser::ast::OrderByKind::Expressions(exprs) = &order_by.kind {
            for order_expr in exprs {
                validate_expr(&order_expr.expr, &all_cte_names)?;
            }
        }
    }

    if let Some(limit_clause) = &query.limit_clause {
        match limit_clause {
            sqlparser::ast::LimitClause::LimitOffset { limit, offset, limit_by } => {
                if let Some(limit) = limit {
                    validate_expr(limit, &all_cte_names)?;
                }
                if let Some(offset) = offset {
                    validate_expr(&offset.value, &all_cte_names)?;
                }
                for expr in limit_by {
                    validate_expr(expr, &all_cte_names)?;
                }
            }
            sqlparser::ast::LimitClause::OffsetCommaLimit { offset, limit } => {
                validate_expr(offset, &all_cte_names)?;
                validate_expr(limit, &all_cte_names)?;
            }
        }
    }

    if !query.locks.is_empty() {
        return Err(anyhow!("FOR UPDATE/FOR SHARE is not allowed"));
    }

    Ok(())
}

fn validate_set_expr(set_expr: &SetExpr, cte_names: &HashSet<String>) -> Result<()> {
    match set_expr {
        SetExpr::Select(select) => {
            // Reject SELECT INTO (creates objects)
            if select.into.is_some() {
                return Err(anyhow!("SELECT INTO is not allowed"));
            }

            for table in &select.from {
                validate_table_with_joins(table, cte_names)?;
            }

            for item in &select.projection {
                if let sqlparser::ast::SelectItem::UnnamedExpr(expr)
                | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } = item
                {
                    validate_expr(expr, cte_names)?;
                }
            }

            if let Some(selection) = &select.selection {
                validate_expr(selection, cte_names)?;
            }

            // Validate GROUP BY expressions
            if let sqlparser::ast::GroupByExpr::Expressions(exprs, _) = &select.group_by {
                for expr in exprs {
                    validate_expr(expr, cte_names)?;
                }
            }

            // Validate HAVING
            if let Some(having) = &select.having {
                validate_expr(having, cte_names)?;
            }

            Ok(())
        }
        SetExpr::Query(q) => validate_query_ast(q, cte_names),
        SetExpr::SetOperation { left, right, .. } => {
            validate_set_expr(left, cte_names)?;
            validate_set_expr(right, cte_names)
        }
        SetExpr::Values(values) => {
            // Validate all expressions in VALUES rows to prevent function call bypass
            for row in &values.rows {
                for expr in row {
                    validate_expr(expr, cte_names)?;
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

fn validate_table_with_joins(table: &TableWithJoins, cte_names: &HashSet<String>) -> Result<()> {
    validate_table_factor(&table.relation, cte_names)?;
    for join in &table.joins {
        validate_table_factor(&join.relation, cte_names)?;
        // Validate JOIN ON expressions
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
            validate_expr(expr, cte_names)?;
        }
    }
    Ok(())
}

fn validate_table_factor(factor: &TableFactor, cte_names: &HashSet<String>) -> Result<()> {
    match factor {
        TableFactor::Table { name, args, .. } => {
            if args.is_some() {
                let func_name = name.to_string().to_lowercase();
                if is_dangerous_table_function(&func_name) {
                    return Err(anyhow!("Table function '{func_name}' is not allowed"));
                }
            }
            validate_table_name(name, cte_names)
        }
        TableFactor::Derived { subquery, .. } => validate_query_ast(subquery, cte_names),
        TableFactor::TableFunction { .. } => {
            Err(anyhow!("Table functions in FROM clause are not allowed"))
        }
        TableFactor::Function { .. } => {
            Err(anyhow!("Table functions in FROM clause are not allowed"))
        }
        TableFactor::NestedJoin { table_with_joins, .. } => {
            validate_table_with_joins(table_with_joins, cte_names)
        }
        _ => Ok(()),
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
            return Err(anyhow!("Access to system catalog '{schema}' is not allowed"));
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

    let bare_name = name.0.last()
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

fn validate_expr(expr: &Expr, cte_names: &HashSet<String>) -> Result<()> {
    match expr {
        Expr::Function(func) => validate_function(func, cte_names),
        Expr::Subquery(q) => validate_query_ast(q, cte_names),
        Expr::InSubquery { subquery, .. } => validate_query_ast(subquery, cte_names),
        Expr::Exists { subquery, .. } => validate_query_ast(subquery, cte_names),
        Expr::BinaryOp { left, right, .. } => {
            validate_expr(left, cte_names)?;
            validate_expr(right, cte_names)
        }
        Expr::UnaryOp { expr, .. } => validate_expr(expr, cte_names),
        Expr::Between { expr, low, high, .. } => {
            validate_expr(expr, cte_names)?;
            validate_expr(low, cte_names)?;
            validate_expr(high, cte_names)
        }
        Expr::Case { operand, conditions, else_result, .. } => {
            if let Some(op) = operand {
                validate_expr(op, cte_names)?;
            }
            for case_when in conditions {
                validate_expr(&case_when.condition, cte_names)?;
                validate_expr(&case_when.result, cte_names)?;
            }
            if let Some(else_r) = else_result {
                validate_expr(else_r, cte_names)?;
            }
            Ok(())
        }
        Expr::Cast { expr, .. } => validate_expr(expr, cte_names),
        Expr::Nested(e) => validate_expr(e, cte_names),
        Expr::InList { expr, list, .. } => {
            validate_expr(expr, cte_names)?;
            for item in list {
                validate_expr(item, cte_names)?;
            }
            Ok(())
        }
        Expr::IsNull(e)
        | Expr::IsNotNull(e)
        | Expr::IsTrue(e)
        | Expr::IsFalse(e)
        | Expr::IsNotTrue(e)
        | Expr::IsNotFalse(e)
        | Expr::IsUnknown(e)
        | Expr::IsNotUnknown(e) => validate_expr(e, cte_names),
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            validate_expr(expr, cte_names)?;
            validate_expr(pattern, cte_names)
        }
        Expr::AnyOp { right, .. } | Expr::AllOp { right, .. } => validate_expr(right, cte_names),
        Expr::Value(_)
        | Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::Wildcard(_)
        | Expr::QualifiedWildcard(_, _) => Ok(()),
        other => Err(anyhow!("Expression type not allowed: {other}")),
    }
}

fn validate_function(func: &Function, cte_names: &HashSet<String>) -> Result<()> {
    let func_name = func.name.to_string().to_lowercase();

    if is_dangerous_function(&func_name) {
        return Err(anyhow!("Function '{func_name}' is not allowed"));
    }

    if let FunctionArguments::List(arg_list) = &func.args {
        for arg in &arg_list.args {
            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
            | FunctionArg::Named { arg: FunctionArgExpr::Expr(expr), .. } = arg
            {
                validate_expr(expr, cte_names)?;
            }
        }
    }

    Ok(())
}

/// Check if a function is dangerous (DoS, file access, side effects).
fn is_dangerous_function(name: &str) -> bool {
    const DANGEROUS: &[&str] = &[
        // PostgreSQL DoS/side-effect functions
        "pg_sleep",
        "pg_terminate_backend",
        "pg_cancel_backend",
        "pg_reload_conf",
        "pg_rotate_logfile",
        "pg_switch_wal",
        "pg_create_restore_point",
        "pg_start_backup",
        "pg_stop_backup",
        "set_config",
        "current_setting",
        // PostgreSQL file access
        "pg_read_file",
        "pg_read_binary_file",
        "pg_ls_dir",
        "pg_stat_file",
        "lo_import",
        "lo_export",
        // PostgreSQL command execution
        "pg_execute_server_program",
        // PostgreSQL dblink (remote connections)
        "dblink",
        "dblink_exec",
        "dblink_connect",
        "dblink_send_query",
        "dblink_get_result",
        // PostgreSQL large object access
        "lo_get",
        "lo_open",
        "lo_close",
        "loread",
        "lowrite",
        "lo_creat",
        "lo_create",
        "lo_unlink",
        "lo_put",
        // PostgreSQL set-returning functions (DoS via row generation)
        "generate_series",
        // PostgreSQL admin extension functions (file access)
        "pg_file_read",
        "pg_file_write",
        "pg_file_rename",
        "pg_file_unlink",
        "pg_logdir_ls",
        // ClickHouse system functions
        "system.flush_logs",
        "system.reload_config",
        "system.shutdown",
        "system.kill_query",
        "system.drop_dns_cache",
        "system.drop_mark_cache",
        "system.drop_uncompressed_cache",
    ];

    DANGEROUS.iter().any(|&d| name == d || name.ends_with(&format!(".{d}")))
}

/// Check if a table function is dangerous (filesystem access).
fn is_dangerous_table_function(name: &str) -> bool {
    const DANGEROUS: &[&str] = &[
        // ClickHouse file/URL table functions
        "file",
        "url",
        "s3",
        "gcs",
        "hdfs",
        "remote",
        "remoteSecure",
        "cluster",
        "clusterAllReplicas",
        // ClickHouse input formats
        "input",
        "format",
        // ClickHouse system access
        "system",
        "numbers",
        "zeros",
        "generateRandom",
        // ClickHouse dictionary access (could leak data)
        "dictGet",
        "dictGetOrDefault",
        "dictHas",
    ];

    DANGEROUS.iter().any(|&d| name == d || name.contains(&format!("{d}(")))
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
        // This is the V1 bypass attempt
        let result = validate_query(
            "WITH x AS (UPDATE blocks SET miner = 'pwn' RETURNING 1) SELECT * FROM x",
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_rejects_comment_bypass() {
        // Comments are stripped by parser, so this becomes a valid UPDATE
        let result = validate_query(
            "WITH x AS (UPDA/**/TE blocks SET miner = 'pwn' RETURNING 1) SELECT * FROM x",
        );
        // Parser will either fail to parse or recognize it as UPDATE
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
        assert!(validate_query(
            "SELECT num, ROW_NUMBER() OVER (ORDER BY num) FROM blocks"
        )
        .is_ok());
    }

    #[test]
    fn test_allows_subquery() {
        assert!(validate_query(
            "SELECT * FROM blocks WHERE num IN (SELECT block_num FROM txs)"
        )
        .is_ok());
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
        assert!(validate_query(
            "WITH my_cte AS (SELECT * FROM blocks) SELECT * FROM my_cte"
        )
        .is_ok());
    }

    #[test]
    fn test_rejects_dblink() {
        assert!(validate_query("SELECT * FROM dblink('host=evil dbname=secrets', 'SELECT * FROM passwords')").is_err());
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
        ).is_err());
    }

    #[test]
    fn test_rejects_generate_series() {
        assert!(validate_query("SELECT generate_series(1, 1000000000)").is_err());
        assert!(validate_query("SELECT * FROM blocks WHERE num IN (SELECT generate_series(1, 1000000))").is_err());
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
        assert!(validate_query("SELECT COUNT(*) FROM blocks GROUP BY num HAVING pg_sleep(1) IS NOT NULL").is_err());
    }

    #[test]
    fn test_rejects_dangerous_function_in_join_on() {
        assert!(validate_query(
            "SELECT * FROM blocks JOIN txs ON pg_sleep(1) IS NOT NULL"
        ).is_err());
    }

    #[test]
    fn test_allows_simple_values() {
        assert!(validate_query("VALUES (1, 'hello'), (2, 'world')").is_ok());
    }

    #[test]
    fn test_rejects_long_query() {
        let long_query = format!("SELECT * FROM blocks WHERE num > {}", "1".repeat(20000));
        assert!(validate_query(&long_query).is_err());
    }

    #[test]
    fn test_rejects_for_update() {
        assert!(validate_query("SELECT * FROM blocks FOR UPDATE").is_err());
        assert!(validate_query("SELECT * FROM blocks FOR SHARE").is_err());
    }

    #[test]
    fn test_rejects_dangerous_function_in_order_by() {
        assert!(validate_query(
            "SELECT * FROM blocks ORDER BY pg_sleep(1)"
        ).is_err());
    }

    #[test]
    fn test_rejects_dangerous_function_in_limit() {
        assert!(validate_query(
            "SELECT * FROM blocks LIMIT (SELECT pg_sleep(1))"
        ).is_err());
    }

    #[test]
    fn test_rejects_table_function_in_from() {
        assert!(validate_query("SELECT * FROM generate_series(1, 100) AS t").is_err());
    }

    #[test]
    fn test_allows_normal_order_by() {
        assert!(validate_query("SELECT * FROM blocks ORDER BY num DESC").is_ok());
    }

    #[test]
    fn test_allows_normal_limit_offset() {
        assert!(validate_query("SELECT * FROM blocks LIMIT 10 OFFSET 5").is_ok());
    }
}
