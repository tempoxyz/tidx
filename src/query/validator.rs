use anyhow::{anyhow, Result};
use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectName, Query, SetExpr,
    Statement, TableFactor, TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Validates that a SQL query is safe to execute.
///
/// Rejects:
/// - Multiple statements
/// - Non-SELECT statements (INSERT, UPDATE, DELETE, etc.)
/// - Data-modifying CTEs
/// - Dangerous functions (pg_sleep, read_csv, pg_read_file, etc.)
/// - System catalog access
pub fn validate_query(sql: &str) -> Result<()> {
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
        Statement::Query(query) => validate_query_ast(query),
        _ => Err(anyhow!("Only SELECT queries are allowed")),
    }
}

fn validate_query_ast(query: &Query) -> Result<()> {
    // Check CTEs for data-modifying statements
    for cte in &query.with.as_ref().map_or(vec![], |w| w.cte_tables.clone()) {
        validate_query_ast(&cte.query)?;
    }

    validate_set_expr(&query.body)
}

fn validate_set_expr(set_expr: &SetExpr) -> Result<()> {
    match set_expr {
        SetExpr::Select(select) => {
            // Validate FROM clause
            for table in &select.from {
                validate_table_with_joins(table)?;
            }

            // Validate SELECT expressions
            for item in &select.projection {
                if let sqlparser::ast::SelectItem::UnnamedExpr(expr)
                | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } = item
                {
                    validate_expr(expr)?;
                }
            }

            // Validate WHERE clause
            if let Some(selection) = &select.selection {
                validate_expr(selection)?;
            }

            Ok(())
        }
        SetExpr::Query(q) => validate_query_ast(q),
        SetExpr::SetOperation { left, right, .. } => {
            validate_set_expr(left)?;
            validate_set_expr(right)
        }
        SetExpr::Values(_) => Ok(()),
        SetExpr::Insert(_) => Err(anyhow!("INSERT not allowed")),
        SetExpr::Update(_) => Err(anyhow!("UPDATE not allowed")),
        SetExpr::Delete(_) => Err(anyhow!("DELETE not allowed")),
        SetExpr::Merge(_) => Err(anyhow!("MERGE not allowed")),
        SetExpr::Table(_) => Ok(()),
    }
}

fn validate_table_with_joins(table: &TableWithJoins) -> Result<()> {
    validate_table_factor(&table.relation)?;
    for join in &table.joins {
        validate_table_factor(&join.relation)?;
    }
    Ok(())
}

fn validate_table_factor(factor: &TableFactor) -> Result<()> {
    match factor {
        TableFactor::Table { name, args, .. } => {
            // Check if this is a table-valued function like read_csv(...)
            if args.is_some() {
                let func_name = name.to_string().to_lowercase();
                if is_dangerous_table_function(&func_name) {
                    return Err(anyhow!("Table function '{func_name}' is not allowed"));
                }
            }
            validate_table_name(name)
        }
        TableFactor::Derived { subquery, .. } => validate_query_ast(subquery),
        TableFactor::TableFunction { expr, .. } => {
            // Block table functions that can read filesystem
            if let Expr::Function(func) = expr {
                let func_name = func.name.to_string().to_lowercase();
                if is_dangerous_table_function(&func_name) {
                    return Err(anyhow!("Table function '{func_name}' is not allowed"));
                }
            }
            Ok(())
        }
        TableFactor::Function { name, .. } => {
            let func_name = name.to_string().to_lowercase();
            if is_dangerous_table_function(&func_name) {
                return Err(anyhow!("Table function '{func_name}' is not allowed"));
            }
            Ok(())
        }
        TableFactor::NestedJoin { table_with_joins, .. } => {
            validate_table_with_joins(table_with_joins)
        }
        _ => Ok(()),
    }
}

fn validate_table_name(name: &ObjectName) -> Result<()> {
    let full_name = name.to_string().to_lowercase();

    // Block system catalogs
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

    // Block specific dangerous tables
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

    Ok(())
}

fn validate_expr(expr: &Expr) -> Result<()> {
    match expr {
        Expr::Function(func) => validate_function(func),
        Expr::Subquery(q) => validate_query_ast(q),
        Expr::InSubquery { subquery, .. } => validate_query_ast(subquery),
        Expr::Exists { subquery, .. } => validate_query_ast(subquery),
        Expr::BinaryOp { left, right, .. } => {
            validate_expr(left)?;
            validate_expr(right)
        }
        Expr::UnaryOp { expr, .. } => validate_expr(expr),
        Expr::Between { expr, low, high, .. } => {
            validate_expr(expr)?;
            validate_expr(low)?;
            validate_expr(high)
        }
        Expr::Case { operand, conditions, else_result, .. } => {
            if let Some(op) = operand {
                validate_expr(op)?;
            }
            for case_when in conditions {
                validate_expr(&case_when.condition)?;
                validate_expr(&case_when.result)?;
            }
            if let Some(else_r) = else_result {
                validate_expr(else_r)?;
            }
            Ok(())
        }
        Expr::Cast { expr, .. } => validate_expr(expr),
        Expr::Nested(e) => validate_expr(e),
        Expr::InList { expr, list, .. } => {
            validate_expr(expr)?;
            for item in list {
                validate_expr(item)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_function(func: &Function) -> Result<()> {
    let func_name = func.name.to_string().to_lowercase();

    if is_dangerous_function(&func_name) {
        return Err(anyhow!("Function '{func_name}' is not allowed"));
    }

    // Recursively validate function arguments
    if let FunctionArguments::List(arg_list) = &func.args {
        for arg in &arg_list.args {
            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
            | FunctionArg::Named { arg: FunctionArgExpr::Expr(expr), .. } = arg
            {
                validate_expr(expr)?;
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
}
