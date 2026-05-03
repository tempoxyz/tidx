use std::collections::HashSet;

use anyhow::{Result, anyhow};
use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectName, Query, SetExpr,
    Statement, TableFactor, TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

const ALLOWED_TABLES: &[&str] = &["blocks", "txs", "logs", "receipts"];

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
    if regex_lite::Regex::new(r"(?i)\blimit\s+all\b")
        .expect("valid LIMIT ALL regex")
        .is_match(sql)
    {
        return Err(anyhow!("LIMIT ALL is not allowed"));
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
            let cte_names = HashSet::new();
            validate_query_ast(query, &cte_names, 0)
        }
        _ => Err(anyhow!("Only SELECT queries are allowed")),
    }
}

/// Validates that a ClickHouse user query is safe to execute.
///
/// This path is deny-list oriented because ClickHouse analytics commonly use
/// engine-specific scalar functions, but table functions, system catalogs, and
/// multi-statement/non-SELECT queries are never needed for public queries.
pub fn validate_clickhouse_query(sql: &str) -> Result<()> {
    if sql.len() > MAX_QUERY_LENGTH {
        return Err(anyhow!(
            "Query too large ({} bytes, max {})",
            sql.len(),
            MAX_QUERY_LENGTH
        ));
    }

    let dialect = sqlparser::dialect::ClickHouseDialect {};
    let statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| anyhow!("SQL parse error: {e}"))?;

    if statements.is_empty() {
        return Err(anyhow!("Empty query"));
    }
    if statements.len() > 1 {
        return Err(anyhow!("Multiple statements not allowed"));
    }

    match &statements[0] {
        Statement::Query(query) => {
            let cte_names = HashSet::new();
            validate_clickhouse_query_ast(query, &cte_names, 0)
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
            validate_query_ast(&cte.query, &all_cte_names, depth + 1)?;
            all_cte_names.insert(cte.alias.name.value.to_lowercase());
        }
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
                } else {
                    return Err(anyhow!("LIMIT ALL is not allowed"));
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
                sqlparser::ast::Value::Null => Err(anyhow!("{context} NULL is not allowed")),
                _ => Err(anyhow!("{context} must be a numeric literal")),
            }
        }
        _ => Err(anyhow!("{context} must be a numeric literal")),
    }
}

const CLICKHOUSE_BLOCKED_SCHEMAS: &[&str] = &["system", "information_schema"];
const CLICKHOUSE_DANGEROUS_FUNCTIONS: &[&str] = &[
    "url",
    "file",
    "s3",
    "remote",
    "remotesecure",
    "mysql",
    "postgresql",
    "jdbc",
    "odbc",
    "hdfs",
    "mongodb",
    "redis",
];

fn validate_clickhouse_query_ast(
    query: &Query,
    cte_names: &HashSet<String>,
    depth: usize,
) -> Result<()> {
    if depth > MAX_SUBQUERY_DEPTH {
        return Err(anyhow!(
            "Subquery nesting too deep (max {} levels)",
            MAX_SUBQUERY_DEPTH
        ));
    }

    if query.fetch.is_some() || !query.locks.is_empty() {
        return Err(anyhow!("Unsupported query clause"));
    }

    let mut all_cte_names = cte_names.clone();
    if let Some(with) = &query.with {
        if with.recursive {
            return Err(anyhow!("Recursive CTEs are not allowed"));
        }
        for cte in &with.cte_tables {
            validate_clickhouse_query_ast(&cte.query, &all_cte_names, depth + 1)?;
            all_cte_names.insert(cte.alias.name.value.to_lowercase());
        }
    }

    validate_clickhouse_set_expr(&query.body, &all_cte_names, depth)?;

    if let Some(order_by) = &query.order_by {
        if let sqlparser::ast::OrderByKind::Expressions(exprs) = &order_by.kind {
            for order_expr in exprs {
                validate_clickhouse_expr(&order_expr.expr, &all_cte_names, depth)?;
            }
        }
    }

    Ok(())
}

fn validate_clickhouse_set_expr(
    set_expr: &SetExpr,
    cte_names: &HashSet<String>,
    depth: usize,
) -> Result<()> {
    match set_expr {
        SetExpr::Select(select) => {
            if select.into.is_some() {
                return Err(anyhow!("SELECT INTO is not allowed"));
            }
            for table in &select.from {
                validate_clickhouse_table_with_joins(table, cte_names, depth)?;
            }
            for item in &select.projection {
                if let sqlparser::ast::SelectItem::UnnamedExpr(expr)
                | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } = item
                {
                    validate_clickhouse_expr(expr, cte_names, depth)?;
                }
            }
            if let Some(selection) = &select.selection {
                validate_clickhouse_expr(selection, cte_names, depth)?;
            }
            if let sqlparser::ast::GroupByExpr::Expressions(exprs, _) = &select.group_by {
                for expr in exprs {
                    validate_clickhouse_expr(expr, cte_names, depth)?;
                }
            }
            if let Some(having) = &select.having {
                validate_clickhouse_expr(having, cte_names, depth)?;
            }
            Ok(())
        }
        SetExpr::Query(query) => validate_clickhouse_query_ast(query, cte_names, depth + 1),
        SetExpr::SetOperation { left, right, .. } => {
            validate_clickhouse_set_expr(left, cte_names, depth + 1)?;
            validate_clickhouse_set_expr(right, cte_names, depth + 1)
        }
        SetExpr::Values(values) => {
            for row in &values.rows {
                for expr in row {
                    validate_clickhouse_expr(expr, cte_names, depth)?;
                }
            }
            Ok(())
        }
        _ => Err(anyhow!("Only SELECT queries are allowed")),
    }
}

fn validate_clickhouse_table_with_joins(
    table: &TableWithJoins,
    cte_names: &HashSet<String>,
    depth: usize,
) -> Result<()> {
    validate_clickhouse_table_factor(&table.relation, cte_names, depth)?;
    for join in &table.joins {
        validate_clickhouse_table_factor(&join.relation, cte_names, depth)?;
        match &join.join_operator {
            sqlparser::ast::JoinOperator::Join(sqlparser::ast::JoinConstraint::On(expr))
            | sqlparser::ast::JoinOperator::Inner(sqlparser::ast::JoinConstraint::On(expr))
            | sqlparser::ast::JoinOperator::Left(sqlparser::ast::JoinConstraint::On(expr))
            | sqlparser::ast::JoinOperator::LeftOuter(sqlparser::ast::JoinConstraint::On(expr))
            | sqlparser::ast::JoinOperator::Right(sqlparser::ast::JoinConstraint::On(expr))
            | sqlparser::ast::JoinOperator::RightOuter(sqlparser::ast::JoinConstraint::On(expr))
            | sqlparser::ast::JoinOperator::FullOuter(sqlparser::ast::JoinConstraint::On(expr))
            | sqlparser::ast::JoinOperator::CrossJoin(sqlparser::ast::JoinConstraint::On(expr)) => {
                validate_clickhouse_expr(expr, cte_names, depth)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_clickhouse_table_factor(
    factor: &TableFactor,
    cte_names: &HashSet<String>,
    depth: usize,
) -> Result<()> {
    match factor {
        TableFactor::Table {
            name,
            args,
            sample,
            with_ordinality,
            ..
        } => {
            if args.is_some() || sample.is_some() || *with_ordinality {
                return Err(anyhow!("Table functions are not allowed"));
            }
            validate_clickhouse_table_name(name, cte_names)
        }
        TableFactor::Derived { subquery, .. } => {
            validate_clickhouse_query_ast(subquery, cte_names, depth + 1)
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => validate_clickhouse_table_with_joins(table_with_joins, cte_names, depth),
        TableFactor::TableFunction { .. } | TableFactor::Function { .. } => {
            Err(anyhow!("Table functions are not allowed"))
        }
        _ => Err(anyhow!("Unsupported FROM clause type")),
    }
}

fn validate_clickhouse_table_name(name: &ObjectName, cte_names: &HashSet<String>) -> Result<()> {
    let parts: Vec<String> = name
        .0
        .iter()
        .map(|part| {
            part.as_ident()
                .map(|ident| ident.value.to_lowercase())
                .ok_or_else(|| anyhow!("Unsupported table identifier"))
        })
        .collect::<Result<_>>()?;

    if parts.len() > 1 && CLICKHOUSE_BLOCKED_SCHEMAS.contains(&parts[0].as_str()) {
        return Err(anyhow!(
            "Access to ClickHouse system schema '{}' is not allowed",
            parts[0]
        ));
    }

    let bare_name = parts.last().cloned().unwrap_or_default();
    if ALLOWED_TABLES.contains(&bare_name.as_str()) {
        return Ok(());
    }
    if parts.len() == 1 && cte_names.contains(&bare_name) {
        return Ok(());
    }

    Err(anyhow!("Access to table '{bare_name}' is not allowed"))
}

fn validate_clickhouse_expr(expr: &Expr, cte_names: &HashSet<String>, depth: usize) -> Result<()> {
    match expr {
        Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::Value(_)
        | Expr::TypedString(_)
        | Expr::Wildcard(_)
        | Expr::QualifiedWildcard(_, _) => Ok(()),
        Expr::Function(func) => validate_clickhouse_function(func, cte_names, depth),
        Expr::Subquery(query) => validate_clickhouse_query_ast(query, cte_names, depth + 1),
        Expr::InSubquery { expr, subquery, .. } => {
            validate_clickhouse_expr(expr, cte_names, depth)?;
            validate_clickhouse_query_ast(subquery, cte_names, depth + 1)
        }
        Expr::Exists { subquery, .. } => {
            validate_clickhouse_query_ast(subquery, cte_names, depth + 1)
        }
        Expr::BinaryOp { left, right, .. } => {
            validate_clickhouse_expr(left, cte_names, depth)?;
            validate_clickhouse_expr(right, cte_names, depth)
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) | Expr::Cast { expr, .. } => {
            validate_clickhouse_expr(expr, cte_names, depth)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            validate_clickhouse_expr(expr, cte_names, depth)?;
            validate_clickhouse_expr(low, cte_names, depth)?;
            validate_clickhouse_expr(high, cte_names, depth)
        }
        Expr::InList { expr, list, .. } => {
            validate_clickhouse_expr(expr, cte_names, depth)?;
            for item in list {
                validate_clickhouse_expr(item, cte_names, depth)?;
            }
            Ok(())
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                validate_clickhouse_expr(operand, cte_names, depth)?;
            }
            for case_when in conditions {
                validate_clickhouse_expr(&case_when.condition, cte_names, depth)?;
                validate_clickhouse_expr(&case_when.result, cte_names, depth)?;
            }
            if let Some(else_result) = else_result {
                validate_clickhouse_expr(else_result, cte_names, depth)?;
            }
            Ok(())
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            validate_clickhouse_expr(expr, cte_names, depth)?;
            validate_clickhouse_expr(pattern, cte_names, depth)
        }
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            validate_clickhouse_expr(expr, cte_names, depth)?;
            if let Some(from) = substring_from {
                validate_clickhouse_expr(from, cte_names, depth)?;
            }
            if let Some(for_expr) = substring_for {
                validate_clickhouse_expr(for_expr, cte_names, depth)?;
            }
            Ok(())
        }
        Expr::Trim {
            expr, trim_what, ..
        } => {
            validate_clickhouse_expr(expr, cte_names, depth)?;
            if let Some(trim_what) = trim_what {
                validate_clickhouse_expr(trim_what, cte_names, depth)?;
            }
            Ok(())
        }
        Expr::Extract { expr, .. }
        | Expr::Ceil { expr, .. }
        | Expr::Floor { expr, .. }
        | Expr::Collate { expr, .. } => validate_clickhouse_expr(expr, cte_names, depth),
        Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotUnknown(expr) => validate_clickhouse_expr(expr, cte_names, depth),
        Expr::Tuple(exprs) => {
            for expr in exprs {
                validate_clickhouse_expr(expr, cte_names, depth)?;
            }
            Ok(())
        }
        Expr::Array(arr) => {
            for expr in &arr.elem {
                validate_clickhouse_expr(expr, cte_names, depth)?;
            }
            Ok(())
        }
        _ => Err(anyhow!("Unsupported expression type")),
    }
}

fn validate_clickhouse_function(
    func: &Function,
    cte_names: &HashSet<String>,
    depth: usize,
) -> Result<()> {
    let func_name = func.name.to_string().to_lowercase();
    let bare_name = func_name.rsplit('.').next().unwrap_or(&func_name);
    if CLICKHOUSE_DANGEROUS_FUNCTIONS.contains(&bare_name) {
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
                validate_clickhouse_expr(expr, cte_names, depth)?;
            }
        }
        for clause in &arg_list.clauses {
            match clause {
                sqlparser::ast::FunctionArgumentClause::OrderBy(order_by) => {
                    for order_expr in order_by {
                        validate_clickhouse_expr(&order_expr.expr, cte_names, depth)?;
                    }
                }
                sqlparser::ast::FunctionArgumentClause::Limit(expr) => {
                    validate_limit_expr(expr, "FUNCTION LIMIT")?;
                }
                sqlparser::ast::FunctionArgumentClause::Having(bound) => {
                    validate_clickhouse_expr(&bound.1, cte_names, depth)?;
                }
                sqlparser::ast::FunctionArgumentClause::IgnoreOrRespectNulls(_) => {}
                _ => return Err(anyhow!("Unsupported function clause")),
            }
        }
    }

    if let Some(filter) = &func.filter {
        validate_clickhouse_expr(filter, cte_names, depth)?;
    }
    for order_expr in &func.within_group {
        validate_clickhouse_expr(&order_expr.expr, cte_names, depth)?;
    }
    if let Some(sqlparser::ast::WindowType::WindowSpec(spec)) = &func.over {
        for expr in &spec.partition_by {
            validate_clickhouse_expr(expr, cte_names, depth)?;
        }
        for order_expr in &spec.order_by {
            validate_clickhouse_expr(&order_expr.expr, cte_names, depth)?;
        }
    }

    Ok(())
}

fn validate_set_expr(set_expr: &SetExpr, cte_names: &HashSet<String>, depth: usize) -> Result<()> {
    if depth > MAX_SUBQUERY_DEPTH {
        return Err(anyhow!(
            "Subquery nesting too deep (max {} levels)",
            MAX_SUBQUERY_DEPTH
        ));
    }

    match set_expr {
        SetExpr::Select(select) => {
            // Reject SELECT INTO (creates objects)
            if select.into.is_some() {
                return Err(anyhow!("SELECT INTO is not allowed"));
            }

            if let Some(sqlparser::ast::Distinct::On(exprs)) = &select.distinct {
                for expr in exprs {
                    validate_expr(expr, cte_names, depth)?;
                }
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

            for window in &select.named_window {
                validate_named_window(window, cte_names, depth)?;
            }

            Ok(())
        }
        SetExpr::Query(q) => validate_query_ast(q, cte_names, depth + 1),
        SetExpr::SetOperation { left, right, .. } => {
            validate_set_expr(left, cte_names, depth + 1)?;
            validate_set_expr(right, cte_names, depth + 1)
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
        TableFactor::Table {
            name,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
            ..
        } => {
            if args.is_some() {
                return Err(anyhow!("Table functions are not allowed"));
            }
            if !with_hints.is_empty()
                || version.is_some()
                || *with_ordinality
                || !partitions.is_empty()
                || json_path.is_some()
                || sample.is_some()
                || !index_hints.is_empty()
            {
                return Err(anyhow!("Unsupported table modifier"));
            }
            validate_table_name(name, cte_names)
        }
        TableFactor::Derived { subquery, .. } => validate_query_ast(subquery, cte_names, depth + 1),
        TableFactor::TableFunction { .. } => Err(anyhow!("Table functions are not allowed")),
        TableFactor::Function { .. } => Err(anyhow!("Table functions are not allowed")),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => validate_table_with_joins(table_with_joins, cte_names, depth),
        _ => Err(anyhow!("Unsupported FROM clause type")),
    }
}

fn validate_table_name(name: &ObjectName, cte_names: &HashSet<String>) -> Result<()> {
    const BLOCKED_SCHEMAS: &[&str] = &["pg_catalog", "information_schema", "pg_temp", "pg_toast"];

    let name_parts = object_name_parts(name)?;
    if name_parts.len() > 1 && BLOCKED_SCHEMAS.contains(&name_parts[0].as_str()) {
        return Err(anyhow!(
            "Access to system catalog '{}' is not allowed",
            name_parts[0]
        ));
    }

    const BLOCKED_TABLES: &[&str] = &[
        "pg_stat_activity",
        "pg_settings",
        "pg_user",
        "pg_shadow",
        "pg_authid",
        "pg_roles",
    ];

    for part in &name_parts {
        if BLOCKED_TABLES.contains(&part.as_str()) {
            return Err(anyhow!("Access to table '{part}' is not allowed"));
        }
    }

    let bare_name = name_parts.last().cloned().unwrap_or_default();

    if ALLOWED_TABLES.contains(&bare_name.as_str()) {
        return Ok(());
    }

    if name_parts.len() == 1 && cte_names.contains(&bare_name) {
        return Ok(());
    }

    Err(anyhow!("Access to table '{bare_name}' is not allowed"))
}

fn object_name_parts(name: &ObjectName) -> Result<Vec<String>> {
    name.0
        .iter()
        .map(|part| {
            part.as_ident()
                .map(|ident| ident.value.to_lowercase())
                .ok_or_else(|| anyhow!("Unsupported table identifier"))
        })
        .collect()
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
        Expr::InSubquery { expr, subquery, .. } => {
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
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            validate_expr(expr, cte_names, depth)?;
            if let Some(from) = substring_from {
                validate_expr(from, cte_names, depth)?;
            }
            if let Some(for_expr) = substring_for {
                validate_expr(for_expr, cte_names, depth)?;
            }
            Ok(())
        }
        Expr::Trim {
            expr, trim_what, ..
        } => {
            validate_expr(expr, cte_names, depth)?;
            if let Some(what) = trim_what {
                validate_expr(what, cte_names, depth)?;
            }
            Ok(())
        }
        Expr::Ceil { expr, .. } | Expr::Floor { expr, .. } => validate_expr(expr, cte_names, depth),
        Expr::Position { expr, r#in, .. } => {
            validate_expr(expr, cte_names, depth)?;
            validate_expr(r#in, cte_names, depth)
        }
        Expr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
            ..
        } => {
            validate_expr(expr, cte_names, depth)?;
            validate_expr(overlay_what, cte_names, depth)?;
            validate_expr(overlay_from, cte_names, depth)?;
            if let Some(for_expr) = overlay_for {
                validate_expr(for_expr, cte_names, depth)?;
            }
            Ok(())
        }
        Expr::Collate { expr, .. } => validate_expr(expr, cte_names, depth),
        Expr::AtTimeZone {
            timestamp,
            time_zone,
            ..
        } => {
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
        Expr::Interval(interval) => validate_expr(&interval.value, cte_names, depth),

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
    "array_agg",
    "string_agg",
    "bool_and",
    "bool_or",
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
    "mod",
    "sign",
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
    "concat_ws",
    "left",
    "right",
    "lpad",
    "rpad",
    "position",
    "strpos",
    "starts_with",
    "repeat",
    "reverse",
    "to_hex",
    // Bytea / hex
    "encode",
    "decode",
    "octet_length",
    // Bytea
    "bit_length",
    // Time
    "date",
    "date_trunc",
    "date_part",
    "extract",
    "to_timestamp",
    "to_char",
    "now",
    "age",
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
        for clause in &arg_list.clauses {
            validate_function_argument_clause(clause, cte_names, depth)?;
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
    if let Some(sqlparser::ast::WindowType::WindowSpec(spec)) = &func.over {
        validate_window_spec(spec, cte_names, depth)?;
    }

    Ok(())
}

fn validate_function_argument_clause(
    clause: &sqlparser::ast::FunctionArgumentClause,
    cte_names: &HashSet<String>,
    depth: usize,
) -> Result<()> {
    match clause {
        sqlparser::ast::FunctionArgumentClause::IgnoreOrRespectNulls(_) => Ok(()),
        sqlparser::ast::FunctionArgumentClause::OrderBy(order_by) => {
            for order_expr in order_by {
                validate_expr(&order_expr.expr, cte_names, depth)?;
            }
            Ok(())
        }
        sqlparser::ast::FunctionArgumentClause::Limit(expr) => {
            validate_limit_expr(expr, "FUNCTION LIMIT")
        }
        sqlparser::ast::FunctionArgumentClause::Having(bound) => {
            validate_expr(&bound.1, cte_names, depth)
        }
        _ => Err(anyhow!("Unsupported function clause")),
    }
}

fn validate_named_window(
    window: &sqlparser::ast::NamedWindowDefinition,
    cte_names: &HashSet<String>,
    depth: usize,
) -> Result<()> {
    match &window.1 {
        sqlparser::ast::NamedWindowExpr::NamedWindow(_) => Ok(()),
        sqlparser::ast::NamedWindowExpr::WindowSpec(spec) => {
            validate_window_spec(spec, cte_names, depth)
        }
    }
}

fn validate_window_spec(
    spec: &sqlparser::ast::WindowSpec,
    cte_names: &HashSet<String>,
    depth: usize,
) -> Result<()> {
    for expr in &spec.partition_by {
        validate_expr(expr, cte_names, depth)?;
    }
    for order_expr in &spec.order_by {
        validate_expr(&order_expr.expr, cte_names, depth)?;
    }
    if let Some(frame) = &spec.window_frame {
        validate_window_frame_bound(&frame.start_bound, cte_names, depth)?;
        if let Some(end_bound) = &frame.end_bound {
            validate_window_frame_bound(end_bound, cte_names, depth)?;
        }
    }
    Ok(())
}

fn validate_window_frame_bound(
    bound: &sqlparser::ast::WindowFrameBound,
    cte_names: &HashSet<String>,
    depth: usize,
) -> Result<()> {
    match bound {
        sqlparser::ast::WindowFrameBound::CurrentRow
        | sqlparser::ast::WindowFrameBound::Preceding(None)
        | sqlparser::ast::WindowFrameBound::Following(None) => Ok(()),
        sqlparser::ast::WindowFrameBound::Preceding(Some(expr))
        | sqlparser::ast::WindowFrameBound::Following(Some(expr)) => {
            validate_expr(expr, cte_names, depth)
        }
    }
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
        assert!(validate_query("SELECT num, ROW_NUMBER() OVER (ORDER BY num) FROM blocks").is_ok());
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
        assert!(
            validate_query(
                "SELECT * FROM dblink('host=evil dbname=secrets', 'SELECT * FROM passwords')"
            )
            .is_err()
        );
        assert!(validate_query("SELECT dblink_connect('myconn', 'host=evil')").is_err());
        assert!(validate_query("SELECT dblink_exec('myconn', 'DROP TABLE blocks')").is_err());
    }

    #[test]
    fn test_allows_schema_qualified_tables() {
        assert!(validate_query("SELECT * FROM public.blocks").is_ok());
    }

    #[test]
    fn test_rejects_recursive_cte() {
        assert!(
            validate_query(
                "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM r) SELECT * FROM r"
            )
            .is_err()
        );
    }

    #[test]
    fn test_rejects_generate_series() {
        assert!(validate_query("SELECT generate_series(1, 1000000000)").is_err());
        assert!(
            validate_query(
                "SELECT * FROM blocks WHERE num IN (SELECT generate_series(1, 1000000))"
            )
            .is_err()
        );
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
        assert!(
            validate_query(
                "SELECT COUNT(*) FROM blocks GROUP BY num HAVING pg_sleep(1) IS NOT NULL"
            )
            .is_err()
        );
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
        assert!(validate_query("SELECT date_trunc('hour', to_timestamp(ts)) FROM blocks").is_ok());
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
        assert!(validate_query("SELECT * FROM blocks ORDER BY pg_sleep(1)").is_err());
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
        let huge = format!(
            "SELECT * FROM blocks WHERE num IN ({})",
            "1,".repeat(70_000)
        );
        assert!(validate_query(&huge).is_err());
    }

    #[test]
    fn test_allows_order_by_column() {
        assert!(validate_query("SELECT * FROM blocks ORDER BY num DESC").is_ok());
        assert!(validate_query("SELECT * FROM blocks ORDER BY num DESC, hash ASC").is_ok());
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
        assert!(
            validate_query("SELECT CASE WHEN num > 100 THEN 'big' ELSE 'small' END FROM blocks")
                .is_ok()
        );
    }

    #[test]
    fn test_allows_array_literal() {
        assert!(validate_query("SELECT * FROM blocks WHERE num = ANY(ARRAY[1,2,3])").is_ok());
    }

    #[test]
    fn test_rejects_filter_clause_bypass() {
        assert!(
            validate_query("SELECT COUNT(*) FILTER (WHERE pg_sleep(1) IS NOT NULL) FROM blocks")
                .is_err()
        );
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
        assert!(validate_query("SELECT * FROM blocks FETCH FIRST 10 ROWS ONLY").is_err());
    }

    #[test]
    fn test_rejects_self_referencing_cte_shadowing() {
        assert!(
            validate_query(
                "WITH sync_state AS (SELECT * FROM sync_state) SELECT * FROM sync_state"
            )
            .is_err()
        );
    }

    #[test]
    fn test_rejects_schema_qualified_cte_shadowing() {
        assert!(
            validate_query(
                "WITH sync_state AS (SELECT * FROM blocks) SELECT * FROM public.sync_state"
            )
            .is_err()
        );
    }

    #[test]
    fn test_rejects_quoted_blocked_schema() {
        assert!(validate_query(r#"SELECT * FROM "pg_catalog".pg_proc"#).is_err());
        assert!(validate_query(r#"SELECT * FROM "information_schema".tables"#).is_err());
    }

    #[test]
    fn test_rejects_dangerous_function_in_distinct_on() {
        assert!(validate_query("SELECT DISTINCT ON (pg_sleep(1)) num FROM blocks").is_err());
    }

    #[test]
    fn test_rejects_dangerous_function_in_named_window() {
        assert!(
            validate_query(
                "SELECT row_number() OVER w FROM blocks WINDOW w AS (ORDER BY pg_sleep(1))"
            )
            .is_err()
        );
    }

    #[test]
    fn test_rejects_dangerous_function_in_aggregate_order_by() {
        assert!(validate_query("SELECT array_agg(num ORDER BY pg_sleep(1)) FROM blocks").is_err());
    }

    #[test]
    fn test_rejects_table_sample_modifier() {
        assert!(validate_query("SELECT * FROM blocks TABLESAMPLE SYSTEM (10)").is_err());
    }

    #[test]
    fn test_rejects_dangerous_interval_expression() {
        assert!(validate_query("SELECT INTERVAL pg_sleep(1) FROM blocks").is_err());
    }

    #[test]
    fn test_rejects_limit_all() {
        assert!(validate_query("SELECT * FROM blocks LIMIT ALL").is_err());
    }

    #[test]
    fn test_rejects_deep_parenthesized_query_nesting() {
        let mut sql = "SELECT * FROM blocks".to_string();
        for _ in 0..32 {
            sql = format!("({sql})");
        }
        let sql = format!("SELECT * FROM {sql} nested");
        assert!(validate_query(&sql).is_err());
    }

    #[test]
    fn test_clickhouse_allows_expected_analytics_query() {
        assert!(
            validate_clickhouse_query(
                "SELECT lower(substring(tx_hash, 3)), count() FROM logs GROUP BY tx_hash LIMIT 10"
            )
            .is_ok()
        );
    }

    #[test]
    fn test_clickhouse_rejects_non_select_and_multiple_statements() {
        assert!(validate_clickhouse_query("DROP TABLE logs").is_err());
        assert!(validate_clickhouse_query("SELECT * FROM logs; SELECT * FROM txs").is_err());
    }

    #[test]
    fn test_clickhouse_rejects_system_tables() {
        assert!(validate_clickhouse_query("SELECT * FROM system.tables").is_err());
        assert!(validate_clickhouse_query(r#"SELECT * FROM "system".tables"#).is_err());
    }

    #[test]
    fn test_clickhouse_rejects_table_functions() {
        assert!(validate_clickhouse_query("SELECT * FROM url('http://169.254.169.254/')").is_err());
        assert!(validate_clickhouse_query("SELECT * FROM file('/etc/passwd')").is_err());
        assert!(validate_clickhouse_query("SELECT * FROM s3('s3://bucket/key')").is_err());
    }

    #[test]
    fn test_clickhouse_rejects_dangerous_scalar_functions() {
        assert!(validate_clickhouse_query("SELECT url('http://example.com') FROM logs").is_err());
        assert!(
            validate_clickhouse_query("SELECT remote('host', 'db', 'table') FROM logs").is_err()
        );
    }

    #[test]
    fn test_clickhouse_rejects_schema_qualified_cte_shadowing() {
        assert!(
            validate_clickhouse_query(
                "WITH system AS (SELECT * FROM logs) SELECT * FROM system.tables"
            )
            .is_err()
        );
    }
}
