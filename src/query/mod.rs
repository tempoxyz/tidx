mod parser;
mod router;
mod tiered_split;
mod validator;

pub use parser::{
    AbiParam, AbiType, EventSignature, apply_event_signature_ctes_clickhouse,
    apply_event_signature_ctes_postgres, apply_event_signature_ctes_tiered,
    extract_column_references, extract_equality_filters, extract_group_by_columns,
    extract_order_by_columns, extract_raw_column_predicates,
};
pub use router::QueryRoute;
pub use tiered_split::{
    HotWindow, TieredSplit, hot_window_confined, hot_window_confinement, plan_tiered_split,
};
pub use validator::{HARD_LIMIT_MAX, validate_clickhouse_query, validate_query};

use regex_lite::Regex;
use sqlparser::ast::{
    Expr, Ident, LimitClause, OrderByKind, Select, SelectItem, SetExpr, Statement, TableFactor,
    VisitMut, VisitorMut,
};
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::LazyLock;

/// Regex to match hex literals: '0x' followed by 40+ hex characters (addresses, topics, hashes)
/// This avoids matching short '0x' prefixes used in concat() expressions
static HEX_LITERAL_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"'0x([0-9a-fA-F]{40,})'").unwrap());

/// Convert '0x...' hex literals to '\x...' for PostgreSQL bytea comparison.
/// Only replaces hex values of 40+ chars (addresses, topics, hashes), not short '0x' prefixes.
pub fn convert_hex_literals_postgres(sql: &str) -> String {
    HEX_LITERAL_RE.replace_all(sql, r"'\x$1'").into_owned()
}

/// Whether `sql` contains a 40+ digit '0x…' literal with uppercase hex.
/// PostgreSQL decodes these case-insensitively as bytea; ClickHouse compares
/// its lowercase hex strings case-sensitively.
pub(crate) fn has_mixed_case_hex_literal(sql: &str) -> bool {
    HEX_LITERAL_RE
        .captures_iter(sql)
        .any(|c| c[1].bytes().any(|b| b.is_ascii_uppercase()))
}

/// Regex to match RFC 3339 timestamp string literals with an explicit UTC
/// offset ('Z' or ±HH:MM). ClickHouse cannot parse these into DateTime64.
static TS_LITERAL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"'(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?)(Z|z|[+-]\d{2}:\d{2})'")
        .unwrap()
});

/// Rewrite RFC 3339 timestamp literals (offset-suffixed, e.g.
/// '2026-07-09T03:30:42+00:00') to ClickHouse-parseable UTC form
/// ('2026-07-09 03:30:42'). PostgreSQL accepts both; ClickHouse rejects
/// offsets when comparing against DateTime64 columns (stored as UTC).
pub fn convert_timestamp_literals_clickhouse(sql: &str) -> String {
    use chrono::{DateTime, SecondsFormat, Utc};
    TS_LITERAL_RE
        .replace_all(sql, |c: &regex_lite::Captures<'_>| {
            let rfc3339 = format!("{}T{}{}", &c[1], &c[2], &c[3]);
            match DateTime::parse_from_rfc3339(&rfc3339) {
                Ok(dt) => {
                    let utc = dt
                        .with_timezone(&Utc)
                        .to_rfc3339_opts(SecondsFormat::AutoSi, true);
                    // '2026-07-09T03:30:42Z' → '2026-07-09 03:30:42'
                    format!("'{}'", utc.trim_end_matches('Z').replace('T', " "))
                }
                Err(_) => c[0].to_string(),
            }
        })
        .into_owned()
}

#[derive(Default)]
struct SetOutputReferences {
    by_source: HashMap<(String, String), Option<Ident>>,
}

impl SetOutputReferences {
    fn from_set(body: &SetExpr) -> Self {
        let mut arms = Vec::new();
        collect_set_selects(body, &mut arms);
        let Some(first) = arms.first() else {
            return Self::default();
        };
        if arms
            .iter()
            .any(|select| select.projection.iter().any(set_item_expands_columns))
        {
            return Self::default();
        }

        let output_names: Vec<_> = first.projection.iter().map(set_output_name).collect();
        let mut references = Self::default();

        for select in arms {
            let relation_qualifiers = table_qualifiers(select);
            for (index, item) in select.projection.iter().enumerate() {
                let Some(output) = output_names.get(index).and_then(Clone::clone) else {
                    continue;
                };
                let Some((qualifier, column)) = projected_source(item) else {
                    continue;
                };
                if let Some(qualifier) = qualifier {
                    references.insert(&qualifier, &column, output);
                } else {
                    for qualifier in &relation_qualifiers {
                        references.insert(qualifier, &column, output.clone());
                    }
                }
            }
        }

        references
    }

    fn insert(&mut self, qualifier: &Ident, column: &Ident, output: Ident) {
        self.by_source
            .entry((set_reference_key(qualifier), set_reference_key(column)))
            .and_modify(|current| {
                if current.as_ref() != Some(&output) {
                    *current = None;
                }
            })
            .or_insert(Some(output));
    }

    fn resolve(&self, parts: &[Ident]) -> Option<Ident> {
        let [.., qualifier, column] = parts else {
            return None;
        };
        self.by_source
            .get(&(set_reference_key(qualifier), set_reference_key(column)))
            .and_then(Clone::clone)
    }
}

fn set_reference_key(identifier: &Ident) -> String {
    if identifier.quote_style.is_some() {
        identifier.value.clone()
    } else {
        identifier.value.to_ascii_lowercase()
    }
}

fn collect_set_selects<'a>(body: &'a SetExpr, selects: &mut Vec<&'a Select>) {
    match body {
        SetExpr::Select(select) => selects.push(select),
        SetExpr::Query(query) => collect_set_selects(&query.body, selects),
        SetExpr::SetOperation { left, right, .. } => {
            collect_set_selects(left, selects);
            collect_set_selects(right, selects);
        }
        _ => {}
    }
}

fn set_item_expands_columns(item: &SelectItem) -> bool {
    matches!(
        item,
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
    )
}

fn set_output_name(item: &SelectItem) -> Option<Ident> {
    match item {
        SelectItem::ExprWithAlias { alias, .. } => Some(alias.clone()),
        SelectItem::UnnamedExpr(Expr::Identifier(column)) => Some(column.clone()),
        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => parts.last().cloned(),
        _ => None,
    }
}

fn projected_source(item: &SelectItem) -> Option<(Option<Ident>, Ident)> {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => return None,
    };
    match expr {
        Expr::Identifier(column) => Some((None, column.clone())),
        Expr::CompoundIdentifier(parts) => {
            let [.., qualifier, column] = parts.as_slice() else {
                return None;
            };
            Some((Some(qualifier.clone()), column.clone()))
        }
        _ => None,
    }
}

fn table_qualifiers(select: &Select) -> Vec<Ident> {
    let mut qualifiers = Vec::new();
    for table in &select.from {
        collect_table_qualifier(&table.relation, &mut qualifiers);
        for join in &table.joins {
            collect_table_qualifier(&join.relation, &mut qualifiers);
        }
    }
    qualifiers
}

fn collect_table_qualifier(table: &TableFactor, qualifiers: &mut Vec<Ident>) {
    if let TableFactor::Table { name, alias, .. } = table {
        let qualifier = alias
            .as_ref()
            .map(|alias| alias.name.clone())
            .or_else(|| name.0.last().and_then(|part| part.as_ident()).cloned());
        if let Some(qualifier) = qualifier {
            qualifiers.push(qualifier);
        }
    }
}

struct SetOutputReferenceNormalizer<'a> {
    query_depth: usize,
    references: &'a SetOutputReferences,
}

impl VisitorMut for SetOutputReferenceNormalizer<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &mut sqlparser::ast::Query) -> ControlFlow<Self::Break> {
        self.query_depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut sqlparser::ast::Query) -> ControlFlow<Self::Break> {
        self.query_depth -= 1;
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if self.query_depth == 0
            && let Expr::CompoundIdentifier(parts) = expr
            && let Some(column) = self.references.resolve(parts)
        {
            *expr = Expr::Identifier(column);
        }
        ControlFlow::Continue(())
    }
}

fn normalize_set_output_references(expr: &mut Expr, references: &SetOutputReferences) {
    let _ = expr.visit(&mut SetOutputReferenceNormalizer {
        query_depth: 0,
        references,
    });
}

/// Hoist a set operation's trailing `ORDER BY`/`LIMIT`/`OFFSET` into a
/// derived-table wrapper: `SELECT * FROM (<set operation>) AS tidx_set_query
/// ORDER BY ... LIMIT ...`. The trailing form is valid PostgreSQL, but
/// ClickHouse grammar only accepts another set operator after a parenthesized
/// arm. Non-matching queries are returned unchanged.
pub fn hoist_set_operation_order_by_clickhouse(sql: &str) -> String {
    // A fully parenthesized set expression parses as nested `SetExpr::Query`
    // layers; unwrap them to find the set operation. Inner clauses (its own
    // ORDER BY/LIMIT) stay inside the derived table.
    fn is_set_operation(body: &SetExpr) -> bool {
        match body {
            SetExpr::SetOperation { .. } => true,
            SetExpr::Query(inner) => is_set_operation(&inner.body),
            _ => false,
        }
    }

    let Ok(mut statements) = Parser::parse_sql(&ClickHouseDialect {}, sql) else {
        return sql.to_string();
    };
    let [Statement::Query(query)] = statements.as_mut_slice() else {
        return sql.to_string();
    };
    if !is_set_operation(query.body.as_ref())
        || (query.order_by.is_none() && query.limit_clause.is_none() && query.fetch.is_none())
    {
        return sql.to_string();
    }
    let output_references = SetOutputReferences::from_set(query.body.as_ref());

    // Trailing clauses address set outputs, not arm relations. Rewrite only
    // references whose projected output can be resolved unambiguously.
    if let Some(order_by) = &mut query.order_by
        && let OrderByKind::Expressions(exprs) = &mut order_by.kind
    {
        for order_expr in exprs {
            normalize_set_output_references(&mut order_expr.expr, &output_references);
        }
    }
    if let Some(LimitClause::LimitOffset { limit_by, .. }) = &mut query.limit_clause {
        for expr in limit_by {
            normalize_set_output_references(expr, &output_references);
        }
    }

    // Splice the set operation into a parsed wrapper; every other query-level
    // clause stays put and now attaches to the wrapper's plain SELECT.
    let wrapper = "SELECT * FROM (SELECT 1) AS tidx_set_query";
    let Some(Statement::Query(mut wrapper_query)) =
        Parser::parse_sql(&ClickHouseDialect {}, wrapper)
            .expect("valid wrapper template")
            .pop()
    else {
        unreachable!("wrapper template is a single query");
    };
    let SetExpr::Select(select) = wrapper_query.body.as_mut() else {
        unreachable!("wrapper template body is a select");
    };
    let Some(TableFactor::Derived { subquery, .. }) =
        select.from.first_mut().map(|t| &mut t.relation)
    else {
        unreachable!("wrapper template selects from a derived table");
    };
    std::mem::swap(&mut subquery.body, &mut query.body);
    query.body = wrapper_query.body;
    statements[0].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_literals_utc_offset() {
        assert_eq!(
            convert_timestamp_literals_clickhouse("ts >= '2026-07-09T03:30:42+00:00'"),
            "ts >= '2026-07-09 03:30:42'"
        );
        assert_eq!(
            convert_timestamp_literals_clickhouse("ts >= '2026-07-09T03:30:42Z'"),
            "ts >= '2026-07-09 03:30:42'"
        );
    }

    #[test]
    fn timestamp_literals_nonzero_offset_converts_to_utc() {
        assert_eq!(
            convert_timestamp_literals_clickhouse("ts < '2026-07-09T05:30:42+02:00'"),
            "ts < '2026-07-09 03:30:42'"
        );
    }

    #[test]
    fn timestamp_literals_fractional_seconds() {
        assert_eq!(
            convert_timestamp_literals_clickhouse("ts = '2026-07-09T03:30:42.123+00:00'"),
            "ts = '2026-07-09 03:30:42.123'"
        );
    }

    #[test]
    fn timestamp_literals_space_separator() {
        assert_eq!(
            convert_timestamp_literals_clickhouse("ts >= '2026-07-09 03:30:42+00:00'"),
            "ts >= '2026-07-09 03:30:42'"
        );
    }

    #[test]
    fn hoist_union_trailing_order_by_limit() {
        assert_eq!(
            hoist_set_operation_order_by_clickhouse(
                "(SELECT num FROM blocks ORDER BY num DESC LIMIT 2) \
                 UNION (SELECT num FROM blocks ORDER BY num DESC LIMIT 2) \
                 ORDER BY num DESC LIMIT 2"
            ),
            "SELECT * FROM ((SELECT num FROM blocks ORDER BY num DESC LIMIT 2) \
             UNION (SELECT num FROM blocks ORDER BY num DESC LIMIT 2)) AS tidx_set_query \
             ORDER BY num DESC LIMIT 2"
        );
    }

    #[test]
    fn hoist_union_trailing_offset() {
        assert_eq!(
            hoist_set_operation_order_by_clickhouse(
                "SELECT num FROM blocks UNION ALL SELECT num FROM txs ORDER BY num LIMIT 5 OFFSET 1"
            ),
            "SELECT * FROM (SELECT num FROM blocks UNION ALL SELECT num FROM txs) \
             AS tidx_set_query ORDER BY num LIMIT 5 OFFSET 1"
        );
    }

    #[test]
    fn hoist_normalizes_qualified_trailing_references() {
        assert_eq!(
            hoist_set_operation_order_by_clickhouse(
                "SELECT b.num FROM blocks AS b UNION ALL SELECT b.num FROM blocks AS b \
                 ORDER BY b.num DESC, abs(b.num) LIMIT 1 BY b.num"
            ),
            "SELECT * FROM (SELECT b.num FROM blocks AS b UNION ALL \
             SELECT b.num FROM blocks AS b) AS tidx_set_query \
             ORDER BY num DESC, abs(num) LIMIT 1 BY num"
        );
    }

    #[test]
    fn hoist_resolves_qualified_reference_to_first_arm_alias() {
        assert_eq!(
            hoist_set_operation_order_by_clickhouse(
                "SELECT b.num AS height FROM blocks AS b \
                 UNION ALL SELECT b.num AS height FROM blocks AS b \
                 ORDER BY b.num DESC"
            ),
            "SELECT * FROM (SELECT b.num AS height FROM blocks AS b UNION ALL \
             SELECT b.num AS height FROM blocks AS b) AS tidx_set_query \
             ORDER BY height DESC"
        );
    }

    #[test]
    fn hoist_resolves_unquoted_qualifier_case_insensitively() {
        assert_eq!(
            hoist_set_operation_order_by_clickhouse(
                "SELECT B.num AS height FROM blocks AS B \
                 UNION ALL SELECT B.num AS height FROM blocks AS B \
                 ORDER BY b.num DESC"
            ),
            "SELECT * FROM (SELECT B.num AS height FROM blocks AS B UNION ALL \
             SELECT B.num AS height FROM blocks AS B) AS tidx_set_query \
             ORDER BY height DESC"
        );
    }

    #[test]
    fn hoist_preserves_quoted_qualifier_case_sensitivity() {
        assert_eq!(
            hoist_set_operation_order_by_clickhouse(
                "SELECT \"B\".num AS height FROM blocks AS \"B\" \
                 UNION ALL SELECT \"B\".num AS height FROM blocks AS \"B\" \
                 ORDER BY \"b\".num DESC"
            ),
            "SELECT * FROM (SELECT \"B\".num AS height FROM blocks AS \"B\" UNION ALL \
             SELECT \"B\".num AS height FROM blocks AS \"B\") AS tidx_set_query \
             ORDER BY \"b\".num DESC"
        );
    }

    #[test]
    fn hoist_resolves_later_arm_reference_to_first_arm_output() {
        assert_eq!(
            hoist_set_operation_order_by_clickhouse(
                "SELECT b.num FROM blocks AS b \
                 UNION ALL SELECT t.block_num FROM txs AS t \
                 ORDER BY t.block_num DESC"
            ),
            "SELECT * FROM (SELECT b.num FROM blocks AS b UNION ALL \
             SELECT t.block_num FROM txs AS t) AS tidx_set_query \
             ORDER BY num DESC"
        );
    }

    #[test]
    fn hoist_preserves_qualified_references_in_trailing_subqueries() {
        assert_eq!(
            hoist_set_operation_order_by_clickhouse(
                "SELECT b.num FROM blocks AS b UNION ALL SELECT b.num FROM blocks AS b \
                 ORDER BY b.num + (SELECT max(t.idx) FROM txs AS t)"
            ),
            "SELECT * FROM (SELECT b.num FROM blocks AS b UNION ALL \
             SELECT b.num FROM blocks AS b) AS tidx_set_query \
             ORDER BY num + (SELECT max(t.idx) FROM txs AS t)"
        );
    }

    #[test]
    fn hoist_preserves_unresolved_compound_output_reference() {
        assert_eq!(
            hoist_set_operation_order_by_clickhouse(
                "SELECT payload AS result FROM events \
                 UNION ALL SELECT payload AS result FROM events \
                 ORDER BY result.field"
            ),
            "SELECT * FROM (SELECT payload AS result FROM events UNION ALL \
             SELECT payload AS result FROM events) AS tidx_set_query \
             ORDER BY result.field"
        );
    }

    #[test]
    fn hoist_preserves_leading_cte() {
        assert_eq!(
            hoist_set_operation_order_by_clickhouse(
                "WITH b AS (SELECT num FROM blocks) \
                 (SELECT num FROM b) UNION (SELECT num FROM b) ORDER BY num"
            ),
            "WITH b AS (SELECT num FROM blocks) \
             SELECT * FROM ((SELECT num FROM b) UNION (SELECT num FROM b)) \
             AS tidx_set_query ORDER BY num"
        );
    }

    #[test]
    fn hoist_parenthesized_set_query() {
        assert_eq!(
            hoist_set_operation_order_by_clickhouse(
                "(SELECT num FROM blocks UNION SELECT num FROM txs) ORDER BY num LIMIT 10"
            ),
            "SELECT * FROM ((SELECT num FROM blocks UNION SELECT num FROM txs)) \
             AS tidx_set_query ORDER BY num LIMIT 10"
        );
    }

    #[test]
    fn hoist_parenthesized_set_query_preserves_inner_clauses() {
        assert_eq!(
            hoist_set_operation_order_by_clickhouse(
                "(SELECT num FROM blocks UNION SELECT num FROM txs ORDER BY num LIMIT 5) \
                 ORDER BY num DESC LIMIT 3"
            ),
            "SELECT * FROM \
             ((SELECT num FROM blocks UNION SELECT num FROM txs ORDER BY num LIMIT 5)) \
             AS tidx_set_query ORDER BY num DESC LIMIT 3"
        );
    }

    #[test]
    fn hoist_untouched() {
        // Plain selects and bare set operations pass through unchanged.
        for sql in [
            "SELECT num FROM blocks ORDER BY num DESC LIMIT 2",
            "(SELECT num FROM blocks LIMIT 1) UNION (SELECT num FROM txs LIMIT 1)",
            "SELECT * FROM (SELECT num FROM blocks UNION SELECT num FROM txs) AS t ORDER BY num",
            "(SELECT num FROM blocks) ORDER BY num LIMIT 2",
        ] {
            assert_eq!(hoist_set_operation_order_by_clickhouse(sql), sql);
        }
    }

    #[test]
    fn timestamp_literals_untouched() {
        // No offset: already ClickHouse-parseable.
        for sql in [
            "ts >= '2026-07-09 03:30:42'",
            "ts >= '2026-07-09T03:30:42'",
            "name = 'not a timestamp'",
            "v = '0x2026'",
        ] {
            assert_eq!(convert_timestamp_literals_clickhouse(sql), sql);
        }
    }
}
