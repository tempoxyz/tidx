//! Push down simple WHERE predicates on a single, directly named event table.

use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, Ident, Query, SelectItem, SetExpr, Spanned, Statement, TableFactor,
    UnaryOperator, Value, Visit, Visitor,
};
use sqlparser::dialect::{ClickHouseDialect, Dialect};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Span;

use super::parser::{EventSignature, byte_index_for_location};

pub(super) fn rewrite(sql: &str, signatures: &[EventSignature], dialect: &dyn Dialect) -> String {
    let Ok(statements) = Parser::parse_sql(dialect, sql) else {
        return sql.into();
    };
    let [Statement::Query(query)] = statements.as_slice() else {
        return sql.into();
    };
    let mut visitor = Filters {
        signatures,
        clickhouse: dialect.is::<ClickHouseDialect>(),
        shadowed: Vec::new(),
        scopes: Vec::new(),
        edits: Vec::new(),
    };
    let _ = query.visit(&mut visitor);
    visitor.edits.sort_unstable_by_key(|(span, _)| span.start);
    let mut result = sql.to_string();
    for (span, replacement) in visitor.edits.into_iter().rev() {
        if let (Some(start), Some(end)) = (
            byte_index_for_location(sql, span.start),
            byte_index_for_location(sql, span.end),
        ) {
            result.replace_range(start..end, &replacement);
        }
    }
    result
}

struct Filters<'a> {
    signatures: &'a [EventSignature],
    clickhouse: bool,
    shadowed: Vec<String>,
    scopes: Vec<usize>,
    edits: Vec<(Span, String)>,
}

impl Visitor for Filters<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        self.scopes.push(self.shadowed.len());
        if let Some(with) = &query.with {
            self.shadowed.extend(
                with.cte_tables
                    .iter()
                    .map(|cte| cte.alias.name.value.clone()),
            );
        }
        self.collect(&query.body);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        self.shadowed.truncate(self.scopes.pop().unwrap());
        ControlFlow::Continue(())
    }
}

impl Filters<'_> {
    fn collect(&mut self, body: &SetExpr) {
        let select = match body {
            SetExpr::Select(select) => select,
            SetExpr::SetOperation { left, right, .. } => {
                self.collect(left);
                self.collect(right);
                return;
            }
            // The visitor handles nested queries independently.
            _ => return,
        };
        let [from] = select.from.as_slice() else {
            return;
        };
        let TableFactor::Table {
            name,
            alias,
            args: None,
            ..
        } = &from.relation
        else {
            return;
        };
        // Complex sources keep their decoded predicates. Do not infer columns across scopes.
        if !from.joins.is_empty()
            || name.0.len() != 1
            || alias.as_ref().is_some_and(|a| !a.columns.is_empty())
        {
            return;
        }
        let Some(name) = name.0[0].as_ident() else {
            return;
        };
        let Some(signature) = self.signatures.iter().find(|s| {
            s.name.eq_ignore_ascii_case(&name.value)
                && !self
                    .shadowed
                    .iter()
                    .any(|name| s.name.eq_ignore_ascii_case(name))
        }) else {
            return;
        };
        let mut filter = Filter {
            signature,
            qualifier: alias.as_ref().map_or(name, |a| &a.name),
            projection: &select.projection,
            clickhouse: self.clickhouse,
            edits: Vec::new(),
        };
        if let Some(selection) = &select.selection {
            filter.predicate(selection);
        }
        if let Some(prewhere) = &select.prewhere {
            filter.predicate(prewhere);
        }
        self.edits.extend(filter.edits);
    }
}

struct Filter<'a> {
    signature: &'a EventSignature,
    qualifier: &'a Ident,
    projection: &'a [SelectItem],
    clickhouse: bool,
    edits: Vec<(Span, String)>,
}

impl Filter<'_> {
    fn predicate(&mut self, expr: &Expr) {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => {
                self.comparison(left, right);
                self.comparison(right, left);
            }
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And | BinaryOperator::Or,
                right,
            } => {
                self.predicate(left);
                self.predicate(right);
            }
            Expr::Nested(expr)
            | Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } => self.predicate(expr),
            // In particular, do not walk into subqueries or rewrite SELECT/HAVING/ORDER BY.
            _ => {}
        }
    }

    fn key(&self, ident: &Ident) -> String {
        if self.clickhouse || ident.quote_style.is_some() {
            ident.value.clone()
        } else {
            ident.value.to_ascii_lowercase()
        }
    }

    fn column<'a>(&self, expr: &'a Expr) -> Option<(&'a Ident, bool)> {
        match unnested(expr) {
            Expr::Identifier(ident) => Some((ident, false)),
            Expr::CompoundIdentifier(parts)
                if parts.len() == 2 && self.key(&parts[0]) == self.key(self.qualifier) =>
            {
                Some((&parts[1], true))
            }
            _ => None,
        }
    }

    fn comparison(&mut self, column: &Expr, literal: &Expr) {
        let column = unnested(column);
        let literal = unnested(literal);
        let Some((ident, qualified)) = self.column(column) else {
            return;
        };
        let Expr::Value(value) = literal else { return };
        let value = match &value.value {
            Value::SingleQuotedString(s) | Value::Number(s, _) => s,
            _ => return,
        };
        let mut topic_index = 0;
        for (i, param) in self.signature.params.iter().enumerate() {
            if !param.indexed {
                continue;
            }
            topic_index += 1;
            let name = param.name.clone().unwrap_or_else(|| format!("arg{i}"));
            if name != self.key(ident) {
                continue;
            }
            let topic = format!("topic{topic_index}");
            // Identity aliases retain column meaning; other aliases can shadow either name.
            if self.clickhouse
                && self.projection.iter().any(|item| {
                    let SelectItem::ExprWithAlias { expr, alias } = item else {
                        return false;
                    };
                    let alias = self.key(alias);
                    (alias == name || alias == topic)
                        && self
                            .column(expr)
                            .is_none_or(|(column, _)| self.key(column) != alias)
                })
            {
                return;
            }
            if let Some(encoded) = EventSignature::encode_value_for_pushdown(&param.ty, value) {
                let topic = if qualified {
                    format!("{}.{topic}", self.qualifier)
                } else {
                    topic
                };
                self.edits.push((column.span(), topic));
                self.edits.push((literal.span(), format!("'0x{encoded}'")));
            }
            return;
        }
    }
}

fn unnested(mut expr: &Expr) -> &Expr {
    while let Expr::Nested(inner) = expr {
        expr = inner;
    }
    expr
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_each_predicate_without_touching_other_tokens() {
        let sig = EventSignature::parse("Fixed(bytes4 indexed tag)").unwrap();
        let sql = "SELECT f.tag AS tag, f.topic1 AS topic1, 'tag = ''0xcafebabe''' FROM Fixed f WHERE (f.tag)='0xcafebabe' OR '0xdeadbeef' = tag /* keep */";
        let expected = format!(
            "SELECT f.tag AS tag, f.topic1 AS topic1, 'tag = ''0xcafebabe''' FROM Fixed f WHERE (f.topic1)='0xcafebabe{}' OR '0xdeadbeef{}' = topic1 /* keep */",
            "0".repeat(56),
            "0".repeat(56)
        );
        assert_eq!(sig.rewrite_filters_for_pushdown(sql), expected);
    }

    #[test]
    fn rewrites_direct_event_scans_inside_each_query_scope() {
        use super::super::parser::{
            apply_event_signature_ctes_clickhouse, apply_event_signature_ctes_postgres,
            apply_event_signature_ctes_tiered,
        };

        let signature = "A(uint256 indexed amount)";
        let predicate = format!("topic1 = '0x{:064x}'", 1);
        for sql in [
            r#"SELECT count(*) FROM (SELECT DISTINCT tx_hash, log_idx FROM A WHERE "amount" = '1' LIMIT 1000) capped"#,
            r#"WITH q AS (SELECT amount FROM A WHERE "amount" = '1') SELECT * FROM q WHERE amount = 1"#,
            r#"SELECT EXISTS (SELECT 1 FROM A WHERE "amount" = '1')"#,
            r#"SELECT amount FROM A WHERE "amount" = '1' UNION ALL SELECT amount FROM A WHERE "amount" = '1'"#,
            r#"SELECT (WITH A AS (SELECT 1 AS amount) SELECT amount FROM A WHERE amount = 1) FROM A WHERE "amount" = '1'"#,
        ] {
            for apply in [
                apply_event_signature_ctes_postgres,
                apply_event_signature_ctes_clickhouse,
                apply_event_signature_ctes_tiered,
            ] {
                let rewritten = apply(sql, &[signature]).unwrap();
                assert!(
                    rewritten.ends_with(
                        &sql.strip_prefix("WITH ")
                            .unwrap_or(sql)
                            .replace(r#""amount" = '1'"#, &predicate)
                    ),
                    "{rewritten}"
                );
            }
        }
    }

    #[test]
    fn leaves_complex_scopes_and_grouped_expressions_alone() {
        let sig = EventSignature::parse("A(uint256 indexed amount)").unwrap();
        for sql in [
            "SELECT amount FROM A CROSS JOIN B WHERE amount = 1",
            "SELECT * FROM (SELECT amount FROM A) q WHERE amount = 1",
            "WITH q AS (SELECT 1 AS amount) SELECT * FROM q WHERE amount = 1",
            "SELECT * FROM A WHERE EXISTS (SELECT 1 WHERE amount = 1)",
            "SELECT * FROM (WITH A AS (SELECT 1 AS amount) SELECT * FROM A WHERE amount = 1) q",
            "WITH A AS (SELECT 1 AS amount) SELECT * FROM (SELECT * FROM A WHERE amount = 1) q",
            "SELECT amount, count(*) FROM A GROUP BY amount HAVING amount = 1 ORDER BY amount = 1",
            "SELECT amount = 1 FROM A GROUP BY amount",
            "SELECT 2 AS amount FROM A WHERE amount = 1",
            "SELECT 2 AS topic1 FROM A WHERE amount = 1",
        ] {
            assert_eq!(sig.rewrite_filters_for_pushdown(sql), sql);
        }
    }
}
