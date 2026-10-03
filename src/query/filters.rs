//! Push down simple WHERE predicates on a single, directly named event table.

use sqlparser::ast::{
    BinaryOperator, Expr, Ident, SelectItem, SetExpr, Spanned, Statement, TableFactor,
    UnaryOperator, Value,
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
    let SetExpr::Select(select) = query.body.as_ref() else {
        return sql.into();
    };
    let [from] = select.from.as_slice() else {
        return sql.into();
    };
    let TableFactor::Table {
        name,
        alias,
        args: None,
        ..
    } = &from.relation
    else {
        return sql.into();
    };
    // Complex sources keep their decoded predicates. Do not infer columns across scopes.
    if query.with.is_some()
        || !from.joins.is_empty()
        || name.0.len() != 1
        || alias.as_ref().is_some_and(|a| !a.columns.is_empty())
    {
        return sql.into();
    }
    let Some(name) = name.0[0].as_ident() else {
        return sql.into();
    };
    let Some(signature) = signatures
        .iter()
        .find(|s| s.name.eq_ignore_ascii_case(&name.value))
    else {
        return sql.into();
    };
    let mut filter = Filter {
        signature,
        qualifier: alias.as_ref().map_or(name, |a| &a.name),
        projection: &select.projection,
        clickhouse: dialect.is::<ClickHouseDialect>(),
        edits: Vec::new(),
    };
    if let Some(selection) = &select.selection {
        filter.predicate(selection);
    }
    if let Some(prewhere) = &select.prewhere {
        filter.predicate(prewhere);
    }
    filter.edits.sort_unstable_by_key(|(span, _)| span.start);
    let mut result = sql.to_string();
    for (span, replacement) in filter.edits.into_iter().rev() {
        if let (Some(start), Some(end)) = (
            byte_index_for_location(sql, span.start),
            byte_index_for_location(sql, span.end),
        ) {
            result.replace_range(start..end, &replacement);
        }
    }
    result
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

    fn comparison(&mut self, column: &Expr, literal: &Expr) {
        let column = unnested(column);
        let literal = unnested(literal);
        let (ident, qualified) = match column {
            Expr::Identifier(ident) => (ident, false),
            Expr::CompoundIdentifier(parts)
                if parts.len() == 2 && self.key(&parts[0]) == self.key(self.qualifier) =>
            {
                (&parts[1], true)
            }
            _ => return,
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
            // ClickHouse SELECT aliases can shadow both the decoded and raw name in WHERE.
            if self.clickhouse && self.projection.iter().any(|item| matches!(item,
                SelectItem::ExprWithAlias { alias, .. } if self.key(alias) == name || self.key(alias) == topic)) {
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
        let sql = "SELECT 'tag = ''0xcafebabe''' FROM Fixed f WHERE (f.tag)='0xcafebabe' OR '0xdeadbeef' = tag /* keep */";
        let expected = format!(
            "SELECT 'tag = ''0xcafebabe''' FROM Fixed f WHERE (f.topic1)='0xcafebabe{}' OR '0xdeadbeef{}' = topic1 /* keep */",
            "0".repeat(56),
            "0".repeat(56)
        );
        assert_eq!(sig.rewrite_filters_for_pushdown(sql), expected);
    }

    #[test]
    fn leaves_complex_scopes_and_grouped_expressions_alone() {
        let sig = EventSignature::parse("A(uint256 indexed amount)").unwrap();
        for sql in [
            "SELECT amount FROM A CROSS JOIN B WHERE amount = 1",
            "SELECT * FROM (SELECT amount FROM A) q WHERE amount = 1",
            "WITH q AS (SELECT 1 AS amount) SELECT * FROM q WHERE amount = 1",
            "SELECT * FROM A WHERE EXISTS (SELECT 1 WHERE amount = 1)",
            "SELECT amount, count(*) FROM A GROUP BY amount HAVING amount = 1 ORDER BY amount = 1",
            "SELECT amount = 1 FROM A GROUP BY amount",
            "SELECT 2 AS amount FROM A WHERE amount = 1",
            "SELECT 2 AS topic1 FROM A WHERE amount = 1",
        ] {
            assert_eq!(sig.rewrite_filters_for_pushdown(sql), sql);
        }
    }
}
