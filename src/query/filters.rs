//! Resolve event columns in their query scope before rewriting comparisons.

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, Ident, OrderBy, Query, Select, SelectItem,
    SelectItemQualifiedWildcardKind, SetExpr, Spanned, Statement, TableAlias, TableFactor, Value,
    Visit, Visitor,
};
use sqlparser::dialect::{ClickHouseDialect, Dialect};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Span;

use super::parser::{AbiType, EventSignature, byte_index_for_location};

#[derive(Clone)]
struct Column {
    name: String,
    ty: Option<AbiType>,
    /// Only a direct event relation exposes the original topic for this column.
    topic: Option<String>,
}

type Columns = Vec<Column>;
type Ctes = HashMap<String, Option<Columns>>;

struct Relation {
    qualifier: Option<Ident>,
    columns: Option<Columns>,
}

struct Rewrite<'a> {
    sql: &'a str,
    signatures: &'a [EventSignature],
    postgres: bool,
    clickhouse: bool,
    edits: Vec<(usize, usize, String)>,
}

pub(super) fn rewrite(
    sql: &str,
    signatures: &[EventSignature],
    dialect: &dyn Dialect,
    postgres: bool,
) -> String {
    let Ok(statements) = Parser::parse_sql(dialect, sql) else {
        return sql.to_string();
    };
    let mut rewrite = Rewrite {
        sql,
        signatures,
        postgres,
        clickhouse: dialect.is::<ClickHouseDialect>(),
        edits: Vec::new(),
    };
    for statement in &statements {
        if let Statement::Query(query) = statement {
            rewrite.query(query, &Ctes::new());
        }
    }
    // Edit tokens in place so comments, quoting and query formatting survive.
    rewrite.edits.sort_unstable_by_key(|edit| edit.0);
    let mut result = sql.to_string();
    for (start, end, replacement) in rewrite.edits.into_iter().rev() {
        result.replace_range(start..end, &replacement);
    }
    result
}

impl Rewrite<'_> {
    fn query(&mut self, query: &Query, inherited: &Ctes) {
        let mut ctes = inherited.clone();
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                // Recursive/self references must never resolve to an event table.
                ctes.insert(key(&cte.alias.name), None);
                self.query(&cte.query, &ctes);
                let columns = self.query_columns(&cte.query, &ctes);
                ctes.insert(key(&cte.alias.name), rename_columns(columns, &cte.alias));
            }
        }
        self.body(&query.body, &ctes, query.order_by.as_ref());
    }

    fn body(&mut self, body: &SetExpr, ctes: &Ctes, order_by: Option<&OrderBy>) {
        match body {
            SetExpr::Select(select) => {
                let relations = self.relations(select, ctes);
                // ClickHouse resolves renamed SELECT aliases in predicates;
                // an unqualified alias must not be mistaken for a source column.
                let aliases = if self.clickhouse {
                    select
                        .projection
                        .iter()
                        .filter_map(|item| match item {
                            SelectItem::ExprWithAlias { expr, alias }
                                if column_ident(expr)
                                    .is_none_or(|column| key(column) != key(alias)) =>
                            {
                                Some(key(alias))
                            }
                            _ => None,
                        })
                        .collect()
                } else {
                    HashSet::new()
                };
                let mut visitor = Comparisons {
                    rewrite: self,
                    relations,
                    aliases,
                    ctes,
                    nested: 0,
                    pushdown: true,
                };
                if matches!(&select.group_by, GroupByExpr::Expressions(exprs, _) if exprs.is_empty())
                {
                    let _ = select.visit(&mut visitor);
                } else {
                    // Only row-level clauses can use raw topics in a grouped query.
                    // Keep the remaining clauses on decoded grouping keys, including
                    // SELECT/HAVING/ORDER BY expressions and GROUP BY expressions.
                    let mut grouped = select.as_ref().clone();
                    let _ = std::mem::take(&mut grouped.from).visit(&mut visitor);
                    let _ = grouped.prewhere.take().visit(&mut visitor);
                    let _ = grouped.selection.take().visit(&mut visitor);
                    visitor.pushdown = false;
                    let _ = grouped.visit(&mut visitor);
                }
                if let Some(order_by) = order_by {
                    let _ = order_by.visit(&mut visitor);
                }
            }
            SetExpr::Query(query) => self.query(query, ctes),
            SetExpr::SetOperation { left, right, .. } => {
                self.body(left, ctes, None);
                self.body(right, ctes, None);
            }
            _ => {}
        }
    }

    fn relations(&self, select: &Select, ctes: &Ctes) -> Vec<Relation> {
        select
            .from
            .iter()
            .flat_map(|from| {
                std::iter::once(&from.relation).chain(from.joins.iter().map(|join| &join.relation))
            })
            .map(|factor| self.relation(factor, ctes))
            .collect()
    }

    fn relation(&self, factor: &TableFactor, ctes: &Ctes) -> Relation {
        let (name, alias, columns) = match factor {
            TableFactor::Table {
                name,
                alias,
                args: None,
                ..
            } if name.0.len() == 1 => {
                let Some(name) = name.0[0].as_ident() else {
                    return Relation {
                        qualifier: None,
                        columns: None,
                    };
                };
                let columns = ctes.get(&key(name)).cloned().unwrap_or_else(|| {
                    self.signatures
                        .iter()
                        .find(|sig| sig.name.eq_ignore_ascii_case(&name.value))
                        .map(event_columns)
                });
                (Some(name.clone()), alias, columns)
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => (None, alias, self.query_columns(subquery, ctes)),
            _ => {
                return Relation {
                    qualifier: None,
                    columns: None,
                };
            }
        };
        Relation {
            qualifier: alias.as_ref().map(|alias| alias.name.clone()).or(name),
            columns: match alias {
                Some(alias) => rename_columns(columns, alias),
                None => columns,
            },
        }
    }

    fn query_columns(&self, query: &Query, inherited: &Ctes) -> Option<Columns> {
        let mut ctes = inherited.clone();
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                ctes.insert(key(&cte.alias.name), None);
                let columns = self.query_columns(&cte.query, &ctes);
                ctes.insert(key(&cte.alias.name), rename_columns(columns, &cte.alias));
            }
        }
        self.body_columns(&query.body, &ctes)
    }

    fn body_columns(&self, body: &SetExpr, ctes: &Ctes) -> Option<Columns> {
        let mut columns = match body {
            SetExpr::Select(select) => {
                let relations = self.relations(select, ctes);
                let mut columns = Vec::new();
                for item in &select.projection {
                    match item {
                        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                            let source = resolve(expr, &relations);
                            let name = match item {
                                SelectItem::ExprWithAlias { alias, .. } => key(alias),
                                _ => column_ident(expr).map_or_else(|| expr.to_string(), key),
                            };
                            columns.push(Column {
                                name,
                                ty: source.and_then(|(_, c)| c.ty.clone()),
                                topic: None,
                            });
                        }
                        SelectItem::Wildcard(options) if plain_wildcard(options) => {
                            for relation in &relations {
                                columns.extend(relation.columns.clone()?);
                            }
                        }
                        SelectItem::QualifiedWildcard(
                            SelectItemQualifiedWildcardKind::ObjectName(name),
                            options,
                        ) if name.0.len() == 1 && plain_wildcard(options) => {
                            let qualifier = key(name.0[0].as_ident()?);
                            columns.extend(
                                relations
                                    .iter()
                                    .find(|r| {
                                        r.qualifier
                                            .as_ref()
                                            .is_some_and(|name| key(name) == qualifier)
                                    })?
                                    .columns
                                    .clone()?,
                            );
                        }
                        _ => return None,
                    }
                }
                columns
            }
            SetExpr::Query(query) => self.query_columns(query, ctes)?,
            SetExpr::SetOperation { left, right, .. } => {
                let mut left = self.body_columns(left, ctes)?;
                let right = self.body_columns(right, ctes)?;
                if left.len() != right.len() {
                    return None;
                }
                for (l, r) in left.iter_mut().zip(right) {
                    if l.ty != r.ty {
                        l.ty = None;
                    }
                }
                left
            }
            _ => return None,
        };
        for column in &mut columns {
            column.topic = None;
        }
        Some(columns)
    }

    fn comparison(
        &mut self,
        column: &Expr,
        literal: &Expr,
        relations: &[Relation],
        pushdown: bool,
    ) {
        let column = unnested(column);
        let literal = unnested(literal);
        let Some((relation, source)) = resolve(column, relations) else {
            return;
        };
        let Some(ty) = &source.ty else {
            return;
        };
        let Expr::Value(value) = literal else {
            return;
        };
        let value = match &value.value {
            Value::SingleQuotedString(s) | Value::Number(s, _) => s,
            _ => return,
        };
        if pushdown
            && let Some(topic) = &source.topic
            && let Some(encoded) = EventSignature::encode_value_for_pushdown(ty, value)
        {
            // Preserve explicit qualifiers and qualify new topic references in joins.
            // A unique decoded column can map to a topic shared by several relations.
            let replacement = match column {
                Expr::CompoundIdentifier(parts) => format!(
                    "{}.{}",
                    parts[..parts.len() - 1]
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join("."),
                    topic
                ),
                _ if relations.len() > 1 => match &relation.qualifier {
                    Some(qualifier) => format!("{qualifier}.{topic}"),
                    None => return,
                },
                _ => topic.clone(),
            };
            self.edit(column.span(), replacement);
            self.edit(literal.span(), format!("'0x{encoded}'"));
        } else if self.postgres
            && matches!(ty, AbiType::Bytes(Some(_)))
            && let Some(encoded) = EventSignature::encode_value_for_pushdown(ty, value)
        {
            let AbiType::Bytes(Some(n)) = ty else {
                unreachable!()
            };
            // A projected/non-indexed bytesN still has bytea type in PostgreSQL.
            // Convert its literal only; string columns containing '0x...' stay text.
            self.edit(
                literal.span(),
                format!("'\\x{}'", &encoded[..2 * usize::from(*n)]),
            );
        }
    }

    fn edit(&mut self, span: Span, replacement: String) {
        if let (Some(start), Some(end)) = (
            byte_index_for_location(self.sql, span.start),
            byte_index_for_location(self.sql, span.end),
        ) && start < end
        {
            self.edits.push((start, end, replacement));
        }
    }
}

struct Comparisons<'a, 'b> {
    rewrite: &'a mut Rewrite<'b>,
    relations: Vec<Relation>,
    aliases: HashSet<String>,
    ctes: &'a Ctes,
    nested: usize,
    pushdown: bool,
}

impl Visitor for Comparisons<'_, '_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        if self.nested == 0 {
            self.rewrite.query(query, self.ctes);
        }
        self.nested += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
        self.nested -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        if self.nested == 0
            && let Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } = expr
        {
            for (column, literal) in [(left, right), (right, left)] {
                if !matches!(unnested(column), Expr::Identifier(ident) if self.aliases.contains(&key(ident)))
                {
                    self.rewrite
                        .comparison(column, literal, &self.relations, self.pushdown);
                }
            }
        }
        ControlFlow::Continue(())
    }
}

fn key(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

fn unnested(mut expr: &Expr) -> &Expr {
    while let Expr::Nested(inner) = expr {
        expr = inner;
    }
    expr
}

fn column_ident(expr: &Expr) -> Option<&Ident> {
    match unnested(expr) {
        Expr::Identifier(ident) => Some(ident),
        Expr::CompoundIdentifier(parts) => parts.last(),
        _ => None,
    }
}

fn resolve<'a>(expr: &Expr, relations: &'a [Relation]) -> Option<(&'a Relation, &'a Column)> {
    let column = key(column_ident(expr)?);
    let candidates = match unnested(expr) {
        Expr::Identifier(_) => {
            // An unknown relation might expose the same name: do not guess.
            if relations.iter().any(|r| r.columns.is_none()) {
                return None;
            }
            relations.iter().collect::<Vec<_>>()
        }
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let qualifier = key(&parts[0]);
            relations
                .iter()
                .filter(|r| {
                    r.qualifier
                        .as_ref()
                        .is_some_and(|name| key(name) == qualifier)
                })
                .collect()
        }
        _ => return None,
    };
    let mut matches = candidates
        .into_iter()
        .flat_map(|relation| {
            relation
                .columns
                .iter()
                .flatten()
                .map(move |column| (relation, column))
        })
        .filter(|(_, c)| c.name == column);
    let result = matches.next()?;
    matches.next().is_none().then_some(result)
}

fn event_columns(sig: &EventSignature) -> Columns {
    let mut columns: Columns = [
        "block_num",
        "block_timestamp",
        "log_idx",
        "tx_idx",
        "tx_hash",
        "address",
        "selector",
        "topic1",
        "topic2",
        "topic3",
        "data",
    ]
    .into_iter()
    .map(|name| Column {
        name: name.into(),
        ty: None,
        topic: None,
    })
    .collect();
    let mut topic = 1;
    for (i, param) in sig.params.iter().enumerate() {
        columns.push(Column {
            name: param.name.clone().unwrap_or_else(|| format!("arg{i}")),
            ty: Some(param.ty.clone()),
            topic: param.indexed.then(|| format!("topic{topic}")),
        });
        if param.indexed {
            topic += 1;
        }
    }
    columns
}

fn rename_columns(mut columns: Option<Columns>, alias: &TableAlias) -> Option<Columns> {
    if let Some(columns) = &mut columns {
        if !alias.columns.is_empty() {
            for column in columns.iter_mut() {
                column.topic = None;
            }
        }
        for (column, alias) in columns.iter_mut().zip(&alias.columns) {
            column.name = key(&alias.name);
        }
    }
    columns
}

fn plain_wildcard(options: &sqlparser::ast::WildcardAdditionalOptions) -> bool {
    options.opt_ilike.is_none()
        && options.opt_exclude.is_none()
        && options.opt_except.is_none()
        && options.opt_replace.is_none()
        && options.opt_rename.is_none()
}

#[cfg(test)]
mod tests {
    use super::super::parser::{
        apply_event_signature_ctes_clickhouse, apply_event_signature_ctes_postgres,
    };
    use super::*;
    use sqlparser::dialect::GenericDialect;

    fn fixed(sql: &str, postgres: bool) -> String {
        rewrite(
            sql,
            &[EventSignature::parse("Fixed(bytes4 indexed tag)").unwrap()],
            &GenericDialect {},
            postgres,
        )
    }

    #[test]
    fn rewrites_every_equality_and_preserves_source_formatting() {
        let sql = "SELECT tag FROM Fixed f WHERE f.tag='0xcafebabe' OR '0xdeadbeef' = f.\"tag\"\nOR tag /* keep */ = '0xcafebabe'";
        let expected = format!(
            "SELECT tag FROM Fixed f WHERE f.topic1='0xcafebabe{}' OR '0xdeadbeef{}' = f.topic1\nOR topic1 /* keep */ = '0xcafebabe{}'",
            "0".repeat(56),
            "0".repeat(56),
            "0".repeat(56)
        );
        assert_eq!(fixed(sql, true), expected);
    }

    #[test]
    fn derived_and_cte_filters_keep_projected_columns() {
        for sql in [
            r#"SELECT * FROM (SELECT "tag" FROM Fixed) q WHERE "tag" = '0xcafebabe'"#,
            r#"WITH q AS (SELECT "tag" FROM Fixed) SELECT * FROM q WHERE "tag" = '0xcafebabe'"#,
            r#"WITH q AS (SELECT tag AS renamed FROM Fixed) SELECT * FROM q WHERE renamed = '0xcafebabe'"#,
            r#"WITH q(renamed) AS (SELECT tag FROM Fixed) SELECT * FROM q WHERE renamed = '0xcafebabe'"#,
            r#"SELECT * FROM (SELECT * FROM Fixed) q WHERE "tag" = '0xcafebabe'"#,
        ] {
            let pg =
                apply_event_signature_ctes_postgres(sql, &["Fixed(bytes4 indexed tag)"]).unwrap();
            let ch =
                apply_event_signature_ctes_clickhouse(sql, &["Fixed(bytes4 indexed tag)"]).unwrap();
            assert!(
                pg.ends_with(
                    sql.replace("'0xcafebabe'", "'\\xcafebabe'")
                        .trim_start_matches("WITH ")
                ),
                "{pg}"
            );
            assert!(ch.ends_with(sql.trim_start_matches("WITH ")), "{ch}");
        }
    }

    #[test]
    fn unrelated_cte_columns_and_string_literals_are_unchanged() {
        let sql = r#"WITH q AS (SELECT '0xcafebabe' AS tag) SELECT q.tag FROM q CROSS JOIN Fixed WHERE q."tag" = '0xcafebabe'"#;
        assert_eq!(fixed(sql, true), sql);
        let sql = r#"SELECT '"tag" = ''0xcafebabe''' FROM Fixed WHERE tag = '0xcafebabe'"#;
        assert!(
            fixed(sql, true)
                .starts_with(r#"SELECT '"tag" = ''0xcafebabe''' FROM Fixed WHERE topic1"#)
        );
    }

    #[test]
    fn signature_order_does_not_change_qualified_filters() {
        let a = EventSignature::parse("A(bytes4 indexed tag)").unwrap();
        let b = EventSignature::parse("B(bytes32 indexed first, bytes4 indexed tag)").unwrap();
        let sql = "SELECT a.tag, b.tag FROM A a JOIN B b ON a.block_num = b.block_num WHERE a.tag = '0xcafebabe' AND b.tag = '0xdeadbeef'";
        let expected = format!(
            "SELECT a.tag, b.tag FROM A a JOIN B b ON a.block_num = b.block_num WHERE a.topic1 = '0xcafebabe{}' AND b.topic2 = '0xdeadbeef{}'",
            "0".repeat(56),
            "0".repeat(56)
        );
        for signatures in [[a.clone(), b.clone()], [b.clone(), a.clone()]] {
            assert_eq!(
                rewrite(sql, &signatures, &GenericDialect {}, true),
                expected
            );
        }
    }

    #[test]
    fn unqualified_join_filters_use_the_resolved_relation() {
        let signatures = [
            EventSignature::parse("A(uint256 indexed amount)").unwrap(),
            EventSignature::parse("B(uint256 indexed other)").unwrap(),
        ];
        for (relation, qualifier) in [
            ("A", "A"),
            ("A a", "a"),
            (r#"A "Event Rows""#, r#""Event Rows""#),
        ] {
            let sql = format!("SELECT amount FROM {relation} CROSS JOIN B WHERE amount = 1");
            let expected = format!(
                "SELECT amount FROM {relation} CROSS JOIN B WHERE {qualifier}.topic1 = '0x{:064x}'",
                1
            );
            assert_eq!(
                rewrite(&sql, &signatures, &GenericDialect {}, true),
                expected
            );
            assert_eq!(
                rewrite(&sql, &signatures, &ClickHouseDialect {}, false),
                expected
            );
        }
    }

    #[test]
    fn postgres_converts_only_typed_fixed_bytes_literals() {
        let signatures = [
            EventSignature::parse("A(bytes4 indexed tag)").unwrap(),
            EventSignature::parse("B(bytes4 tag, string label)").unwrap(),
        ];
        let sql = "SELECT b.tag FROM A a JOIN B b ON a.block_num = b.block_num WHERE b.tag = '0xcafebabe' AND b.label = '0xcafebabe'";
        assert_eq!(
            rewrite(sql, &signatures, &GenericDialect {}, true),
            sql.replacen("b.tag = '0xcafebabe'", "b.tag = '\\xcafebabe'", 1)
        );
        assert_eq!(rewrite(sql, &signatures, &GenericDialect {}, false), sql);
    }

    #[test]
    fn union_arms_and_nested_filters_resolve_independently() {
        let sql = "SELECT tag FROM Fixed WHERE tag = '0xcafebabe' UNION ALL SELECT tag FROM Fixed WHERE tag = '0xdeadbeef'";
        let rewritten = fixed(sql, true);
        assert_eq!(rewritten.matches("WHERE topic1").count(), 2);
        let sql = "SELECT * FROM (SELECT tag FROM Fixed WHERE tag = '0xcafebabe') q WHERE tag = '0xdeadbeef'";
        assert_eq!(
            fixed(sql, true),
            format!(
                "SELECT * FROM (SELECT tag FROM Fixed WHERE topic1 = '0xcafebabe{}') q WHERE tag = '\\xdeadbeef'",
                "0".repeat(56)
            )
        );
    }

    #[test]
    fn clickhouse_projection_alias_does_not_become_a_topic_filter() {
        let signature = EventSignature::parse("Fixed(bytes4 indexed tag, bytes4 value)").unwrap();
        let sql = "SELECT value AS tag FROM Fixed WHERE tag = '0xdeadbeef'";
        assert_eq!(
            rewrite(sql, &[signature], &ClickHouseDialect {}, false),
            sql
        );
    }

    #[test]
    fn parenthesized_operands_keep_their_qualifier() {
        let sql = "SELECT tag FROM Fixed f WHERE (f.tag) = ('0xcafebabe')";
        assert_eq!(
            fixed(sql, true),
            format!(
                "SELECT tag FROM Fixed f WHERE (f.topic1) = ('0xcafebabe{}')",
                "0".repeat(56)
            )
        );
    }

    #[test]
    fn grouped_comparisons_keep_decoded_columns() {
        let signatures = [EventSignature::parse("A(uint256 indexed amount)").unwrap()];
        for sql in [
            "SELECT amount, count(*) FROM A GROUP BY amount HAVING amount = 1",
            "SELECT amount, count(*) FROM A GROUP BY amount ORDER BY amount = 1",
            "SELECT amount = 1, count(*) FROM A GROUP BY amount",
            "SELECT amount = 1, count(*) FROM A GROUP BY amount = 1",
        ] {
            assert_eq!(rewrite(sql, &signatures, &GenericDialect {}, true), sql);
            assert_eq!(rewrite(sql, &signatures, &ClickHouseDialect {}, false), sql);
        }
    }

    #[test]
    fn grouped_fixed_bytes_convert_literals_and_keep_row_filter_pushdown() {
        let sql = "SELECT tag = '0xcafebabe', count(*) FROM Fixed WHERE tag = '0xcafebabe' GROUP BY tag HAVING tag = '0xcafebabe' ORDER BY tag = '0xcafebabe'";
        let expected = sql.replace("'0xcafebabe'", "'\\xcafebabe'").replace(
            "WHERE tag = '\\xcafebabe'",
            &format!("WHERE topic1 = '0xcafebabe{}'", "0".repeat(56)),
        );
        assert_eq!(fixed(sql, true), expected);
        let expected = sql.replace(
            "WHERE tag = '0xcafebabe'",
            &format!("WHERE topic1 = '0xcafebabe{}'", "0".repeat(56)),
        );
        assert_eq!(fixed(sql, false), expected);
    }

    #[test]
    fn grouped_queries_rewrite_nested_filters_in_their_own_scope() {
        let sql = "SELECT tag FROM Fixed GROUP BY tag HAVING tag = '0xcafebabe' AND EXISTS (SELECT 1 FROM Fixed WHERE tag = '0xdeadbeef')";
        assert_eq!(
            fixed(sql, true),
            format!(
                "SELECT tag FROM Fixed GROUP BY tag HAVING tag = '\\xcafebabe' AND EXISTS (SELECT 1 FROM Fixed WHERE topic1 = '0xdeadbeef{}')",
                "0".repeat(56)
            )
        );
    }

    #[test]
    fn order_by_comparisons_use_the_select_scope() {
        let sql = "SELECT tag FROM Fixed f ORDER BY f.tag = '0xcafebabe'";
        assert_eq!(
            fixed(sql, true),
            format!(
                "SELECT tag FROM Fixed f ORDER BY f.topic1 = '0xcafebabe{}'",
                "0".repeat(56)
            )
        );
    }
}
