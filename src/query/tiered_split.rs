//! Tiered fast-path query splitting.
//!
//! The tiered engine's default path queries `tiered.*` UNION ALL views where
//! the cold arm is a pg_clickhouse foreign table. The FDW cannot push
//! ORDER BY/LIMIT or bytea-normalized equality filters down to ClickHouse,
//! so head pages and point lookups degrade badly.
//!
//! This module detects query shapes that can instead run as two independent
//! native queries — hot on PostgreSQL `public.*`, cold on ClickHouse — split
//! at the prune boundary `B` (`sync_state.pruned_below`):
//!
//! - hot arm:  original query AND `boundary_col > B`
//! - cold arm: original query AND `boundary_col <= B`
//!
//! Stitching hot+cold row sets is only valid when the arms partition the
//! result rows and the ORDER BY (if any) sorts every hot row before every
//! cold row (or vice versa). That holds when the *first* sort key is the
//! boundary column itself: hot keys live in `(B, ∞)`, cold keys in `[0, B]`,
//! so the primary comparison alone decides cross-arm order. Joins stay
//! arm-local when the join key ties rows to a single block (`tx_hash`
//! equality, or block-number equality).
//!
//! Anything not provably safe returns `None` and falls back to the FDW view
//! path unchanged.

use std::collections::HashSet;
use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, Ident, JoinConstraint, JoinOperator, LimitClause,
    ObjectNamePart, OrderByKind, Query, Select, SelectFlavor, SelectItem, SetExpr, Statement,
    TableFactor, Value, visit_expressions,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Core tables with a physical block-number column in both stores.
const CORE_TABLES: [&str; 4] = ["blocks", "txs", "logs", "receipts"];

/// A table reference participating in the split.
struct SplitRelation {
    /// Identifier used to qualify the injected boundary predicate.
    /// `None` for a single unaliased table (inject unqualified).
    qualifier: Option<Ident>,
    /// Lowercased table name (for ORDER BY qualifier matching).
    name_lc: String,
    /// Lowercased alias, if any.
    alias_lc: Option<String>,
    /// Block-number column partitioning this relation across tiers.
    boundary_col: &'static str,
}

/// An eligible query, ready to render per-tier arm SQL.
pub struct TieredSplit {
    stmt: Statement,
    relations: Vec<SplitRelation>,
    /// `true` when the first ORDER BY key is ascending: cold rows sort first.
    pub cold_leads: bool,
    /// Literal `LIMIT n` from the query, if present.
    pub sql_limit: Option<i64>,
}

/// Analyze `sql` for tiered fast-path eligibility.
///
/// `event_tables` holds lowercased event names from the request's
/// `signature` params (their CTE tables expose `block_num`).
pub fn plan_tiered_split(sql: &str, event_tables: &HashSet<String>) -> Option<TieredSplit> {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    if statements.len() != 1 {
        return None;
    }
    let stmt = statements.pop()?;
    let Statement::Query(query) = &stmt else {
        return None;
    };

    // Reject functions (aggregates/windows) and subqueries anywhere: splitting
    // changes their inputs. Plain scalar expressions are row-local and safe.
    let blocked = visit_expressions(query, |e| match e {
        Expr::Function(_)
        | Expr::Subquery(_)
        | Expr::InSubquery { .. }
        | Expr::Exists { .. }
        | Expr::AnyOp { .. }
        | Expr::AllOp { .. } => ControlFlow::Break(()),
        _ => ControlFlow::Continue(()),
    });
    if blocked.is_break() {
        return None;
    }

    // Query-level clauses that change row multiplicity/order semantics.
    if query.with.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return None;
    }

    let sql_limit = match &query.limit_clause {
        None => None,
        Some(LimitClause::LimitOffset {
            limit: Some(Expr::Value(v)),
            offset: None,
            limit_by,
        }) if limit_by.is_empty() => match &v.value {
            Value::Number(n, _) => Some(n.parse::<i64>().ok().filter(|n| *n >= 0)?),
            _ => return None,
        },
        // OFFSET / LIMIT ALL / MySQL comma form: stitching would be wrong.
        Some(_) => return None,
    };

    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if !select_shape_eligible(select) {
        return None;
    }

    let relations = extract_relations(select, event_tables)?;
    // No ORDER BY (inner None): any row order is valid; serve hot first.
    let cold_leads = order_direction(query, &relations)?.unwrap_or(false);

    Some(TieredSplit {
        stmt,
        relations,
        cold_leads,
        sql_limit,
    })
}

impl TieredSplit {
    /// Render one arm: the original query with the tier's boundary predicate
    /// injected (and, optionally, its LIMIT replaced).
    pub fn arm_sql(&self, hot: bool, boundary: i64, limit_override: Option<i64>) -> String {
        let mut stmt = self.stmt.clone();
        let Statement::Query(query) = &mut stmt else {
            unreachable!("plan_tiered_split only accepts Statement::Query");
        };
        let SetExpr::Select(select) = query.body.as_mut() else {
            unreachable!("plan_tiered_split only accepts a bare SELECT body");
        };

        let mut pred: Option<Expr> = None;
        for rel in &self.relations {
            let col = match &rel.qualifier {
                Some(q) => Expr::CompoundIdentifier(vec![q.clone(), Ident::new(rel.boundary_col)]),
                None => Expr::Identifier(Ident::new(rel.boundary_col)),
            };
            let cmp = Expr::BinaryOp {
                left: Box::new(col),
                op: if hot {
                    BinaryOperator::Gt
                } else {
                    BinaryOperator::LtEq
                },
                right: Box::new(number(boundary)),
            };
            pred = Some(match pred {
                None => cmp,
                Some(acc) => and(acc, cmp),
            });
        }
        let pred = pred.expect("split plans always have at least one relation");

        select.selection = Some(match select.selection.take() {
            // Parenthesize: Display renders the AST verbatim, so an unwrapped
            // `a OR b` would re-associate under the injected AND.
            Some(existing) => and(Expr::Nested(Box::new(existing)), pred),
            None => pred,
        });

        if let Some(n) = limit_override {
            query.limit_clause = Some(LimitClause::LimitOffset {
                limit: Some(number(n)),
                offset: None,
                limit_by: vec![],
            });
        }

        stmt.to_string()
    }
}

fn and(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::And,
        right: Box::new(right),
    }
}

fn number(n: i64) -> Expr {
    Expr::value(Value::Number(n.to_string(), false))
}

/// SELECT-level shape gate: plain projection over plain FROM.
fn select_shape_eligible(select: &Select) -> bool {
    if select.distinct.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || select.connect_by.is_some()
        || select.flavor != SelectFlavor::Standard
    {
        return false;
    }
    match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) if exprs.is_empty() && modifiers.is_empty() => {}
        _ => return false,
    }
    // Explicit columns only: `*` column order may differ between stores.
    select.projection.iter().all(|item| {
        matches!(
            item,
            SelectItem::UnnamedExpr(Expr::Identifier(_) | Expr::CompoundIdentifier(_))
                | SelectItem::ExprWithAlias {
                    expr: Expr::Identifier(_) | Expr::CompoundIdentifier(_),
                    ..
                }
        )
    })
}

/// Extract the FROM relations if they form an eligible single table or a
/// single arm-local INNER JOIN of core tables.
fn extract_relations(
    select: &Select,
    event_tables: &HashSet<String>,
) -> Option<Vec<SplitRelation>> {
    if select.from.len() != 1 {
        return None;
    }
    let twj = &select.from[0];
    let (name, alias) = plain_table(&twj.relation)?;
    let name_lc = name.value.to_lowercase();
    let boundary = boundary_col(&name_lc, event_tables)?;

    if twj.joins.is_empty() {
        // Only qualify when aliased (an alias makes the bare name invalid).
        return Some(vec![SplitRelation {
            qualifier: alias.clone(),
            name_lc,
            alias_lc: alias.as_ref().map(|a| a.value.to_lowercase()),
            boundary_col: boundary,
        }]);
    }

    // Join case: exactly one INNER JOIN of core tables, tied per-block.
    if twj.joins.len() != 1 {
        return None;
    }
    if !CORE_TABLES.contains(&name_lc.as_str()) {
        return None;
    }
    let join = &twj.joins[0];
    let constraint = match &join.join_operator {
        JoinOperator::Inner(c) | JoinOperator::Join(c) => c,
        _ => return None,
    };
    let JoinConstraint::On(on) = constraint else {
        return None;
    };
    let (name2, alias2) = plain_table(&join.relation)?;
    let name2_lc = name2.value.to_lowercase();
    if !CORE_TABLES.contains(&name2_lc.as_str()) {
        return None;
    }
    let boundary2 = boundary_col(&name2_lc, event_tables)?;

    let rel1 = SplitRelation {
        qualifier: Some(alias.clone().unwrap_or(name)),
        name_lc,
        alias_lc: alias.as_ref().map(|a| a.value.to_lowercase()),
        boundary_col: boundary,
    };
    let rel2 = SplitRelation {
        qualifier: Some(alias2.clone().unwrap_or(name2)),
        name_lc: name2_lc,
        alias_lc: alias2.as_ref().map(|a| a.value.to_lowercase()),
        boundary_col: boundary2,
    };
    // Ambiguous qualifiers would misdirect the injected predicates.
    if effective_qualifier(&rel1) == effective_qualifier(&rel2) {
        return None;
    }
    if !join_is_arm_local(on, &rel1, &rel2) {
        return None;
    }
    Some(vec![rel1, rel2])
}

fn effective_qualifier(rel: &SplitRelation) -> &str {
    rel.alias_lc.as_deref().unwrap_or(&rel.name_lc)
}

/// Bare single-part table name with an optional simple alias.
fn plain_table(factor: &TableFactor) -> Option<(Ident, Option<Ident>)> {
    let TableFactor::Table {
        name,
        alias,
        args: None,
        with_hints,
        version: None,
        with_ordinality: false,
        partitions,
        json_path: None,
        sample: None,
        index_hints,
    } = factor
    else {
        return None;
    };
    if !with_hints.is_empty() || !partitions.is_empty() || !index_hints.is_empty() {
        return None;
    }
    // Schema-qualified names (e.g. `public.blocks`) intentionally bypass the
    // tiered views today; preserve that by falling back.
    let [ObjectNamePart::Identifier(ident)] = name.0.as_slice() else {
        return None;
    };
    let alias = match alias {
        None => None,
        Some(a) if a.columns.is_empty() => Some(a.name.clone()),
        Some(_) => return None,
    };
    Some((ident.clone(), alias))
}

fn boundary_col(table_lc: &str, event_tables: &HashSet<String>) -> Option<&'static str> {
    match table_lc {
        "blocks" => Some("num"),
        "txs" | "logs" | "receipts" => Some("block_num"),
        _ if event_tables.contains(table_lc) => Some("block_num"),
        _ => None,
    }
}

/// Whether the ON clause guarantees joined rows share a tier: some top-level
/// AND conjunct equates `tx_hash` across both relations (a tx hash lives in
/// exactly one block) or equates the two boundary columns.
fn join_is_arm_local(on: &Expr, rel1: &SplitRelation, rel2: &SplitRelation) -> bool {
    let mut conjuncts = Vec::new();
    collect_conjuncts(on, &mut conjuncts);
    conjuncts.iter().any(|c| {
        let Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } = c
        else {
            return false;
        };
        let Some((q_l, col_l)) = qualified_column(left) else {
            return false;
        };
        let Some((q_r, col_r)) = qualified_column(right) else {
            return false;
        };
        let (side_l, side_r) = match (
            match_relation(&q_l, rel1, rel2),
            match_relation(&q_r, rel1, rel2),
        ) {
            (Some(a), Some(b)) if a != b => (a, b),
            _ => return false,
        };
        let (rel_l, rel_r) = if side_l == 0 {
            (rel1, rel2)
        } else {
            (rel2, rel1)
        };
        let _ = side_r;
        (col_l == "tx_hash"
            && col_r == "tx_hash"
            && rel_l.name_lc != "blocks"
            && rel_r.name_lc != "blocks")
            || (col_l == rel_l.boundary_col && col_r == rel_r.boundary_col)
    })
}

fn collect_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_conjuncts(left, out);
            collect_conjuncts(right, out);
        }
        Expr::Nested(inner) => collect_conjuncts(inner, out),
        other => out.push(other),
    }
}

/// `qualifier.column` as lowercased strings.
fn qualified_column(expr: &Expr) -> Option<(String, String)> {
    match expr {
        Expr::CompoundIdentifier(parts) => match parts.as_slice() {
            [q, c] => Some((q.value.to_lowercase(), c.value.to_lowercase())),
            _ => None,
        },
        Expr::Nested(inner) => qualified_column(inner),
        _ => None,
    }
}

/// Which relation (0 or 1) a qualifier refers to.
fn match_relation(qualifier: &str, rel1: &SplitRelation, rel2: &SplitRelation) -> Option<usize> {
    if qualifier == effective_qualifier(rel1) {
        Some(0)
    } else if qualifier == effective_qualifier(rel2) {
        Some(1)
    } else {
        None
    }
}

/// Validate the ORDER BY and return `Some(asc)` of the first key, or `None`
/// (inner) when there is no ORDER BY. Outer `None` = ineligible.
#[allow(clippy::option_option)]
fn order_direction(query: &Query, relations: &[SplitRelation]) -> Option<Option<bool>> {
    let Some(order_by) = &query.order_by else {
        return Some(None);
    };
    if order_by.interpolate.is_some() {
        return None;
    }
    let OrderByKind::Expressions(exprs) = &order_by.kind else {
        return None;
    };
    let Some(first) = exprs.first() else {
        return Some(None);
    };
    if exprs.iter().any(|e| e.with_fill.is_some()) {
        return None;
    }
    if first.options.nulls_first.is_some() {
        return None;
    }

    // First key must be the boundary column of exactly one relation, so the
    // primary comparison alone orders hot rows against cold rows.
    let (qualifier, col) = match &first.expr {
        Expr::Identifier(id) => (None, id.value.to_lowercase()),
        Expr::CompoundIdentifier(parts) => match parts.as_slice() {
            [q, c] => (Some(q.value.to_lowercase()), c.value.to_lowercase()),
            _ => return None,
        },
        _ => return None,
    };
    let matched = relations
        .iter()
        .filter(|rel| {
            rel.boundary_col == col
                && qualifier
                    .as_deref()
                    .is_none_or(|q| q == effective_qualifier(rel))
        })
        .count();
    if matched != 1 {
        return None;
    }

    Some(Some(first.options.asc.unwrap_or(true)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(sql: &str) -> Option<TieredSplit> {
        plan_tiered_split(sql, &HashSet::new())
    }

    fn plan_ev(sql: &str, events: &[&str]) -> Option<TieredSplit> {
        let set = events.iter().map(|s| s.to_lowercase()).collect();
        plan_tiered_split(sql, &set)
    }

    // ── Eligibility: accepted shapes ───────────────────────────────────────

    #[test]
    fn head_page_desc() {
        let p = plan("SELECT hash, num, timestamp FROM blocks ORDER BY num DESC LIMIT 11").unwrap();
        assert!(!p.cold_leads);
        assert_eq!(p.sql_limit, Some(11));
    }

    #[test]
    fn head_page_asc_leads_cold() {
        let p = plan("SELECT num FROM blocks ORDER BY num ASC LIMIT 5").unwrap();
        assert!(p.cold_leads);
    }

    #[test]
    fn implicit_asc_leads_cold() {
        let p = plan("SELECT num FROM blocks ORDER BY num LIMIT 5").unwrap();
        assert!(p.cold_leads);
    }

    #[test]
    fn point_lookup_no_order() {
        let p =
            plan("SELECT block_num, tx_idx FROM receipts WHERE tx_hash = '0xab' LIMIT 1").unwrap();
        assert!(!p.cold_leads);
        assert_eq!(p.sql_limit, Some(1));
    }

    #[test]
    fn no_limit_is_eligible() {
        let p = plan("SELECT num FROM blocks ORDER BY num DESC").unwrap();
        assert_eq!(p.sql_limit, None);
    }

    #[test]
    fn secondary_order_keys_allowed() {
        assert!(
            plan("SELECT block_num FROM receipts ORDER BY block_num DESC, tx_idx DESC LIMIT 11")
                .is_some()
        );
    }

    #[test]
    fn quoted_columns_and_filters() {
        assert!(plan(
            "SELECT \"from\", \"to\" FROM txs WHERE \"from\" = '0xab' ORDER BY block_num DESC LIMIT 11"
        )
        .is_some());
    }

    #[test]
    fn event_table_with_signature() {
        let p = plan_ev(
            "SELECT \"from\", value, block_num FROM Transfer ORDER BY block_num DESC LIMIT 11",
            &["Transfer"],
        )
        .unwrap();
        assert_eq!(p.sql_limit, Some(11));
    }

    #[test]
    fn event_table_without_signature_rejected() {
        assert!(plan("SELECT value FROM Transfer ORDER BY block_num DESC LIMIT 1").is_none());
    }

    #[test]
    fn tx_hash_join_accepted() {
        let p = plan(
            "SELECT receipts.tx_hash, logs.data FROM receipts \
             INNER JOIN logs ON logs.tx_hash = receipts.tx_hash \
             WHERE receipts.\"from\" = '0xab' \
             ORDER BY receipts.block_num DESC, receipts.tx_idx DESC LIMIT 80",
        )
        .unwrap();
        assert!(!p.cold_leads);
    }

    #[test]
    fn block_num_join_accepted() {
        assert!(
            plan(
                "SELECT b.num, t.hash FROM blocks b JOIN txs t ON b.num = t.block_num \
             ORDER BY b.num DESC LIMIT 10"
            )
            .is_some()
        );
    }

    #[test]
    fn aliased_single_table() {
        assert!(plan("SELECT b.num FROM blocks b ORDER BY b.num DESC LIMIT 1").is_some());
    }

    // ── Eligibility: rejected shapes ───────────────────────────────────────

    #[test]
    fn rejects_unknown_table() {
        assert!(plan("SELECT chain_id FROM sync_state LIMIT 1").is_none());
    }

    #[test]
    fn rejects_aggregates_and_functions() {
        assert!(plan("SELECT count(*) FROM blocks").is_none());
        assert!(plan("SELECT max(num) FROM blocks").is_none());
        assert!(plan("SELECT length(input) FROM txs LIMIT 1").is_none());
    }

    #[test]
    fn rejects_group_by_distinct_having() {
        assert!(plan("SELECT num FROM blocks GROUP BY num LIMIT 1").is_none());
        assert!(plan("SELECT DISTINCT \"from\" FROM txs LIMIT 1").is_none());
    }

    #[test]
    fn rejects_offset_but_allows_limit_all() {
        assert!(plan("SELECT num FROM blocks ORDER BY num DESC LIMIT 10 OFFSET 5").is_none());
        // sqlparser normalizes LIMIT ALL to no limit clause; equivalent to no LIMIT.
        let p = plan("SELECT num FROM blocks LIMIT ALL").unwrap();
        assert_eq!(p.sql_limit, None);
    }

    #[test]
    fn rejects_order_by_non_boundary_column() {
        assert!(plan("SELECT num FROM blocks ORDER BY timestamp DESC LIMIT 1").is_none());
        assert!(plan("SELECT hash FROM txs ORDER BY nonce DESC LIMIT 1").is_none());
    }

    #[test]
    fn rejects_order_by_secondary_boundary_position() {
        // Boundary col must be the FIRST key.
        assert!(plan("SELECT num FROM blocks ORDER BY timestamp DESC, num DESC LIMIT 1").is_none());
    }

    #[test]
    fn rejects_subqueries_and_set_ops() {
        assert!(plan("SELECT num FROM blocks WHERE num IN (SELECT block_num FROM txs)").is_none());
        assert!(plan("SELECT num FROM blocks UNION ALL SELECT block_num FROM txs").is_none());
    }

    #[test]
    fn rejects_wildcard_and_schema_qualified() {
        assert!(plan("SELECT * FROM blocks ORDER BY num DESC LIMIT 1").is_none());
        assert!(plan("SELECT num FROM public.blocks ORDER BY num DESC LIMIT 1").is_none());
    }

    #[test]
    fn rejects_cte_and_multi_statement() {
        assert!(plan("WITH b AS (SELECT num FROM blocks) SELECT num FROM b LIMIT 1").is_none());
        assert!(plan("SELECT num FROM blocks; SELECT num FROM blocks").is_none());
    }

    #[test]
    fn rejects_non_arm_local_join() {
        // Equality on a non-linking column can pair rows across tiers.
        assert!(
            plan(
                "SELECT r.tx_hash FROM receipts r JOIN logs l ON r.\"from\" = l.address \
             ORDER BY r.block_num DESC LIMIT 10"
            )
            .is_none()
        );
        // LEFT JOIN could emit unmatched hot rows whose partner is cold.
        assert!(
            plan(
                "SELECT r.tx_hash FROM receipts r LEFT JOIN logs l ON l.tx_hash = r.tx_hash \
             ORDER BY r.block_num DESC LIMIT 10"
            )
            .is_none()
        );
    }

    #[test]
    fn rejects_ambiguous_join_order_key() {
        // Both relations expose block_num; unqualified first key is ambiguous.
        assert!(
            plan(
                "SELECT r.tx_hash FROM receipts r JOIN logs l ON l.tx_hash = r.tx_hash \
             ORDER BY block_num DESC LIMIT 10"
            )
            .is_none()
        );
    }

    // ── Arm SQL rendering ──────────────────────────────────────────────────

    #[test]
    fn renders_hot_and_cold_arms() {
        let p = plan("SELECT hash, num FROM blocks ORDER BY num DESC LIMIT 11").unwrap();
        assert_eq!(
            p.arm_sql(true, 1000, None),
            "SELECT hash, num FROM blocks WHERE num > 1000 ORDER BY num DESC LIMIT 11"
        );
        assert_eq!(
            p.arm_sql(false, 1000, Some(4)),
            "SELECT hash, num FROM blocks WHERE num <= 1000 ORDER BY num DESC LIMIT 4"
        );
    }

    #[test]
    fn parenthesizes_existing_or_filter() {
        let p = plan("SELECT num FROM blocks WHERE num = 1 OR num = 2 LIMIT 5").unwrap();
        assert_eq!(
            p.arm_sql(true, 100, None),
            "SELECT num FROM blocks WHERE (num = 1 OR num = 2) AND num > 100 LIMIT 5"
        );
    }

    #[test]
    fn renders_join_predicates_for_both_relations() {
        let p = plan(
            "SELECT receipts.tx_hash FROM receipts JOIN logs ON logs.tx_hash = receipts.tx_hash \
             WHERE receipts.\"from\" = '0xab' ORDER BY receipts.block_num DESC LIMIT 80",
        )
        .unwrap();
        assert_eq!(
            p.arm_sql(false, 500, Some(10)),
            "SELECT receipts.tx_hash FROM receipts JOIN logs ON logs.tx_hash = receipts.tx_hash \
             WHERE (receipts.\"from\" = '0xab') AND receipts.block_num <= 500 AND logs.block_num <= 500 \
             ORDER BY receipts.block_num DESC LIMIT 10"
        );
    }

    #[test]
    fn renders_aliased_qualifiers() {
        let p = plan("SELECT b.num FROM blocks b ORDER BY b.num DESC LIMIT 1").unwrap();
        assert_eq!(
            p.arm_sql(true, 7, None),
            "SELECT b.num FROM blocks b WHERE b.num > 7 ORDER BY b.num DESC LIMIT 1"
        );
    }
}
