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
//! Because the two arms run on different engines, eligibility also requires
//! that both engines resolve identifiers identically and return the same
//! value representations:
//!
//! - unquoted identifiers must be lowercase (PostgreSQL case-folds, ClickHouse
//!   does not);
//! - projection aliases that *rename* a column must not be referenced
//!   unqualified elsewhere (ClickHouse resolves aliases in WHERE, PostgreSQL
//!   does not);
//! - projected output names must be distinct (ClickHouse renames duplicates,
//!   e.g. `hash, txs.hash`, where PostgreSQL repeats them);
//! - WHERE/ON predicates must fit the conservative grammar of
//!   [`filter_pred_safe`] so both engines compute identical row sets;
//! - secondary ORDER BY keys come from a per-table allowlist of NOT NULL
//!   numeric/time columns that sort identically in both stores;
//! - event tables are ineligible when the signature decodes `bool` (PG
//!   boolean vs CH UInt8) or non-indexed `string` (PG decodes text, CH
//!   returns hex) parameters;
//! - `logs.is_virtual_forward` is ineligible (PG boolean vs CH UInt8).
//!
//! Anything not provably safe returns `None` and falls back to the FDW view
//! path unchanged.

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, Ident, JoinConstraint, JoinOperator, LimitClause,
    ObjectNamePart, OrderByKind, Query, Select, SelectFlavor, SelectItem, SetExpr, Statement,
    TableFactor, UnaryOperator, Value, visit_expressions,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use super::parser::{AbiType, EventSignature};

/// Core tables with a physical block-number column in both stores.
const CORE_TABLES: [&str; 4] = ["blocks", "txs", "logs", "receipts"];

/// How a column's values serialize across the hot (PostgreSQL) and cold
/// (ClickHouse) arms. Kinds gate where a column may appear so the stitched
/// result is representation-identical to plain PostgreSQL.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ColKind {
    /// ≤64-bit integers: JSON numbers on both arms.
    Num,
    /// Decoded uint/int event params: PG NUMERIC and CH UInt256 as decimal
    /// strings; cold normalization nulls values past rust_decimal's range,
    /// matching PG's formatter.
    NumText,
    /// timestamptz vs DateTime64: RFC 3339 strings after cold normalization.
    Ts,
    /// PG BYTEA vs CH lowercase '0x…' String: values compare equal only for
    /// canonical lowercase hex literals of 40+ digits (the form
    /// `convert_hex_literals_postgres` rewrites for the hot arm).
    Hex,
    /// Plain text on both arms.
    Text,
    /// `logs.selector`: PG NULL is stored as '' in ClickHouse. Allowed only
    /// as a bare projection; the cold arm's '' cells are rewritten to NULL.
    Selector,
    /// Divergent representations (JSONB vs String, bool vs UInt8).
    Blocked,
}

use ColKind::{Blocked, Hex, Num, NumText, Selector, Text, Ts};

/// Per-table column kinds, mirroring `db/*.sql` against `db/clickhouse/*.sql`.
/// Columns absent here (e.g. ClickHouse-only `receipts.type`/`fee_token`)
/// don't resolve, making any query that references them ineligible.
fn core_columns(table: &str) -> Option<&'static [(&'static str, ColKind)]> {
    match table {
        "blocks" => Some(&[
            ("num", Num),
            ("hash", Hex),
            ("parent_hash", Hex),
            ("timestamp", Ts),
            ("timestamp_ms", Num),
            ("gas_limit", Num),
            ("gas_used", Num),
            ("miner", Hex),
            ("extra_data", Hex),
            ("consensus_proposer", Hex),
        ]),
        "txs" => Some(&[
            ("block_num", Num),
            ("block_timestamp", Ts),
            ("idx", Num),
            ("hash", Hex),
            ("type", Num),
            ("from", Hex),
            ("to", Hex),
            ("value", Text),
            ("input", Hex),
            ("gas_limit", Num),
            ("max_fee_per_gas", Text),
            ("max_priority_fee_per_gas", Text),
            ("gas_used", Num),
            ("nonce_key", Hex),
            ("nonce", Num),
            ("fee_token", Hex),
            ("fee_payer", Hex),
            // JSONB in PG vs String in ClickHouse.
            ("calls", Blocked),
            ("call_count", Num),
            ("valid_before", Num),
            ("valid_after", Num),
            ("signature_type", Num),
        ]),
        "logs" => Some(&[
            ("block_num", Num),
            ("block_timestamp", Ts),
            ("log_idx", Num),
            ("tx_idx", Num),
            ("tx_hash", Hex),
            ("address", Hex),
            ("selector", Selector),
            ("topic0", Hex),
            ("topic1", Hex),
            ("topic2", Hex),
            ("topic3", Hex),
            ("data", Hex),
            // BOOLEAN in PG vs UInt8 in ClickHouse.
            ("is_virtual_forward", Blocked),
        ]),
        "receipts" => Some(&[
            ("block_num", Num),
            ("block_timestamp", Ts),
            ("tx_idx", Num),
            ("tx_hash", Hex),
            ("from", Hex),
            ("to", Hex),
            ("contract_address", Hex),
            ("gas_used", Num),
            ("cumulative_gas_used", Num),
            ("effective_gas_price", Text),
            ("status", Num),
            ("fee_payer", Hex),
        ]),
        _ => None,
    }
}

/// Raw columns every event CTE projects (both engines, identical names).
/// `selector` is `Hex` here, not `Selector`: the CTE filters
/// `selector = <topic0>`, so it is never NULL/'' on either arm.
const EVENT_META_COLUMNS: &[(&str, ColKind)] = &[
    ("block_num", Num),
    ("block_timestamp", Ts),
    ("log_idx", Num),
    ("tx_idx", Num),
    ("tx_hash", Hex),
    ("address", Hex),
    ("selector", Hex),
    ("topic1", Hex),
    ("topic2", Hex),
    ("topic3", Hex),
    ("data", Hex),
];

/// Column map for an event CTE, or `None` when any decoded param diverges
/// across engines: `bool` renders PG boolean vs CH UInt8, non-indexed
/// `string` PG text vs CH hex, and composite types are unsupported.
fn event_columns(sig: &EventSignature) -> Option<HashMap<String, ColKind>> {
    let mut map: HashMap<String, ColKind> = EVENT_META_COLUMNS
        .iter()
        .map(|(c, k)| ((*c).to_string(), *k))
        .collect();
    for (i, p) in sig.params.iter().enumerate() {
        let kind = match &p.ty {
            AbiType::Address | AbiType::Bytes(_) => Hex,
            AbiType::String if p.indexed => Hex,
            AbiType::Uint(_) | AbiType::Int(_) => NumText,
            _ => return None,
        };
        let name = p
            .name
            .as_deref()
            .map_or_else(|| format!("arg{i}"), str::to_lowercase);
        // A param shadowing a raw CTE column is ambiguous in both engines.
        if map.insert(name, kind).is_some() {
            return None;
        }
    }
    Some(map)
}

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
    /// Lowercased column name → representation kind.
    columns: HashMap<String, ColKind>,
}

/// An eligible query, ready to render per-tier arm SQL.
pub struct TieredSplit {
    stmt: Statement,
    relations: Vec<SplitRelation>,
    /// `true` when the first ORDER BY key is ascending: cold rows sort first.
    pub cold_leads: bool,
    /// Literal `LIMIT n` from the query, if present.
    pub sql_limit: Option<i64>,
    /// Projection indexes of `Selector`-kind columns: the cold arm rewrites
    /// their '' cells to NULL (ClickHouse stores PG NULL selectors as '').
    pub selector_null_cols: Vec<usize>,
}

/// Analyze `sql` for tiered fast-path eligibility.
///
/// `events` holds the parsed event signatures from the request's
/// `signature` params (their CTE tables expose `block_num`).
pub fn plan_tiered_split(sql: &str, events: &[EventSignature]) -> Option<TieredSplit> {
    // Mixed-case hex literals decode case-insensitively as PG bytea but
    // compare case-sensitively against ClickHouse's lowercase hex strings,
    // silently dropping cold-arm matches.
    if super::has_mixed_case_hex_literal(sql) {
        return None;
    }
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    if statements.len() != 1 {
        return None;
    }
    let stmt = statements.pop()?;
    let Statement::Query(query) = &stmt else {
        return None;
    };

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

    // Projection aliases that rename a column: ClickHouse resolves them in
    // WHERE/ORDER BY where PostgreSQL uses the source column, so any
    // unqualified reference to one is ambiguous across engines.
    let renamed = renamed_aliases(select);

    // Alias idents aren't Exprs, so visit_expressions misses them: unquoted
    // uppercase aliases case-fold in PG result columns but not ClickHouse.
    for item in &select.projection {
        if let SelectItem::ExprWithAlias { alias, .. } = item
            && !ident_case_ok(alias)
        {
            return None;
        }
    }

    // Reject, anywhere in the query:
    // - functions (aggregates/windows) and subqueries: splitting changes
    //   their inputs (plain scalar expressions are row-local and safe);
    // - unquoted identifiers with uppercase letters: PostgreSQL case-folds,
    //   ClickHouse does not;
    // - unqualified references to renaming projection aliases.
    let blocked = visit_expressions(query, |e| match e {
        Expr::Function(_)
        | Expr::Subquery(_)
        | Expr::InSubquery { .. }
        | Expr::Exists { .. }
        | Expr::AnyOp { .. }
        | Expr::AllOp { .. } => ControlFlow::Break(()),
        Expr::Identifier(id) => {
            if !ident_case_ok(id) || renamed.contains(&id.value.to_lowercase()) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
        Expr::CompoundIdentifier(parts) => {
            if parts.iter().all(ident_case_ok) {
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break(())
            }
        }
        _ => ControlFlow::Continue(()),
    });
    if blocked.is_break() {
        return None;
    }

    let event_map: HashMap<String, &EventSignature> = events
        .iter()
        .map(|sig| (sig.name.to_lowercase(), sig))
        .collect();
    let relations = extract_relations(select, &event_map)?;

    // Typed resolution: every referenced column must resolve to exactly one
    // relation and carry a hot/cold-identical representation.
    let selector_null_cols = resolve_projection(select, &relations)?;
    let mut filter_exprs: Vec<&Expr> = select.selection.iter().collect();
    for join in &select.from[0].joins {
        if let JoinOperator::Inner(JoinConstraint::On(on))
        | JoinOperator::Join(JoinConstraint::On(on)) = &join.join_operator
        {
            filter_exprs.push(on);
        }
    }
    if !filter_exprs.iter().all(|e| filter_pred_safe(e, &relations)) {
        return None;
    }

    // No ORDER BY (inner None): any row order is valid; serve hot first.
    let cold_leads = order_direction(query, &relations)?.unwrap_or(false);

    Some(TieredSplit {
        stmt,
        relations,
        cold_leads,
        sql_limit,
        selector_null_cols,
    })
}

/// Resolve every projected column; reject `Blocked` kinds, unresolvable
/// references, and duplicate output names (PostgreSQL repeats them,
/// ClickHouse renames the duplicate, e.g. `hash, txs.hash`). Returns
/// projection indexes needing the cold-arm '' → NULL selector rewrite.
fn resolve_projection(select: &Select, relations: &[SplitRelation]) -> Option<Vec<usize>> {
    let mut selector_cols = Vec::new();
    let mut names = HashSet::new();
    for (i, item) in select.projection.iter().enumerate() {
        let (expr, name) = match item {
            SelectItem::UnnamedExpr(expr) => {
                let name = match expr {
                    Expr::Identifier(id) => &id.value,
                    Expr::CompoundIdentifier(parts) => &parts.last()?.value,
                    _ => return None,
                };
                (expr, name)
            }
            SelectItem::ExprWithAlias { expr, alias } => (expr, &alias.value),
            _ => return None,
        };
        if !names.insert(name.clone()) {
            return None;
        }
        let (qualifier, col) = column_parts(expr)?;
        match resolve_kind(relations, qualifier.as_deref(), &col)? {
            Blocked => return None,
            Selector => selector_cols.push(i),
            _ => {}
        }
    }
    Some(selector_cols)
}

/// Filter predicates (WHERE / join ON) must produce identical row sets on
/// both engines, so only a conservative grammar is accepted:
///
/// - `AND` / `OR` / `NOT` / parentheses;
/// - `col = col` / `col != col` between same-kind columns (join keys);
/// - `col <op> literal` where the literal form is engine-safe for the
///   column's kind: numbers for `Num`/`NumText` (any comparison operator),
///   canonical lowercase '0x…' literals of 40+ hex digits for `Hex`
///   (equality only — exactly the form `convert_hex_literals_postgres`
///   rewrites to bytea on the hot arm), plain strings for `Text` (equality
///   only: range order is collation-dependent);
/// - `col [NOT] IN (literal, …)` under the same literal rules;
/// - `col [NOT] BETWEEN n AND n` for numeric kinds;
/// - `col IS [NOT] NULL` (PG NULL ↔ CH `Nullable` NULL for allowed kinds).
///
/// Everything else — `LIKE`, casts, arithmetic, `Ts` comparisons (timestamp
/// literal parsing differs), `Selector`/`Blocked` columns, short or
/// non-canonical hex literals (PG decodes them as escape-format bytea while
/// ClickHouse matches its hex strings) — rejects the split.
fn filter_pred_safe(expr: &Expr, relations: &[SplitRelation]) -> bool {
    match unwrap_nested(expr) {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And | BinaryOperator::Or,
            right,
        } => filter_pred_safe(left, relations) && filter_pred_safe(right, relations),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => filter_pred_safe(expr, relations),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => matches!(
            filter_column_kind(inner, relations),
            Some(Num | NumText | Ts | Hex | Text)
        ),
        Expr::BinaryOp { left, op, right } => comparison_safe(left, op, right, relations),
        Expr::InList { expr, list, .. } => {
            let Some(kind) = filter_column_kind(expr, relations) else {
                return false;
            };
            !list.is_empty() && list.iter().all(|item| literal_safe(kind, item, false))
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            matches!(filter_column_kind(expr, relations), Some(Num | NumText))
                && numeric_literal(low)
                && numeric_literal(high)
        }
        _ => false,
    }
}

/// One comparison: `col <op> col` (equality between same-kind columns) or
/// `col <op> literal` in either operand order.
fn comparison_safe(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    relations: &[SplitRelation],
) -> bool {
    let range = match op {
        BinaryOperator::Eq | BinaryOperator::NotEq => false,
        BinaryOperator::Lt | BinaryOperator::LtEq | BinaryOperator::Gt | BinaryOperator::GtEq => {
            true
        }
        _ => return false,
    };
    match (
        filter_column_kind(left, relations),
        filter_column_kind(right, relations),
    ) {
        (Some(a), Some(b)) => !range && a == b && !matches!(a, Selector | Blocked),
        (Some(kind), None) => literal_safe(kind, right, range),
        (None, Some(kind)) => literal_safe(kind, left, range),
        (None, None) => false,
    }
}

/// Whether `expr` is a literal that compares identically on both engines
/// against a column of `kind`.
fn literal_safe(kind: ColKind, expr: &Expr, range: bool) -> bool {
    match kind {
        Num | NumText => numeric_literal(expr),
        Hex => !range && canonical_hex_literal(unwrap_nested(expr)),
        Text => {
            !range
                && matches!(
                    unwrap_nested(expr),
                    Expr::Value(v) if matches!(v.value, Value::SingleQuotedString(_))
                )
        }
        Ts | Selector | Blocked => false,
    }
}

/// Plain (optionally negated) numeric literal.
fn numeric_literal(expr: &Expr) -> bool {
    match unwrap_nested(expr) {
        Expr::Value(v) => matches!(v.value, Value::Number(..)),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => matches!(
            unwrap_nested(expr),
            Expr::Value(v) if matches!(v.value, Value::Number(..))
        ),
        _ => false,
    }
}

/// '0x' + 40+ lowercase hex digits: exactly the literal form
/// `convert_hex_literals_postgres` rewrites to bytea for the hot arm, and
/// the form the ClickHouse sink stores.
fn canonical_hex_literal(expr: &Expr) -> bool {
    let Expr::Value(v) = expr else {
        return false;
    };
    let Value::SingleQuotedString(s) = &v.value else {
        return false;
    };
    s.strip_prefix("0x").is_some_and(|h| {
        h.len() >= 40
            && h.bytes()
                .all(|b| b.is_ascii_digit() || matches!(b, b'a'..=b'f'))
    })
}

/// Kind of a (possibly parenthesized) column reference, or `None` when the
/// expression is not a resolvable column.
fn filter_column_kind(expr: &Expr, relations: &[SplitRelation]) -> Option<ColKind> {
    let (qualifier, col) = column_parts(unwrap_nested(expr))?;
    resolve_kind(relations, qualifier.as_deref(), &col)
}

fn unwrap_nested(expr: &Expr) -> &Expr {
    let mut e = expr;
    while let Expr::Nested(inner) = e {
        e = inner;
    }
    e
}

/// `(qualifier, column)` of a plain column reference, lowercased.
fn column_parts(expr: &Expr) -> Option<(Option<String>, String)> {
    match expr {
        Expr::Identifier(id) => Some((None, id.value.to_lowercase())),
        Expr::CompoundIdentifier(parts) => match parts.as_slice() {
            [q, c] => Some((Some(q.value.to_lowercase()), c.value.to_lowercase())),
            _ => None,
        },
        _ => None,
    }
}

/// Kind of `qualifier.col` across the split relations, or `None` when the
/// reference is unknown or ambiguous.
fn resolve_kind(
    relations: &[SplitRelation],
    qualifier: Option<&str>,
    col: &str,
) -> Option<ColKind> {
    let mut found = None;
    for rel in relations {
        if qualifier.is_some_and(|q| q != effective_qualifier(rel)) {
            continue;
        }
        match rel.columns.get(col) {
            Some(_) if found.is_some() => return None, // ambiguous
            Some(k) => found = Some(*k),
            None if qualifier.is_some() => return None, // unknown in the named table
            None => {}
        }
    }
    found
}

/// Unquoted identifiers must be lowercase: PostgreSQL folds them, ClickHouse
/// resolves them case-sensitively. Quoted identifiers pass through verbatim
/// in both engines.
fn ident_case_ok(id: &Ident) -> bool {
    id.quote_style.is_some() || !id.value.bytes().any(|b| b.is_ascii_uppercase())
}

/// Lowercased projection aliases that rename their source column
/// (`SELECT "to" AS sender`). Same-name aliases (`block_num AS block_num`)
/// are transparent and excluded.
fn renamed_aliases(select: &Select) -> HashSet<String> {
    select
        .projection
        .iter()
        .filter_map(|item| {
            let SelectItem::ExprWithAlias { expr, alias } = item else {
                return None;
            };
            let source = match expr {
                Expr::Identifier(id) => &id.value,
                Expr::CompoundIdentifier(parts) => &parts.last()?.value,
                _ => return None,
            };
            let alias_lc = alias.value.to_lowercase();
            (!source.eq_ignore_ascii_case(&alias.value)).then_some(alias_lc)
        })
        .collect()
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
    event_map: &HashMap<String, &EventSignature>,
) -> Option<Vec<SplitRelation>> {
    if select.from.len() != 1 {
        return None;
    }
    let twj = &select.from[0];
    let (name, alias) = plain_table(&twj.relation)?;
    if !alias.as_ref().is_none_or(ident_case_ok) {
        return None;
    }
    let name_lc = name.value.to_lowercase();
    let (boundary, columns) = resolve_relation(&name, event_map)?;

    if twj.joins.is_empty() {
        // Only qualify when aliased (an alias makes the bare name invalid).
        return Some(vec![SplitRelation {
            qualifier: alias.clone(),
            name_lc,
            alias_lc: alias.as_ref().map(|a| a.value.to_lowercase()),
            boundary_col: boundary,
            columns,
        }]);
    }

    // Join case: exactly one INNER JOIN of core tables, tied per-block.
    // Event CTEs shadow same-named core tables; keep joins to plain core.
    if twj.joins.len() != 1 {
        return None;
    }
    if !CORE_TABLES.contains(&name_lc.as_str()) || event_map.contains_key(&name_lc) {
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
    if !alias2.as_ref().is_none_or(ident_case_ok) {
        return None;
    }
    let name2_lc = name2.value.to_lowercase();
    if !CORE_TABLES.contains(&name2_lc.as_str()) || event_map.contains_key(&name2_lc) {
        return None;
    }
    let (boundary2, columns2) = resolve_relation(&name2, event_map)?;

    let rel1 = SplitRelation {
        qualifier: Some(alias.clone().unwrap_or(name)),
        name_lc,
        alias_lc: alias.as_ref().map(|a| a.value.to_lowercase()),
        boundary_col: boundary,
        columns,
    };
    let rel2 = SplitRelation {
        qualifier: Some(alias2.clone().unwrap_or(name2)),
        name_lc: name2_lc,
        alias_lc: alias2.as_ref().map(|a| a.value.to_lowercase()),
        boundary_col: boundary2,
        columns: columns2,
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

/// Boundary column and column kinds for a FROM reference, or `None` when
/// ineligible.
///
/// Event CTEs are matched first: they shadow same-named core tables in both
/// engines. Core-table references must be written in lowercase because
/// ClickHouse resolves names verbatim (no case folding); event references are
/// exempt since `normalize_table_references` repairs their case per engine.
fn resolve_relation(
    name: &Ident,
    event_map: &HashMap<String, &EventSignature>,
) -> Option<(&'static str, HashMap<String, ColKind>)> {
    let name_lc = name.value.to_lowercase();
    if let Some(sig) = event_map.get(&name_lc) {
        return Some(("block_num", event_columns(sig)?));
    }
    if name.value != name_lc {
        return None;
    }
    let boundary = match name_lc.as_str() {
        "blocks" => "num",
        "txs" | "logs" | "receipts" => "block_num",
        _ => return None,
    };
    let columns = core_columns(&name_lc)?
        .iter()
        .map(|(c, k)| ((*c).to_string(), *k))
        .collect();
    Some((boundary, columns))
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

/// NOT NULL numeric/time columns that sort identically in PostgreSQL and
/// ClickHouse. Other columns (bytea/text/nullable) differ in collation or
/// NULL placement across engines, so they can't be stitch-order keys.
fn sortable_columns(rel: &SplitRelation) -> &'static [&'static str] {
    match rel.name_lc.as_str() {
        "blocks" => &["num", "timestamp", "timestamp_ms", "gas_limit", "gas_used"],
        "txs" => &["block_num", "block_timestamp", "idx"],
        "logs" => &["block_num", "block_timestamp", "log_idx", "tx_idx"],
        "receipts" => &[
            "block_num",
            "block_timestamp",
            "tx_idx",
            "gas_used",
            "cumulative_gas_used",
        ],
        // Event CTE passthrough keys sourced from logs.
        _ => &["block_num", "block_timestamp", "log_idx", "tx_idx"],
    }
}

/// Validate the ORDER BY and return `Some(asc)` of the first key, or `None`
/// (inner) when there is no ORDER BY. Outer `None` = ineligible.
///
/// The first key must be the boundary column of exactly one relation so the
/// primary comparison alone orders hot rows against cold rows. Every other
/// key must resolve to a [`sortable_columns`] entry of an unambiguous
/// relation so both engines produce the same within-boundary order.
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
    if exprs.is_empty() {
        return Some(None);
    }

    let mut first_asc = None;
    for (i, e) in exprs.iter().enumerate() {
        if e.with_fill.is_some() || e.options.nulls_first.is_some() {
            return None;
        }
        let (qualifier, col) = match &e.expr {
            Expr::Identifier(id) if ident_case_ok(id) => (None, id.value.to_lowercase()),
            Expr::CompoundIdentifier(parts) => match parts.as_slice() {
                [q, c] if ident_case_ok(q) && ident_case_ok(c) => {
                    (Some(q.value.to_lowercase()), c.value.to_lowercase())
                }
                _ => return None,
            },
            _ => return None,
        };
        let matched = relations
            .iter()
            .filter(|rel| {
                qualifier
                    .as_deref()
                    .is_none_or(|q| q == effective_qualifier(rel))
                    && if i == 0 {
                        rel.boundary_col == col
                    } else {
                        sortable_columns(rel).contains(&col.as_str())
                    }
            })
            .count();
        if matched != 1 {
            return None;
        }
        if i == 0 {
            first_asc = Some(e.options.asc.unwrap_or(true));
        }
    }

    Some(first_asc)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Canonical lowercase test literals (40- and 64-digit hex).
    const ADDR: &str = "0x385193793fe875cd9f2341409563932023fb4fab";
    const HASH: &str = "0xb82100be8a8f8d5cf127764bb338dba3667cf439901f9b030434f1c262edb178";

    fn plan(sql: &str) -> Option<TieredSplit> {
        plan_tiered_split(sql, &[])
    }

    fn plan_ev(sql: &str, signatures: &[&str]) -> Option<TieredSplit> {
        let events: Vec<EventSignature> = signatures
            .iter()
            .map(|s| EventSignature::parse(s).unwrap())
            .collect();
        plan_tiered_split(sql, &events)
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
        let p = plan(&format!(
            "SELECT block_num, tx_idx FROM receipts WHERE tx_hash = '{HASH}' LIMIT 1"
        ))
        .unwrap();
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
        assert!(
            plan(&format!(
                "SELECT \"from\", \"to\" FROM txs WHERE \"from\" = '{ADDR}' \
                 ORDER BY block_num DESC LIMIT 11"
            ))
            .is_some()
        );
    }

    #[test]
    fn event_table_with_signature() {
        let p = plan_ev(
            "SELECT \"from\", value, block_num FROM Transfer ORDER BY block_num DESC LIMIT 11",
            &["Transfer(address indexed from, address indexed to, uint256 value)"],
        )
        .unwrap();
        assert_eq!(p.sql_limit, Some(11));
    }

    #[test]
    fn event_table_without_signature_rejected() {
        assert!(plan("SELECT value FROM Transfer ORDER BY block_num DESC LIMIT 1").is_none());
    }

    #[test]
    fn event_with_divergent_decoded_params_rejected() {
        // bool: PG boolean vs CH UInt8.
        assert!(
            plan_ev(
                "SELECT ok FROM Flagged ORDER BY block_num DESC LIMIT 1",
                &["Flagged(address indexed who, bool ok)"],
            )
            .is_none()
        );
        // Non-indexed string: PG decodes text, CH returns hex.
        assert!(
            plan_ev(
                "SELECT token, name FROM TokenCreated ORDER BY block_num DESC LIMIT 11",
                &["TokenCreated(address indexed token, string name)"],
            )
            .is_none()
        );
    }

    #[test]
    fn event_secondary_order_keys_use_log_columns() {
        let sig = "Transfer(address indexed from, address indexed to, uint256 value)";
        assert!(
            plan_ev(
                "SELECT value FROM Transfer ORDER BY block_num DESC, tx_idx DESC LIMIT 80",
                &[sig],
            )
            .is_some()
        );
        // Decoded columns are not valid stitch-order keys.
        assert!(
            plan_ev(
                "SELECT value FROM Transfer ORDER BY block_num DESC, value DESC LIMIT 80",
                &[sig],
            )
            .is_none()
        );
    }

    #[test]
    fn tx_hash_join_accepted() {
        let p = plan(&format!(
            "SELECT receipts.tx_hash, logs.data FROM receipts \
             INNER JOIN logs ON logs.tx_hash = receipts.tx_hash \
             WHERE receipts.\"from\" = '{ADDR}' \
             ORDER BY receipts.block_num DESC, receipts.tx_idx DESC LIMIT 80"
        ))
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
    fn rejects_uppercase_unquoted_identifiers() {
        // PostgreSQL case-folds these; ClickHouse resolves them verbatim.
        assert!(plan("SELECT num FROM Blocks ORDER BY num DESC LIMIT 1").is_none());
        assert!(plan("SELECT Num FROM blocks ORDER BY num DESC LIMIT 1").is_none());
        assert!(plan("SELECT num FROM blocks ORDER BY NUM DESC LIMIT 1").is_none());
        assert!(plan("SELECT b.num FROM blocks B ORDER BY b.num DESC LIMIT 1").is_none());
    }

    #[test]
    fn rejects_renaming_alias_referenced_unqualified() {
        // ClickHouse resolves SELECT aliases in WHERE; PostgreSQL does not.
        assert!(plan("SELECT \"to\" AS sender FROM txs WHERE sender = '0xab' LIMIT 1").is_none());
        // Same-name aliases are transparent.
        assert!(
            plan(
                "SELECT block_num AS block_num FROM receipts \
                 ORDER BY block_num DESC LIMIT 1"
            )
            .is_some()
        );
    }

    #[test]
    fn rejects_divergent_column_representation() {
        assert!(
            plan("SELECT is_virtual_forward FROM logs ORDER BY block_num DESC LIMIT 1").is_none()
        );
    }

    #[test]
    fn rejects_order_by_non_sortable_secondary_key() {
        // hash is bytea (PG) vs String (CH): collation differs.
        assert!(plan("SELECT hash FROM txs ORDER BY block_num DESC, hash DESC LIMIT 1").is_none());
    }

    #[test]
    fn rejects_duplicate_projection_names() {
        // ClickHouse renames the duplicate (`hash, txs.hash`); PG repeats it.
        assert!(
            plan(
                "SELECT blocks.hash, txs.hash FROM blocks \
                 JOIN txs ON blocks.num = txs.block_num ORDER BY blocks.num DESC LIMIT 10"
            )
            .is_none()
        );
        assert!(plan("SELECT num, num FROM blocks LIMIT 1").is_none());
        assert!(plan("SELECT num, timestamp_ms AS num FROM blocks LIMIT 1").is_none());
    }

    #[test]
    fn rejects_non_canonical_hex_filters() {
        // Short literals aren't rewritten to bytea for PG: PG decodes '0x'
        // as escape-format bytes while CH matches its stored hex strings.
        assert!(plan("SELECT block_num FROM logs WHERE data = '0x' LIMIT 1").is_none());
        assert!(plan("SELECT block_num FROM receipts WHERE tx_hash = '0xab' LIMIT 1").is_none());
        // Mixed-case long literal: PG bytea decode is case-insensitive, CH
        // string compare is not (also caught by the global gate).
        let upper = HASH.to_uppercase().replace("0X", "0x");
        assert!(
            plan(&format!(
                "SELECT block_num FROM receipts WHERE tx_hash = '{upper}' LIMIT 1"
            ))
            .is_none()
        );
    }

    #[test]
    fn rejects_unsafe_predicate_shapes() {
        // Range over hex: bytea order vs string order.
        assert!(
            plan(&format!(
                "SELECT block_num FROM receipts WHERE tx_hash > '{HASH}' LIMIT 1"
            ))
            .is_none()
        );
        // LIKE, arithmetic, and casts are outside the grammar.
        assert!(plan("SELECT block_num FROM txs WHERE input LIKE '0xab%' LIMIT 1").is_none());
        assert!(plan("SELECT num FROM blocks WHERE num + 1 > 5 LIMIT 1").is_none());
        assert!(plan("SELECT num FROM blocks WHERE num = '5'::int LIMIT 1").is_none());
        // Timestamp literals parse differently across engines.
        assert!(plan("SELECT num FROM blocks WHERE timestamp > '2025-01-01' LIMIT 1").is_none());
        // Text equality is exact on both engines; ranges are collation-bound.
        assert!(plan("SELECT hash FROM txs WHERE value = '1000' LIMIT 1").is_some());
        assert!(plan("SELECT hash FROM txs WHERE value > '1000' LIMIT 1").is_none());
    }

    #[test]
    fn accepts_grammar_predicates() {
        // Parenthesized operands (`((num) < (n))` is a real client shape).
        let p = plan("SELECT hash, num, timestamp FROM blocks WHERE ((num) < (29000441)) ORDER BY num DESC LIMIT 11")
            .unwrap();
        assert_eq!(p.sql_limit, Some(11));
        assert!(
            plan(&format!(
                "SELECT block_num FROM receipts WHERE tx_hash IN ('{HASH}', '{HASH}') LIMIT 10"
            ))
            .is_some()
        );
        assert!(plan("SELECT num FROM blocks WHERE num BETWEEN 5 AND 10 LIMIT 10").is_some());
        assert!(plan("SELECT block_num FROM receipts WHERE \"to\" IS NULL LIMIT 10").is_some());
        assert!(plan("SELECT block_num FROM receipts WHERE NOT (status = 0) LIMIT 10").is_some());
    }

    #[test]
    fn rejects_selector_and_blocked_in_filters() {
        // selector: PG NULL is '' in ClickHouse, so predicates diverge.
        assert!(plan("SELECT block_num FROM logs WHERE selector IS NULL LIMIT 1").is_none());
        assert!(plan("SELECT block_num FROM txs WHERE calls IS NOT NULL LIMIT 1").is_none());
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
        let p = plan(&format!(
            "SELECT receipts.tx_hash FROM receipts JOIN logs ON logs.tx_hash = receipts.tx_hash \
             WHERE receipts.\"from\" = '{ADDR}' ORDER BY receipts.block_num DESC LIMIT 80"
        ))
        .unwrap();
        assert_eq!(
            p.arm_sql(false, 500, Some(10)),
            format!(
                "SELECT receipts.tx_hash FROM receipts JOIN logs ON logs.tx_hash = receipts.tx_hash \
                 WHERE (receipts.\"from\" = '{ADDR}') AND receipts.block_num <= 500 AND logs.block_num <= 500 \
                 ORDER BY receipts.block_num DESC LIMIT 10"
            )
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
