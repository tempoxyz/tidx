use super::{BackfillPolicy, ClickHouseObject, ClickHouseObjectKind};

const TOKEN_HOLDER_DELTAS_SCHEMA: &str =
    include_str!("../../db/clickhouse/token_holder_deltas.sql");
const TOKEN_HOLDER_DELTAS_SELECT: &str =
    include_str!("../../db/clickhouse/token_holder_deltas_select.sql");
const BALANCE_DIRTY_KEYS_SCHEMA: &str = include_str!("../../db/clickhouse/balance_dirty_keys.sql");
const BALANCE_DIRTY_KEYS_SELECT: &str =
    include_str!("../../db/clickhouse/balance_dirty_keys_select.sql");
const BALANCE_REORG_KEYS_SCHEMA: &str = include_str!("../../db/clickhouse/balance_reorg_keys.sql");
const BALANCE_STATE_SCHEMA: &str = include_str!("../../db/clickhouse/balance_state.sql");
const BALANCE_STATE_CLEAN_SELECT: &str =
    include_str!("../../db/clickhouse/balance_state_clean_select.sql");
const BALANCE_STATE_REFRESH: &str = include_str!("../../db/clickhouse/balance_state_refresh.sql");
const BOOTSTRAP_BALANCE_STATE_20260714: &str =
    include_str!("../../db/clickhouse/migrations/20260714_bootstrap_balance_state.sql");
const TOKEN_BALANCES_VIEW: &str = include_str!("../../db/clickhouse/token_balances.sql");
const TOKEN_BALANCES_SNAPSHOT: &str =
    include_str!("../../db/clickhouse/token_balances_snapshot.sql");
const TOKEN_HOLDER_COUNTS: &str = include_str!("../../db/clickhouse/token_holder_counts.sql");

pub const OBJECTS: &[ClickHouseObject] = &[
    ClickHouseObject {
        name: "token_holder_deltas",
        kind: ClickHouseObjectKind::Table(TOKEN_HOLDER_DELTAS_SCHEMA),
        depends_on: &["token_transfers"],
        public_query: true,
        block_column: Some("block_num"),
        backfill: Some(BackfillPolicy::Ranged {
            select_sql: TOKEN_HOLDER_DELTAS_SELECT,
        }),
    },
    ClickHouseObject {
        name: "token_holder_deltas_mv",
        kind: ClickHouseObjectKind::MaterializedView {
            target_table: "token_holder_deltas",
            select_sql: TOKEN_HOLDER_DELTAS_SELECT,
        },
        depends_on: &["token_transfers", "token_holder_deltas"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "balance_dirty_keys",
        kind: ClickHouseObjectKind::Table(BALANCE_DIRTY_KEYS_SCHEMA),
        depends_on: &["token_holder_deltas"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "balance_dirty_keys_mv",
        kind: ClickHouseObjectKind::MaterializedView {
            target_table: "balance_dirty_keys",
            select_sql: BALANCE_DIRTY_KEYS_SELECT,
        },
        depends_on: &["token_holder_deltas", "balance_dirty_keys"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "balance_reorg_keys",
        kind: ClickHouseObjectKind::Table(BALANCE_REORG_KEYS_SCHEMA),
        depends_on: &["token_holder_deltas"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "balance_state",
        kind: ClickHouseObjectKind::Table(BALANCE_STATE_SCHEMA),
        depends_on: &["token_holder_deltas"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "balance_state_clean_mv",
        kind: ClickHouseObjectKind::MaterializedView {
            target_table: "balance_dirty_keys",
            select_sql: BALANCE_STATE_CLEAN_SELECT,
        },
        depends_on: &["balance_state", "balance_dirty_keys"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "balance_state_refresh",
        kind: ClickHouseObjectKind::RefreshableMaterializedView(BALANCE_STATE_REFRESH),
        depends_on: &["balance_dirty_keys", "balance_state", "token_holder_deltas"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    // This migration intentionally sits between the reducer and the public
    // views. Existing snapshots remain available while the one-time state
    // bootstrap scans historical deltas.
    ClickHouseObject {
        name: "balance_state_20260714_bootstrap",
        kind: ClickHouseObjectKind::Migration(BOOTSTRAP_BALANCE_STATE_20260714),
        depends_on: &[
            "balance_state",
            "balance_state_clean_mv",
            "token_holder_deltas",
        ],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_balances",
        kind: ClickHouseObjectKind::View(TOKEN_BALANCES_VIEW),
        depends_on: &["token_holder_deltas", "balance_dirty_keys", "balance_state"],
        public_query: true,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_balances_snapshot",
        kind: ClickHouseObjectKind::RefreshableMaterializedView(TOKEN_BALANCES_SNAPSHOT),
        depends_on: &["token_balances"],
        public_query: true,
        // Self-storing refreshable MV: it owns its rows and is fully replaced
        // each refresh, so it isn't block-scoped and reorg cleanup skips it.
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_holder_counts",
        kind: ClickHouseObjectKind::RefreshableMaterializedView(TOKEN_HOLDER_COUNTS),
        depends_on: &["token_balances_snapshot"],
        public_query: true,
        // Self-storing refreshable MV: fully replaced each refresh, so it isn't
        // block-scoped and reorg cleanup skips it.
        block_column: None,
        backfill: None,
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn materialized_view_ddl_uses_shared_select() {
        let mv = OBJECTS
            .iter()
            .find(|object| object.name == "token_holder_deltas_mv")
            .unwrap();
        let ddl = mv.ddl();
        assert!(ddl.starts_with("CREATE MATERIALIZED VIEW IF NOT EXISTS token_holder_deltas_mv"));
        assert!(ddl.contains("TO token_holder_deltas AS\nSELECT"));
        assert!(ddl.contains("FROM token_transfers"));
    }

    #[test]
    fn token_holder_delta_backfill_lives_on_target_descriptor() {
        let table = OBJECTS
            .iter()
            .find(|object| object.name == "token_holder_deltas")
            .unwrap();
        let Some(BackfillPolicy::Ranged { select_sql }) = table.backfill else {
            panic!("token holder delta table should declare its backfill");
        };
        assert_eq!(select_sql, TOKEN_HOLDER_DELTAS_SELECT);
        // The MV select uses ARRAY JOIN over a tuple of (holder, leg, delta)
        // instead of UNION ALL, because ClickHouse materialized views only
        // trigger on the FIRST branch of a UNION ALL — using UNION ALL silently
        // drops the sender (-1) leg and corrupts holder balances.
        assert!(select_sql.contains("ARRAY JOIN"));
        assert!(select_sql.contains("CAST(1 AS Int8)"));
        assert!(select_sql.contains("CAST(-1 AS Int8)"));
        assert!(!select_sql.contains("UNION ALL"));
        assert!(!select_sql.contains("CAST(amount AS Int256)"));
        assert!(select_sql.contains("CAST(1 AS Int8),  amount)"));
        assert!(select_sql.contains("CAST(-1 AS Int8), amount)"));
    }

    #[test]
    fn token_holder_deltas_store_unsigned_magnitude() {
        assert!(TOKEN_HOLDER_DELTAS_SCHEMA.contains("balance_delta   UInt256"));
        let view_ddl = OBJECTS
            .iter()
            .find(|object| object.name == "token_balances")
            .unwrap()
            .ddl();
        assert!(
            view_ddl.contains("sumIf(balance_delta, leg = 1) - sumIf(balance_delta, leg = -1)")
        );
        assert!(!view_ddl.contains("sum(balance_delta)"));
    }

    #[test]
    fn token_balances_view_uses_final_for_dedup() {
        let view = OBJECTS
            .iter()
            .find(|object| object.name == "token_balances")
            .unwrap();
        assert!(view.is_view());
        let ddl = view.ddl();
        assert!(ddl.contains("FROM token_holder_deltas FINAL"));
        assert!(ddl.contains("FROM balance_state FINAL"));
        assert!(ddl.contains("FROM balance_dirty_keys FINAL"));
        assert!(ddl.contains("HAVING balance > 0"));
    }

    #[test]
    fn token_balances_snapshot_is_a_refreshable_materialized_view() {
        let snapshot = OBJECTS
            .iter()
            .find(|object| object.name == "token_balances_snapshot")
            .unwrap();
        assert!(snapshot.is_materialized_view());
        assert!(snapshot.is_refreshable_materialized_view());
        // Publicly queryable so Cadent / the /query surface can read it instead
        // of re-aggregating token_holder_deltas on every request.
        assert!(snapshot.public_query);
        // Self-storing and fully replaced each refresh, so reorg cleanup skips it.
        assert!(snapshot.block_column.is_none());

        let ddl = snapshot.ddl();
        assert!(ddl.contains("CREATE MATERIALIZED VIEW IF NOT EXISTS token_balances_snapshot"));
        assert!(ddl.contains("REFRESH AFTER"));
        assert!(ddl.contains("FROM token_balances"));
        assert!(!ddl.contains("FROM token_holder_deltas FINAL"));

        // Drops the view (and its inner target table) on definition drift.
        assert_eq!(
            snapshot.drop_sql().as_deref(),
            Some("DROP VIEW IF EXISTS token_balances_snapshot")
        );
    }

    #[test]
    fn balance_state_reducer_is_versioned_and_reorg_aware() {
        let dirty = OBJECTS
            .iter()
            .find(|object| object.name == "balance_dirty_keys")
            .unwrap();
        assert!(dirty.ddl().contains("ReplacingMergeTree(version)"));

        let state = OBJECTS
            .iter()
            .find(|object| object.name == "balance_state")
            .unwrap();
        assert!(state.ddl().contains("ReplacingMergeTree(version)"));
        assert!(state.ddl().contains("is_deleted UInt8"));

        let refresh = OBJECTS
            .iter()
            .find(|object| object.name == "balance_state_refresh")
            .unwrap();
        let ddl = refresh.ddl();
        assert!(ddl.contains("REFRESH AFTER 1 MINUTE APPEND TO balance_state"));
        assert!(ddl.contains("FROM token_holder_deltas FINAL"));
        assert!(ddl.contains("FROM balance_dirty_keys FINAL"));
        assert!(ddl.contains("max_threads = 8"));

        let bootstrap = OBJECTS
            .iter()
            .find(|object| object.name == "balance_state_20260714_bootstrap")
            .unwrap();
        assert!(matches!(bootstrap.kind, ClickHouseObjectKind::Migration(_)));
        assert!(bootstrap.ddl().contains("INSERT INTO balance_state"));
    }

    #[test]
    fn token_holder_counts_is_a_refreshable_materialized_view() {
        let counts = OBJECTS
            .iter()
            .find(|object| object.name == "token_holder_counts")
            .unwrap();
        assert!(counts.is_refreshable_materialized_view());
        // Publicly queryable so Cadent reads a single summed row per token
        // instead of counting snapshot rows on every token-detail render.
        assert!(counts.public_query);
        assert!(counts.block_column.is_none());

        let ddl = counts.ddl();
        assert!(ddl.contains("CREATE MATERIALIZED VIEW IF NOT EXISTS token_holder_counts"));
        assert!(ddl.contains("REFRESH EVERY"));
        assert!(ddl.contains("DEPENDS ON token_balances_snapshot"));
        // Derives from the already-deduped snapshot, not the raw deltas, so each
        // refresh is a cheap GROUP BY over one row per (token, holder).
        assert!(ddl.contains("FROM token_balances_snapshot"));
        assert!(ddl.contains("count() AS holder_count"));
        assert!(ddl.contains("GROUP BY token"));
        assert_eq!(
            counts.drop_sql().as_deref(),
            Some("DROP VIEW IF EXISTS token_holder_counts")
        );
    }
}
