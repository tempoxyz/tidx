use super::{BackfillPolicy, ClickHouseObject, ClickHouseObjectKind};

const TOKEN_HOLDER_DELTAS_SCHEMA: &str =
    include_str!("../../db/clickhouse/token_holder_deltas.sql");
const TOKEN_HOLDER_DELTAS_SELECT: &str =
    include_str!("../../db/clickhouse/token_holder_deltas_select.sql");
const TOKEN_BALANCE_DIRTY_EVENTS_SCHEMA: &str =
    include_str!("../../db/clickhouse/token_balance_dirty_events.sql");
const TOKEN_BALANCE_DIRTY_EVENTS_SELECT: &str =
    include_str!("../../db/clickhouse/token_balance_dirty_events_select.sql");
const TOKEN_BALANCE_REORG_KEYS_V2_SCHEMA: &str =
    include_str!("../../db/clickhouse/token_balance_reorg_keys_v2.sql");
const TOKEN_BALANCE_CHECKPOINTS_SCHEMA: &str =
    include_str!("../../db/clickhouse/token_balance_checkpoints.sql");
const TOKEN_BALANCE_DIRTY_EVENTS_CLEAN_SELECT: &str =
    include_str!("../../db/clickhouse/token_balance_dirty_events_clean_select.sql");
const BOOTSTRAP_TOKEN_BALANCE_CHECKPOINTS_20260715: &str =
    include_str!("../../db/clickhouse/migrations/20260715_bootstrap_token_balance_checkpoints.sql");
const TOKEN_BALANCE_CHECKPOINT_REFRESH: &str =
    include_str!("../../db/clickhouse/token_balance_checkpoint_refresh.sql");
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
        name: "token_balance_dirty_events",
        kind: ClickHouseObjectKind::Table(TOKEN_BALANCE_DIRTY_EVENTS_SCHEMA),
        depends_on: &["token_holder_deltas"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_balance_dirty_events_mv",
        kind: ClickHouseObjectKind::MaterializedView {
            target_table: "token_balance_dirty_events",
            select_sql: TOKEN_BALANCE_DIRTY_EVENTS_SELECT,
        },
        depends_on: &["token_holder_deltas", "token_balance_dirty_events"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_balance_reorg_keys_v2",
        kind: ClickHouseObjectKind::Table(TOKEN_BALANCE_REORG_KEYS_V2_SCHEMA),
        depends_on: &["token_holder_deltas"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_balance_checkpoints",
        kind: ClickHouseObjectKind::Table(TOKEN_BALANCE_CHECKPOINTS_SCHEMA),
        depends_on: &["token_holder_deltas"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_balance_dirty_events_clean_mv",
        kind: ClickHouseObjectKind::MaterializedView {
            target_table: "token_balance_dirty_events",
            select_sql: TOKEN_BALANCE_DIRTY_EVENTS_CLEAN_SELECT,
        },
        depends_on: &["token_balance_checkpoints", "token_balance_dirty_events"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    // Run once after the checkpoint table and its queue-cleaning MV exist, but
    // before any public view switches to the checkpoint-backed generation.
    ClickHouseObject {
        name: "token_balance_checkpoints_20260715_bootstrap",
        kind: ClickHouseObjectKind::Migration(BOOTSTRAP_TOKEN_BALANCE_CHECKPOINTS_20260715),
        depends_on: &["token_holder_deltas", "token_balance_checkpoints"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_balance_checkpoint_refresh",
        kind: ClickHouseObjectKind::RefreshableMaterializedView(TOKEN_BALANCE_CHECKPOINT_REFRESH),
        depends_on: &[
            "token_holder_deltas",
            "token_balance_dirty_events",
            "token_balance_checkpoints",
        ],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_balances",
        kind: ClickHouseObjectKind::View(TOKEN_BALANCES_VIEW),
        depends_on: &["token_holder_deltas"],
        public_query: true,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_balances_snapshot",
        kind: ClickHouseObjectKind::RefreshableMaterializedView(TOKEN_BALANCES_SNAPSHOT),
        depends_on: &[
            "token_balance_checkpoints",
            "token_balance_checkpoint_refresh",
        ],
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
        assert!(ddl.contains("HAVING balance > 0"));
    }

    #[test]
    fn token_balances_snapshot_publishes_checkpoint_state() {
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
        assert!(
            ddl.contains("REFRESH EVERY 15 MINUTE DEPENDS ON token_balance_checkpoint_refresh")
        );
        assert!(ddl.contains("FROM token_balance_checkpoints FINAL"));
        assert!(!ddl.contains("FROM token_holder_deltas FINAL"));
        assert!(ddl.contains("WHERE credited > debited"));
        assert!(ddl.contains("max_threads = 4"));

        // Drops the view (and its inner target table) on definition drift.
        assert_eq!(
            snapshot.drop_sql().as_deref(),
            Some("DROP VIEW IF EXISTS token_balances_snapshot")
        );
    }

    #[test]
    fn balance_checkpoint_reducer_uses_post_checkpoint_deltas() {
        let events = OBJECTS
            .iter()
            .find(|object| object.name == "token_balance_dirty_events")
            .unwrap();
        assert!(events.ddl().contains("CollapsingMergeTree(sign)"));

        let checkpoints = OBJECTS
            .iter()
            .find(|object| object.name == "token_balance_checkpoints")
            .unwrap();
        let checkpoint_ddl = checkpoints.ddl();
        assert!(checkpoint_ddl.contains("credited         UInt256"));
        assert!(checkpoint_ddl.contains("debited          UInt256"));
        assert!(checkpoint_ddl.contains("checkpoint_from_block Int64"));
        assert!(checkpoint_ddl.contains("checkpoint_block Int64"));
        assert!(checkpoint_ddl.contains("ReplacingMergeTree(version)"));

        let refresh = OBJECTS
            .iter()
            .find(|object| object.name == "token_balance_checkpoint_refresh")
            .unwrap();
        let refresh_ddl = refresh.ddl();
        assert!(refresh_ddl.contains("REFRESH AFTER 1 MINUTE APPEND TO"));
        assert!(refresh_ddl.contains("block_num > work.scan_from_exclusive"));
        assert!(refresh_ddl.contains("pending.min_changed_block > current.checkpoint_block"));
        assert!(refresh_ddl.contains("pending.max_changed_block < current.checkpoint_from_block"));
        assert!(refresh_ddl.contains("FROM token_holder_deltas FINAL"));
        assert!(refresh_ddl.contains("FROM token_balance_dirty_events FINAL"));

        let bootstrap = OBJECTS
            .iter()
            .find(|object| object.name == "token_balance_checkpoints_20260715_bootstrap")
            .unwrap();
        assert!(matches!(bootstrap.kind, ClickHouseObjectKind::Migration(_)));
        assert!(
            bootstrap
                .ddl()
                .contains("INSERT INTO token_balance_checkpoints")
        );
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
        assert!(ddl.contains("REFRESH EVERY 15 MINUTE DEPENDS ON token_balances_snapshot"));
        // Derives from the already-deduped snapshot, not the raw deltas, so each
        // refresh is a cheap GROUP BY over one row per (token, holder).
        assert!(ddl.contains("FROM token_balances_snapshot"));
        assert!(ddl.contains("count() AS holder_count"));
        assert!(ddl.contains("GROUP BY token"));
        assert!(ddl.contains("optimize_aggregation_in_order = 1"));
        assert!(ddl.contains("max_threads = 4"));
        assert_eq!(
            counts.drop_sql().as_deref(),
            Some("DROP VIEW IF EXISTS token_holder_counts")
        );
    }
}
