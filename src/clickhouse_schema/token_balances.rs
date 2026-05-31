use super::{BackfillPolicy, ClickHouseObject, ClickHouseObjectKind};

const TOKEN_HOLDER_DELTAS_SCHEMA: &str =
    include_str!("../../db/clickhouse/token_holder_deltas.sql");
const TOKEN_HOLDER_DELTAS_SELECT: &str =
    include_str!("../../db/clickhouse/token_holder_deltas_select.sql");
const TOKEN_BALANCES_VIEW: &str = include_str!("../../db/clickhouse/token_balances.sql");
const TOKEN_BALANCES_SNAPSHOT: &str =
    include_str!("../../db/clickhouse/token_balances_snapshot.sql");

pub const OBJECTS: &[ClickHouseObject] = &[
    ClickHouseObject {
        name: "token_holder_deltas",
        kind: ClickHouseObjectKind::Table(TOKEN_HOLDER_DELTAS_SCHEMA),
        depends_on: &["token_transfers"],
        public_query: true,
        block_column: Some("block_num"),
        backfill: Some(BackfillPolicy::IfEmpty {
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
        depends_on: &["token_holder_deltas"],
        public_query: true,
        // Self-storing refreshable MV: it owns its rows and is fully replaced
        // each refresh, so it isn't block-scoped and reorg cleanup skips it.
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
        let Some(BackfillPolicy::IfEmpty { select_sql }) = table.backfill else {
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
        assert!(ddl.contains("REFRESH EVERY"));
        assert!(ddl.contains("FROM token_holder_deltas FINAL"));
        assert!(ddl.contains("HAVING balance > 0"));

        // Drops the view (and its inner target table) on definition drift.
        assert_eq!(
            snapshot.drop_sql().as_deref(),
            Some("DROP VIEW IF EXISTS token_balances_snapshot")
        );
    }
}
