use super::{BackfillPolicy, ClickHouseObject, ClickHouseObjectKind};

const ADDRESS_HOLDER_DELTAS_SCHEMA: &str =
    include_str!("../../db/clickhouse/address_holder_deltas.sql");
const ADDRESS_HOLDER_DELTAS_SELECT: &str =
    include_str!("../../db/clickhouse/address_holder_deltas_select.sql");
const ADDRESS_BALANCES_VIEW: &str = include_str!("../../db/clickhouse/address_balances.sql");
const ADDRESS_BALANCES_SNAPSHOT: &str =
    include_str!("../../db/clickhouse/address_balances_snapshot.sql");

pub const OBJECTS: &[ClickHouseObject] = &[
    ClickHouseObject {
        name: "address_holder_deltas",
        kind: ClickHouseObjectKind::Table(ADDRESS_HOLDER_DELTAS_SCHEMA),
        depends_on: &["token_transfers"],
        public_query: true,
        block_column: Some("block_num"),
        backfill: Some(BackfillPolicy::Ranged {
            select_sql: ADDRESS_HOLDER_DELTAS_SELECT,
        }),
    },
    ClickHouseObject {
        name: "address_holder_deltas_mv",
        kind: ClickHouseObjectKind::MaterializedView {
            target_table: "address_holder_deltas",
            select_sql: ADDRESS_HOLDER_DELTAS_SELECT,
        },
        depends_on: &["token_transfers", "address_holder_deltas"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "address_balances",
        kind: ClickHouseObjectKind::View(ADDRESS_BALANCES_VIEW),
        depends_on: &["address_holder_deltas"],
        public_query: true,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "address_balances_snapshot",
        kind: ClickHouseObjectKind::RefreshableMaterializedView(ADDRESS_BALANCES_SNAPSHOT),
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
    fn materialized_view_uses_array_join_and_skips_zero_address() {
        let mv = OBJECTS
            .iter()
            .find(|object| object.name == "address_holder_deltas_mv")
            .unwrap();
        let ddl = mv.ddl();
        assert!(ddl.starts_with("CREATE MATERIALIZED VIEW IF NOT EXISTS address_holder_deltas_mv"));
        assert!(ddl.contains("TO address_holder_deltas AS\nSELECT"));
        assert!(ddl.contains("FROM token_transfers"));
        assert!(ddl.contains("ARRAY JOIN"));
        assert!(!ddl.contains("UNION ALL"));
    }

    #[test]
    fn address_holder_deltas_store_unsigned_magnitude() {
        assert!(ADDRESS_HOLDER_DELTAS_SCHEMA.contains("balance_delta   UInt256"));
        assert!(!ADDRESS_HOLDER_DELTAS_SELECT.contains("CAST(amount AS Int256)"));
        assert!(ADDRESS_HOLDER_DELTAS_SELECT.contains("CAST(1 AS Int8),  amount)"));
        assert!(ADDRESS_HOLDER_DELTAS_SELECT.contains("CAST(-1 AS Int8), amount)"));
        let view_ddl = OBJECTS
            .iter()
            .find(|object| object.name == "address_balances")
            .unwrap()
            .ddl();
        assert!(
            view_ddl.contains("sumIf(balance_delta, leg = 1) - sumIf(balance_delta, leg = -1)")
        );
        assert!(!view_ddl.contains("sum(balance_delta)"));
    }

    #[test]
    fn address_balances_view_groups_by_holder_first() {
        let view = OBJECTS
            .iter()
            .find(|object| object.name == "address_balances")
            .unwrap();
        assert!(view.is_view());
        let ddl = view.ddl();
        assert!(ddl.contains("FROM address_holder_deltas FINAL"));
        assert!(ddl.contains("GROUP BY holder, token"));
        assert!(ddl.contains("HAVING balance > 0"));
    }

    #[test]
    fn address_balances_snapshot_is_a_refreshable_materialized_view() {
        let snapshot = OBJECTS
            .iter()
            .find(|object| object.name == "address_balances_snapshot")
            .unwrap();
        assert!(snapshot.is_materialized_view());
        assert!(snapshot.is_refreshable_materialized_view());
        // Publicly queryable so Cadent can read account balance pages without
        // re-aggregating high-cardinality address_holder_deltas on every call.
        assert!(snapshot.public_query);
        // Self-storing and fully replaced each refresh, so reorg cleanup skips it.
        assert!(snapshot.block_column.is_none());

        let ddl = snapshot.ddl();
        assert!(ddl.contains("CREATE MATERIALIZED VIEW IF NOT EXISTS address_balances_snapshot"));
        assert!(ddl.contains("REFRESH AFTER 15 MINUTE DEPENDS ON token_balances_snapshot"));
        assert!(ddl.contains("ORDER BY (holder, balance, token)"));
        assert!(ddl.contains("FROM token_balances_snapshot"));
        assert!(!ddl.contains("FROM address_holder_deltas FINAL"));
        assert!(!ddl.contains("GROUP BY holder, token"));
        assert_eq!(
            snapshot.drop_sql().as_deref(),
            Some("DROP VIEW IF EXISTS address_balances_snapshot")
        );
    }
}
