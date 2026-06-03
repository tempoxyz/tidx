use super::{BackfillPolicy, ClickHouseObject, ClickHouseObjectKind};

const ADDRESS_HOLDER_DELTAS_SCHEMA: &str =
    include_str!("../../db/clickhouse/address_holder_deltas.sql");
const ADDRESS_HOLDER_DELTAS_SELECT: &str =
    include_str!("../../db/clickhouse/address_holder_deltas_select.sql");
const ADDRESS_BALANCES_VIEW: &str = include_str!("../../db/clickhouse/address_balances.sql");

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
}
