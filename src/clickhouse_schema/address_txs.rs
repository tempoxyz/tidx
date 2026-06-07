use super::{BackfillPolicy, ClickHouseObject, ClickHouseObjectKind};

const ADDRESS_TXS_SCHEMA: &str = include_str!("../../db/clickhouse/address_txs.sql");
const ADDRESS_TXS_SELECT: &str = include_str!("../../db/clickhouse/address_txs_select.sql");

pub const OBJECTS: &[ClickHouseObject] = &[
    ClickHouseObject {
        name: "address_txs",
        kind: ClickHouseObjectKind::Table(ADDRESS_TXS_SCHEMA),
        depends_on: &["txs"],
        public_query: true,
        block_column: Some("block_num"),
        backfill: Some(BackfillPolicy::Ranged {
            select_sql: ADDRESS_TXS_SELECT,
        }),
    },
    ClickHouseObject {
        name: "address_txs_mv",
        kind: ClickHouseObjectKind::MaterializedView {
            target_table: "address_txs",
            select_sql: ADDRESS_TXS_SELECT,
        },
        depends_on: &["txs", "address_txs"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn materialized_view_emits_from_and_to_rows_skipping_null_to() {
        let mv = OBJECTS
            .iter()
            .find(|object| object.name == "address_txs_mv")
            .unwrap();
        let ddl = mv.ddl();
        assert!(ddl.starts_with("CREATE MATERIALIZED VIEW IF NOT EXISTS address_txs_mv"));
        assert!(ddl.contains("TO address_txs AS\nSELECT"));
        assert!(ddl.contains("FROM txs"));
        // ARRAY JOIN tuple expansion — same reason as token_holder_deltas:
        // ClickHouse MV on UNION ALL silently drops branches.
        assert!(ddl.contains("ARRAY JOIN"));
        assert!(!ddl.contains("UNION ALL"));
        assert!(ddl.contains("'from'"));
        assert!(ddl.contains("'to'"));
        assert!(ddl.contains("IS NOT NULL"));
    }
}
