use super::{BackfillPolicy, ClickHouseObject, ClickHouseObjectKind};

const ADDRESS_TRANSFERS_SCHEMA: &str = include_str!("../../db/clickhouse/address_transfers.sql");
const ADDRESS_TRANSFERS_SELECT: &str =
    include_str!("../../db/clickhouse/address_transfers_select.sql");

pub const OBJECTS: &[ClickHouseObject] = &[
    ClickHouseObject {
        name: "address_transfers",
        kind: ClickHouseObjectKind::Table(ADDRESS_TRANSFERS_SCHEMA),
        depends_on: &["token_transfers"],
        public_query: true,
        block_column: Some("block_num"),
        backfill: Some(BackfillPolicy::Ranged {
            select_sql: ADDRESS_TRANSFERS_SELECT,
        }),
    },
    ClickHouseObject {
        name: "address_transfers_mv",
        kind: ClickHouseObjectKind::MaterializedView {
            target_table: "address_transfers",
            select_sql: ADDRESS_TRANSFERS_SELECT,
        },
        depends_on: &["token_transfers", "address_transfers"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn materialized_view_emits_in_and_out_legs_skipping_zero_address() {
        let mv = OBJECTS
            .iter()
            .find(|object| object.name == "address_transfers_mv")
            .unwrap();
        let ddl = mv.ddl();
        assert!(ddl.starts_with("CREATE MATERIALIZED VIEW IF NOT EXISTS address_transfers_mv"));
        assert!(ddl.contains("TO address_transfers AS\nSELECT"));
        assert!(ddl.contains("FROM token_transfers"));
        // Same UNION-ALL bug applies — use ARRAY JOIN tuple expansion.
        assert!(ddl.contains("ARRAY JOIN"));
        assert!(!ddl.contains("UNION ALL"));
        assert!(ddl.contains("'in'"));
        assert!(ddl.contains("'out'"));
        // The zero address rows (mint sender, burn recipient) are dropped at
        // the per-row WHERE because nobody queries 0x0 as an account.
        assert!(ddl.contains("0x0000000000000000000000000000000000000000"));
    }
}
