use super::{BackfillPolicy, ClickHouseObject, ClickHouseObjectKind};

const CONTRACT_CREATIONS_SCHEMA: &str = include_str!("../../db/clickhouse/contract_creations.sql");
const CONTRACT_CREATIONS_SELECT: &str =
    include_str!("../../db/clickhouse/contract_creations_select.sql");

pub const OBJECTS: &[ClickHouseObject] = &[
    ClickHouseObject {
        name: "contract_creations",
        kind: ClickHouseObjectKind::Table(CONTRACT_CREATIONS_SCHEMA),
        depends_on: &["receipts"],
        public_query: true,
        block_column: Some("block_num"),
        backfill: Some(BackfillPolicy::Ranged {
            select_sql: CONTRACT_CREATIONS_SELECT,
        }),
    },
    ClickHouseObject {
        name: "contract_creations_mv",
        kind: ClickHouseObjectKind::MaterializedView {
            target_table: "contract_creations",
            select_sql: CONTRACT_CREATIONS_SELECT,
        },
        depends_on: &["receipts", "contract_creations"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn materialized_view_filters_to_contract_creating_receipts() {
        let mv = OBJECTS
            .iter()
            .find(|object| object.name == "contract_creations_mv")
            .unwrap();
        let ddl = mv.ddl();
        assert!(ddl.starts_with("CREATE MATERIALIZED VIEW IF NOT EXISTS contract_creations_mv"));
        assert!(ddl.contains("TO contract_creations AS\nSELECT"));
        assert!(ddl.contains("FROM receipts"));
        assert!(ddl.contains("WHERE contract_address IS NOT NULL"));
    }
}
