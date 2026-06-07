use super::{BackfillPolicy, ClickHouseObject, ClickHouseObjectKind};

const TOKEN_TRANSFERS_SCHEMA: &str = include_str!("../../db/clickhouse/token_transfers.sql");
const TOKEN_TRANSFERS_SELECT: &str = include_str!("../../db/clickhouse/token_transfers_select.sql");

pub const OBJECTS: &[ClickHouseObject] = &[
    ClickHouseObject {
        name: "token_transfers",
        kind: ClickHouseObjectKind::Table(TOKEN_TRANSFERS_SCHEMA),
        depends_on: &["logs"],
        public_query: true,
        block_column: Some("block_num"),
        backfill: Some(BackfillPolicy::Ranged {
            select_sql: TOKEN_TRANSFERS_SELECT,
        }),
    },
    ClickHouseObject {
        name: "token_transfers_mv",
        kind: ClickHouseObjectKind::MaterializedView {
            target_table: "token_transfers",
            select_sql: TOKEN_TRANSFERS_SELECT,
        },
        depends_on: &["logs", "token_transfers"],
        public_query: false,
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
            .find(|object| object.name == "token_transfers_mv")
            .unwrap();
        let ddl = mv.ddl();
        assert!(ddl.starts_with("CREATE MATERIALIZED VIEW IF NOT EXISTS token_transfers_mv"));
        assert!(ddl.contains("TO token_transfers AS\nSELECT"));
        assert!(ddl.contains("FROM logs"));
    }

    #[test]
    fn token_transfers_backfill_lives_on_target_descriptor() {
        let table = OBJECTS
            .iter()
            .find(|object| object.name == "token_transfers")
            .unwrap();
        let Some(BackfillPolicy::Ranged { select_sql }) = table.backfill else {
            panic!("token transfers table should declare its backfill");
        };
        assert_eq!(select_sql, TOKEN_TRANSFERS_SELECT);
        assert!(select_sql.contains("reinterpretAsUInt256"));
    }
}
