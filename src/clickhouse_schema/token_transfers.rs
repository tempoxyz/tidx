use super::{BackfillPolicy, ClickHouseObject, ClickHouseObjectKind};

const TOKEN_TRANSFER_EVENTS_SCHEMA: &str =
    include_str!("../../db/clickhouse/token_transfer_events.sql");
const TOKEN_TRANSFER_EVENTS_SELECT: &str =
    include_str!("../../db/clickhouse/token_transfer_events_select.sql");

pub const OBJECTS: &[ClickHouseObject] = &[
    ClickHouseObject {
        name: "token_transfer_events",
        kind: ClickHouseObjectKind::Table(TOKEN_TRANSFER_EVENTS_SCHEMA),
        depends_on: &["logs"],
        public_query: true,
        block_column: Some("block_num"),
        backfill: Some(BackfillPolicy::IfEmpty {
            select_sql: TOKEN_TRANSFER_EVENTS_SELECT,
        }),
    },
    ClickHouseObject {
        name: "token_transfer_events_mv",
        kind: ClickHouseObjectKind::MaterializedView {
            target_table: "token_transfer_events",
            select_sql: TOKEN_TRANSFER_EVENTS_SELECT,
        },
        depends_on: &["logs", "token_transfer_events"],
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
            .find(|object| object.name == "token_transfer_events_mv")
            .unwrap();
        let ddl = mv.ddl();
        assert!(ddl.starts_with("CREATE MATERIALIZED VIEW IF NOT EXISTS token_transfer_events_mv"));
        assert!(ddl.contains("TO token_transfer_events AS\nSELECT"));
        assert!(ddl.contains("FROM logs"));
    }

    #[test]
    fn token_transfer_events_backfill_lives_on_target_descriptor() {
        let table = OBJECTS
            .iter()
            .find(|object| object.name == "token_transfer_events")
            .unwrap();
        let Some(BackfillPolicy::IfEmpty { select_sql }) = table.backfill else {
            panic!("token transfer events table should declare its backfill");
        };
        assert_eq!(select_sql, TOKEN_TRANSFER_EVENTS_SELECT);
        assert!(select_sql.contains("reinterpretAsUInt256"));
    }
}
