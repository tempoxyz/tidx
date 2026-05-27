use super::{BackfillPolicy, ClickHouseObject, ClickHouseObjectKind};

const TOKEN_HOLDER_DELTAS_SCHEMA: &str =
    include_str!("../../db/clickhouse/token_holder_deltas.sql");
const TOKEN_HOLDER_DELTAS_SELECT: &str =
    include_str!("../../db/clickhouse/token_holder_deltas_select.sql");
const TOKEN_HOLDERS_VIEW: &str = include_str!("../../db/clickhouse/token_holders.sql");

pub const OBJECTS: &[ClickHouseObject] = &[
    ClickHouseObject {
        name: "token_holder_deltas",
        kind: ClickHouseObjectKind::Static(TOKEN_HOLDER_DELTAS_SCHEMA),
        depends_on: &["token_transfer_events"],
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
        depends_on: &["token_transfer_events", "token_holder_deltas"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_holders",
        kind: ClickHouseObjectKind::Static(TOKEN_HOLDERS_VIEW),
        depends_on: &["token_holder_deltas"],
        public_query: true,
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
        assert!(ddl.contains("FROM token_transfer_events"));
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
        assert!(select_sql.contains("GROUP BY block_num"));
    }
}
