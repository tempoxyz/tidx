use super::{BackfillPolicy, ClickHouseObject, ClickHouseObjectKind};

const TOKEN_APPROVALS_SCHEMA: &str = include_str!("../../db/clickhouse/token_approvals.sql");
const TOKEN_APPROVALS_SELECT: &str = include_str!("../../db/clickhouse/token_approvals_select.sql");

pub const OBJECTS: &[ClickHouseObject] = &[
    ClickHouseObject {
        name: "token_approvals",
        kind: ClickHouseObjectKind::Table(TOKEN_APPROVALS_SCHEMA),
        depends_on: &["logs"],
        public_query: true,
        block_column: Some("block_num"),
        backfill: Some(BackfillPolicy::Ranged {
            select_sql: TOKEN_APPROVALS_SELECT,
        }),
    },
    ClickHouseObject {
        name: "token_approvals_mv",
        kind: ClickHouseObjectKind::MaterializedView {
            target_table: "token_approvals",
            select_sql: TOKEN_APPROVALS_SELECT,
        },
        depends_on: &["logs", "token_approvals"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn materialized_view_decodes_approval_logs() {
        let mv = OBJECTS
            .iter()
            .find(|object| object.name == "token_approvals_mv")
            .unwrap();
        let ddl = mv.ddl();
        assert!(ddl.starts_with("CREATE MATERIALIZED VIEW IF NOT EXISTS token_approvals_mv"));
        assert!(ddl.contains("TO token_approvals AS\nSELECT"));
        assert!(ddl.contains("FROM logs"));
        // Approval(address indexed owner, address indexed spender, uint256 value)
        // keccak256 selector
        assert!(ddl.contains("0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"));
    }

    #[test]
    fn token_approvals_backfill_lives_on_target_descriptor() {
        let table = OBJECTS
            .iter()
            .find(|object| object.name == "token_approvals")
            .unwrap();
        let Some(BackfillPolicy::Ranged { select_sql }) = table.backfill else {
            panic!("token approvals table should declare its backfill");
        };
        assert_eq!(select_sql, TOKEN_APPROVALS_SELECT);
        assert!(select_sql.contains("reinterpretAsUInt256"));
    }
}
