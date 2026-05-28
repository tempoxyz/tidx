use super::{BackfillPolicy, ClickHouseObject, ClickHouseObjectKind};

const TOKEN_METADATA_BLOCKS_SCHEMA: &str =
    include_str!("../../db/clickhouse/token_metadata_blocks.sql");
const TOKEN_METADATA_BLOCKS_SELECT: &str =
    include_str!("../../db/clickhouse/token_metadata_blocks_select.sql");
const TOKEN_METADATA_VIEW: &str = include_str!("../../db/clickhouse/token_metadata.sql");

pub const OBJECTS: &[ClickHouseObject] = &[
    ClickHouseObject {
        name: "token_metadata_blocks",
        kind: ClickHouseObjectKind::Table(TOKEN_METADATA_BLOCKS_SCHEMA),
        depends_on: &["token_transfers"],
        public_query: false,
        block_column: Some("block_num"),
        backfill: Some(BackfillPolicy::IfEmpty {
            select_sql: TOKEN_METADATA_BLOCKS_SELECT,
        }),
    },
    ClickHouseObject {
        name: "token_metadata_blocks_mv",
        kind: ClickHouseObjectKind::MaterializedView {
            target_table: "token_metadata_blocks",
            select_sql: TOKEN_METADATA_BLOCKS_SELECT,
        },
        depends_on: &["token_transfers", "token_metadata_blocks"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_metadata",
        kind: ClickHouseObjectKind::View(TOKEN_METADATA_VIEW),
        depends_on: &["token_metadata_blocks"],
        public_query: true,
        block_column: None,
        backfill: None,
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backing_storage_is_summing_merge_tree_per_token_block() {
        let table = OBJECTS
            .iter()
            .find(|object| object.name == "token_metadata_blocks")
            .unwrap();
        assert!(table.is_table());
        assert!(table.block_column == Some("block_num"));
        let ddl = table.ddl();
        assert!(ddl.contains("ENGINE = SummingMergeTree(transfer_count)"));
        assert!(ddl.contains("ORDER BY (token, block_num)"));
    }

    #[test]
    fn materialized_view_emits_one_row_per_transfer_with_unit_count() {
        let mv = OBJECTS
            .iter()
            .find(|object| object.name == "token_metadata_blocks_mv")
            .unwrap();
        let ddl = mv.ddl();
        assert!(ddl.starts_with("CREATE MATERIALIZED VIEW IF NOT EXISTS token_metadata_blocks_mv"));
        assert!(ddl.contains("TO token_metadata_blocks AS\nSELECT"));
        assert!(ddl.contains("FROM token_transfers"));
        assert!(ddl.contains("CAST(1 AS UInt64) AS transfer_count"));
    }

    #[test]
    fn public_view_aggregates_from_backing_storage_not_token_transfers() {
        let view = OBJECTS
            .iter()
            .find(|object| object.name == "token_metadata")
            .unwrap();
        assert!(view.is_view());
        let ddl = view.ddl();
        assert!(ddl.contains("FROM token_metadata_blocks"));
        assert!(!ddl.contains("FROM token_transfers"));
        assert!(ddl.contains("min(block_num)"));
        assert!(ddl.contains("sum(transfer_count)"));
        assert!(ddl.contains("GROUP BY token"));
    }
}
