use super::{ClickHouseObject, ClickHouseObjectKind};

const EARN_SHARE_PRICES_SCHEMA: &str = include_str!("../../db/clickhouse/earn_share_prices.sql");

/// Confirmed historical `EarnVault.previewRedeem` observations.
///
/// Unlike insert-time derived tables, these rows require EVM execution at the
/// sampled block. The RPC-backed materializer owns population and repair; the
/// catalog owns schema creation, public query routing, and reorg cleanup.
pub const OBJECTS: &[ClickHouseObject] = &[ClickHouseObject {
    name: "earn_share_prices",
    kind: ClickHouseObjectKind::Table(EARN_SHARE_PRICES_SCHEMA),
    depends_on: &["blocks", "logs"],
    public_query: true,
    block_column: Some("block_num"),
    backfill: None,
}];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_is_public_and_block_scoped() {
        let table = &OBJECTS[0];
        assert!(table.is_table());
        assert!(table.public_query);
        assert_eq!(table.block_column, Some("block_num"));
        assert!(table.depends_on.contains(&"blocks"));
        assert!(table.depends_on.contains(&"logs"));
        assert!(table.backfill.is_none());
    }

    #[test]
    fn table_has_one_replaceable_row_per_vault_bucket() {
        let ddl = OBJECTS[0].ddl();
        assert!(ddl.contains("ReplacingMergeTree(block_num)"));
        assert!(ddl.contains("ORDER BY (vault, bucket)"));
        assert!(ddl.contains("quoted_shares    UInt256"));
        assert!(ddl.contains("quoted_assets    UInt256"));
    }
}
