use super::{BackfillPolicy, ClickHouseObject, ClickHouseObjectKind};

const BLOCK_TX_COUNTS_SCHEMA: &str = include_str!("../../db/clickhouse/block_tx_counts.sql");
const BLOCK_TX_COUNTS_SELECT: &str = include_str!("../../db/clickhouse/block_tx_counts_select.sql");

pub const OBJECTS: &[ClickHouseObject] = &[
    ClickHouseObject {
        name: "block_tx_counts",
        kind: ClickHouseObjectKind::Table(BLOCK_TX_COUNTS_SCHEMA),
        depends_on: &["txs"],
        public_query: true,
        block_column: Some("block_num"),
        backfill: Some(BackfillPolicy::Ranged {
            select_sql: BLOCK_TX_COUNTS_SELECT,
        }),
    },
    ClickHouseObject {
        name: "block_tx_counts_mv",
        kind: ClickHouseObjectKind::MaterializedView {
            target_table: "block_tx_counts",
            select_sql: BLOCK_TX_COUNTS_SELECT,
        },
        depends_on: &["txs", "block_tx_counts"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn materialized_view_counts_txs_per_block() {
        let mv = OBJECTS
            .iter()
            .find(|object| object.name == "block_tx_counts_mv")
            .unwrap();
        let ddl = mv.ddl();
        assert!(ddl.starts_with("CREATE MATERIALIZED VIEW IF NOT EXISTS block_tx_counts_mv"));
        assert!(ddl.contains("TO block_tx_counts AS\nSELECT"));
        assert!(ddl.contains("FROM txs"));
        assert!(ddl.contains("count() AS tx_count"));
        assert!(ddl.contains("GROUP BY block_num, block_timestamp"));
    }

    #[test]
    fn count_table_sums_on_block_num() {
        let table = OBJECTS
            .iter()
            .find(|object| object.name == "block_tx_counts")
            .unwrap();
        assert!(table.is_table());
        // SummingMergeTree collapses the per-insert-batch counts into one total
        // per block, so concurrent backfill + realtime inserts still sum to the
        // correct count. The table is block-scoped so reorg cleanup prunes it.
        assert_eq!(table.block_column, Some("block_num"));
        let ddl = table.ddl();
        assert!(ddl.contains("ENGINE = SummingMergeTree()"));
        assert!(ddl.contains("ORDER BY (block_num)"));

        // Backfill replays the same aggregating SELECT so historical ranges are
        // repaired identically to insert-time materialization.
        let Some(BackfillPolicy::Ranged { select_sql }) = table.backfill else {
            panic!("block_tx_counts should declare its backfill");
        };
        assert_eq!(select_sql, BLOCK_TX_COUNTS_SELECT);
    }
}
