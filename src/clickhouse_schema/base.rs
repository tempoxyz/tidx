use super::{ClickHouseObject, ClickHouseObjectKind};

const BLOCKS_SCHEMA: &str = include_str!("../../db/clickhouse/blocks.sql");
const TXS_SCHEMA: &str = include_str!("../../db/clickhouse/txs.sql");
const LOGS_SCHEMA: &str = include_str!("../../db/clickhouse/logs.sql");
const RECEIPTS_SCHEMA: &str = include_str!("../../db/clickhouse/receipts.sql");

const LOGS_MIGRATION_20260416: &str =
    include_str!("../../db/clickhouse/migrations/20260416_add_is_virtual_forward.sql");
const LOGS_MIGRATION_20260417: &str =
    include_str!("../../db/clickhouse/migrations/20260417_add_logs_virtual_forward_index.sql");
const BLOCKS_MIGRATION_20260430: &str =
    include_str!("../../db/clickhouse/migrations/20260430_add_blocks_consensus_proposer.sql");
const RECEIPTS_MIGRATION_20260604: &str =
    include_str!("../../db/clickhouse/migrations/20260604_add_receipts_type_fee_token.sql");

pub const TABLES: &[ClickHouseObject] = &[
    ClickHouseObject {
        name: "blocks",
        kind: ClickHouseObjectKind::Table(BLOCKS_SCHEMA),
        depends_on: &[],
        public_query: true,
        block_column: Some("num"),
        backfill: None,
    },
    ClickHouseObject {
        name: "txs",
        kind: ClickHouseObjectKind::Table(TXS_SCHEMA),
        depends_on: &["blocks"],
        public_query: true,
        block_column: Some("block_num"),
        backfill: None,
    },
    ClickHouseObject {
        name: "logs",
        kind: ClickHouseObjectKind::Table(LOGS_SCHEMA),
        depends_on: &["blocks", "txs"],
        public_query: true,
        block_column: Some("block_num"),
        backfill: None,
    },
    ClickHouseObject {
        name: "receipts",
        kind: ClickHouseObjectKind::Table(RECEIPTS_SCHEMA),
        depends_on: &["blocks", "txs"],
        public_query: true,
        block_column: Some("block_num"),
        backfill: None,
    },
];

pub const MIGRATIONS: &[ClickHouseObject] = &[
    ClickHouseObject {
        name: "logs_20260416_is_virtual_forward",
        kind: ClickHouseObjectKind::Migration(LOGS_MIGRATION_20260416),
        depends_on: &["logs"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "logs_20260417_virtual_forward_index",
        kind: ClickHouseObjectKind::Migration(LOGS_MIGRATION_20260417),
        depends_on: &["logs"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "blocks_20260430_consensus_proposer",
        kind: ClickHouseObjectKind::Migration(BLOCKS_MIGRATION_20260430),
        depends_on: &["blocks"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "receipts_20260604_type_fee_token",
        kind: ClickHouseObjectKind::Migration(RECEIPTS_MIGRATION_20260604),
        depends_on: &["receipts"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
];
