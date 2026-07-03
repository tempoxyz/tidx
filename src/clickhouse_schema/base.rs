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
const TOKEN_HOLDER_DELTAS_MIGRATION_20260618: &str =
    include_str!("../../db/clickhouse/migrations/20260618_delete_guard_token_holder_deltas.sql");
const ADDRESS_HOLDER_DELTAS_MIGRATION_20260618: &str =
    include_str!("../../db/clickhouse/migrations/20260618_delete_guard_address_holder_deltas.sql");
const REFRESH_ADDRESS_BALANCES_SNAPSHOT_20260618: &str =
    include_str!("../../db/clickhouse/migrations/20260618_refresh_address_balances_snapshot.sql");
const WAIT_ADDRESS_BALANCES_SNAPSHOT_20260618: &str =
    include_str!("../../db/clickhouse/migrations/20260618_wait_address_balances_snapshot.sql");
const REFRESH_TOKEN_BALANCES_SNAPSHOT_20260618: &str =
    include_str!("../../db/clickhouse/migrations/20260618_refresh_token_balances_snapshot.sql");
const WAIT_TOKEN_BALANCES_SNAPSHOT_20260618: &str =
    include_str!("../../db/clickhouse/migrations/20260618_wait_token_balances_snapshot.sql");
const REFRESH_TOKEN_HOLDER_COUNTS_20260618: &str =
    include_str!("../../db/clickhouse/migrations/20260618_refresh_token_holder_counts.sql");
const WAIT_TOKEN_HOLDER_COUNTS_20260618: &str =
    include_str!("../../db/clickhouse/migrations/20260618_wait_token_holder_counts.sql");

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

/// One-shot migrations that mutate derived tables, so they run after those
/// tables are created. Tracked and replayed idempotently like `MIGRATIONS`.
pub const POST_DERIVED_MIGRATIONS: &[ClickHouseObject] = &[
    ClickHouseObject {
        name: "token_holder_deltas_20260618_drop_guard_rows",
        kind: ClickHouseObjectKind::Migration(TOKEN_HOLDER_DELTAS_MIGRATION_20260618),
        depends_on: &["token_holder_deltas"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "address_holder_deltas_20260618_drop_guard_rows",
        kind: ClickHouseObjectKind::Migration(ADDRESS_HOLDER_DELTAS_MIGRATION_20260618),
        depends_on: &["address_holder_deltas"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    // After deleting the guard rows above, force the refreshable holder-balance
    // aggregates to rebuild so the public surface stops serving the stale guard
    // holder before these migrations are recorded as applied.
    ClickHouseObject {
        name: "address_balances_snapshot_20260618_refresh_after_guard_delete",
        kind: ClickHouseObjectKind::Migration(REFRESH_ADDRESS_BALANCES_SNAPSHOT_20260618),
        depends_on: &["address_balances_snapshot"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "address_balances_snapshot_20260618_wait_after_guard_delete",
        kind: ClickHouseObjectKind::Migration(WAIT_ADDRESS_BALANCES_SNAPSHOT_20260618),
        depends_on: &["address_balances_snapshot"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_balances_snapshot_20260618_refresh_after_guard_delete",
        kind: ClickHouseObjectKind::Migration(REFRESH_TOKEN_BALANCES_SNAPSHOT_20260618),
        depends_on: &["token_balances_snapshot"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_balances_snapshot_20260618_wait_after_guard_delete",
        kind: ClickHouseObjectKind::Migration(WAIT_TOKEN_BALANCES_SNAPSHOT_20260618),
        depends_on: &["token_balances_snapshot"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_holder_counts_20260618_refresh_after_guard_delete",
        kind: ClickHouseObjectKind::Migration(REFRESH_TOKEN_HOLDER_COUNTS_20260618),
        depends_on: &["token_holder_counts"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_holder_counts_20260618_wait_after_guard_delete",
        kind: ClickHouseObjectKind::Migration(WAIT_TOKEN_HOLDER_COUNTS_20260618),
        depends_on: &["token_holder_counts"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
];
