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
const RECEIPTS_MIGRATION_20260709: &str =
    include_str!("../../db/clickhouse/migrations/20260709_add_receipts_lookup_indexes.sql");
const TXS_MIGRATION_20260709: &str =
    include_str!("../../db/clickhouse/migrations/20260709_add_txs_hash_index.sql");
const LOGS_MIGRATION_20260709: &str =
    include_str!("../../db/clickhouse/migrations/20260709_add_logs_tx_hash_index.sql");
const MATERIALIZE_RECEIPTS_INDEXES_20260709: &str =
    include_str!("../../db/clickhouse/migrations/20260709_materialize_receipts_lookup_indexes.sql");
const MATERIALIZE_TXS_INDEX_20260709: &str =
    include_str!("../../db/clickhouse/migrations/20260709_materialize_txs_hash_index.sql");
const MATERIALIZE_LOGS_INDEX_20260709: &str =
    include_str!("../../db/clickhouse/migrations/20260709_materialize_logs_tx_hash_index.sql");
const BLOCKS_MIGRATION_20260710: &str =
    include_str!("../../db/clickhouse/migrations/20260710_add_blocks_hash_index.sql");
const LOGS_MIGRATION_20260710: &str =
    include_str!("../../db/clickhouse/migrations/20260710_add_logs_topic3_index.sql");
const TXS_MIGRATION_20260710: &str =
    include_str!("../../db/clickhouse/migrations/20260710_add_txs_from_to_indexes.sql");
const RECEIPTS_MIGRATION_20260710: &str =
    include_str!("../../db/clickhouse/migrations/20260710_add_receipts_contract_address_index.sql");
const MATERIALIZE_BLOCKS_INDEX_20260710: &str =
    include_str!("../../db/clickhouse/migrations/20260710_materialize_blocks_hash_index.sql");
const MATERIALIZE_LOGS_INDEX_20260710: &str =
    include_str!("../../db/clickhouse/migrations/20260710_materialize_logs_topic3_index.sql");
const MATERIALIZE_TXS_INDEXES_20260710: &str =
    include_str!("../../db/clickhouse/migrations/20260710_materialize_txs_from_to_indexes.sql");
const MATERIALIZE_RECEIPTS_INDEX_20260710: &str = include_str!(
    "../../db/clickhouse/migrations/20260710_materialize_receipts_contract_address_index.sql"
);
const LOGS_MIGRATION_20260714: &str =
    include_str!("../../db/clickhouse/migrations/20260714_add_logs_block_num_index.sql");
const MATERIALIZE_LOGS_INDEX_20260714: &str =
    include_str!("../../db/clickhouse/migrations/20260714_materialize_logs_block_num_index.sql");
const TXS_MIGRATION_20260806: &str =
    include_str!("../../db/clickhouse/migrations/20260806_add_txs_fee_indexes.sql");
const MATERIALIZE_TXS_INDEXES_20260806: &str =
    include_str!("../../db/clickhouse/migrations/20260806_materialize_txs_fee_indexes.sql");
const TOKEN_TRANSFERS_PROJECTION_MODE_20260806: &str =
    include_str!("../../db/clickhouse/migrations/20260806_set_token_transfers_projection_mode.sql");
const TOKEN_TRANSFERS_RECIPIENT_PROJECTION_20260806: &str = include_str!(
    "../../db/clickhouse/migrations/20260806_add_token_transfers_recipient_projection.sql"
);
const MATERIALIZE_TOKEN_TRANSFERS_RECIPIENT_PROJECTION_20260806: &str = include_str!(
    "../../db/clickhouse/migrations/20260806_materialize_token_transfers_recipient_projection.sql"
);
const BLOCKS_PROJECTION_MODE_20260809: &str =
    include_str!("../../db/clickhouse/migrations/20260809_set_blocks_projection_mode.sql");
const LOGS_PROJECTION_MODE_20260809: &str =
    include_str!("../../db/clickhouse/migrations/20260809_set_logs_projection_mode.sql");
const RECEIPTS_PROJECTION_MODE_20260809: &str =
    include_str!("../../db/clickhouse/migrations/20260809_set_receipts_projection_mode.sql");
const TXS_PROJECTION_MODE_20260809: &str =
    include_str!("../../db/clickhouse/migrations/20260809_set_txs_projection_mode.sql");
const BLOCKS_MIGRATION_20260809: &str =
    include_str!("../../db/clickhouse/migrations/20260809_add_blocks_access_paths.sql");
const LOGS_MIGRATION_20260809: &str =
    include_str!("../../db/clickhouse/migrations/20260809_add_logs_access_paths.sql");
const RECEIPTS_MIGRATION_20260809: &str =
    include_str!("../../db/clickhouse/migrations/20260809_add_receipts_access_paths.sql");
const TXS_MIGRATION_20260809: &str =
    include_str!("../../db/clickhouse/migrations/20260809_add_txs_access_paths.sql");
const MATERIALIZE_BLOCKS_ACCESS_PATHS_20260809: &str =
    include_str!("../../db/clickhouse/migrations/20260809_materialize_blocks_access_paths.sql");
const MATERIALIZE_LOGS_ACCESS_PATHS_20260809: &str =
    include_str!("../../db/clickhouse/migrations/20260809_materialize_logs_access_paths.sql");
const MATERIALIZE_RECEIPTS_ACCESS_PATHS_20260809: &str =
    include_str!("../../db/clickhouse/migrations/20260809_materialize_receipts_access_paths.sql");
const MATERIALIZE_TXS_ACCESS_PATHS_20260809: &str =
    include_str!("../../db/clickhouse/migrations/20260809_materialize_txs_access_paths.sql");
const TOKEN_HOLDER_DELTAS_MIGRATION_20260618: &str =
    include_str!("../../db/clickhouse/migrations/20260618_delete_guard_token_holder_deltas.sql");
const ADDRESS_HOLDER_DELTAS_MIGRATION_20260618: &str =
    include_str!("../../db/clickhouse/migrations/20260618_delete_guard_address_holder_deltas.sql");
const REFRESH_TOKEN_BALANCES_SNAPSHOT_20260618: &str =
    include_str!("../../db/clickhouse/migrations/20260618_refresh_token_balances_snapshot.sql");
const WAIT_TOKEN_BALANCES_SNAPSHOT_20260618: &str =
    include_str!("../../db/clickhouse/migrations/20260618_wait_token_balances_snapshot.sql");
const REFRESH_TOKEN_HOLDER_COUNTS_20260618: &str =
    include_str!("../../db/clickhouse/migrations/20260618_refresh_token_holder_counts.sql");
const WAIT_TOKEN_HOLDER_COUNTS_20260618: &str =
    include_str!("../../db/clickhouse/migrations/20260618_wait_token_holder_counts.sql");
const FIX_SIGNED_TOKEN_HOLDER_DELTAS_20260704: &str =
    include_str!("../../db/clickhouse/migrations/20260704_fix_signed_token_holder_deltas.sql");
const FIX_SIGNED_ADDRESS_HOLDER_DELTAS_20260704: &str =
    include_str!("../../db/clickhouse/migrations/20260704_fix_signed_address_holder_deltas.sql");
const WIDEN_TOKEN_HOLDER_DELTAS_20260704: &str =
    include_str!("../../db/clickhouse/migrations/20260704_widen_token_holder_deltas.sql");
const WIDEN_ADDRESS_HOLDER_DELTAS_20260704: &str =
    include_str!("../../db/clickhouse/migrations/20260704_widen_address_holder_deltas.sql");
const REINSERT_OVERFLOW_TOKEN_HOLDER_DELTAS_20260704: &str = include_str!(
    "../../db/clickhouse/migrations/20260704_reinsert_overflow_token_holder_deltas.sql"
);
const REINSERT_OVERFLOW_ADDRESS_HOLDER_DELTAS_20260704: &str = include_str!(
    "../../db/clickhouse/migrations/20260704_reinsert_overflow_address_holder_deltas.sql"
);
const REFRESH_TOKEN_BALANCES_SNAPSHOT_20260704: &str =
    include_str!("../../db/clickhouse/migrations/20260704_refresh_token_balances_snapshot.sql");
const WAIT_TOKEN_BALANCES_SNAPSHOT_20260704: &str =
    include_str!("../../db/clickhouse/migrations/20260704_wait_token_balances_snapshot.sql");
const REFRESH_TOKEN_HOLDER_COUNTS_20260704: &str =
    include_str!("../../db/clickhouse/migrations/20260704_refresh_token_holder_counts.sql");
const WAIT_TOKEN_HOLDER_COUNTS_20260704: &str =
    include_str!("../../db/clickhouse/migrations/20260704_wait_token_holder_counts.sql");
const REFRESH_ADDRESS_BALANCES_SNAPSHOT_20260704: &str =
    include_str!("../../db/clickhouse/migrations/20260704_refresh_address_balances_snapshot.sql");
const WAIT_ADDRESS_BALANCES_SNAPSHOT_20260704: &str =
    include_str!("../../db/clickhouse/migrations/20260704_wait_address_balances_snapshot.sql");
const DROP_TOKEN_APPROVALS_CURRENT_20260711: &str =
    include_str!("../../db/clickhouse/migrations/20260711_drop_token_approvals_current.sql");
const DROP_TOKEN_APPROVALS_MV_20260711: &str =
    include_str!("../../db/clickhouse/migrations/20260711_drop_token_approvals_mv.sql");
const DROP_TOKEN_APPROVALS_20260711: &str =
    include_str!("../../db/clickhouse/migrations/20260711_drop_token_approvals.sql");
const DROP_ADDRESS_TRANSFERS_MV_20260711: &str =
    include_str!("../../db/clickhouse/migrations/20260711_drop_address_transfers_mv.sql");
const DROP_ADDRESS_TRANSFERS_20260711: &str =
    include_str!("../../db/clickhouse/migrations/20260711_drop_address_transfers.sql");
const DROP_ADDRESS_TXS_MV_20260711: &str =
    include_str!("../../db/clickhouse/migrations/20260711_drop_address_txs_mv.sql");
const DROP_ADDRESS_TXS_20260711: &str =
    include_str!("../../db/clickhouse/migrations/20260711_drop_address_txs.sql");
const DROP_CONTRACT_CREATIONS_MV_20260711: &str =
    include_str!("../../db/clickhouse/migrations/20260711_drop_contract_creations_mv.sql");
const DROP_CONTRACT_CREATIONS_20260711: &str =
    include_str!("../../db/clickhouse/migrations/20260711_drop_contract_creations.sql");
const DROP_BALANCE_STATE_REFRESH_20260715: &str =
    include_str!("../../db/clickhouse/migrations/20260715_drop_balance_state_refresh.sql");
const DROP_TOKEN_HOLDER_COUNTS_20260715: &str =
    include_str!("../../db/clickhouse/migrations/20260715_drop_token_holder_counts.sql");
const DROP_BALANCE_STATE_CLEAN_MV_20260715: &str =
    include_str!("../../db/clickhouse/migrations/20260715_drop_balance_state_clean_mv.sql");
const DROP_BALANCE_DIRTY_KEYS_MV_20260715: &str =
    include_str!("../../db/clickhouse/migrations/20260715_drop_balance_dirty_keys_mv.sql");
const DROP_BALANCE_STATE_20260715: &str =
    include_str!("../../db/clickhouse/migrations/20260715_drop_balance_state.sql");
const DROP_BALANCE_REORG_KEYS_20260715: &str =
    include_str!("../../db/clickhouse/migrations/20260715_drop_balance_reorg_keys.sql");
const DROP_BALANCE_DIRTY_KEYS_20260715: &str =
    include_str!("../../db/clickhouse/migrations/20260715_drop_balance_dirty_keys.sql");
const FORGET_BALANCE_STATE_BOOTSTRAP_20260715: &str =
    include_str!("../../db/clickhouse/migrations/20260715_forget_balance_state_bootstrap.sql");

/// Idempotent cleanup for objects removed from the managed catalog.
pub(super) const RETIRED_OBJECT_DROPS: &[&str] = &[
    DROP_TOKEN_APPROVALS_CURRENT_20260711,
    DROP_TOKEN_APPROVALS_MV_20260711,
    DROP_TOKEN_APPROVALS_20260711,
    DROP_ADDRESS_TRANSFERS_MV_20260711,
    DROP_ADDRESS_TRANSFERS_20260711,
    DROP_ADDRESS_TXS_MV_20260711,
    DROP_ADDRESS_TXS_20260711,
    DROP_CONTRACT_CREATIONS_MV_20260711,
    DROP_CONTRACT_CREATIONS_20260711,
    DROP_BALANCE_STATE_REFRESH_20260715,
    DROP_BALANCE_STATE_CLEAN_MV_20260715,
    DROP_BALANCE_DIRTY_KEYS_MV_20260715,
    DROP_BALANCE_STATE_20260715,
    DROP_BALANCE_REORG_KEYS_20260715,
    DROP_BALANCE_DIRTY_KEYS_20260715,
    FORGET_BALANCE_STATE_BOOTSTRAP_20260715,
];

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
    ClickHouseObject {
        name: "receipts_20260709_lookup_indexes",
        kind: ClickHouseObjectKind::Migration(RECEIPTS_MIGRATION_20260709),
        depends_on: &["receipts"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "txs_20260709_hash_index",
        kind: ClickHouseObjectKind::Migration(TXS_MIGRATION_20260709),
        depends_on: &["txs"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "logs_20260709_tx_hash_index",
        kind: ClickHouseObjectKind::Migration(LOGS_MIGRATION_20260709),
        depends_on: &["logs"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "receipts_20260709_materialize_lookup_indexes",
        kind: ClickHouseObjectKind::Migration(MATERIALIZE_RECEIPTS_INDEXES_20260709),
        depends_on: &["receipts"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "txs_20260709_materialize_hash_index",
        kind: ClickHouseObjectKind::Migration(MATERIALIZE_TXS_INDEX_20260709),
        depends_on: &["txs"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "logs_20260709_materialize_tx_hash_index",
        kind: ClickHouseObjectKind::Migration(MATERIALIZE_LOGS_INDEX_20260709),
        depends_on: &["logs"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "blocks_20260710_hash_index",
        kind: ClickHouseObjectKind::Migration(BLOCKS_MIGRATION_20260710),
        depends_on: &["blocks"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "logs_20260710_topic3_index",
        kind: ClickHouseObjectKind::Migration(LOGS_MIGRATION_20260710),
        depends_on: &["logs"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "txs_20260710_from_to_indexes",
        kind: ClickHouseObjectKind::Migration(TXS_MIGRATION_20260710),
        depends_on: &["txs"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "receipts_20260710_contract_address_index",
        kind: ClickHouseObjectKind::Migration(RECEIPTS_MIGRATION_20260710),
        depends_on: &["receipts"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "blocks_20260710_materialize_hash_index",
        kind: ClickHouseObjectKind::Migration(MATERIALIZE_BLOCKS_INDEX_20260710),
        depends_on: &["blocks"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "logs_20260710_materialize_topic3_index",
        kind: ClickHouseObjectKind::Migration(MATERIALIZE_LOGS_INDEX_20260710),
        depends_on: &["logs"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "txs_20260710_materialize_from_to_indexes",
        kind: ClickHouseObjectKind::Migration(MATERIALIZE_TXS_INDEXES_20260710),
        depends_on: &["txs"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "receipts_20260710_materialize_contract_address_index",
        kind: ClickHouseObjectKind::Migration(MATERIALIZE_RECEIPTS_INDEX_20260710),
        depends_on: &["receipts"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "logs_20260714_block_num_index",
        kind: ClickHouseObjectKind::Migration(LOGS_MIGRATION_20260714),
        depends_on: &["logs"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "logs_20260714_materialize_block_num_index",
        kind: ClickHouseObjectKind::Migration(MATERIALIZE_LOGS_INDEX_20260714),
        depends_on: &["logs_20260714_block_num_index"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    // PR #271 made token_holder_counts a refresh-dependent child of
    // token_balances_snapshot. Stop the high-frequency reducer and remove that
    // child before derived-object reconciliation replaces the snapshot.
    ClickHouseObject {
        name: "balance_state_20260715_stop_refresh",
        kind: ClickHouseObjectKind::Migration(DROP_BALANCE_STATE_REFRESH_20260715),
        depends_on: &[],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_holder_counts_20260715_drop_before_snapshot_replace",
        kind: ClickHouseObjectKind::Migration(DROP_TOKEN_HOLDER_COUNTS_20260715),
        depends_on: &[],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "txs_20260806_fee_indexes",
        kind: ClickHouseObjectKind::Migration(TXS_MIGRATION_20260806),
        depends_on: &["txs"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "txs_20260806_materialize_fee_indexes",
        kind: ClickHouseObjectKind::Migration(MATERIALIZE_TXS_INDEXES_20260806),
        depends_on: &["txs_20260806_fee_indexes"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "blocks_20260809_projection_mode",
        kind: ClickHouseObjectKind::Migration(BLOCKS_PROJECTION_MODE_20260809),
        depends_on: &["blocks"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "logs_20260809_projection_mode",
        kind: ClickHouseObjectKind::Migration(LOGS_PROJECTION_MODE_20260809),
        depends_on: &["logs"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "receipts_20260809_projection_mode",
        kind: ClickHouseObjectKind::Migration(RECEIPTS_PROJECTION_MODE_20260809),
        depends_on: &["receipts"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "txs_20260809_projection_mode",
        kind: ClickHouseObjectKind::Migration(TXS_PROJECTION_MODE_20260809),
        depends_on: &["txs"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "blocks_20260809_access_paths",
        kind: ClickHouseObjectKind::Migration(BLOCKS_MIGRATION_20260809),
        depends_on: &["blocks_20260809_projection_mode"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "logs_20260809_access_paths",
        kind: ClickHouseObjectKind::Migration(LOGS_MIGRATION_20260809),
        depends_on: &["logs_20260809_projection_mode"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "receipts_20260809_access_paths",
        kind: ClickHouseObjectKind::Migration(RECEIPTS_MIGRATION_20260809),
        depends_on: &["receipts_20260809_projection_mode"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "txs_20260809_access_paths",
        kind: ClickHouseObjectKind::Migration(TXS_MIGRATION_20260809),
        depends_on: &["txs_20260809_projection_mode"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "blocks_20260809_materialize_access_paths",
        kind: ClickHouseObjectKind::Migration(MATERIALIZE_BLOCKS_ACCESS_PATHS_20260809),
        depends_on: &["blocks_20260809_access_paths"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "logs_20260809_materialize_access_paths",
        kind: ClickHouseObjectKind::Migration(MATERIALIZE_LOGS_ACCESS_PATHS_20260809),
        depends_on: &["logs_20260809_access_paths"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "receipts_20260809_materialize_access_paths",
        kind: ClickHouseObjectKind::Migration(MATERIALIZE_RECEIPTS_ACCESS_PATHS_20260809),
        depends_on: &["receipts_20260809_access_paths"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "txs_20260809_materialize_access_paths",
        kind: ClickHouseObjectKind::Migration(MATERIALIZE_TXS_ACCESS_PATHS_20260809),
        depends_on: &["txs_20260809_access_paths"],
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
    // holder before these migrations are recorded as applied. Order matters:
    // refresh + wait the snapshot first, then the counts that read from it.
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
    ClickHouseObject {
        name: "token_holder_deltas_20260704_fix_signed",
        kind: ClickHouseObjectKind::Migration(FIX_SIGNED_TOKEN_HOLDER_DELTAS_20260704),
        depends_on: &["token_holder_deltas"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_holder_deltas_20260704_widen_uint256",
        kind: ClickHouseObjectKind::Migration(WIDEN_TOKEN_HOLDER_DELTAS_20260704),
        depends_on: &["token_holder_deltas"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_holder_deltas_20260704_reinsert_overflow",
        kind: ClickHouseObjectKind::Migration(REINSERT_OVERFLOW_TOKEN_HOLDER_DELTAS_20260704),
        depends_on: &["token_holder_deltas", "token_transfers"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "address_holder_deltas_20260704_fix_signed",
        kind: ClickHouseObjectKind::Migration(FIX_SIGNED_ADDRESS_HOLDER_DELTAS_20260704),
        depends_on: &["address_holder_deltas"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "address_holder_deltas_20260704_widen_uint256",
        kind: ClickHouseObjectKind::Migration(WIDEN_ADDRESS_HOLDER_DELTAS_20260704),
        depends_on: &["address_holder_deltas"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "address_holder_deltas_20260704_reinsert_overflow",
        kind: ClickHouseObjectKind::Migration(REINSERT_OVERFLOW_ADDRESS_HOLDER_DELTAS_20260704),
        depends_on: &["address_holder_deltas", "token_transfers"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_balances_snapshot_20260704_refresh_after_sign_fix",
        kind: ClickHouseObjectKind::Migration(REFRESH_TOKEN_BALANCES_SNAPSHOT_20260704),
        depends_on: &["token_balances_snapshot"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_balances_snapshot_20260704_wait_after_sign_fix",
        kind: ClickHouseObjectKind::Migration(WAIT_TOKEN_BALANCES_SNAPSHOT_20260704),
        depends_on: &["token_balances_snapshot"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_holder_counts_20260704_refresh_after_sign_fix",
        kind: ClickHouseObjectKind::Migration(REFRESH_TOKEN_HOLDER_COUNTS_20260704),
        depends_on: &["token_holder_counts"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_holder_counts_20260704_wait_after_sign_fix",
        kind: ClickHouseObjectKind::Migration(WAIT_TOKEN_HOLDER_COUNTS_20260704),
        depends_on: &["token_holder_counts"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "address_balances_snapshot_20260704_refresh_after_sign_fix",
        kind: ClickHouseObjectKind::Migration(REFRESH_ADDRESS_BALANCES_SNAPSHOT_20260704),
        depends_on: &["address_balances_snapshot"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "address_balances_snapshot_20260704_wait_after_sign_fix",
        kind: ClickHouseObjectKind::Migration(WAIT_ADDRESS_BALANCES_SNAPSHOT_20260704),
        depends_on: &["address_balances_snapshot"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_transfers_20260806_projection_mode",
        kind: ClickHouseObjectKind::Migration(TOKEN_TRANSFERS_PROJECTION_MODE_20260806),
        depends_on: &["token_transfers"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_transfers_20260806_recipient_projection",
        kind: ClickHouseObjectKind::Migration(TOKEN_TRANSFERS_RECIPIENT_PROJECTION_20260806),
        depends_on: &["token_transfers_20260806_projection_mode"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "token_transfers_20260806_materialize_recipient_projection",
        kind: ClickHouseObjectKind::Migration(
            MATERIALIZE_TOKEN_TRANSFERS_RECIPIENT_PROJECTION_20260806,
        ),
        depends_on: &["token_transfers_20260806_recipient_projection"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
];
