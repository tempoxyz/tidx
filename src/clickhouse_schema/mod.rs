mod address_balances;
mod base;
mod catalog;
mod dex;
mod token_balances;
mod token_metadata;
mod token_supply;
mod token_transfer_stats;
mod token_transfers;

pub use catalog::{BackfillPolicy, BlockScopedTable, ClickHouseObject, ClickHouseObjectKind};

pub fn base_objects() -> &'static [ClickHouseObject] {
    base::TABLES
}

pub fn migrations() -> &'static [ClickHouseObject] {
    base::MIGRATIONS
}

pub(crate) fn retired_object_drops() -> &'static [&'static str] {
    base::RETIRED_OBJECT_DROPS
}

/// One-shot migrations applied after `derived_objects()`, since they mutate
/// derived tables.
pub fn post_derived_migrations() -> &'static [ClickHouseObject] {
    base::POST_DERIVED_MIGRATIONS
}

pub fn derived_objects() -> impl DoubleEndedIterator<Item = &'static ClickHouseObject> {
    // Order matters: each object's `depends_on` must reference an object that
    // appears earlier in this iterator (validated by tests). `reorg_tables`
    // reverses this order so dependents are pruned before their sources.
    token_transfers::OBJECTS
        .iter()
        .chain(token_balances::OBJECTS.iter())
        .chain(token_supply::OBJECTS.iter())
        .chain(token_metadata::OBJECTS.iter())
        .chain(token_transfer_stats::OBJECTS.iter())
        .chain(address_balances::OBJECTS.iter())
        .chain(dex::OBJECTS.iter())
}

/// Tables and views that the public `/query` HTTP surface may reference.
pub fn public_query_objects() -> impl Iterator<Item = &'static ClickHouseObject> {
    base_objects()
        .iter()
        .chain(derived_objects())
        .filter(|object| object.public_query)
}

/// Every catalog object — used to validate sink-internal table/view names.
pub fn all_objects() -> impl Iterator<Item = &'static ClickHouseObject> {
    base_objects()
        .iter()
        .chain(migrations().iter())
        .chain(derived_objects())
        .chain(post_derived_migrations().iter())
}

pub fn derived_backfills() -> impl Iterator<Item = &'static ClickHouseObject> {
    derived_objects().filter(|object| object.backfill.is_some())
}

pub fn reorg_tables() -> impl Iterator<Item = BlockScopedTable> {
    derived_objects()
        .rev()
        .chain(base_objects())
        .filter_map(BlockScopedTable::from_object)
}

pub fn block_column(table: &str) -> Option<&'static str> {
    public_query_objects()
        .find(|object| object.name == table)
        .and_then(|object| object.block_column)
}

/// True for any catalog object (table, view, materialized view, migration).
/// Used by the sink to validate names it interpolates into DDL/queries.
pub fn is_known_table(table: &str) -> bool {
    all_objects().any(|object| object.name == table)
}

/// True for tables/views in the public query allowlist.
pub fn is_public_query_table(table: &str) -> bool {
    public_query_objects().any(|object| object.name == table)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_holder_tables_are_registered_for_routing() {
        assert!(is_public_query_table("token_transfers"));
        assert!(is_known_table("token_transfers"));
        assert_eq!(block_column("token_transfers"), Some("block_num"));
        assert!(is_public_query_table("token_balances"));
        // Pre-aggregated holder balances refreshed on a schedule — public so the
        // /query surface and Cadent can read it instead of re-aggregating deltas.
        assert!(is_public_query_table("token_balances_snapshot"));
        assert!(is_known_table("token_balances_snapshot"));
        assert!(is_known_table("token_holder_deltas"));
        assert_eq!(block_column("token_holder_deltas"), Some("block_num"));
    }

    #[test]
    fn aggregate_objects_are_registered_for_public_query() {
        assert!(is_public_query_table("token_supply"));
        assert!(is_public_query_table("token_transfer_stats"));
        assert!(is_public_query_table("token_metadata"));
        // Per-token holder counts, refreshed on a schedule — public so Cadent
        // reads one summed row per token instead of counting snapshot rows.
        assert!(is_public_query_table("token_holder_counts"));
    }

    #[test]
    fn address_keyed_objects_are_registered_for_public_query() {
        assert!(is_public_query_table("address_holder_deltas"));
        assert_eq!(block_column("address_holder_deltas"), Some("block_num"));
        assert!(is_public_query_table("address_balances"));
        assert!(is_public_query_table("address_balances_snapshot"));
    }

    #[test]
    fn dex_decoded_tables_are_registered_for_public_query() {
        // Pre-decoded stablecoin-DEX event tables so the exchange endpoints read
        // sort-key seeks + a plain join instead of re-decoding `logs` per request.
        for table in ["dex_pairs", "dex_orders", "dex_fills"] {
            assert!(is_public_query_table(table), "{table} should be public");
            assert_eq!(block_column(table), Some("block_num"));
        }
        // Pre-bucketed 1m OHLC candles, refreshed on a schedule — public so the
        // exchange OHLC endpoint reads candles instead of scanning raw fills.
        assert!(is_public_query_table("dex_ohlc_1m"));
        // Pairs joined to their DEX-escrow base liquidity — public so the
        // "pairs by liquidity" endpoint reads ranked pairs directly.
        assert!(is_public_query_table("dex_pair_liquidity"));
    }

    #[test]
    fn materialized_views_are_known_but_not_public_query_tables() {
        for mv in [
            "token_transfers_mv",
            "token_holder_deltas_mv",
            "address_holder_deltas_mv",
            "dex_pairs_mv",
            "dex_orders_mv",
            "dex_fills_mv",
        ] {
            assert!(!is_public_query_table(mv), "{mv} should not be public");
            assert!(is_known_table(mv), "{mv} should be known to the sink");
        }
    }

    #[test]
    fn reorg_tables_delete_derived_tables_before_base_logs() {
        let tables: Vec<_> = reorg_tables().map(|table| table.name).collect();
        let position = |name: &str| {
            tables
                .iter()
                .position(|t| *t == name)
                .unwrap_or_else(|| panic!("{name} not in reorg list"))
        };
        // Derived tables that read from token_transfers must be pruned first.
        let logs = position("logs");
        let token_transfers = position("token_transfers");
        let token_holder_deltas = position("token_holder_deltas");
        let address_holder_deltas = position("address_holder_deltas");

        // token_transfers consumers prune before it
        assert!(token_holder_deltas < token_transfers);
        assert!(address_holder_deltas < token_transfers);

        // token_transfers prunes before its source `logs`.
        assert!(token_transfers < logs);
    }

    #[test]
    fn removed_objects_are_not_registered() {
        for name in [
            "address_transfers",
            "address_transfers_mv",
            "address_txs",
            "address_txs_mv",
            "contract_creations",
            "contract_creations_mv",
            "token_approvals",
            "token_approvals_current",
            "token_approvals_mv",
            "balance_dirty_keys",
            "balance_dirty_keys_mv",
            "balance_reorg_keys",
            "balance_state",
            "balance_state_clean_mv",
            "balance_state_refresh",
        ] {
            assert!(!is_known_table(name), "{name} should be removed");
            assert!(!is_public_query_table(name), "{name} should not be public");
        }
    }

    #[test]
    fn retired_objects_drop_dependents_first() {
        let drops: Vec<_> = retired_object_drops()
            .iter()
            .map(|ddl| ddl.trim())
            .collect();

        assert_eq!(
            drops,
            [
                "DROP VIEW IF EXISTS token_approvals_current SYNC",
                "DROP VIEW IF EXISTS token_approvals_mv SYNC",
                "DROP TABLE IF EXISTS token_approvals SYNC",
                "DROP VIEW IF EXISTS address_transfers_mv SYNC",
                "DROP TABLE IF EXISTS address_transfers SYNC",
                "DROP VIEW IF EXISTS address_txs_mv SYNC",
                "DROP TABLE IF EXISTS address_txs SYNC",
                "DROP VIEW IF EXISTS contract_creations_mv SYNC",
                "DROP TABLE IF EXISTS contract_creations SYNC",
                "DROP VIEW IF EXISTS balance_state_refresh SYNC",
                "DROP VIEW IF EXISTS balance_state_clean_mv SYNC",
                "DROP VIEW IF EXISTS balance_dirty_keys_mv SYNC",
                "DROP TABLE IF EXISTS balance_state SYNC",
                "DROP TABLE IF EXISTS balance_reorg_keys SYNC",
                "DROP TABLE IF EXISTS balance_dirty_keys SYNC",
                "ALTER TABLE tidx_schema_objects\n    DELETE WHERE name = \
                 'balance_state_20260714_bootstrap'\n    SETTINGS mutations_sync = 1",
            ]
        );
    }

    #[test]
    fn descriptors_name_dependencies_that_exist() {
        let names: Vec<_> = all_objects().map(|object| object.name).collect();

        for object in all_objects() {
            for dependency in object.depends_on {
                assert!(
                    names.contains(dependency),
                    "{} depends on unknown object {}",
                    object.name,
                    dependency
                );
            }
        }
    }

    #[test]
    fn refresh_dependencies_use_supported_schedule() {
        for object in derived_objects() {
            let ddl = object.ddl();
            if ddl.contains("DEPENDS ON") {
                assert!(
                    ddl.contains("REFRESH EVERY"),
                    "{} uses DEPENDS ON without REFRESH EVERY",
                    object.name
                );
                assert!(
                    !ddl.contains("REFRESH AFTER"),
                    "{} uses unsupported REFRESH AFTER with DEPENDS ON",
                    object.name
                );
            }
        }
    }

    #[test]
    fn dependency_array_order_is_consistent_with_topo_order() {
        // ensure_schema() iterates base_objects -> migrations -> derived_objects
        // in array order. Assert each object's depends_on resolves to an object
        // declared earlier in that sequence.
        let mut seen: Vec<&str> = Vec::new();
        for object in all_objects() {
            for dep in object.depends_on {
                assert!(
                    seen.contains(dep),
                    "{} depends on {} but {} is declared later",
                    object.name,
                    dep,
                    dep
                );
            }
            seen.push(object.name);
        }
    }

    #[test]
    fn logs_block_range_index_is_created_and_materialized() {
        let logs = base_objects()
            .iter()
            .find(|object| object.name == "logs")
            .expect("logs table should be registered")
            .ddl();
        assert!(logs.contains("INDEX idx_block_num block_num TYPE minmax GRANULARITY 1"));

        let add = migrations()
            .iter()
            .find(|object| object.name == "logs_20260714_block_num_index")
            .expect("logs block number index migration should be registered")
            .ddl();
        assert!(add.contains("ADD INDEX IF NOT EXISTS idx_block_num"));

        let materialize = migrations()
            .iter()
            .find(|object| object.name == "logs_20260714_materialize_block_num_index")
            .expect("logs block number index materialization should be registered")
            .ddl();
        assert!(materialize.contains("MATERIALIZE INDEX idx_block_num"));
    }

    #[test]
    fn txs_fee_indexes_are_created_and_materialized() {
        let txs = base_objects()
            .iter()
            .find(|object| object.name == "txs")
            .expect("txs table should be registered")
            .ddl();
        assert!(txs.contains("INDEX idx_fee_payer fee_payer TYPE bloom_filter GRANULARITY 1"));
        assert!(txs.contains("INDEX idx_fee_token fee_token TYPE bloom_filter GRANULARITY 1"));

        let add = migrations()
            .iter()
            .find(|object| object.name == "txs_20260806_fee_indexes")
            .expect("txs fee index migration should be registered")
            .ddl();
        assert!(add.contains("ADD INDEX IF NOT EXISTS idx_fee_payer"));
        assert!(add.contains("ADD INDEX IF NOT EXISTS idx_fee_token"));

        let materialize = migrations()
            .iter()
            .find(|object| object.name == "txs_20260806_materialize_fee_indexes")
            .expect("txs fee index materialization should be registered")
            .ddl();
        assert!(materialize.contains("MATERIALIZE INDEX idx_fee_payer"));
        assert!(materialize.contains("MATERIALIZE INDEX idx_fee_token"));
    }

    #[test]
    fn lookup_access_paths_are_created_and_materialized() {
        let table = |name| {
            base_objects()
                .iter()
                .find(|object| object.name == name)
                .unwrap_or_else(|| panic!("{name} table should be registered"))
                .ddl()
        };

        for name in ["blocks", "txs", "receipts", "logs"] {
            let ddl = table(name);
            assert!(ddl.contains("deduplicate_merge_projection_mode = 'rebuild'"));
        }

        for (name, fragments) in [
            (
                "blocks",
                &[
                    "PROJECTION prj_hash INDEX hash TYPE basic",
                    "PROJECTION prj_timestamp_position INDEX (timestamp, num) TYPE basic",
                ][..],
            ),
            (
                "txs",
                &[
                    "INDEX idx_from_nonce_key_nonce (`from`, nonce_key, nonce)",
                    "PROJECTION prj_from_position INDEX (`from`, block_num, idx) TYPE basic",
                    "PROJECTION prj_to_position INDEX (`to`, block_num, idx) TYPE basic",
                    "PROJECTION prj_fee_payer_position INDEX (fee_payer, block_num, idx) TYPE basic",
                    "PROJECTION prj_fee_token_position INDEX (fee_token, block_num, idx) TYPE basic",
                ][..],
            ),
            (
                "receipts",
                &[
                    "INDEX idx_to        `to`      TYPE bloom_filter(0.01)",
                    "PROJECTION prj_tx_hash INDEX tx_hash TYPE basic",
                    "PROJECTION prj_fee_payer_position INDEX (fee_payer, block_num, tx_idx) TYPE basic",
                ][..],
            ),
            (
                "logs",
                &[
                    "INDEX idx_selector_topic1 (selector, topic1) TYPE bloom_filter(0.01)",
                    "INDEX idx_selector_topic2 (selector, topic2) TYPE bloom_filter(0.01)",
                    "INDEX idx_selector_topic3 (selector, topic3) TYPE bloom_filter(0.01)",
                    "PROJECTION prj_address_position INDEX (address, block_num, log_idx) TYPE basic",
                    "INDEX (selector, address, block_num, log_idx) TYPE basic",
                    "INDEX (selector, topic1, block_num, log_idx) TYPE basic",
                    "INDEX (selector, topic2, block_num, log_idx) TYPE basic",
                    "INDEX (selector, topic3, block_num, log_idx) TYPE basic",
                    "PROJECTION prj_tx_hash INDEX tx_hash TYPE basic",
                ][..],
            ),
        ] {
            let ddl = table(name);
            for fragment in fragments {
                assert!(ddl.contains(fragment), "{name} is missing `{fragment}`");
            }
        }

        for name in ["blocks", "txs", "receipts", "logs"] {
            let setting = migrations()
                .iter()
                .find(|object| object.name == format!("{name}_20260809_projection_rebuild"))
                .unwrap_or_else(|| panic!("{name} projection rebuild migration should exist"))
                .ddl();
            assert!(setting.contains("deduplicate_merge_projection_mode = 'rebuild'"));

            let add = migrations()
                .iter()
                .find(|object| object.name == format!("{name}_20260809_access_paths"))
                .unwrap_or_else(|| panic!("{name} access path migration should exist"))
                .ddl();
            assert!(add.contains("ADD PROJECTION IF NOT EXISTS"));

            let materialize = migrations()
                .iter()
                .find(|object| object.name == format!("{name}_20260809_materialize_access_paths"))
                .unwrap_or_else(|| panic!("{name} access path materialization should exist"))
                .ddl();
            assert!(materialize.contains("MATERIALIZE PROJECTION"));
        }
    }

    #[test]
    fn post_derived_migrations_run_after_their_target_tables() {
        // all_objects() is the apply order; each migration must come after the
        // table it mutates.
        let order: Vec<&str> = all_objects().map(|object| object.name).collect();
        let position = |name: &str| {
            order
                .iter()
                .position(|n| *n == name)
                .unwrap_or_else(|| panic!("{name} not in all_objects()"))
        };

        for (migration_name, target) in [
            (
                "token_holder_deltas_20260618_drop_guard_rows",
                "token_holder_deltas",
            ),
            (
                "address_holder_deltas_20260618_drop_guard_rows",
                "address_holder_deltas",
            ),
            (
                "token_balances_snapshot_20260618_refresh_after_guard_delete",
                "token_balances_snapshot",
            ),
            (
                "token_balances_snapshot_20260618_wait_after_guard_delete",
                "token_balances_snapshot",
            ),
            (
                "token_holder_counts_20260618_refresh_after_guard_delete",
                "token_holder_counts",
            ),
            (
                "token_holder_counts_20260618_wait_after_guard_delete",
                "token_holder_counts",
            ),
        ] {
            assert!(
                position(migration_name) > position(target),
                "{migration_name} must be applied after {target}"
            );
            let migration = all_objects()
                .find(|object| object.name == migration_name)
                .unwrap();
            assert!(
                matches!(migration.kind, ClickHouseObjectKind::Migration(_)),
                "{migration_name} should be a one-shot migration"
            );
            assert!(!migration.public_query, "{migration_name} is internal");
            assert!(!is_public_query_table(migration_name));
        }
    }

    #[test]
    fn managed_clickhouse_sql_has_no_client_bind_placeholders() {
        for object in all_objects() {
            let ddl = object.ddl();
            assert!(
                !ddl.contains('?'),
                "{} DDL contains `?`, which the clickhouse crate treats as a bind placeholder even in comments",
                object.name
            );
        }
    }

    #[test]
    fn managed_merge_tree_storage_uses_zstd_by_default() {
        for object in base_objects().iter().chain(derived_objects()) {
            let ddl = object.ddl();
            if object.is_table() || object.is_refreshable_materialized_view() {
                assert!(
                    ddl.contains("SETTINGS default_compression_codec = 'ZSTD(1)'"),
                    "physical table {} does not set the default ZSTD codec",
                    object.name
                );
            }
        }
    }
}
