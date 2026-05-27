mod base;
mod catalog;
mod token_approvals;
mod token_balances;
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

pub fn derived_objects() -> impl DoubleEndedIterator<Item = &'static ClickHouseObject> {
    token_transfers::OBJECTS
        .iter()
        .chain(token_balances::OBJECTS.iter())
        .chain(token_supply::OBJECTS.iter())
        .chain(token_approvals::OBJECTS.iter())
        .chain(token_transfer_stats::OBJECTS.iter())
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
        assert!(is_known_table("token_holder_deltas"));
        assert_eq!(block_column("token_holder_deltas"), Some("block_num"));
    }

    #[test]
    fn new_aggregate_objects_are_registered_for_public_query() {
        assert!(is_public_query_table("token_supply"));
        assert!(is_public_query_table("token_approvals"));
        assert_eq!(block_column("token_approvals"), Some("block_num"));
        assert!(is_public_query_table("token_transfer_stats"));
    }

    #[test]
    fn materialized_views_are_known_but_not_public_query_tables() {
        assert!(!is_public_query_table("token_transfers_mv"));
        assert!(is_known_table("token_transfers_mv"));
        assert!(!is_public_query_table("token_holder_deltas_mv"));
        assert!(is_known_table("token_holder_deltas_mv"));
        assert!(!is_public_query_table("token_approvals_mv"));
        assert!(is_known_table("token_approvals_mv"));
    }

    #[test]
    fn reorg_tables_delete_derived_tables_before_base_logs() {
        let tables: Vec<_> = reorg_tables().map(|table| table.name).collect();
        let transfers = tables
            .iter()
            .position(|table| *table == "token_transfers")
            .unwrap();
        let deltas = tables
            .iter()
            .position(|table| *table == "token_holder_deltas")
            .unwrap();
        let approvals = tables
            .iter()
            .position(|table| *table == "token_approvals")
            .unwrap();
        let logs = tables.iter().position(|table| *table == "logs").unwrap();
        assert!(deltas < transfers);
        assert!(transfers < logs);
        assert!(deltas < logs);
        assert!(approvals < logs);
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
}
