mod base;
mod catalog;
mod token_holders;
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
        .chain(token_holders::OBJECTS.iter())
}

pub fn all_queryable_objects() -> impl Iterator<Item = &'static ClickHouseObject> {
    base_objects()
        .iter()
        .chain(derived_objects())
        .filter(|object| object.public_query)
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
    all_queryable_objects()
        .find(|object| object.name == table)
        .and_then(|object| object.block_column)
}

pub fn is_known_table(table: &str) -> bool {
    all_queryable_objects().any(|object| object.name == table)
}

pub fn is_public_query_table(table: &str) -> bool {
    all_queryable_objects().any(|object| object.name == table)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_holder_tables_are_registered_for_routing() {
        assert!(is_public_query_table("token_transfer_events"));
        assert!(is_known_table("token_transfer_events"));
        assert_eq!(block_column("token_transfer_events"), Some("block_num"));
        assert!(is_public_query_table("token_holders"));
        assert!(is_known_table("token_holder_deltas"));
        assert_eq!(block_column("token_holder_deltas"), Some("block_num"));
    }

    #[test]
    fn materialized_views_are_not_public_query_tables() {
        assert!(!is_public_query_table("token_transfer_events_mv"));
        assert!(!is_known_table("token_transfer_events_mv"));
        assert!(!is_public_query_table("token_holder_deltas_mv"));
        assert!(!is_known_table("token_holder_deltas_mv"));
    }

    #[test]
    fn reorg_tables_delete_derived_tables_before_base_logs() {
        let tables: Vec<_> = reorg_tables().map(|table| table.name).collect();
        let transfers = tables
            .iter()
            .position(|table| *table == "token_transfer_events")
            .unwrap();
        let deltas = tables
            .iter()
            .position(|table| *table == "token_holder_deltas")
            .unwrap();
        let logs = tables.iter().position(|table| *table == "logs").unwrap();
        assert!(deltas < transfers);
        assert!(transfers < logs);
        assert!(deltas < logs);
    }

    #[test]
    fn descriptors_name_dependencies_that_exist() {
        let names: Vec<_> = all_queryable_objects()
            .map(|object| object.name)
            .chain(migrations().iter().map(|object| object.name))
            .chain(derived_objects().map(|object| object.name))
            .collect();

        for object in base_objects()
            .iter()
            .chain(migrations().iter())
            .chain(derived_objects())
        {
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
}
