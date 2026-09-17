use super::{ClickHouseObject, ClickHouseObjectKind};

const TOKEN_METADATA_VIEW: &str = include_str!("../../db/clickhouse/token_metadata.sql");

pub const OBJECTS: &[ClickHouseObject] = &[ClickHouseObject {
    name: "token_metadata",
    kind: ClickHouseObjectKind::View(TOKEN_METADATA_VIEW),
    depends_on: &["token_transfers"],
    public_query: true,
    block_column: None,
    backfill: None,
}];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_metadata_view_aggregates_first_last_seen_per_token() {
        let view = OBJECTS
            .iter()
            .find(|object| object.name == "token_metadata")
            .unwrap();
        assert!(view.is_view());
        let ddl = view.ddl();
        assert!(ddl.contains("FROM token_transfers FINAL"));
        assert!(ddl.contains("min(block_num)"));
        assert!(ddl.contains("max(block_num)"));
        assert!(ddl.contains("GROUP BY token"));
    }
}
