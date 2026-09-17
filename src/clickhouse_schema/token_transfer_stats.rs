use super::{ClickHouseObject, ClickHouseObjectKind};

const TOKEN_TRANSFER_STATS_VIEW: &str =
    include_str!("../../db/clickhouse/token_transfer_stats.sql");

pub const OBJECTS: &[ClickHouseObject] = &[ClickHouseObject {
    name: "token_transfer_stats",
    kind: ClickHouseObjectKind::View(TOKEN_TRANSFER_STATS_VIEW),
    depends_on: &["token_transfers"],
    public_query: true,
    block_column: None,
    backfill: None,
}];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_transfer_stats_aggregates_daily_volume_from_token_transfers_final() {
        let view = OBJECTS
            .iter()
            .find(|object| object.name == "token_transfer_stats")
            .unwrap();
        assert!(view.is_view());
        let ddl = view.ddl();
        assert!(ddl.contains("FROM token_transfers FINAL"));
        assert!(ddl.contains("count() AS transfer_count"));
        assert!(ddl.contains("sum(amount) AS volume"));
        assert!(ddl.contains("GROUP BY day, token"));
    }
}
