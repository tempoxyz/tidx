use super::{ClickHouseObject, ClickHouseObjectKind};

const TOKEN_SUPPLY_VIEW: &str = include_str!("../../db/clickhouse/token_supply.sql");

pub const OBJECTS: &[ClickHouseObject] = &[ClickHouseObject {
    name: "token_supply",
    kind: ClickHouseObjectKind::View(TOKEN_SUPPLY_VIEW),
    depends_on: &["token_transfers"],
    public_query: true,
    block_column: None,
    backfill: None,
}];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_supply_view_sums_mints_minus_burns_from_token_transfers_final() {
        let view = OBJECTS
            .iter()
            .find(|object| object.name == "token_supply")
            .unwrap();
        assert!(view.is_view());
        let ddl = view.ddl();
        assert!(ddl.contains("FROM token_transfers FINAL"));
        assert!(ddl.contains("0x0000000000000000000000000000000000000000"));
        assert!(ddl.contains("HAVING supply > 0"));
    }
}
