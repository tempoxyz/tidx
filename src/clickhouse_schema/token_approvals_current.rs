use super::{ClickHouseObject, ClickHouseObjectKind};

const TOKEN_APPROVALS_CURRENT_VIEW: &str =
    include_str!("../../db/clickhouse/token_approvals_current.sql");

pub const OBJECTS: &[ClickHouseObject] = &[ClickHouseObject {
    name: "token_approvals_current",
    kind: ClickHouseObjectKind::View(TOKEN_APPROVALS_CURRENT_VIEW),
    depends_on: &["token_approvals"],
    public_query: true,
    block_column: None,
    backfill: None,
}];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_view_uses_argmax_for_latest_amount_per_pair() {
        let view = OBJECTS
            .iter()
            .find(|object| object.name == "token_approvals_current")
            .unwrap();
        assert!(view.is_view());
        let ddl = view.ddl();
        assert!(ddl.contains("FROM token_approvals FINAL"));
        assert!(ddl.contains("argMax(amount,          (block_num, log_idx))"));
        assert!(ddl.contains("AS last_block_num"));
        assert!(ddl.contains("GROUP BY token, owner, spender"));
        assert!(ddl.contains("HAVING amount > 0"));
    }
}
