use super::{ClickHouseObject, ClickHouseObjectKind};

const EARN_VAULTS_SCHEMA: &str = include_str!("../../db/clickhouse/earn_vaults.sql");
const EARN_VERIFIED_VAULTS_SCHEMA: &str =
    include_str!("../../db/clickhouse/earn_verified_vaults.sql");
const EARN_FLOWS_SCHEMA: &str = include_str!("../../db/clickhouse/earn_flows.sql");
const EARN_VAULT_PRICE_HOURS_SCHEMA: &str =
    include_str!("../../db/clickhouse/earn_vault_price_hours.sql");

/// Named Earn objects are private. Generic holder balances remain public.
pub const OBJECTS: &[ClickHouseObject] = &[
    ClickHouseObject {
        name: "earn_verified_vaults",
        kind: ClickHouseObjectKind::Table(EARN_VERIFIED_VAULTS_SCHEMA),
        depends_on: &[],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "earn_vaults",
        kind: ClickHouseObjectKind::View(EARN_VAULTS_SCHEMA),
        depends_on: &["earn_verified_vaults"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "earn_flows",
        kind: ClickHouseObjectKind::View(EARN_FLOWS_SCHEMA),
        depends_on: &["logs", "earn_vaults"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "earn_vault_price_hours",
        kind: ClickHouseObjectKind::Table(EARN_VAULT_PRICE_HOURS_SCHEMA),
        depends_on: &["earn_vaults"],
        public_query: false,
        // Written by the sampler, not from logs: reorg cleanup prunes rows at
        // or above the fork block and the sampler writes them again.
        block_column: Some("block_num"),
        backfill: None,
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    fn object(name: &str) -> &'static ClickHouseObject {
        OBJECTS.iter().find(|object| object.name == name).unwrap()
    }

    #[test]
    fn registry_is_private_and_views_require_no_reorg_cleanup_or_backfill() {
        for descriptor in OBJECTS {
            assert!(!descriptor.public_query, "{}", descriptor.name);
            assert!(descriptor.backfill.is_none());
        }
        assert!(object("earn_verified_vaults").block_column.is_none());
        for name in ["earn_vaults", "earn_flows"] {
            assert!(object(name).is_view());
            // The API reader can SELECT these views without direct access
            // to the underlying registry table.
            assert!(object(name).ddl().contains("SQL SECURITY DEFINER"));
            assert!(object(name).block_column.is_none());
        }
    }

    #[test]
    fn earn_flows_decode_every_event() {
        let ddl = object("earn_flows").ddl();
        for (kind, topic0) in [
            (
                "deposited",
                "0xf5681f9d0db1b911ac18ee83d515a1cf1051853a9eae418316a2fdf7dea427c5",
            ),
            (
                "redeemed",
                "0x5cdf07ad0fc222442720b108e3ed4c4640f0fadc2ab2253e66f259a0fea83480",
            ),
            (
                "withdrew_exact",
                "0x667b1fd1a4d0290e6423170325223d7815bf90da8b978f2ad2001b314ac49c12",
            ),
            (
                "redeem_requested",
                "0xaa7dc873a7d37b94883eae5b185beafe0223de340014bc7a1690496363d5bb28",
            ),
            (
                "redeem_finalized",
                "0xd638999d611edc2680566b9afcd2c3ab1af17bf7438f625503928b75bd39e3cb",
            ),
            (
                "redeem_cancelled",
                "0xba481b3d46265a658a5106b2462d1fe3c6c50c858b33e5548f598073326b23ab",
            ),
            (
                "venue_shares_deposited",
                "0x35b15b3f988e6b5a7c3c72c7f44307862e2a93754541f8c81992b9365997bbc0",
            ),
        ] {
            assert!(ddl.contains(kind), "{kind} should be decoded");
            assert!(ddl.contains(topic0), "{kind} topic0 should be matched");
        }
    }

    #[test]
    fn earn_event_topics_match_signatures() {
        use alloy::primitives::keccak256;
        for (signature, topic0) in [
            (
                "Deposited(address,address,uint256,uint256)",
                "f5681f9d0db1b911ac18ee83d515a1cf1051853a9eae418316a2fdf7dea427c5",
            ),
            (
                "Redeemed(address,address,uint256,uint256)",
                "5cdf07ad0fc222442720b108e3ed4c4640f0fadc2ab2253e66f259a0fea83480",
            ),
            (
                "WithdrewExact(address,address,uint256,uint256)",
                "667b1fd1a4d0290e6423170325223d7815bf90da8b978f2ad2001b314ac49c12",
            ),
            (
                "RedeemRequested(bytes32,address,address,uint256)",
                "aa7dc873a7d37b94883eae5b185beafe0223de340014bc7a1690496363d5bb28",
            ),
            (
                "RedeemFinalized(bytes32,address,uint256,address,uint256)",
                "d638999d611edc2680566b9afcd2c3ab1af17bf7438f625503928b75bd39e3cb",
            ),
            (
                "RedeemCancelled(bytes32,address,uint256)",
                "ba481b3d46265a658a5106b2462d1fe3c6c50c858b33e5548f598073326b23ab",
            ),
            (
                "VenueSharesDeposited(address,address,uint256,uint256,uint256)",
                "35b15b3f988e6b5a7c3c72c7f44307862e2a93754541f8c81992b9365997bbc0",
            ),
        ] {
            assert_eq!(hex::encode(keccak256(signature)), topic0, "{signature}");
        }
    }

    #[test]
    fn price_hours_are_idempotent_and_reorg_scoped() {
        let price = object("earn_vault_price_hours");
        let ddl = price.ddl();
        assert!(ddl.contains("ReplacingMergeTree(sampled_at)"));
        assert!(ddl.contains("ORDER BY (vault, hour)"));
        assert_eq!(price.block_column, Some("block_num"));
        assert!(price.backfill.is_none());
    }
}
