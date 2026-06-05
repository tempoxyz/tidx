use super::{BackfillPolicy, ClickHouseObject, ClickHouseObjectKind};

const DEX_PAIRS_SCHEMA: &str = include_str!("../../db/clickhouse/dex_pairs.sql");
const DEX_PAIRS_SELECT: &str = include_str!("../../db/clickhouse/dex_pairs_select.sql");
const DEX_ORDERS_SCHEMA: &str = include_str!("../../db/clickhouse/dex_orders.sql");
const DEX_ORDERS_SELECT: &str = include_str!("../../db/clickhouse/dex_orders_select.sql");
const DEX_FILLS_SCHEMA: &str = include_str!("../../db/clickhouse/dex_fills.sql");
const DEX_FILLS_SELECT: &str = include_str!("../../db/clickhouse/dex_fills_select.sql");
const DEX_PAIR_LIQUIDITY: &str = include_str!("../../db/clickhouse/dex_pair_liquidity.sql");

/// Decoded stablecoin-DEX event tables.
///
/// `PairCreated`, `OrderPlaced`, and `OrderFilled` are otherwise only available
/// through the runtime signature-decoded CTE surface, which re-decodes millions
/// of `logs` rows on every request and forces the exchange endpoints into a
/// correlated `OrderFilled … IN (SELECT … FROM OrderPlaced …)` subquery. These
/// pre-decoded `ReplacingMergeTree` tables make those reads a sort-key seek plus
/// a plain `dex_fills ⋈ dex_orders` join, mirroring how `token_transfers`
/// pre-decodes `Transfer`.
pub const OBJECTS: &[ClickHouseObject] = &[
    ClickHouseObject {
        name: "dex_pairs",
        kind: ClickHouseObjectKind::Table(DEX_PAIRS_SCHEMA),
        depends_on: &["logs"],
        public_query: true,
        block_column: Some("block_num"),
        backfill: Some(BackfillPolicy::Ranged {
            select_sql: DEX_PAIRS_SELECT,
        }),
    },
    ClickHouseObject {
        name: "dex_pairs_mv",
        kind: ClickHouseObjectKind::MaterializedView {
            target_table: "dex_pairs",
            select_sql: DEX_PAIRS_SELECT,
        },
        depends_on: &["logs", "dex_pairs"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "dex_orders",
        kind: ClickHouseObjectKind::Table(DEX_ORDERS_SCHEMA),
        depends_on: &["logs"],
        public_query: true,
        block_column: Some("block_num"),
        backfill: Some(BackfillPolicy::Ranged {
            select_sql: DEX_ORDERS_SELECT,
        }),
    },
    ClickHouseObject {
        name: "dex_orders_mv",
        kind: ClickHouseObjectKind::MaterializedView {
            target_table: "dex_orders",
            select_sql: DEX_ORDERS_SELECT,
        },
        depends_on: &["logs", "dex_orders"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "dex_fills",
        kind: ClickHouseObjectKind::Table(DEX_FILLS_SCHEMA),
        depends_on: &["logs"],
        public_query: true,
        block_column: Some("block_num"),
        backfill: Some(BackfillPolicy::Ranged {
            select_sql: DEX_FILLS_SELECT,
        }),
    },
    ClickHouseObject {
        name: "dex_fills_mv",
        kind: ClickHouseObjectKind::MaterializedView {
            target_table: "dex_fills",
            select_sql: DEX_FILLS_SELECT,
        },
        depends_on: &["logs", "dex_fills"],
        public_query: false,
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "dex_pair_liquidity",
        kind: ClickHouseObjectKind::View(DEX_PAIR_LIQUIDITY),
        depends_on: &["dex_pairs", "token_balances_snapshot"],
        public_query: true,
        block_column: None,
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
    fn decoded_tables_are_block_scoped_and_public() {
        for name in ["dex_pairs", "dex_orders", "dex_fills"] {
            let table = object(name);
            assert!(table.is_table(), "{name} should be a table");
            assert!(table.public_query, "{name} should be public");
            assert_eq!(table.block_column, Some("block_num"), "{name} block scope");
            assert!(table.backfill.is_some(), "{name} should declare backfill");
        }
    }

    #[test]
    fn materialized_views_decode_from_logs_by_selector() {
        // PairCreated / OrderPlaced / OrderFilled selectors (keccak256 of each
        // canonical signature). Asserted here so an accidental edit to a select
        // can't silently point the MV at the wrong event.
        let cases = [
            (
                "dex_pairs_mv",
                "dex_pairs",
                "0xaff90cfc97c741e6d1ffffa62656c16a763f41dc773055d7b0c36950a823babf",
            ),
            (
                "dex_orders_mv",
                "dex_orders",
                "0xc200d837816d02c5ee9bf081cba1a32ab1482de7a738b41c0b357186b0b998cd",
            ),
            (
                "dex_fills_mv",
                "dex_fills",
                "0x16c08f8f2c17b3c8879b3e3cf5efdbdcdfdbd0fcb3890f9d3086f470cd601ddd",
            ),
        ];
        for (mv_name, target, selector) in cases {
            let ddl = object(mv_name).ddl();
            assert!(ddl.starts_with(&format!("CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_name}")));
            assert!(ddl.contains(&format!("TO {target} AS\n")));
            assert!(ddl.contains("FROM logs"));
            assert!(
                ddl.contains(selector),
                "{mv_name} should filter on {selector}"
            );
        }
    }

    #[test]
    fn pair_liquidity_joins_pairs_to_dex_escrow_balances() {
        let view = object("dex_pair_liquidity");
        assert!(view.is_view());
        // Public so Cadent reads ranked pairs with liquidity directly instead of
        // over-fetching DEX balances and intersecting with pairs in memory.
        assert!(view.public_query);
        let ddl = view.ddl();
        assert!(ddl.contains("CREATE VIEW IF NOT EXISTS dex_pair_liquidity"));
        assert!(ddl.contains("FROM dex_pairs FINAL\nINNER JOIN token_balances_snapshot"));
        assert!(!ddl.contains("FINAL AS"));
        assert!(ddl.contains("token_balances_snapshot"));
        // Joins each pair's base to its DEX-escrow balance; the DEX precompile
        // address is fixed across Tempo chains.
        assert!(ddl.contains("token_balances_snapshot.token = dex_pairs.base"));
        assert!(ddl.contains("0xdec0000000000000000000000000000000000000"));
        assert!(ddl.contains("token_balances_snapshot.balance > 0"));
        assert_eq!(
            view.drop_sql().as_deref(),
            Some("DROP VIEW IF EXISTS dex_pair_liquidity")
        );
    }

    #[test]
    fn order_decode_reads_signed_ticks_little_endian() {
        // int16 ticks are sign-extended in their ABI word; decoding must read the
        // trailing 2 bytes little-endian so two's-complement negatives survive.
        let ddl = object("dex_orders_mv").ddl();
        assert!(
            ddl.contains("reinterpretAsInt16(reverse(unhex(substring(data, 191, 4)))) AS tick")
        );
        assert!(ddl.contains("reinterpretAsUInt8(unhex(substring(data, 129, 2))) AS isBid"));
    }
}
