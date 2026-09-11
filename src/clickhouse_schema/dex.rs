use super::{BackfillPolicy, ClickHouseObject, ClickHouseObjectKind};

const DEX_PAIRS_SCHEMA: &str = include_str!("../../db/clickhouse/dex_pairs.sql");
const DEX_PAIRS_SELECT: &str = include_str!("../../db/clickhouse/dex_pairs_select.sql");
const DEX_ORDERS_SCHEMA: &str = include_str!("../../db/clickhouse/dex_orders.sql");
const DEX_ORDERS_SELECT: &str = include_str!("../../db/clickhouse/dex_orders_select.sql");
const DEX_FILLS_SCHEMA: &str = include_str!("../../db/clickhouse/dex_fills.sql");
const DEX_FILLS_SELECT: &str = include_str!("../../db/clickhouse/dex_fills_select.sql");
const DEX_ORDER_EVENTS_SCHEMA: &str = include_str!("../../db/clickhouse/dex_order_events.sql");
const DEX_ORDER_EVENTS_SELECT: &str =
    include_str!("../../db/clickhouse/dex_order_events_select.sql");
const DEX_FILLS_ENRICHED: &str = include_str!("../../db/clickhouse/dex_fills_enriched.sql");
const DEX_OHLC_1M: &str = include_str!("../../db/clickhouse/dex_ohlc_1m.sql");
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
        name: "dex_order_events",
        kind: ClickHouseObjectKind::Table(DEX_ORDER_EVENTS_SCHEMA),
        depends_on: &["logs"],
        public_query: true,
        block_column: Some("block_num"),
        backfill: Some(BackfillPolicy::Ranged {
            select_sql: DEX_ORDER_EVENTS_SELECT,
        }),
    },
    ClickHouseObject {
        name: "dex_order_events_mv",
        kind: ClickHouseObjectKind::MaterializedView {
            target_table: "dex_order_events",
            select_sql: DEX_ORDER_EVENTS_SELECT,
        },
        depends_on: &["logs", "dex_order_events"],
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
    ClickHouseObject {
        name: "dex_ohlc_1m",
        kind: ClickHouseObjectKind::RefreshableMaterializedView(DEX_OHLC_1M),
        depends_on: &["dex_fills", "dex_orders"],
        public_query: true,
        // Self-storing refreshable MV keyed by (token, bucket): fully replaced
        // each refresh, so it's reorg-correct by construction and reorg cleanup
        // skips it.
        block_column: None,
        backfill: None,
    },
    ClickHouseObject {
        name: "dex_fills_enriched",
        kind: ClickHouseObjectKind::View(DEX_FILLS_ENRICHED),
        depends_on: &["dex_fills", "dex_order_events", "dex_pairs"],
        public_query: true,
        // Plain view: ASOF-joins each fill to its point-in-time order state at
        // query time over the already-decoded, sort-keyed source tables. Stores
        // nothing, so it's realtime (no refresh lag), reorg-correct (reads live
        // tables) and carries no block_column.
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
        for name in ["dex_pairs", "dex_orders", "dex_fills", "dex_order_events"] {
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
            assert!(
                ddl.contains("address = '0xdec0000000000000000000000000000000000000'"),
                "{mv_name} should only decode DEX precompile logs"
            );
        }
    }

    #[test]
    fn ohlc_is_a_reorg_safe_refreshable_view() {
        let ohlc = object("dex_ohlc_1m");
        assert!(ohlc.is_refreshable_materialized_view());
        // Public so Cadent reads pre-bucketed candles instead of scanning raw
        // fills; self-storing, so it carries no block_column.
        assert!(ohlc.public_query);
        assert!(ohlc.block_column.is_none());

        let ddl = ohlc.ddl();
        assert!(ddl.contains("CREATE MATERIALIZED VIEW IF NOT EXISTS dex_ohlc_1m"));
        assert!(ddl.contains("REFRESH EVERY"));
        assert!(ddl.contains("toStartOfInterval(block_timestamp, INTERVAL 1 MINUTE)"));
        // OHLC built from the decoded join, with open/close anchored on the
        // first/last fill in the bucket by (block_num, log_idx).
        assert!(ddl.contains("FROM dex_fills"));
        assert!(ddl.contains("dex_orders"));
        assert!(ddl.contains("argMin(rate, (block_num, log_idx)) AS open"));
        assert!(ddl.contains("argMax(rate, (block_num, log_idx)) AS close"));
        // Price math mirrors the API's priceScale = 100000 rate/quote formulas.
        assert!(ddl.contains("100000"));
        assert_eq!(
            ohlc.drop_sql().as_deref(),
            Some("DROP VIEW IF EXISTS dex_ohlc_1m")
        );
    }

    #[test]
    fn order_events_decode_placed_and_flipped_from_logs() {
        let mv = object("dex_order_events_mv");
        let ddl = mv.ddl();
        assert!(ddl.starts_with("CREATE MATERIALIZED VIEW IF NOT EXISTS dex_order_events_mv"));
        assert!(ddl.contains("TO dex_order_events AS\n"));
        assert!(ddl.contains("FROM logs"));
        assert!(
            ddl.contains("address = '0xdec0000000000000000000000000000000000000'"),
            "should only decode DEX precompile logs"
        );
        // Unions OrderPlaced (0xc200d837…) and OrderFlipped (0x37a42d10…) so
        // flip orders' mutated (isBid, tick) are captured — `dex_orders` misses
        // them. Asserted here so an accidental edit can't drop a selector.
        for selector in [
            "0xc200d837816d02c5ee9bf081cba1a32ab1482de7a738b41c0b357186b0b998cd",
            "0x37a42d10bbce3e94e109a6a44e4479f0ee45dd6ecc6ca902168ea58e01ba32fe",
        ] {
            assert!(ddl.contains(selector), "should decode selector {selector}");
        }
        // Both events share the data-word layout read here (isBid word1, tick
        // word2 as a sign-extended int16).
        assert!(ddl.contains("reinterpretAsUInt8(unhex(substring(data, 129, 2))) AS isBid"));
        assert!(
            ddl.contains("reinterpretAsInt16(reverse(unhex(substring(data, 191, 4)))) AS tick")
        );
    }

    #[test]
    fn enriched_fills_are_realtime_point_in_time_join() {
        let enriched = object("dex_fills_enriched");
        // Plain view, not a materialized view: the join runs at query time so a
        // fill is filterable the instant it lands, and the join always sees the
        // complete, reorg-corrected source state (no insert-time ordering hazard,
        // no refresh lag).
        assert!(enriched.is_view());
        // Public so Cadent reads flip-correct, book-resolved fills instead of
        // replaying the raw order-state stream; stores nothing, so no block_column.
        assert!(enriched.public_query);
        assert!(enriched.block_column.is_none());

        let ddl = enriched.ddl();
        assert!(ddl.contains("CREATE VIEW IF NOT EXISTS dex_fills_enriched"));
        // A plain view stores nothing, so it must not carry materialization or
        // refresh clauses.
        assert!(!ddl.contains("MATERIALIZED VIEW"));
        assert!(!ddl.contains("REFRESH"));
        // ASOF join resolves each fill's latest state strictly before its
        // position, against the placed+flipped events stream.
        assert!(ddl.contains("ASOF INNER JOIN"));
        assert!(ddl.contains("FROM dex_fills FINAL"));
        assert!(ddl.contains("dex_order_events FINAL"));
        assert!(ddl.contains("f.orderId = e.orderId AND f.pos > e.pos"));
        // Quote token resolved from dex_pairs; genesis books fall back to ''.
        assert!(ddl.contains("LEFT JOIN dex_pairs AS p FINAL"));
        // Book-native fill columns: base/quote tokens, side, at-peg, price and
        // the quote-side amount become column reads. Taker source/destination is
        // a swap-level (route) notion derived during assembly, not stored here.
        assert!(ddl.contains("e.token AS token"));
        assert!(ddl.contains("p.quote AS quote_token"));
        assert!(ddl.contains("e.isBid AS isBid"));
        assert!(ddl.contains("toUInt8(e.tick = 0) AS at_peg"));
        assert!(ddl.contains("AS quote_amount"));
        assert!(!ddl.contains("source_token"));
        assert!(!ddl.contains("destination_token"));
        assert!(ddl.contains("100000"));
        assert_eq!(
            enriched.drop_sql().as_deref(),
            Some("DROP VIEW IF EXISTS dex_fills_enriched")
        );
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
