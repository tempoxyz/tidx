-- Trading pairs ranked-ready by on-DEX base-token liquidity.
--
-- The "pairs by liquidity" endpoint currently reads DEX-escrow balances from
-- `token_balances_snapshot WHERE holder = <DEX>` ranked by balance, over-fetches
-- ~3x, then intersects the result with the pair set in memory (the DEX escrows
-- both base and quote tokens, but only base addresses map to a pair). This view
-- pushes that intersection into ClickHouse: it joins each pair's `base` to its
-- DEX-escrow balance, so callers get pair rows with `liquidity` directly and
-- just add `ORDER BY liquidity DESC, base ASC LIMIT …`.
--
-- The DEX precompile address (`0xdec0…0000`) is fixed across Tempo chains, so
-- it is inlined here the same way the API inlines it.
--
-- `dex_pairs FINAL` collapses any reorg-duplicated `PairCreated` rows;
-- `token_balances_snapshot` is a refreshable MergeTree (one row per
-- (token, holder)) so it needs no FINAL.
CREATE VIEW IF NOT EXISTS dex_pair_liquidity AS
SELECT
    dex_pairs.`key`           AS `key`,
    dex_pairs.base            AS base,
    dex_pairs.quote           AS quote,
    dex_pairs.block_num       AS block_num,
    dex_pairs.log_idx         AS log_idx,
    dex_pairs.block_timestamp AS block_timestamp,
    dex_pairs.tx_hash         AS tx_hash,
    token_balances_snapshot.balance AS liquidity
FROM dex_pairs FINAL
INNER JOIN token_balances_snapshot
    ON token_balances_snapshot.token = dex_pairs.base
WHERE token_balances_snapshot.holder = '0xdec0000000000000000000000000000000000000'
  AND token_balances_snapshot.balance > 0
