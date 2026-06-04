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
    p.`key`           AS `key`,
    p.base            AS base,
    p.quote           AS quote,
    p.block_num       AS block_num,
    p.log_idx         AS log_idx,
    p.block_timestamp AS block_timestamp,
    p.tx_hash         AS tx_hash,
    b.balance         AS liquidity
FROM dex_pairs FINAL AS p
INNER JOIN token_balances_snapshot AS b
    ON b.token = p.base
WHERE b.holder = '0xdec0000000000000000000000000000000000000'
  AND b.balance > 0
