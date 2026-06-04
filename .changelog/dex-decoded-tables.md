---
tidx: minor
---

Added decoded stablecoin-DEX event tables `dex_pairs`, `dex_orders`, and `dex_fills` as insert-time ClickHouse materialized views over `logs`. `dex_fills` denormalizes the `OrderFilled`/`OrderPlaced` join at ingest (token, side, tick attached per fill, ordered by `(token, block_num, log_idx)`), turning pair-swap and OHLC scans into a primary-key range read instead of a join plus correlated subquery.
