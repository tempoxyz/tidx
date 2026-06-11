---
tidx: minor
---

Added `dex_order_events` and `dex_fills_enriched`, two ClickHouse objects that resolve each DEX fill's point-in-time order state server-side. `dex_order_events` decodes the union of `OrderPlaced` and `OrderFlipped` into one positioned state stream (the existing `dex_orders` captures `OrderPlaced` only, so it is stale for flip orders). `dex_fills_enriched` is a refreshable materialized view that ASOF-joins each fill to the latest order-state event strictly before it, materializing `token`/`isBid`/`tick`, `at_peg`, `price`, the quote-side amount, and taker-oriented `source_*`/`destination_*` tokens. This lets the swaps feed restore its at-peg and source/destination-token filters as plain SQL aggregates instead of replaying the raw `logs` order-state stream and assembling routes per request.
