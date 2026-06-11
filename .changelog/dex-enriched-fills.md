---
tidx: minor
---

Added `dex_order_events` and `dex_fills_enriched`, two ClickHouse objects that resolve each DEX fill's point-in-time order state. `dex_order_events` decodes the union of `OrderPlaced` and `OrderFlipped` into one positioned state stream (the existing `dex_orders` captures `OrderPlaced` only, so it is stale for flip orders). `dex_fills_enriched` is a plain view that ASOF-joins each fill to the latest order-state event strictly before it, exposing `token`/`isBid`/`tick`, `at_peg`, `price`, the quote-side amount, and taker-oriented `source_*`/`destination_*` tokens. Resolving the join at query time over the already-decoded, sort-keyed source tables keeps it realtime (no refresh lag) and correct for same-block flips and reorgs, while letting the swaps feed restore its at-peg and source/destination-token filters as plain SQL predicates instead of replaying the raw `logs` order-state stream per request.
