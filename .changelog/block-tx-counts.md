---
tidx: minor
---

Added `block_tx_counts`, a `SummingMergeTree` ClickHouse materialized view that maintains per-block transaction counts from `txs`, so block-list pages can read tx counts from the primary key instead of running a `GROUP BY` over `txs` on every request.
