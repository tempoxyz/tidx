---
tidx: minor
---

Added `token_holder_counts`, a refreshable ClickHouse materialized view that pre-aggregates per-token holder counts from `token_balances_snapshot`, so token detail and holder endpoints hit a point lookup instead of a high-cardinality `count()` scan over every holder row.
