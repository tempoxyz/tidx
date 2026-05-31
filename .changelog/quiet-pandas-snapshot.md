---
tidx: patch
---

Added `token_balances_snapshot`, a refreshable ClickHouse materialized view that pre-aggregates holder balances from `token_holder_deltas` on a schedule so holder counts and listings hit the primary key instead of timing out on high-cardinality tokens.
