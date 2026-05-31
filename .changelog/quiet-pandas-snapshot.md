---
tidx: minor
---

Added `token_balances_snapshot`, a refreshable ClickHouse materialized view that pre-aggregates holder balances from `token_holder_deltas` on a schedule (15 minutes) and stores them in its own MergeTree ordered by `(token, balance)`. Reading holder counts and holder listings now hits the primary key instead of re-aggregating the full delta history on every request, which previously timed out for high-cardinality tokens (e.g. PathUSD) and surfaced as "0 holders". The refresh spills to disk past a memory threshold so it completes reliably over hundreds of millions of delta rows. Available when running with `engine="clickhouse"`.
