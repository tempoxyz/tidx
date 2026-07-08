---
tidx: minor
---

Added `replicated_database` option under `[chains.clickhouse]`. When enabled, the sink creates the database with `ENGINE = Replicated` and rewrites MergeTree-family table engines to their `Replicated*` counterparts, so schema and data replicate across self-hosted multi-replica clusters coordinated by Keeper. Defaults to off; ClickHouse Cloud and single-node deployments are unaffected.
