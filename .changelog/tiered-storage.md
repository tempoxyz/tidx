---
tidx: minor
---

Added tiered storage: an optional `[chains.retention]` hot window in PostgreSQL with pruning once rows are durable in ClickHouse, plus a `source` query parameter routing queries across tiers.
