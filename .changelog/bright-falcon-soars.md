---
tidx: minor
---

Add ClickHouse failover support for multi-instance per chain. Reads go to the primary instance; connection-level errors (refused/timeout/DNS) trigger automatic failover to the next instance. Each instance runs its own MaterializedPostgreSQL replication. Configure with `failover_urls` in `[chains.clickhouse]`. Existing single-URL configs work unchanged.
