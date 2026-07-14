---
tidx: minor
---

Changed tiered-storage backfill to write full history directly from RPC to ClickHouse, then hydrate PostgreSQL's configured hot window from checkpointed ClickHouse archive ranges. PostgreSQL storage is now bounded during initial sync, historical RPC work is not duplicated, and increasing `pg_keep` restores the additional hot range before moving the query boundary.
