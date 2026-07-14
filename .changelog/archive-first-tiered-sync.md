---
tidx: minor
---

Changed tiered-storage backfill to write full history directly to ClickHouse while independently reconciling PostgreSQL to the configured hot window. PostgreSQL storage is now bounded during initial sync, and increasing `pg_keep` restores the additional hot range before moving the query boundary.
