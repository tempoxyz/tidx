---
tidx: minor
---

Added `tidx sync` (indexer only) and `tidx serve` (stateless, read-only HTTP API) so the API can scale horizontally without running extra indexers. Serve skips migrations, database creation and ClickHouse DDL, builds its ClickHouse engines from config, and drives live query streams by polling `sync_state` on the database it reads. `tidx up` is unchanged. Also fixed `tidx up --no-watch` leaving `engine=clickhouse` queries unavailable.
