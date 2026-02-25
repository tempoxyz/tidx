---
tidx: minor
---

### Dual-Sink Architecture & ClickHouse Direct-Write

Migrated ClickHouse from MaterializedPostgreSQL replication to a direct-write dual-sink architecture. Data is now written to both PostgreSQL (primary) and ClickHouse (secondary) in parallel using the official `clickhouse` crate with RowBinary format and LZ4 compression.

**New features:**

- **Dual-sink fan-out writer** — new `SinkSet` abstraction writes to PG and optionally CH in sequence. CH failures are fatal and propagate to the caller. Reorg deletes cascade to both sinks. ([#89](https://github.com/tempoxyz/tidx/pull/89))
- **ClickHouse direct-write sink** — replaces reqwest/JSONEachRow with the official `clickhouse` crate. Uses typed `Row`-derived wire structs, chunked inserts (2,000 rows/chunk), retry with exponential backoff (3 attempts), and per-chunk send/end timeouts. ([#101](https://github.com/tempoxyz/tidx/pull/101))
- **Automatic CH backfill from PG** — on startup, each table is independently backfilled from its PG high-water mark using block-range pagination (5,000 blocks/batch) with short-lived connections to avoid blocking autovacuum. ([#89](https://github.com/tempoxyz/tidx/pull/89))
- **ReplacingMergeTree** — CH tables use `ReplacingMergeTree()` for idempotent writes, allowing safe retries without duplicate data after background merges. ([#101](https://github.com/tempoxyz/tidx/pull/101))
- **Per-sink write rates in status** — rolling block/sec rate tracker for each sink, displayed in both CLI and HTTP status endpoints. ([#90](https://github.com/tempoxyz/tidx/pull/90))
- **Instant status via in-memory watermarks** — per-table high-water marks and row counts tracked with atomics, eliminating table scans from status queries. Seeded from DB on startup for immediate accuracy. ([#91](https://github.com/tempoxyz/tidx/pull/91), [#97](https://github.com/tempoxyz/tidx/pull/97), [#98](https://github.com/tempoxyz/tidx/pull/98))
- **Improved status display** — blocks show backfill progress as percentage, other tables show row counts, backfill ETA based on sync rate, and gap count. ([#93](https://github.com/tempoxyz/tidx/pull/93), [#94](https://github.com/tempoxyz/tidx/pull/94), [#99](https://github.com/tempoxyz/tidx/pull/99))

**Fixes:**

- Terminate stale PG connections before migrations to prevent DDL lock contention on container restart. ([#95](https://github.com/tempoxyz/tidx/pull/95))
- Retry sync engine creation on transient RPC failures with 10s backoff. ([#96](https://github.com/tempoxyz/tidx/pull/96))
- Sanitize all format-interpolated SQL identifiers (database names, table names, view order_by columns, engine parameter) against injection. Whitelist known tables, validate identifiers, and restrict engine to allowed MergeTree variants. ([#102](https://github.com/tempoxyz/tidx/pull/102))
