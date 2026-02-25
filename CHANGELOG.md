# Changelog

## `tidx@0.4.0`

### Minor Changes

- ### Dual-Sink Architecture & ClickHouse Direct-Write
- Migrated ClickHouse from MaterializedPostgreSQL replication to a direct-write dual-sink architecture. Data is now written to both PostgreSQL (primary) and ClickHouse (secondary) in parallel using the official `clickhouse` crate with RowBinary format and LZ4 compression.
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
- Sanitize all format-interpolated SQL identifiers (database names, table names, view order_by columns, engine parameter) against injection. Whitelist known tables, validate identifiers, and restrict engine to allowed MergeTree variants. ([#102](https://github.com/tempoxyz/tidx/pull/102)) (by @jxom, [#103](https://github.com/tempoxyz/tidx/pull/103))

## `tidx@0.3.1`

### Patch Changes

- Fixed array type parsing in event signatures (`uint256[]`, `uint256[N]`) which previously returned "Invalid uint size". Added missing SQL functions to query allowlist (`date`, `date_part`, `to_char`, `array_agg`, `string_agg`, etc.). Removed references to non-existent `token_holders`/`token_balances` tables. (by @jxom, [#84](https://github.com/tempoxyz/tidx/pull/84))

## `tidx@0.3.0`

### Minor Changes

- Added support for multiple event signatures per query. The HTTP API now accepts repeated `signature` query params (`?signature=Transfer(...)&signature=Approval(...)`) and the CLI accepts multiple `-s` flags. Each signature generates a separate CTE, enabling cross-event queries like `SELECT * FROM Transfer UNION ALL SELECT * FROM Approval`. (by @jxom, [#83](https://github.com/tempoxyz/tidx/pull/83))

### Patch Changes

- Hardened SQL query API: replaced string-based injection with AST manipulation, switched from function blocklist to allowlist, added table allowlist, enforced reject-by-default expression validation, capped LIMIT/depth/size, and locked down API role with connection and resource limits. (by @jxom, [f9da1eb](https://github.com/tempoxyz/tidx/commit/f9da1eb))

## 0.2.0 (2026-02-06)

### Minor Changes

- Support PostgreSQL password via environment variable. Add `pg_password_env` config option to inject the password from an env var into `pg_url` at runtime, avoiding plaintext passwords in config files. Existing configs without `pg_password_env` work unchanged. (by @GeorgiosKonstantopoulos, [#72](https://github.com/tempoxyz/tidx/pull/72))
- Add ClickHouse failover support for multi-instance per chain. Reads go to the primary instance; connection-level errors (refused/timeout/DNS) trigger automatic failover to the next instance. Each instance runs its own MaterializedPostgreSQL replication. Configure with `failover_urls` in `[chains.clickhouse]`. Existing single-URL configs work unchanged. (by @GeorgiosKonstantopoulos, [#71](https://github.com/tempoxyz/tidx/pull/71))

### Patch Changes

- Handle SIGTERM for graceful container shutdown. Previously only SIGINT (ctrl-c) triggered graceful shutdown; now SIGTERM from Kubernetes/Docker also triggers the same broadcast for clean connection draining. (by @GeorgiosKonstantopoulos, [#71](https://github.com/tempoxyz/tidx/pull/71))

## 0.2.0 (2026-02-06)

### Minor Changes

- Add ClickHouse failover support for multi-instance per chain. Reads go to the primary instance; connection-level errors (refused/timeout/DNS) trigger automatic failover to the next instance. Each instance runs its own MaterializedPostgreSQL replication. Configure with `failover_urls` in `[chains.clickhouse]`. Existing single-URL configs work unchanged. (by @tempo-ai, [a74f174](https://github.com/tempoxyz/tidx/commit/a74f174))

## 0.1.3 (2026-02-03)

### Patch Changes

- Trigger release. (by @jxom, [4f89896](https://github.com/tempoxyz/tidx/commit/4f89896))

## 0.1.2 (2026-02-03)

### Patch Changes

- Fix ClickHouse DDL statement handling. DDL statements like `CREATE DATABASE` and `CREATE TABLE` return empty responses, which previously caused JSON parsing errors. Now handles empty responses gracefully. (by @jxom, [09573e1](https://github.com/tempoxyz/tidx/commit/09573e1))

## 0.1.1 (2026-02-03)

### Patch Changes

- Fix ClickHouse hex literal handling for MaterializedPostgreSQL.
**Input**: Use `concat(char(92), 'x...')` instead of `'\x...'` for WHERE clause comparisons because ClickHouse interprets `\x` as an escape sequence.
**Output**: Convert `\x...` to `0x...` in query results for standard Ethereum hex format, matching PostgreSQL behavior. (by @jxom, [e15afa5](https://github.com/tempoxyz/tidx/commit/e15afa5))

## 0.1.0 (2026-02-03)

### Minor Changes

- Add predicate pushdown for indexed event parameters.
- Rewrites SQL filters like `"from" = '0x...'` to `topic1 = '0x000...'` to enable index usage
- Add `signature` parameter to `/views` API for automatic CTE generation and decoding
- Support both PostgreSQL and ClickHouse query engines (by @jxom, [0bca021](https://github.com/tempoxyz/tidx/commit/0bca021))

### Patch Changes

- Fix hex literal conversion to preserve `0x` prefix in concat expressions.
- The naive `replace("'0x", "'\\x")` was incorrectly converting `concat('0x', ...)` to `concat('\x', ...)`, causing addresses to display as `\x...` instead of `0x...`. Now uses regex to only convert hex literals with 40+ characters. (by @jxom, [0bca021](https://github.com/tempoxyz/tidx/commit/0bca021))

## 0.0.37 (2026-02-03)

### Patch Changes

- Adds columns array to the /views?chainId=... response with column names and types. (by @jxom, [c2001e4](https://github.com/tempoxyz/tidx/commit/c2001e4))

## 0.0.36 (2026-02-03)

### Patch Changes

- Initial release. (by @jxom, [9bba8d5](https://github.com/tempoxyz/tidx/commit/9bba8d5))

