---
tidx: minor
---

### Performance

- **Parallel PG and CH writes** — `SinkSet` now writes to PostgreSQL and ClickHouse concurrently using `tokio::join!` instead of sequentially. ([#122](https://github.com/tempoxyz/tidx/pull/122))
- **Parallel block/tx/log/receipt writes** — Within each batch, all four table writes run concurrently within a single PG transaction. ([#124](https://github.com/tempoxyz/tidx/pull/124))
- **Single PG transaction for batch writes** — Wraps all COPY operations in one transaction to reduce WAL flushes and round-trips. ([#138](https://github.com/tempoxyz/tidx/pull/138))
- **Staging table ON CONFLICT DO NOTHING** — Replaced DELETE+COPY with staging table pattern for idempotent upserts without lock contention. ([#129](https://github.com/tempoxyz/tidx/pull/129))
- **Bare CREATE TEMP TABLE** — Removed `LIKE` clause from temp table creation for faster DDL. ([#135](https://github.com/tempoxyz/tidx/pull/135), [#143](https://github.com/tempoxyz/tidx/pull/143))
- **Single transaction for realtime writes** — Realtime block+tx writes use a single transaction for atomicity and reduced overhead. ([#144](https://github.com/tempoxyz/tidx/pull/144))
- **Drop redundant indexes** — Removed `idx_logs_topic1` (covered by composite), `idx_txs_selector` expression index, and all redundant ASC indexes. ([#121](https://github.com/tempoxyz/tidx/pull/121), [#133](https://github.com/tempoxyz/tidx/pull/133), [#137](https://github.com/tempoxyz/tidx/pull/137))
- **Bounded gap detection** — Gap detection query is now bounded by `max(tip_num)` to avoid full-table scans. ([#127](https://github.com/tempoxyz/tidx/pull/127))
- **Pool size and concurrency tuning** — Increased pool size to 48 and reduced backfill semaphore to 6 for better throughput. ([#130](https://github.com/tempoxyz/tidx/pull/130))
- **Statement timeout in pool config** — Moved `statement_timeout=0` to pool-level config instead of per-connection SET. ([#123](https://github.com/tempoxyz/tidx/pull/123))
- **Adaptive receipt backfill** — Batch UPDATE txs once per tick instead of per-range, with adaptive range splitting on RPC response too large errors. ([#116](https://github.com/tempoxyz/tidx/pull/116), [#117](https://github.com/tempoxyz/tidx/pull/117), [#118](https://github.com/tempoxyz/tidx/pull/118))
- **Faster ClickHouse backfill** — Improved CH backfill performance with retry on failure instead of giving up. ([#108](https://github.com/tempoxyz/tidx/pull/108), [#109](https://github.com/tempoxyz/tidx/pull/109))

### Features

- **Read replica support** — New `api_pg_url` and `api_pg_password_env` config options to route API queries to a separate PostgreSQL read replica. ([#119](https://github.com/tempoxyz/tidx/pull/119))
- **Filter pushdown for raw columns** — `address`, `block_num`, `tx_hash`, and other raw log columns in event CTE queries are now pushed down into the inner WHERE clause for index utilization. ([#113](https://github.com/tempoxyz/tidx/pull/113))
- **CLI short flags** — All `tidx query` flags now have short aliases: `-u` (url), `-n` (chain-id), `-e` (engine), `-f` (format), `-l` (limit), `-s` (signature), `-t` (timeout). ([#145](https://github.com/tempoxyz/tidx/pull/145))
- **Basic auth in CLI** — `--url http://user:pass@host:8080` extracts credentials and sends them as HTTP basic auth. ([#145](https://github.com/tempoxyz/tidx/pull/145))
- **TOON output format** — New `-f toon` output format using token-efficient notation for LLM consumption. ([#145](https://github.com/tempoxyz/tidx/pull/145))
- **ClickHouse HTTP basic auth** — Support `user` and `password_env` in `[chains.clickhouse]` config. ([#105](https://github.com/tempoxyz/tidx/pull/105))
- **Agent skills** — Added `querying-tempo` and `indexing-tempo` skills for AI-assisted development. ([#145](https://github.com/tempoxyz/tidx/pull/145))

### Fixes

- **Realtime receipt/log sync** — Receipts and logs are now fetched inline during realtime sync instead of relying on separate backfill. ([#141](https://github.com/tempoxyz/tidx/pull/141))
- **Atomic receipt backfill writes** — Receipt backfill writes are now atomic to prevent partial state on failure. ([#142](https://github.com/tempoxyz/tidx/pull/142))
- **Native PG transactions for writers** — All writer functions use native transactions instead of manual BEGIN/COMMIT. ([#128](https://github.com/tempoxyz/tidx/pull/128))
- **Restore per-connection statement_timeout** — Fixed statement timeout being lost after pool-level config change. ([#132](https://github.com/tempoxyz/tidx/pull/132))
- **Populate txs.gas_used** — Fixed `gas_used` not being populated on the `txs` table. ([#115](https://github.com/tempoxyz/tidx/pull/115))
- **Cap receipt backfill batch range** — Capped to 10 blocks to prevent RPC timeouts. ([#116](https://github.com/tempoxyz/tidx/pull/116))
- **Correct backfill metrics** — Fixed `backfill_remaining_blocks` metric in gap-fill paths. ([#110](https://github.com/tempoxyz/tidx/pull/110))
- **Persist CH backfill cursor** — ClickHouse backfill cursor is now persisted in PG to prevent gaps after restart. ([#107](https://github.com/tempoxyz/tidx/pull/107))
- **ClickHouse native-tls** — Enabled native-tls for ClickHouse HTTPS connections. ([#106](https://github.com/tempoxyz/tidx/pull/106))
- **Basic auth in rate limit bypass** — Fixed rate limit bypass to support Basic auth scheme. ([#111](https://github.com/tempoxyz/tidx/pull/111))
- **Remove API rate limiting** — Removed rate limiting from the query API. ([#112](https://github.com/tempoxyz/tidx/pull/112))
- **Docker image tag** — Use `latest` tag for tidx Docker image. ([#146](https://github.com/tempoxyz/tidx/pull/146))

### Tests

- **Gap detection tests** — Added `has_gaps` integration tests. ([#131](https://github.com/tempoxyz/tidx/pull/131))
