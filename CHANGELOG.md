# Changelog

## `tidx@1.0.0`

### Major Changes

**Breaking:** Moved PostgreSQL chain settings into a nested `[chains.postgres]` section (`url`, `password_env`, `api_url`, `api_password_env`), removing the root-level `pg_url`, `pg_password_env`, `api_pg_url`, and `api_pg_password_env` fields.
  ```diff
   [[chains]]
   name = "mainnet"
   chain_id = 4217
   rpc_url = "https://rpc.tempo.xyz"
  -pg_url = "postgres://user@host:5432/tidx_mainnet"
  -pg_password_env = "TIDX_PG_PASSWORD"
  -api_pg_url = "postgres://user@host:5432/tidx_mainnet_r"
  -api_pg_password_env = "TIDX_API_PG_PASSWORD"
  +
  +[chains.postgres]
  +url = "postgres://user@host:5432/tidx_mainnet"
  +password_env = "TIDX_PG_PASSWORD"
  +api_url = "postgres://user@host:5432/tidx_mainnet_r"
  +api_password_env = "TIDX_API_PG_PASSWORD"
  ```
  (by @jxom, [#254](https://github.com/tempoxyz/tidx/pull/254))

### Minor Changes

- Changed tiered-storage backfill to write full history directly from RPC to ClickHouse, then hydrate PostgreSQL's configured hot window from checkpointed ClickHouse archive ranges. PostgreSQL storage is now bounded during initial sync, historical RPC work is not duplicated, and increasing `pg_keep` restores the additional hot range before moving the query boundary. (by @KamilSzczygieł, [#261](https://github.com/tempoxyz/tidx/pull/261))
- Added `dex_ohlc_1m`, a refreshable ClickHouse materialized view that rolls `dex_fills` into per-minute OHLC candles keyed by `(token, bucket)`, removing the 1000-fill in-memory bucketing cap on the OHLC endpoint and enabling multi-month windows with the work moved off the request path. (by @jxom, [#228](https://github.com/tempoxyz/tidx/pull/228))
- Added support for the Tempo T6 network upgrade (TIP-1028 receive policies, TIP-1049 admin keys). Excluded the `ReceivePolicyGuard` precompile (`0xb10c...`) from holder/balance derivation so blocked transfers no longer credit the guard as a fake holder, while leaving raw `token_transfers`/`token_supply` movement intact. Added a one-time post-derived migration that deletes any pre-existing guard rows from `token_holder_deltas`/`address_holder_deltas` (a no-op on fresh deployments) so historical balances and refreshable holder aggregates stop counting the guard. Pinned the Tempo crates to the T6 release commit. (by @stevencartavia, [#244](https://github.com/tempoxyz/tidx/pull/244))
- Added tiered storage: with `[chains.retention]`, PostgreSQL keeps a hot window of recent blocks and prunes the rest once durable in the ClickHouse archive.
  ```diff
   [[chains]]
   name = "mainnet"
   chain_id = 4217
   rpc_url = "https://rpc.tempo.xyz"
  
   [chains.clickhouse]
   enabled = true
   url = "http://clickhouse:8123"
  +
  +[chains.retention]
  +pg_keep = "30d"
  ```
  (by @jxom, [#254](https://github.com/tempoxyz/tidx/pull/254))

### Patch Changes

- Fixed backfill-first mode skipping the blocks above the highest stored block after a lagging restart, and the blocks realtime sync jumped over after falling more than ten blocks behind. Both ranges stayed missing until the next restart; they are now backfilled before realtime sync starts and caught up in order afterwards. (by @MatthiasSeitz, [#355](https://github.com/tempoxyz/tidx/pull/355))
- Fixed receipts and logs being dropped or attached to the wrong block when the RPC node answers a block and its receipts inconsistently. Per-item batch errors and truncated batch responses now fail the fetch, and receipts are only written when they carry the hash and transactions of the block they are stored with. Batch responses are matched by request ID so out-of-order replies remain valid; missing, duplicate, and unexpected IDs are rejected. (by @MatthiasSeitz, [#354](https://github.com/tempoxyz/tidx/pull/354))
- Fixed PostgreSQL `/query` validation accepting unbounded `lpad` and `rpad` lengths. The length must now be an integer literal no larger than 1024, so oversized padding is rejected before the database builds it. (by @MatthiasSeitz, [#349](https://github.com/tempoxyz/tidx/pull/349))
- Added ClickHouse projections and bloom filters for common block, log, receipt, and transaction access paths, requiring ClickHouse 25.11 or newer. (by @jxom, [#297](https://github.com/tempoxyz/tidx/pull/297))
- Sped up decoding of ClickHouse query results about fourfold. Queries now request `JSONCompact`, whose rows are arrays in column order, and are deserialized directly instead of through a JSON tree that was cloned cell by cell. Result bodies are also about a fifth smaller. (by @MatthiasSeitz, [#365](https://github.com/tempoxyz/tidx/pull/365))
- Fixed ClickHouse query validation accepting calls to functions that reach external systems, other servers, local files, or programs, such as `gcs`, `azureBlobStorage`, `iceberg`, `cluster`, and `executable`. These are now rejected like `url` and `s3`. (by @MatthiasSeitz, [#352](https://github.com/tempoxyz/tidx/pull/352))
- Capped ClickHouse working memory at 1 GiB per public API query, so array-building functions can no longer allocate up to the server profile's limit before the result caps apply. Internal queries and the indexer's own writes are unaffected. (by @MatthiasSeitz, [#350](https://github.com/tempoxyz/tidx/pull/350))
- Added `replicated_database` option under `[chains.clickhouse]`. When enabled, the sink creates the database with `ENGINE = Replicated` and rewrites MergeTree-family table engines to their `Replicated*` counterparts, so schema and data replicate across self-hosted multi-replica clusters coordinated by Keeper. Defaults to off; ClickHouse Cloud and single-node deployments are unaffected. (by @KamilSzczygieł, [#252](https://github.com/tempoxyz/tidx/pull/252))
- Fixed ClickHouse failover classification so client timeouts stop immediately while non-timeout transport and protocol failures try a healthy secondary. (by @jxom, [#287](https://github.com/tempoxyz/tidx/pull/287))
- Fixed ClickHouse query validation to check functions and tables in every clause of a query. `TOP` and `IN` with a single identifier are now rejected; use `LIMIT` and `=` or a subquery instead. (by @MatthiasSeitz, [#357](https://github.com/tempoxyz/tidx/pull/357))
- Bound the PostgreSQL, ClickHouse, Prometheus and Grafana ports in the production compose file to `127.0.0.1`, so their default credentials are no longer reachable from other hosts. (by @MatthiasSeitz, [#347](https://github.com/tempoxyz/tidx/pull/347))
- Bounded the `dex_ohlc_1m` refresh: the join now only loads orders that were filled inside the retention window instead of every order ever placed, and the refresh carries explicit memory and thread limits. (by @MatthiasSeitz, [#348](https://github.com/tempoxyz/tidx/pull/348))
- Fixed the Docker installer skipping `clickhouse-config.xml`, `prometheus.yml` and `alerts.yml`, which the production compose file bind-mounts and Docker otherwise creates as empty directories. Failed downloads now abort the install instead of being saved as the file. (by @MatthiasSeitz, [#358](https://github.com/tempoxyz/tidx/pull/358))
- Fixed duplicate, stale, or missing ClickHouse rows across retries, reorgs, partial writes, and startup backfills using bounded canonical-row repair, fresh deduplication generations, and durable handoffs. (by @jxom, [#280](https://github.com/tempoxyz/tidx/pull/280))
- Fixed the ClickHouse sink failing against ClickHouse 26.9+ with `decompression error: incorrect magic number` by requesting LZ4-framed responses via `network_compression_method=lz4`. (by @MatthiasSeitz, [#371](https://github.com/tempoxyz/tidx/pull/371))
- Fixed `engine=clickhouse` returning timestamps without a timezone (`2026-09-11 22:38:08.000`), which JavaScript and Python parse as local time. Native ClickHouse timestamps now use the same RFC 3339 UTC format as PostgreSQL (`2026-09-11T22:38:08+00:00`). (by @EmmaJamieson-Hoare, [#342](https://github.com/tempoxyz/tidx/pull/342))
- Fixed ClickHouse set operations with trailing ordering or limits, including case-insensitive aliases and unresolved compound outputs, by hoisting clauses into a derived-table wrapper. (by @jxom, [#285](https://github.com/tempoxyz/tidx/pull/285))
- Fixed unquoted and repeated indexed event equality filters. Topic pushdown now edits individual WHERE predicates on a directly named event table, including safe event scans inside subqueries and CTEs. Filters on joined or derived columns and grouped expressions keep their decoded references. PostgreSQL short fixed-byte comparisons outside this path still require explicit `\x` literals. (by @MatthiasSeitz, [#373](https://github.com/tempoxyz/tidx/pull/373))
- Fixed PostgreSQL-over-ClickHouse (`engine=postgres&source=clickhouse`) queries returning `columns: []` for an empty event result. Column names now come from the statement prepared inside the query transaction. (by @EmmaJamieson-Hoare, [#342](https://github.com/tempoxyz/tidx/pull/342))
- Fixed `bytes1`–`bytes31` event parameters returning the whole zero-padded 32-byte ABI word instead of exactly N bytes on every query engine, so a `bytes4` value now reads `0xdeadbeef` rather than `0xdeadbeef000…000`. Signatures with a `bytesN` width outside 1–32 are now rejected.
- Filters must now compare against the N-byte value: `WHERE "tag" = '0xdeadbeef'` matches, while the padded 32-byte literal no longer does. Equality filters on indexed `bytesN` params are pushed down to the topic column, so they match on every engine and use the topic index. On PostgreSQL, filters on non-indexed values shorter than 20 bytes need a `'\x…'` literal, since only `'0x…'` literals of 40+ hex digits are converted to bytea. ClickHouse materialized views created from a `bytesN` signature keep the padded values until they are recreated. (by @EmmaJamieson-Hoare, [#342](https://github.com/tempoxyz/tidx/pull/342))
- Fixed PostgreSQL `/query` results returning `null` for `float4`, `timestamp` (without time zone), and `char(n)` columns. `timestamp` values are read as UTC and formatted as RFC 3339 like `timestamptz`. (by @MatthiasSeitz, [#370](https://github.com/tempoxyz/tidx/pull/370))
- Fixed event-query predicate pushdown dropping incremental Earn deposits by turning OR-based pagination cursors into contradictory AND filters. Only required WHERE conjuncts from a single, unambiguous reference to the matching event table are pushed down; shared event sources retain their consumer-specific filters. (by @DerekCofausper, [#311](https://github.com/tempoxyz/tidx/pull/311))
- Sped up PostgreSQL hot-window hydration in retention-enabled deployments. The first block of the hot window is now looked up once per weekly boundary instead of by a binary search over RPC on every tick, and each scan of the hot window for gaps now hydrates every gap it finds instead of stopping after one batch per worker. (by @MatthiasSeitz, [#364](https://github.com/tempoxyz/tidx/pull/364))
- Raised the public `/query` concurrency limit from eight to 32 requests. (by @jxom, [#299](https://github.com/tempoxyz/tidx/pull/299))
- Reduced PostgreSQL load of gap-fill on a synced chain. The check that runs every two seconds now counts only the blocks above `synced_num` instead of every block since the prune floor, and the full range is verified on start and every ten minutes. (by @MatthiasSeitz, [#361](https://github.com/tempoxyz/tidx/pull/361))
- Live `/query` streams now rewrite and validate their per-block query once per stream instead of for every new block. The block number is bound as a query parameter, which saves about 25 µs of CPU per block and subscriber for plain queries and about 130 µs for queries with event signatures. (by @MatthiasSeitz, [#374](https://github.com/tempoxyz/tidx/pull/374))
- Counted every statement of a live `/query` stream against the API query concurrency limit. The limit previously covered a live request only until its SSE response started, so open streams ran their statements on top of it. (by @MatthiasSeitz, [#351](https://github.com/tempoxyz/tidx/pull/351))
- Sped up converting PostgreSQL `/query` results to JSON, most noticeably for BYTEA-heavy results such as raw logs. Response output is unchanged. (by @MatthiasSeitz, [#369](https://github.com/tempoxyz/tidx/pull/369))
- Cut PostgreSQL write latency for small batches about fourfold. Batches are now written with one `INSERT ... SELECT FROM unnest(...)` per table instead of a temporary staging table, a COPY and an `INSERT ... SELECT`, and all statements of a batch are sent without waiting for each other, so a write takes about four round trips instead of about forty and no longer creates and drops temporary tables. (by @MatthiasSeitz, [#367](https://github.com/tempoxyz/tidx/pull/367))
- Added PostgreSQL head-page indexes for sender, fee payer, and indexed-address lookups, built concurrently per partition with interrupted-build recovery. (by @jxom, [#288](https://github.com/tempoxyz/tidx/pull/288))
- Added PostgreSQL log indexes for contract event history and indexed-address lookups, built concurrently per partition with interrupted-build recovery. (by @jxom, [#282](https://github.com/tempoxyz/tidx/pull/282))
- Fixed `/query` errors flattening PostgreSQL failures to `db error`; the server message and SQLSTATE code (e.g. `division by zero (22012)`) now surface, and statement timeouts classify as timeouts. (by @jxom, [#284](https://github.com/tempoxyz/tidx/pull/284))
- Fixed PostgreSQL `/query` validation skipping the characters argument of `trim()`, which let a subquery there read tables the validator otherwise rejects, such as `pg_authid`. The argument is now validated like the rest of the expression. (by @jxom, [#376](https://github.com/tempoxyz/tidx/pull/376))
- Reduced PostgreSQL write latency on partitioned installs. The deletes that replace a batch's existing txs, logs and receipts are now bounded by the batch's block timestamps, so PostgreSQL plans and scans only the weekly partitions around the batch instead of every partition. (by @MatthiasSeitz, [#363](https://github.com/tempoxyz/tidx/pull/363))
- Reduced PostgreSQL round trips per `/query` request from seven to five, and from fifteen to five for `engine=postgres-via-clickhouse`. Session settings are sent in one batch together with statement preparation, and the tiered prune-boundary lookup no longer prepares its statement. Result types are resolved before execution so composite columns cannot stall the result stream. (by @MatthiasSeitz, [#372](https://github.com/tempoxyz/tidx/pull/372))
- Fixed reorg handling skipping the replaced blocks: the sync pointers are now rewound to the fork point and realtime sync refetches the chain from there instead of writing the batch that exposed the reorg. The parent hash of a pipelined batch is now checked after the previous batch is committed, so two consecutive batches from different forks are no longer written. (by @MatthiasSeitz, [#356](https://github.com/tempoxyz/tidx/pull/356))
- Improved sync throughput against RPC endpoints that limit batch sizes. Oversized block and receipt batches are now split into halves that are fetched concurrently, and the accepted batch size is remembered for a minute so later ranges are requested in batches that fit instead of being rejected and split again. (by @MatthiasSeitz, [#359](https://github.com/tempoxyz/tidx/pull/359))
- Tiered storage bootstrap now creates a `tidx_clickhouse` user mapping for the PostgreSQL role in `postgres.api_url`, alongside the indexer role's mapping. API queries that fall back to the pg_clickhouse FDW (`ch.*` / `tiered.*`) no longer fail with `user mapping not found` when a separate API role is configured. (by @KamilSzczygieł, [#378](https://github.com/tempoxyz/tidx/pull/378))
- Fixed tiered split queries erroring on PostgreSQL availability failures; they now degrade to the full-history ClickHouse archive while preserving PostgreSQL semantic errors. (by @jxom, [#286](https://github.com/tempoxyz/tidx/pull/286))
- Stored holder balance deltas as unsigned `UInt256` magnitude with the sign carried in `leg`. (by @jxom, [#247](https://github.com/tempoxyz/tidx/pull/247))

## `tidx@0.7.0`

### Minor Changes

- Added `address_balances_snapshot`, a refreshable ClickHouse materialized view that pre-aggregates per-address token balances from `address_holder_deltas`, so hot account balance pages and counts hit a holder-keyed range instead of re-aggregating tens of millions of delta rows.
- Added decoded stablecoin-DEX event tables `dex_pairs`, `dex_orders`, and `dex_fills` as insert-time ClickHouse materialized views over `logs`. `dex_fills` denormalizes the `OrderFilled`/`OrderPlaced` join at ingest (token, side, tick attached per fill, ordered by `(token, block_num, log_idx)`), turning pair-swap and OHLC scans into a primary-key range read instead of a join plus correlated subquery. (by @jxom, [#227](https://github.com/tempoxyz/tidx/pull/227))
- Added `dex_pair_liquidity`, a ClickHouse view that joins `dex_pairs` to the DEX escrow balances in `token_balances_snapshot`, so the exchange pairs-by-liquidity endpoint can read ranked pairs directly instead of over-fetching escrow balances and intersecting base/quote pairs in memory. (by @jxom, [#227](https://github.com/tempoxyz/tidx/pull/227))
- Added `token_holder_counts`, a refreshable ClickHouse materialized view that pre-aggregates per-token holder counts from `token_balances_snapshot`, so token detail and holder endpoints hit a point lookup instead of a high-cardinality `count()` scan over every holder row. (by @jxom, [#226](https://github.com/tempoxyz/tidx/pull/226))

### Patch Changes

- Denormalized the tx-level `type` and `fee_token` onto the ClickHouse `receipts` table (populated from the matching tx at ingest, with a migration for existing deployments), so receipt-list queries no longer have to join `txs` to read those fields. (by @jxom, [#230](https://github.com/tempoxyz/tidx/pull/230))

## `tidx@0.6.1`

### Patch Changes

- Added `token_balances_snapshot`, a refreshable ClickHouse materialized view that pre-aggregates holder balances from `token_holder_deltas` on a schedule so holder counts and listings hit the primary key instead of timing out on high-cardinality tokens. (by @jxom, [#208](https://github.com/tempoxyz/tidx/pull/208))

## `tidx@0.6.0`

### Minor Changes

- Added ClickHouse materialized views for token and address analytics: `token_transfers`, `token_balances`, `token_supply`, `token_approvals`, `token_transfer_stats`, `token_metadata`, `address_transfers`, `address_balances`, `address_txs`, and `contract_creations`. Available when running with `engine="clickhouse"`. (by @jxom, [#198](https://github.com/tempoxyz/tidx/pull/198))
- Removed pgroll runtime support so PostgreSQL schema upgrades are handled by tidx's idempotent startup migrations.

## `tidx@0.5.6`

### Patch Changes

- Added a `consensus_proposer` column to the `blocks` table for `TIP-1031` (by @0xrusowsky, [#178](https://github.com/tempoxyz/tidx/pull/178))

## `tidx@0.5.5`

### Patch Changes

- Harden PostgreSQL SQL validation by fixing CTE scope handling, schema-qualified table checks, recursive depth accounting, LIMIT ALL rejection, and traversal of previously unchecked AST clauses. (by @BrendanRyan, [#179](https://github.com/tempoxyz/tidx/pull/179))
- Validate public ClickHouse queries, block system catalogs and dangerous table functions, enforce ClickHouse request timeouts, and validate view SELECT SQL before execution. (by @BrendanRyan, [#180](https://github.com/tempoxyz/tidx/pull/180))
- Bound PostgreSQL query result processing by streaming rows with a hard request limit and appending automatic LIMIT clauses on a separate line. (by @BrendanRyan, [#181](https://github.com/tempoxyz/tidx/pull/181))
- Hardened view administration by failing closed for trusted CIDR checks, rejecting malformed CIDR configuration, hot-reloading active trusted CIDRs, and requiring an explicit admin mutation header. (by @BrendanRyan, [#182](https://github.com/tempoxyz/tidx/pull/182))

## `tidx@0.5.4`

### Patch Changes

- Added pgroll support to the tidx binary and Docker image, including bundled (by @o-az, [#175](https://github.com/tempoxyz/tidx/pull/175))

## `tidx@0.5.3`

### Patch Changes

- Fixed a migration ordering issue. (by @o-az, [#172](https://github.com/tempoxyz/tidx/pull/172))

## `tidx@0.5.2`

### Patch Changes

- Added support for virtual address detection (by @o-az, [#170](https://github.com/tempoxyz/tidx/pull/170))

## `tidx@0.5.1`

### Patch Changes

- feat: /status endpoint & remove Andantino references (by @o-az, [#149](https://github.com/tempoxyz/tidx/pull/149))

## `tidx@0.5.0`

### Minor Changes

- ### Performance
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
- ### Features
- **Read replica support** — New `api_pg_url` and `api_pg_password_env` config options to route API queries to a separate PostgreSQL read replica. ([#119](https://github.com/tempoxyz/tidx/pull/119))
- **Filter pushdown for raw columns** — `address`, `block_num`, `tx_hash`, and other raw log columns in event CTE queries are now pushed down into the inner WHERE clause for index utilization. ([#113](https://github.com/tempoxyz/tidx/pull/113))
- **CLI short flags** — All `tidx query` flags now have short aliases: `-u` (url), `-n` (chain-id), `-e` (engine), `-f` (format), `-l` (limit), `-s` (signature), `-t` (timeout). ([#145](https://github.com/tempoxyz/tidx/pull/145))
- **Basic auth in CLI** — `--url http://user:pass@host:8080` extracts credentials and sends them as HTTP basic auth. ([#145](https://github.com/tempoxyz/tidx/pull/145))
- **TOON output format** — New `-f toon` output format using token-efficient notation for LLM consumption. ([#145](https://github.com/tempoxyz/tidx/pull/145))
- **ClickHouse HTTP basic auth** — Support `user` and `password_env` in `[chains.clickhouse]` config. ([#105](https://github.com/tempoxyz/tidx/pull/105))
- **Agent skills** — Added `querying-tempo` and `indexing-tempo` skills for AI-assisted development. ([#145](https://github.com/tempoxyz/tidx/pull/145))
- ### Fixes
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
- ### Tests
- **Gap detection tests** — Added `has_gaps` integration tests. ([#131](https://github.com/tempoxyz/tidx/pull/131)) (by @jxom, [#147](https://github.com/tempoxyz/tidx/pull/147))

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
