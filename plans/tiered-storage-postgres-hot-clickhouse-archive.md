# Plan: Tiered Storage — Postgres Hot Window + ClickHouse Archive

**Status:** Proposed
**Scope:** Keep only the last **30 days** of blocks in PostgreSQL (hot tier) and rely on
ClickHouse as the full archive (cold tier). Prune PG by dropping time partitions once
ClickHouse durably holds the data. Optionally expose full history through the postgres
engine via `pg_clickhouse` foreign tables.

**Why:** PG replicas are ~2.5 TB and growing. ClickHouse already mirrors the full chain,
so PG storing all history is pure duplication.

**Relation to other plans:** `clickhouse-only-and-api-decoupling.md` proposes removing PG
entirely. This plan is the intermediate (or alternative) step: PG stays as a small,
fast, indexed hot store; ClickHouse becomes the only place full history lives. Phase 1
here reduces PG to a bounded size regardless of which end-state wins.

---

## 1. Prior Art: TimescaleDB Tiered Storage

Tiger/Timescale tiers hypertables to object storage. Their implementation is
proprietary cloud infra, but four principles transfer directly:

1. **Tier by chunk (time partition)**, never by row.
2. **Migrate asynchronously** — tiering never blocks ingest.
3. **Delete the hot copy only after the cold copy is durable.**
4. **Tiered reads are opt-in** (`timescaledb.enable_tiered_reads` defaults to off) so
   hot-path queries never pay the cold-storage round trip.

TIDX is better positioned than a generic Postgres app: the cold tier (ClickHouse)
already exists as a first-class sink with its own schema, sync path, and query engine.

## 2. Current Architecture Facts (verified in repo)

| Fact | Where |
|---|---|
| Realtime + gap-fill dual-write PG and CH concurrently via `try_join!` — both succeed or the batch retries | `src/sync/sink.rs` (`SinkSet::write_blocks` etc.) |
| CH catch-up backfill **reads rows out of PG** and advances a sequential cursor | `src/sync/sink.rs` (`backfill_clickhouse`) |
| Cursor `ch_backfill_block` persisted in PG `sync_state`; advances only after all four CH tables succeed for a range | `db/sync_state.sql`, `src/sync/sink.rs` |
| Gap detection scans PG for missing block ranges from genesis | `src/sync/engine.rs` (`detect_all_gaps`) |
| `sync_state.synced_num` = highest contiguous block from genesis (PG) | `db/sync_state.sql` |
| PG stores binary columns as `bytea`; query layer rewrites `'0x…'` → `'\x…'` | `src/query/mod.rs` (`convert_hex_literals_postgres`) |
| CH stores binary columns as **`0x`-prefixed hex text** | `src/sync/ch_sink.rs` (`hex_encode`) |
| PG PKs lead with timestamp (e.g. `blocks (timestamp, num)`) — partition-key compatible | `db/blocks.sql` etc. |
| Query validator is reject-by-default with a PG table allowlist | `src/query/validator.rs` (`POSTGRES_ALLOWED_TABLES`) |
| Engine selection per request: `QueryEngine::{Postgres, ClickHouse}` | `src/query/router.rs` |
| No retention/pruning mechanism exists today | grep of `src/config.rs`, `src/sync/` |

Two consequences worth calling out:

- **Prune gating is mandatory, not defensive.** `backfill_clickhouse` sources from PG.
  Pruning PG rows the CH cursor has not passed = permanent data loss.
- **The representation mismatch (bytea vs `0x` hex text) shapes the read design.**
  A CH foreign table cannot be a partition of a PG hot table (partitions must share the
  parent row type), and any hot∪cold view must normalize on the **PG side**
  (`'0x' || encode(col,'hex')`) — a `decode()` around CH columns cannot push down and
  would force full-column fetches from the big side.

## 3. Target Architecture

```
                 ┌─────────────┐
   RPC ─────────▶│ Sync engine │
                 └──────┬──────┘
              SinkSet   │   (one fetch, two sinks)
            ┌───────────┴────────────┐
            ▼                        ▼
   ┌─────────────────┐      ┌────────────────┐
   │ PG sink         │      │ CH sink        │
   │ (all writes,    │      │ (all writes)   │
   │  pruned behind) │      └───────┬────────┘
   └────────┬────────┘              ▼
            ▼                ┌────────────────┐
   ┌─────────────────┐       │ ClickHouse     │
   │ Postgres (hot)  │◀─FDW──│ full archive   │
   │ last 30d,       │       │ genesis → head │
   │ partitioned     │       └────────────────┘
   └─────────────────┘
     prune = DROP PARTITION,
     gated on CH watermark
```

- Default `engine=postgres`: hot 30d window only (no regression in latency).
- `engine=clickhouse`: full history (exists today).
- Phase 2 `engine=tiered` (optional): hot∪cold through PG via `pg_clickhouse`.

## 4. Phase 1 — Retention in Postgres

This alone fixes the storage problem. Ship it first; Phase 2 is independent.

### 4.1 Config

New optional block on `ChainConfig` (`src/config.rs`):

```toml
[chains.retention]
pg_keep = "30d"            # retention window (duration; blocks derivable)
require_clickhouse = true  # refuse to prune unless the CH archive is caught up
```

Absent config = today's behavior (keep everything). `require_clickhouse = true` is the
default; setting it `false` is an explicit foot-gun opt-in for PG-only deployments that
genuinely want a rolling window.

### 4.2 Partition, don't DELETE

Convert `blocks`, `txs`, `logs`, `receipts` to **native range partitions on the
timestamp column** (weekly partitions; 30d window ⇒ ~5–6 live partitions per table).
All PKs already lead with timestamp, satisfying the partition-key constraint.

Pruning becomes `DROP TABLE` per partition:

- near-zero WAL (replicas replay a catalog change, not row deletes)
- no autovacuum churn
- disk returned to the OS immediately

`src/db/schema.rs` already owns DDL, so it grows partition management:

- on startup and periodically: `CREATE TABLE IF NOT EXISTS <t>_pYYYYWW PARTITION OF <t> FOR VALUES FROM (…) TO (…)` for current + next period
- pruner drops partitions whose entire range is below the prune boundary

Fallback if partitioning is vetoed: batched `DELETE` by block range + autovacuum.
Works, but means permanent vacuum pressure at this scale and a one-time `pg_repack`
to reclaim disk. Not recommended.

### 4.3 Prune gating (the "durable before delete" rule)

Prune boundary `B` (a block number, and its timestamp `T_B`) must satisfy **all**:

1. `T_B ≤ now − pg_keep` (30d window)
2. `B ≤ sync_state.synced_num` (PG is contiguous through B — no gaps that still need
   filling below the boundary)
3. `B ≤ sync_state.ch_backfill_block` (CH catch-up backfill has consumed PG rows
   through B) — when `require_clickhouse = true`
4. CH sink configured and healthy — when `require_clickhouse = true`

Because realtime/gap-fill writes are atomic across both stores (`try_join!`: both
succeed or the batch retries) and `ch_backfill_block` advances sequentially only after
all four CH tables succeed, conditions 2+3 together imply CH holds everything ≤ B.

Run a **one-time CH completeness audit** (count/min/max per table vs PG over the
boundary range) before the first large prune on the production deployment.

A partition is dropped only when its entire time range is below `T_B`.

### 4.4 Sync-engine integration (skip these and pruning corrupts sync)

New persisted watermark `sync_state.pruned_below` (block num, default 0):

- **Gap scanner floor.** `detect_all_gaps` (`src/sync/engine.rs`) must scan only above
  `pruned_below`, otherwise the dropped range looks like one giant gap and the
  gap-filler re-downloads the chain forever.
- **`synced_num` semantics** become "highest contiguous block from `pruned_below`"
  instead of "from genesis". Same for anything else assuming genesis-anchored
  contiguity (status output, metrics).
- **`backfill_num`** ("lowest block synced going backwards, 0=complete") treats
  `pruned_below` as its floor: backfill for PG is complete when it reaches
  `pruned_below`, not 0.

**Deliberately unchanged in v1:** the fetch pipeline still backfills to genesis and the
PG writer still writes everything; the pruner deletes behind the CH watermark. This
keeps CH archive completeness guaranteed by existing code paths (fresh deployments
included, since `backfill_clickhouse` sources from PG). Skipping PG writes below the
floor would require a CH-driven backfill bookkeeper — noted as future work in §7.

### 4.5 Pruner task

New `src/sync/pruner.rs`, spawned per chain alongside gap-fill in
`src/sync/engine.rs`:

1. every interval (e.g. 1h): compute `B` per §4.3; no-op if nothing qualifies
2. drop qualifying partitions in dependency-safe order: `logs`, `receipts`, `txs`,
   `blocks`
3. advance `sync_state.pruned_below = B` **before** dropping `blocks` partitions
   (crash between steps leaves extra data, never a lying watermark)
4. emit metrics: `tidx_pruned_below_block`, `tidx_prune_partitions_dropped_total`,
   last-prune timestamp; log each drop

### 4.6 Migration of the existing 2.5 TB deployment

**Re-bootstrap, don't migrate in place.** Retention means PG only needs the hot window:

1. deploy the new version with `[chains.retention]` configured but pointed at **new
   partitioned tables** created alongside the old ones (schema-managed swap), or more
   simply: stand up a fresh PG, let TIDX sync only `head − 30d … head` into it
2. verify CH archive completeness (§4.3 audit) against the **old** PG before cutover
3. cut the API over, drop the old tables / decommission the old PG

Hours of resync (30d of blocks) instead of days of `pg_repack` on 2.5 TB, and replicas
shrink to window size immediately. The one-time audit in step 2 is the point of no
return — old PG rows are the only copy of anything CH is missing.

### 4.7 Expected size

PG steady-state ≈ 30d of blocks/txs/logs/receipts + indexes + one partition of slack.
If 2.5 TB represents the full chain history, the hot window lands at a small fraction
of that (exact figure measurable today:
`SELECT pg_size_pretty(sum(pg_total_relation_size(…)))` over rows newer than 30d).
Growth becomes bounded: size tracks 30d of chain activity, not chain age.

## 5. Phase 2 (optional) — Full History via `pg_clickhouse`

Independent of Phase 1; vetoable. TIDX already routes full-history queries to
`engine=clickhouse`, so Phase 2's value is **mixed hot⋈cold queries** and
single-connection PG tooling (BI, ORMs) — not correctness.

### 5.1 What `pg_clickhouse` is

ClickHouse Inc's revival of Percona's `clickhouse_fdw`: a real PostgreSQL FDW
(`CREATE SERVER` + foreign tables, binary/HTTP + TLS). Strong pushdown: aggregates
(incl. `percentile_cont` → `quantile`, `FILTER (WHERE)` → `-If` combinators),
semi-joins. Caveats: first release — 10 of 22 TPC-H queries still don't push down
fully; full type coverage, subquery pushdown, and DML are roadmap items. Treat it as a
convenience layer; storage savings never depend on it.

### 5.2 Design

- Foreign tables `ch.blocks/txs/logs/receipts` mirroring the CH schemas
  (`String`→`text`, `Int64`→`bigint`, `DateTime64`→`timestamptz`).
- **Tiered views** = `UNION ALL` of the CH foreign table and the hot PG table, with PG
  bytea columns normalized to `0x`-hex text (`'0x' || encode(col,'hex')`).
  Normalization on the small side preserves pushdown on the big side (§2).
- **CHECK constraints on the foreign tables** (`CHECK (num < B)`,
  `CHECK (timestamp < T_B)`), refreshed by the pruner as the boundary advances.
  Postgres's default `constraint_exclusion = partition` applies constraint exclusion to
  `UNION ALL` arms, so hot-window-bounded queries plan away the CH scan entirely — no
  round trip.
- New `QueryEngine::Tiered` in `src/query/router.rs`, **opt-in per request**
  (`engine=tiered`). It reuses the ClickHouse-style hex handling (literals stay `0x…`
  text) and resolves table names to the tiered views. The validator allowlist
  (`src/query/validator.rs`) admits the tiered view names for this mode only.
- Default `engine=postgres` stays hot-only. No existing query regresses.

### 5.3 Known tradeoffs

- Hot-side hash/address lookups through tiered views lose bytea indexes (the
  `encode()` expression isn't indexed). Acceptable: point lookups belong on the default
  engines; expression indexes on the now-small hot tables are a cheap mitigation if
  needed.
- The event-signature CTE surface (`src/service/mod.rs`) needs a tiered variant that
  emits CH-style hex comparisons over the views.
- `pg_clickhouse` must be installed in the PG image (`Dockerfile`) and the extension
  version pinned; FDW server credentials wired from chain config.

## 6. Decided Parameters

| Decision | Value | Rationale |
|---|---|---|
| Retention window | **30d** | chosen by owner |
| Partition granularity | weekly | ~5–6 live partitions; coarse enough for cheap management, fine enough for prompt reclaim |
| Partition key | timestamp column | PKs already lead with it; aligns with `after=` API filters and CH monthly partitioning |
| Default read scope | hot-only for `engine=postgres` | Timescale's opt-in-tiered-reads lesson; protects hot-path latency |
| Migration strategy | re-bootstrap hot window, swap, drop old | hours instead of days; instant replica shrink |
| CH sink representation | unchanged (`0x` hex text) | changing it breaks all existing CH data and hex-text-assuming derived tables |
| Backfill/writes below floor | unchanged in v1 (write-then-prune) | keeps CH completeness guaranteed by existing paths |

## 7. Open Questions / Future Work

- Any consumers relying on full history via default `engine=postgres` today? They must
  move to `engine=clickhouse` (or Phase 2 `engine=tiered`) before the first prune.
- Skip PG writes below the retention floor during deep backfill (saves write
  amplification on fresh deployments; requires CH-driven backfill bookkeeping).
- Does `tidx compress` (`src/sync/compress.rs`) interact with partitioned tables?
  Verify before Phase 1 lands.
- Partition-aware `COPY BINARY` performance in the writer: routing overhead is expected
  to be small, but benchmark ingest before/after conversion (`cargo bench`).
- Reorg depth vs prune boundary: with a 30d window this is a non-issue (reorgs are
  orders of magnitude shallower), but the pruner should still refuse `B > head − 1000`
  as a belt-and-braces guard.

## 8. Implementation Order (incremental commits)

1. `feat(config): add [chains.retention] with pg_keep and require_clickhouse`
2. `feat(db): partitioned DDL for blocks/txs/logs/receipts + partition manager in schema.rs`
3. `feat(sync): pruned_below watermark; clamp gap detection and backfill floor`
4. `feat(sync): pruner task with CH-watermark gating and metrics`
5. `test: retention integration test (sync → prune → verify CH retains, PG window, gap scanner quiet)`
6. `docs: retention config + migration runbook (re-bootstrap)`
7. — Phase 2, if approved —
8. `feat(db): pg_clickhouse foreign tables + tiered views + boundary CHECK refresh`
9. `feat(query): engine=tiered routing, validator allowlist, CTE variant`
10. `test: tiered engine integration test (hot∪cold correctness, constraint exclusion)`
