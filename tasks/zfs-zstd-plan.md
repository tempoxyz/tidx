# ZFS zstd Under Stock Postgres — Experiment Plan

> Status: **executed 2026-07-06 — results below; lz4 adopted as optional
> deployment guidance** (`docker/prod/docker-compose.zfs.yml`).
> Branch: `jxom/zfs-zstd-experiment` off main `2f37b44`.

## Results (2026-07-06, Hetzner 16c/30GB, Moderato ~1M blocks/lane, authed RPC)

Pool: 150G file-backed vdev on the local root disk (ratios exact; ingest
numbers carry some file-vdev overhead). Each lane synced ~1.0M blocks
(~9M txs, ~20M logs) backward from the moving tip; block ranges differ
slightly between lanes, ratios are per-lane exact.

### Storage

| lane | config | physical | zfs compressratio | GB per 1M blocks |
|---|---|---:|---:|---:|
| l0 | ext4 baseline | 23.4 GB | — | 23.2 |
| l1 | ZFS 16k zstd-3 | 8.2 GB | **2.88x** | **8.1** |
| l2 | ZFS 32k zstd-3 | 6.9 GB | **3.78x** | **6.9** |
| l3 | ZFS 16k lz4 | 9.9 GB | 2.42x | 9.7 |

Better than the 1.6–2.6x estimate; l2 live-syncs to the same footprint as
OrioleDB's best *bulk-loaded* lane (6.1 GB/M). WAL side effect: with
`full_page_writes=off` + `wal_recycle=off` the WAL dir holds ~4 MB residual
vs 17 GB of recycled segments on the ext4 baseline.

### Performance (shared_buffers=256MB — see caveat)

| lane | ingest blk/s | vs l0 | point lookups | keyset pages |
|---|---:|---:|---|---|
| l0 | 1512 | — | 0.10–0.26 ms | 0.24–0.26 ms |
| l1 | 493 | 33% | parity to +14% | 1.0–1.8 ms (4–7x) |
| l2 | 272 | 18% | +35–58% | 1.5–2.6 ms (6–10x) |
| l3 | 985 | **65%** | parity | 0.37–0.43 ms |

Caveat: with `shared_buffers=256MB` nearly every page read goes through the
compressed ARC and pays decompression; a production-sized buffer pool pulls
hot-path reads back toward parity (validation run with 4GB below). The
keyset regression is a read-amplification artifact of that config, worst for
zstd-32k, minor for lz4.

### Verdict vs gate

- **Ratio gate (≥1.7x): pass** on every ZFS lane.
- **Ingest gate (≥90%): fail** — zstd pays 3–5x, lz4 ~1.5x.
- **OLTP gate (≤15%): pass for lz4 point lookups; fail for zstd keysets**
  under the small-buffer config.

**Decision:** not a default, and not a competitor to the tiered
(partitioned PG + pg_clickhouse) design — that remains the production
recommendation (capped ~7 GB with full-speed serving). ZFS **lz4** is
adopted as *optional deployment guidance* for self-hosted monolithic
deployments (no ClickHouse) and archive/replica boxes: 2.4x smaller at
near-parity reads and ~⅔ ingest. zstd-3/16k is the documented alternative
when storage cost dominates and sync speed only needs to beat the chain's
real-time block rate. Deliverables: `docker/prod/docker-compose.zfs.yml`,
`scripts/zfs/provision.sh`, and the "Postgres on ZFS" section in the README.

Follow-up validation (same box): rerunning the lz4 lane with
`shared_buffers=4GB` lifted ingest to ~1,606 blk/s — at or above the ext4
baseline (1,512 blk/s, measured with 256MB buffers) — and pulled keyset
pages back to 0.28–0.30 ms (from 0.37–0.43) with point lookups at parity
(0.10–0.20 ms). The ingest/read penalties were dominated by the undersized
buffer pool, not compression. The production override therefore defaults to
`shared_buffers=4GB`.

The experiment tooling (lane runner, measurement, OLTP suite) lived at
`docker/zfs/` + `scripts/zfs/` through commit `a58efd2` and was removed
after the experiment concluded; only the minimal production setup remains.

## Goal

Measure whether transparent ZFS zstd compression under stock Postgres meaningfully
shrinks tidx's Postgres footprint at acceptable ingest/query cost — with **zero**
schema, engine, or code changes. This is the "alternative 5" lever from the storage
investigation: unlike OrioleDB (patched PG, beta bugs) or tiering (serving-layer
contracts), ZFS compresses heap **and indexes** (45–48% of PG bytes) while preserving
every OLTP property exactly: point lookups, keyset pagination, joins, `ON CONFLICT`,
`COPY BINARY`.

Reference numbers from prior experiments (Moderato, 1.09M blocks):

| Lane | Size per 1M blocks | Notes |
|---|---|---|
| Stock heap PG16 on ext4 | **20.2 GB** | 10.2 GB of that is btree indexes |
| OrioleDB zstd-5 (live sync) | 13.1 GB (−35%) | rejected: beta bugs, patched PG |
| ClickHouse (archive) | ~11.5 GiB | columnar, already in stack |

Public guidance suggests ZFS zstd gets 2–4× on database data, but tidx's bytes are
hash-heavy (random 32-byte values), so the honest expectation is **1.6–2.5×**.
That's what this experiment pins down.

## Non-goals

- No changes to tidx source, schema, or migrations.
- No ClickHouse-on-ZFS tuning (CH already compresses; its volume stays untouched).
- No interaction with the tiering/partitioning work — this stacks under whatever
  layout Postgres has. Measurements here use main's monolithic layout.
- Not a durability/HA evaluation (single-disk pool, no raidz/mirror opinions).

## Phase 0 — Environment

Requires a Linux host with OpenZFS ≥ 2.1 (zstd support; 2.2+ preferred) and Docker.
macOS is out — this runs on the benchmark box used for the prior storage experiments.

1. Install ZFS (`zfs --version` to confirm).
2. Create the pool. Preferred: dedicated NVMe partition/device.
   Fallback (ratio-accurate, perf-approximate): file-backed vdev on the NVMe fs:
   ```sh
   truncate -s 100G /var/tmp/tidx-zpool.img
   sudo zpool create -o ashift=12 tidx /var/tmp/tidx-zpool.img
   ```
3. Base properties (apply to pool root, inherited by lanes):
   ```sh
   sudo zfs set atime=off xattr=sa dedup=off tidx
   ```
4. Cap ARC so it coexists with tidx + CH on the box, e.g. 4 GB:
   `echo 4294967296 | sudo tee /sys/module/zfs/parameters/zfs_arc_max`
5. Per lane, two datasets (data + WAL), bind-mounted into the postgres container:
   ```sh
   sudo zfs create -o recordsize=16k -o compression=zstd-3 tidx/l1-pgdata
   sudo zfs create -o recordsize=128k -o compression=lz4   tidx/l1-pgwal
   ```
   Bind mounts avoid Docker storage-driver concerns entirely; only the volume
   mapping changes vs. the stock compose files.

## Lane matrix

Each lane: fresh datasets → sync Moderato from block 0 to **1,000,000** (authed RPC,
same protocol as prior runs) → measure sizes → run the OLTP query suite.

| Lane | Filesystem | recordsize | compression | PG overrides beyond current compose |
|---|---|---|---|---|
| **L0** | ext4 (baseline) | — | — | none (current `docker/prod` settings) |
| **L1** | ZFS | 16k | zstd-3 | `full_page_writes=off`, `wal_init_zero=off`, `wal_recycle=off` |
| **L2** | ZFS | 32k | zstd-3 | same as L1 |
| **L3** | ZFS | 16k | lz4 | same as L1 (perf floor / CPU control) |
| **L4** (optional) | ZFS | winner of L1/L2 | zstd-9 | same; run only if L1/L2 show CPU headroom |

Notes on the knobs (verified against current guidance, mid-2026):

- `recordsize=8k` (PG page size) kills compression — each 8k page compresses alone.
  16k is the common DB recommendation; 32k trades a little read/write amplification
  for better ratio. Both are in the matrix; 8k is not (known-bad for this goal).
- `full_page_writes=off` is safe on ZFS: CoW makes torn pages impossible. It must be
  loudly documented as ZFS-only. Also shrinks WAL volume substantially.
- `wal_init_zero=off`, `wal_recycle=off`: standard on ZFS; zero-filling and segment
  recycling fight CoW.
- `logbias` stays `latency` (default): `throughput` causes long-term fragmentation
  and tidx already runs `synchronous_commit=off`, so the ZIL is barely exercised.
- `primarycache=all` (default): with `shared_buffers=256MB` in prod compose, the ARC
  is the real cache, and it caches blocks **compressed** — more hot data per GB of RAM.
- OpenZFS 2.3 direct IO (`direct=standard`) is irrelevant here: PG uses buffered IO.

## Measurements per lane

Sizes (the headline):

- `zfs get used,logicalused,compressratio tidx/lN-pgdata` — physical vs logical.
- `du -sh` of PGDATA (cross-check).
- `pg_total_relation_size()` per table (blocks/txs/logs/receipts, heap vs indexes) —
  these report *logical* bytes, so `logical / zfs-used` is the true ratio, and the
  per-table logical numbers stay comparable with all prior experiments.
- Report as **GB per 1M blocks** next to the reference table above.

Ingest:

- Wall time to block 1M and sustained blk/s (tidx Prometheus metrics / logs).
- Host CPU during sync (`pidstat 5` on the postgres and tidx processes) — the
  compression tax shows up here.
- WAL bytes written (`pg_current_wal_lsn()` delta) — quantifies the FPW=off win.

OLTP suite (after sync completes, warm cache, `pgbench -T 60` per shape):

- keyset page: txs by `(block_num, tx_idx)` descending
- keyset page: logs by `(block_num, log_idx)` descending
- block by number
- tx by hash (block-bounded and unbounded)
- logs by address (recent range)
- receipt by tx hash (bounded)
- recent 100-block range scan

These mirror the B3 shapes from the tiering evaluation so results are comparable.
Suite lives in `scripts/zfs/oltp/*.sql` + a small `bench.sh` runner (new, this branch).

## Decision gate

Adopt (as deployment guidance, not code) if the winning ZFS lane shows:

- **ratio:** ≥ 1.7× physical reduction (≤ ~12 GB per 1M blocks), AND
- **ingest:** ≥ 90% of L0 blk/s, AND
- **OLTP:** ≤ 15% p95 regression on the bounded shapes.

If it passes → deliverables:

1. `docker/zfs/README.md` + a compose override example: bind-mount datasets, PG
   flag additions (`full_page_writes=off` etc.) with the ZFS-only warning.
2. `scripts/zfs/provision.sh` (pool/dataset creation per the matrix winner).
3. Results appended to this doc (`tasks/zfs-zstd-plan.md` → results section).
4. README/storage docs paragraph: when to choose ZFS (self-hosted, monolithic or
   hot-window PG) and when not to (managed PG, no root).

If it fails → record the negative result here with the numbers, so the option is
dead with evidence rather than assumption.

## Risks / gotchas

- **File-backed vdev** perf numbers are indicative only; re-run the winner on a real
  partition before publishing guidance.
- **Fragmentation**: CoW pools degrade past ~80% full; long-lived deployments should
  keep headroom. Note in docs if adopted.
- **`full_page_writes=off` portability**: catastrophic if someone moves PGDATA to a
  non-CoW filesystem with the flag still off. The override file must scream this.
- **Double compression**: ZFS under ClickHouse volumes is wasted CPU — CH volume
  stays off-pool (or `compression=off` dataset if it must share).
- **RPC throttling**: prior runs hit 429s on public testnet RPC; use the authed
  endpoint from the earlier experiments.

## Open questions

1. Which box? Assumed: the Linux bench box from the OrioleDB/tiering runs (has
   Docker, room for ~100 GB pool). Needs root for the kernel module.
2. Reuse the existing 20.2 GB/M ext4 baseline, or re-run L0 on the same box for a
   clean same-hardware comparison? (Plan assumes re-run; it's ~25 min.)
3. Is the authed Moderato RPC key still available for unthrottled sync?
