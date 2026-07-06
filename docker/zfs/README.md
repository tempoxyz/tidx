# ZFS zstd Storage Experiment

Tooling for the experiment in [`tasks/zfs-zstd-plan.md`](../../tasks/zfs-zstd-plan.md):
measure whether transparent ZFS zstd compression under **stock Postgres 16**
meaningfully shrinks tidx's footprint at acceptable ingest/query cost. No tidx
code changes — only filesystem properties and three ZFS-safe Postgres flags.

> **Warning** — the ZFS lanes set `full_page_writes=off`. That is only safe
> while PGDATA lives on a copy-on-write filesystem (ZFS): CoW makes torn pages
> impossible. Never reuse these Postgres flags on ext4/xfs.

## Requirements

Linux host with Docker (compose v2) and OpenZFS ≥ 2.1 (`apt install
zfsutils-linux`). macOS cannot run this directly — OrbStack/Docker Desktop VMs
lack the ZFS kernel module. Use a Lima VM with a stock Ubuntu kernel, or any
Linux box.

## Usage

```sh
# one-time: pool (dedicated device preferred; file vdev = ratio-accurate only)
sudo POOL=tidx scripts/zfs/provision.sh pool --file /var/tmp/tidx-zpool.img 100G

# authed RPC strongly recommended (public endpoint throttles at ~429s)
export TEMPO_RPC_AUTH_MODERATO='user:pass'

# lanes (each: fresh datasets -> sync 1M Moderato blocks -> measure -> OLTP suite)
sudo -E scripts/zfs/run-lane.sh l0   # ext4 baseline
sudo -E scripts/zfs/run-lane.sh l1   # 16k zstd-3
sudo -E scripts/zfs/run-lane.sh l2   # 32k zstd-3
sudo -E scripts/zfs/run-lane.sh l3   # 16k lz4
sudo -E scripts/zfs/run-lane.sh l4   # 32k zstd-9 (optional ratio ceiling)
```

Results land in `results/zfs/<lane>/`:

| file | contents |
|---|---|
| `summary.md` / `raw.env` | physical vs logical bytes, zfs `compressratio`, GB per 1M blocks |
| `tables.tsv` | per-table logical sizes (heap / indexes / total) |
| `oltp.md` / `oltp.tsv` | latency + tps per query shape |
| `sync.env` | wall time + average blocks/s |
| `status.json`, `compose.log` | last tidx status snapshot, container logs |

Between reruns of the same lane: `sudo scripts/zfs/provision.sh destroy <lane>`
(ZFS lanes) or clear `$L0_DIR` (baseline).

## Knobs and why

- `recordsize=16k/32k`: compresses 2–4 Postgres pages per record; `8k` defeats
  compression (each page compresses alone) and is deliberately not a lane.
- `compression=zstd-3` default lanes, `lz4` as the CPU-floor control, `zstd-9`
  as the ratio ceiling.
- WAL lives on its own dataset (`128k`/`lz4`, sequential writes; excluded from
  the size verdict).
- `wal_init_zero=off`, `wal_recycle=off`: zero-filling and segment recycling
  fight CoW filesystems.
- `logbias=latency`, `primarycache=all` (defaults): tidx runs
  `synchronous_commit=off`, so the ZIL is barely exercised; the ARC caches
  blocks compressed.
- Postgres settings otherwise match `docker/prod` for comparability.

## Interpreting results

The per-lane compression ratio (`logical / physical`) is exact regardless of
which block range synced. Cross-lane `GB per 1M blocks` drifts slightly because
backfill walks backward from the moving tip — check `raw.env` for each lane's
block range and row counts before comparing, and prefer ratios plus bytes/row
when ranges diverge. Decision gate and expected outcomes: see the plan doc.
