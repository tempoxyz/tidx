---
name: indexing-tempo
description: Set up and run tidx to index Tempo blockchain data. Covers installation, configuration, deployment (Docker, bare metal), PostgreSQL + ClickHouse setup, sync architecture, monitoring, and operational best practices. Use when deploying or operating a tidx indexer.
---

# Indexing Tempo with tidx

tidx indexes Tempo chain data into PostgreSQL (OLTP) and optionally ClickHouse (OLAP) for fast point lookups and analytics.

## Quick Start

### One-line Install

```bash
curl -sSf https://tidx.vercel.app/install | sh
```

This installs the `tidx` binary to `~/.local/bin/`.

### Initialize & Run

```bash
tidx init                # Generate config.toml
# Edit config.toml with your chain RPC + PostgreSQL URL
tidx up                  # Start indexing + HTTP API
tidx status              # Watch sync progress
```

### Docker (Recommended for Production)

```bash
curl -L https://tidx.vercel.app/docker | bash
```

Or manually:

```bash
git clone https://github.com/tempoxyz/tidx
cd tidx
docker compose -f docker/prod/docker-compose.yml up -d
```

This starts PostgreSQL, ClickHouse, tidx, and optionally Prometheus + Grafana.

## Installation

### Binary (Linux / macOS)

```bash
# Latest
curl -sSf https://tidx.vercel.app/install | sh

# Specific version
curl -sSf https://tidx.vercel.app/install | sh -s v0.4.0

# Self-update
tidx upgrade
```

Supported: `linux/amd64`, `linux/arm64`, `darwin/amd64`, `darwin/arm64`

### Docker

```bash
docker pull ghcr.io/tempoxyz/tidx:latest
docker run -v $(pwd)/config.toml:/config.toml ghcr.io/tempoxyz/tidx up
```

### From Source

```bash
git clone https://github.com/tempoxyz/tidx
cd tidx
cargo build --release
# Binary at ./target/release/tidx
```

## Configuration

tidx uses a `config.toml` file. Generate one with `tidx init`.

### Minimal Config

```toml
[http]
port = 8080

[[chains]]
name = "mainnet"
chain_id = 4217
rpc_url = "https://rpc.tempo.xyz"
pg_url = "postgres://user:pass@localhost:5432/tidx_mainnet"
```

### Full Config

```toml
[http]
enabled = true
port = 8080
bind = "0.0.0.0"
trusted_cidrs = ["100.64.0.0/10"]  # Tailscale, for admin ops (views)

[prometheus]
enabled = true
port = 9090

[[chains]]
name = "mainnet"
chain_id = 4217
rpc_url = "https://rpc.tempo.xyz"
pg_url = "postgres://user@host:5432/tidx_mainnet"
pg_password_env = "PG_PASSWORD"         # Password from env var
api_pg_url = "postgres://user@replica:5432/tidx_mainnet"  # Read replica for API
api_pg_password_env = "PG_API_PASSWORD"
batch_size = 500                         # Blocks per RPC batch (default: 100)
concurrency = 8                          # Parallel gap-fill workers (default: 4)
backfill = true                          # Sync to genesis (default: true)
trust_rpc = false                        # Skip reorg detection (default: false)

[chains.clickhouse]
enabled = true
url = "http://clickhouse:8123"
failover_urls = ["http://clickhouse-2:8123"]
user = "default"
password_env = "CH_PASSWORD"

[[chains]]
name = "testnet"
chain_id = 42429
rpc_url = "https://rpc.testnet.tempo.xyz"
pg_url = "postgres://user@host:5432/tidx_testnet"
pg_password_env = "PG_PASSWORD"
```

### Config Reference

```
[http]
├── enabled           bool      = true       Enable HTTP API
├── port              u16       = 8080       HTTP port
├── bind              string    = "0.0.0.0"  Bind address
└── trusted_cidrs     string[]  = []         CIDRs for admin ops (view creation/deletion)

[prometheus]
├── enabled           bool      = true       Enable metrics endpoint
└── port              u16       = 9090       Metrics port

[[chains]]
├── name              string    (required)   Display name
├── chain_id          u64       (required)   Chain ID
├── rpc_url           string    (required)   JSON-RPC URL
├── pg_url            string    (required)   PostgreSQL URL
├── pg_password_env   string    (optional)   Env var for PG password
├── api_pg_url        string    (optional)   Separate PG URL for API (read replica)
├── api_pg_password_env string  (optional)   Env var for API PG password
├── batch_size        u64       = 100        Blocks per RPC batch
├── concurrency       usize     = 4          Parallel gap-fill workers
├── backfill          bool      = true       Sync to genesis
├── backfill_first    bool      = false      Complete backfill before realtime
├── trust_rpc         bool      = false      Skip reorg detection
└── [clickhouse]
    ├── enabled       bool      = false      Enable ClickHouse
    ├── url           string    = "http://clickhouse:8123"
    ├── failover_urls string[]  = []         Failover ClickHouse URLs
    ├── database      string    (optional)   DB name (default: tidx_{chain_id})
    ├── user          string    (optional)   HTTP basic auth user
    └── password_env  string    (optional)   Env var for CH password
```

## Tempo Networks

| Network | Chain ID | RPC |
|---------|----------|-----|
| Presto (mainnet) | 4217 | https://rpc.presto.tempo.xyz |
| Andantino (testnet) | 42429 | https://rpc.testnet.tempo.xyz |
| Moderato | 42431 | https://rpc.moderato.tempo.xyz |

## CLI Commands

```
tidx init       Initialize config.toml
tidx up         Start indexing + HTTP API
tidx status     Show sync status (-w for watch mode, --json for JSON)
tidx query      Run SQL queries (see querying-tempo skill)
tidx views      Manage ClickHouse materialized views
tidx upgrade    Self-update to latest version
```

## Sync Architecture

tidx runs two concurrent sync operations:

- **Realtime** — follows chain head with ~0 lag
- **Gap Sync** — fills all missing blocks from most recent to earliest

```
Block Numbers:  0                                                    HEAD
                │                                                      │
    INDEXED:    ░░░░░░░░░████████████░░░░░░░░░░░░░░░░░░░░░██████████
                         │           │                     │        │
                       gap 2       gap 1                tip_num   head_num
                     (fills 2nd) (fills 1st)

    Gap Sync fills ALL gaps, most recent first.
    Realtime follows head immediately.
```

Recent gaps are prioritized so users can query recent data during initial sync. Gap sync detects discontinuities via SQL and fills them with configurable concurrency.

### Sync Tuning

| Setting | Default | Recommendation |
|---------|---------|----------------|
| `batch_size` | 100 | 500 for fast RPCs, 50 for rate-limited |
| `concurrency` | 4 | 8–16 for bare metal, 4 for shared |
| `backfill_first` | false | true if you need complete history before serving queries |
| `trust_rpc` | false | true for trusted/private RPCs (skips reorg detection, faster) |

## Deployment

### Docker Compose (Recommended)

The production compose file at `docker/prod/docker-compose.yml` includes:

- **PostgreSQL 16** — tuned for write-heavy indexing (WAL, checkpoints, shared_buffers)
- **ClickHouse** — for OLAP analytics
- **tidx** — the indexer
- **Prometheus** — metrics collection (optional, `--profile monitoring`)
- **Grafana** — dashboards (optional, `--profile monitoring`)

```bash
# Start core services
docker compose -f docker/prod/docker-compose.yml up -d

# With monitoring
docker compose -f docker/prod/docker-compose.yml --profile monitoring up -d

# View logs
docker compose -f docker/prod/docker-compose.yml logs -f tidx

# Access:
#   HTTP API:    http://localhost:8080
#   Prometheus:  http://localhost:9091
#   Grafana:     http://localhost:3000 (admin/admin)
```

### Bare Metal

For maximum performance, run on bare metal with NVMe storage.

**Recommended providers:**
- [Latitude.sh](https://latitude.sh) — bare metal with NVMe, good for high-throughput indexing
- [Hetzner](https://hetzner.com) — affordable dedicated servers with NVMe
- [OVHcloud](https://ovhcloud.com) — dedicated servers in EU/US

## PostgreSQL Tuning

### For Indexing (Write-Heavy)

```
shared_buffers = 4GB              # 25% of RAM
work_mem = 64MB                   # Per-query sort/hash memory
maintenance_work_mem = 512MB      # For CREATE INDEX, VACUUM
max_wal_size = 16GB               # Reduce checkpoint frequency
checkpoint_timeout = 30min        # Less frequent checkpoints
synchronous_commit = off          # 2-3x faster writes (safe for indexer)
wal_level = minimal               # Minimal WAL if no replication
max_wal_senders = 0               # Disable replication slots
```

### For Queries (Read-Heavy)

If using a read replica (`api_pg_url`):

```
shared_buffers = 8GB              # More cache for reads
effective_cache_size = 24GB       # Tell planner about OS cache
random_page_cost = 1.1            # NVMe is fast
```

### Read Replica

For production, use a PostgreSQL read replica for the API to isolate query load from write load:

```toml
[[chains]]
pg_url = "postgres://tidx@primary:5432/tidx"           # Writer
api_pg_url = "postgres://tidx@replica:5432/tidx"        # Reader (API)
api_pg_password_env = "PG_API_PASSWORD"
```

## Monitoring

### Prometheus Metrics

tidx exposes metrics on `:9090/metrics` (configurable). Key metrics:

- `tidx_sync_head_block` — chain head block number
- `tidx_sync_tip_block` — highest indexed block
- `tidx_sync_lag_blocks` — blocks behind head
- `tidx_query_duration_seconds` — query latency histogram
- `tidx_query_rows_total` — total rows returned
- `tidx_sink_block_rate` — write throughput (blocks/sec)

### Status Endpoint

```bash
curl http://localhost:8080/status
```

Returns per-chain sync progress, gap info, backfill ETA, and per-store watermarks.

### Health Check

```bash
curl http://localhost:8080/health
# Returns "OK"
```
