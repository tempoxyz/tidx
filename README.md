<p align="center">
  <img src=".github/banner.png" alt="ak47" width="100%" />
</p>

<h1 align="center">ak47</h1>

<p align="center">
  <strong>High-throughput Tempo indexer in Rust</strong>
</p>

<p align="center">
  <a href="#quickstart">Quickstart</a> •
  <a href="#installation">Installation</a> •
  <a href="#configuration">Configuration</a> •
  <a href="#cli-reference">CLI</a> •
  <a href="#http-api">API</a> •
  <a href="#query-cookbook">Queries</a>
</p>

---

**ak47** indexes [Tempo](https://tempo.xyz) chain data into a hybrid PostgreSQL + DuckDB architecture for fast point lookups (OLTP) and lightning-fast analytics (OLAP). 

## Features

- **Hybrid Query Routing** — Automatic routing to DuckDB for analytics, PostgreSQL for point lookups
- **Dual Storage** — PostgreSQL for OLTP + DuckDB columnar for OLAP
- **Continuous Aggregates** — Materialized views that auto-refresh for instant analytics
- **Event Decoding** — Query decoded events by ABI signature (no pre-registration)
- **HTTP API + CLI** — Query data via REST, SQL, or command line
- **Tempo-Native** — Optimized for instant finality, TIP-20 tokens, and fast block times

## Table of Contents

- [Quickstart](#quickstart)
- [Overview](#overview)
- [Query Routing](#query-routing)
- [Installation](#installation)
- [Configuration](#configuration)
- [CLI](#cli)
- [HTTP API](#http-api)
- [Query Cookbook](#query-cookbook)
- [Database Schema](#database-schema)
- [Sync Architecture](#sync-architecture)
- [Development](#development)
- [License](#license)

## Quickstart

### Requirements

- [PostgreSQL](https://www.postgresql.org/download/)

### Install

```bash
curl -L https://ak47.wevm.dev/install | bash
```

### Run

```bash
# Initialize config (interactive)
ak47 init

# Start indexing
ak47 up

# Check status
ak47 status
```

### Docker Compose

```bash
git clone https://github.com/tempoxyz/ak47 && cd ak47
make up

# Query data
curl "http://localhost:8080/query?sql=SELECT * FROM blocks ORDER BY num DESC LIMIT 5"
```

## Overview

ak47 uses a hybrid PostgreSQL + DuckDB architecture that automatically routes queries to the optimal engine:

```
                           ┌─────────────────┐
                           │   ak47 Router   │
                           └────────┬────────┘
                                    │
         ┌──────────────────────────┴──────────────────────────┐
         │                                                     │
         │ WHERE hash = '0x...'                 GROUP BY date  │
         │ WHERE block_num = 123                COUNT(*), SUM()│
         ▼                                                     ▼
┌─────────────────────┐                         ┌─────────────────────┐
│    PostgreSQL       │          sync           │      DuckDB         │
│    (OLTP)           │ ─────────────────────►  │      (OLAP)         │
└─────────────────────┘                         └─────────────────────┘
```

| Engine | Use Case | Example |
|--------|----------|---------|
| **PostgreSQL** | Point lookups, recent data | `WHERE hash = '0x...'` |
| **DuckDB** | Aggregations, scans, analytics | `GROUP BY`, `COUNT(*)`, `SUM()` |

## Installation

### Quick Install

```bash
curl -L https://ak47.wevm.dev/install | bash
```

### Docker

```bash
docker pull ghcr.io/tempoxyz/ak47:latest
docker run -v $(pwd)/config.toml:/config.toml ghcr.io/tempoxyz/ak47 up
```

### From Source

```bash
git clone https://github.com/tempoxyz/ak47
cd ak47
cargo build --release
```

## Configuration

ak47 uses a `config.toml` file to configure the indexer.

### Example

```toml
# config.toml

[http]
enabled = true
port = 8080
bind = "0.0.0.0"
api_keys = ["your-secret-api-key"]  # Optional: keys that bypass rate limiting

[http.rate_limit]
enabled = true
requests_per_window = 100  
window_secs = 60           
max_sse_connections = 5    

[prometheus]
enabled = true
port = 9090

[[chains]]
name = "mainnet"
chain_id = 4217
rpc_url = "https://rpc.tempo.xyz"
pg_url = "postgres://user:pass@localhost:5432/ak47_mainnet"
duckdb_path = "/data/mainnet.duckdb"  # Optional: enables OLAP queries
backfill = true
batch_size = 100

[[chains]]
name = "moderato"
chain_id = 42431
rpc_url = "https://rpc.moderato.tempo.xyz"
pg_url = "postgres://user:pass@localhost:5432/ak47_moderato"
duckdb_path = "/data/moderato.duckdb"
```

### Reference

#### `[http]`

HTTP server configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Enable HTTP API server |
| `port` | u16 | `8080` | HTTP server port |
| `bind` | string | `"0.0.0.0"` | Bind address |
| `api_keys` | string[] | `[]` | API keys that bypass rate limiting |

#### `[http.rate_limit]`

Rate limiting configuration for unauthenticated requests

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Enable rate limiting |
| `requests_per_window` | u32 | `100` | Max requests per window |
| `window_secs` | u64 | `60` | Rate limit window in seconds |
| `max_sse_connections` | u32 | `5` | Max concurrent SSE connections per IP |

**Authentication:** Pass API key via `Authorization: Bearer <key>` header.

#### `[prometheus]`

Prometheus metrics server configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Enable Prometheus metrics endpoint |
| `port` | u16 | `9090` | Metrics server port |

#### `[[chains]]`

Chain configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | ✓ | - | Display name for logging |
| `chain_id` | u64 | ✓ | - | Chain ID |
| `rpc_url` | string | ✓ | - | JSON-RPC endpoint URL |
| `pg_url` | string | ✓ | - | PostgreSQL connection string |
| `duckdb_path` | string | - | - | Path to DuckDB file (enables OLAP). Omit to disable DuckDB for this chain |
| `backfill` | bool | - | `true` | Enable backfill to genesis |
| `batch_size` | u64 | - | `100` | Blocks per RPC batch request |

## CLI

```
Usage: ak47 <COMMAND>

Commands:
  init         Initialize a new config.toml
  up           Start syncing blocks from the chain (continuous) and serve HTTP API
  status       Show sync status
  query        Run a SQL query (use --signature to decode event logs)
  help         Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```

### `ak47 init`

```
Initialize a new config.toml

Usage: ak47 init [OPTIONS]

Options:
  -o, --output <OUTPUT>  Output path for config file [default: config.toml]
      --force            Overwrite existing config file
  -h, --help             Print help
```

### `ak47 up`

```
Start syncing blocks from the chain (continuous) and serve HTTP API

Usage: ak47 up [OPTIONS]

Options:
  -c, --config <CONFIG>  Path to config file [default: config.toml]
  -h, --help             Print help
```

### `ak47 status`

```
Show sync status

Usage: ak47 status [OPTIONS]

Options:
  -c, --config <CONFIG>  Path to config file [default: config.toml]
  -w, --watch            Watch mode - continuously update status
      --json             Output as JSON
  -h, --help             Print help
```

### `ak47 query`

```
Run a SQL query (use --signature to decode event logs)

Usage: ak47 query [OPTIONS] <SQL>

Arguments:
  <SQL>  SQL query (SELECT only). Use event name from --signature as table

Options:
  -c, --config <CONFIG>        Path to config file [default: config.toml]
  -s, --signature <SIGNATURE>  Event signature to create a CTE
      --chain <CHAIN>          Chain name to query (uses first chain if not specified)
      --format <FORMAT>        Output format (table, json, csv) [default: table]
      --timeout <TIMEOUT>      Query timeout in milliseconds [default: 30000]
      --limit <LIMIT>          Maximum rows to return [default: 10000]
  -h, --help                   Print help
```

### Examples

```bash
# Start with config
ak47 up --config config.toml

# Watch sync status (updates every second)
ak47 status --watch

# Run SQL query
ak47 query "SELECT COUNT(*) FROM txs"

# Query with event decoding
ak47 query \
  --signature "Transfer(address indexed from, address indexed to, uint256 value)" \
  "SELECT * FROM Transfer LIMIT 10"
```

## HTTP API

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/status` | GET | Sync status for all chains + DuckDB |
| `/query` | GET | Execute SQL query (auto-routed) |
| `/metrics` | GET | Prometheus metrics |

### Query Parameters

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `sql` | string | ✓ | - | SQL query (SELECT only) |
| `chainId` | number | ✓ | - | Chain ID to query |
| `signature` | string | | - | Event signature for CTE generation |
| `engine` | string | | (auto) | Force engine: `postgres` or `duckdb` |
| `timeout_ms` | number | | `5000` | Query timeout in milliseconds |
| `limit` | number | | `10000` | Maximum rows to return |

### Examples

```bash
# Point lookup (auto-routed to PostgreSQL)
curl "http://localhost:8080/query?chainId=4217&sql=SELECT * FROM blocks WHERE num = 12345"
> {"columns":["num","hash","timestamp"],"rows":[[12345,"0xabc...","2024-01-01T00:00:00Z"]],"row_count":1,"engine":"postgres","ok":true}

# Aggregation (auto-routed to DuckDB)
curl "http://localhost:8080/query?chainId=4217&sql=SELECT type, COUNT(*) FROM txs GROUP BY type"
> {"columns":["type","count"],"rows":[[0,50000],[2,120000]],"row_count":2,"engine":"duckdb","ok":true}

# Status
curl http://localhost:8080/status
> {"ok":true,"chains":[{"chain_id":4217,"synced_num":567890,"head_num":567890,"lag":0,"duckdb_synced_num":567840,"duckdb_lag":50}]}
```

## Schemas

All tables use composite primary keys with timestamps for efficient range queries:

### blocks

| Column | Type | Description |
|--------|------|-------------|
| `num` | `INT8` | Block number |
| `hash` | `BYTEA` | Block hash |
| `parent_hash` | `BYTEA` | Parent block hash |
| `timestamp` | `TIMESTAMPTZ` | Block timestamp |
| `timestamp_ms` | `INT8` | Block timestamp (milliseconds) |
| `gas_limit` | `INT8` | Gas limit |
| `gas_used` | `INT8` | Gas used |
| `miner` | `BYTEA` | Block producer |
| `extra_data` | `BYTEA` | Extra data field |

### txs

| Column | Type | Description |
|--------|------|-------------|
| `block_num` | `INT8` | Block number |
| `block_timestamp` | `TIMESTAMPTZ` | Block timestamp |
| `idx` | `INT4` | Transaction index |
| `hash` | `BYTEA` | Transaction hash |
| `type` | `INT2` | Transaction type |
| `from` | `BYTEA` | Sender address |
| `to` | `BYTEA` | Recipient address |
| `value` | `TEXT` | Transfer value (wei) |
| `input` | `BYTEA` | Calldata |
| `gas_limit` | `INT8` | Gas limit |
| `max_fee_per_gas` | `TEXT` | Max fee per gas |
| `max_priority_fee_per_gas` | `TEXT` | Max priority fee |
| `gas_used` | `INT8` | Gas consumed |
| `nonce_key` | `BYTEA` | Nonce key (2D nonces) |
| `nonce` | `INT8` | Nonce value |
| `fee_token` | `BYTEA` | Fee token address |
| `calls` | `JSONB` | Batch call data |
| `call_count` | `INT2` | Number of calls |
| `valid_before` | `INT8` | Validity window start |
| `valid_after` | `INT8` | Validity window end |
| `signature_type` | `INT2` | Signature type |

### logs

| Column | Type | Description |
|--------|------|-------------|
| `block_num` | `INT8` | Block number |
| `block_timestamp` | `TIMESTAMPTZ` | Block timestamp |
| `log_idx` | `INT4` | Log index |
| `tx_idx` | `INT4` | Transaction index |
| `tx_hash` | `BYTEA` | Transaction hash |
| `address` | `BYTEA` | Emitting contract |
| `selector` | `BYTEA` | Event selector (topic0) |
| `topics` | `BYTEA[]` | All topics |
| `data` | `BYTEA` | Event data |

### receipts

| Column | Type | Description |
|--------|------|-------------|
| `block_num` | `INT8` | Block number |
| `block_timestamp` | `TIMESTAMPTZ` | Block timestamp |
| `tx_idx` | `INT4` | Transaction index |
| `tx_hash` | `BYTEA` | Transaction hash |
| `from` | `BYTEA` | Sender address |
| `to` | `BYTEA` | Recipient address |
| `contract_address` | `BYTEA` | Created contract (if deploy) |
| `gas_used` | `INT8` | Gas consumed |
| `cumulative_gas_used` | `INT8` | Cumulative gas in block |
| `effective_gas_price` | `TEXT` | Actual gas price paid |
| `status` | `INT2` | Success (1) or failure (0) |
| `fee_payer` | `BYTEA` | Tempo fee payer (if sponsored) |

### sync_state

| Column | Type | Description |
|--------|------|-------------|
| `chain_id` | `INT8` | Chain identifier |
| `head_num` | `INT8` | Remote chain head from RPC |
| `synced_num` | `INT8` | Highest contiguous block (no gaps from backfill_num to here) |
| `tip_num` | `INT8` | Highest block near chain head (realtime follows this) |
| `backfill_num` | `INT8` | Lowest synced block going backwards (NULL=not started, 0=complete) |
| `started_at` | `TIMESTAMPTZ` | Sync start time |
| `updated_at` | `TIMESTAMPTZ` | Last update time |

## Sync Architecture

ak47 uses three concurrent sync operations to maximize throughput while ensuring realtime data availability:

```
Block Numbers:  0                                                              HEAD
                │                                                                │
                ▼                                                                ▼
    ════════════╪════════════════════════════════════════════════════════════════╪═══▶ time
                │                                                                │
    INDEXED:    ░░░░░░░░░░░████████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░██████████
                │         │               │                            │        │
                ▼         ▼               ▼                            ▼        ▼
              genesis  backfill_num   synced_num                   tip_num   head_num
               (0)       (500)          (800)                       (1900)    (2000)
                │         │               │                            │        │
                └─────────┘               └────────────────────────────┘        │
                 BACKFILL                         GAP-FILL                      │
              (toward genesis)              (fills skipped blocks)              │
                                                                       └────────┘
                                                                        REALTIME
                                                                     (following head)

Legend:
  ████  = indexed blocks
  ░░░░  = not yet indexed
```

### Sync Cursors

| Cursor | Description |
|--------|-------------|
| `head_num` | Current chain head from RPC |
| `tip_num` | Highest block indexed (realtime keeps this near head) |
| `synced_num` | Highest contiguous block (no gaps from backfill_num to here) |
| `backfill_num` | Lowest block indexed going backwards toward genesis |

### Metrics

| Metric | Formula | Description |
|--------|---------|-------------|
| **Realtime lag** | `head_num - tip_num` | Should be ~0, realtime keeps up with new blocks |
| **Gap** | `tip_num - synced_num` | Blocks realtime skipped, gap-fill backfills these |
| **Backfill remaining** | `backfill_num` | Blocks left to index toward genesis |

### Concurrent Operations

1. **REALTIME** — Jumps to chain head immediately, follows new blocks with minimal lag (~0 blocks)
2. **GAP-FILL** — Fills the gap between `synced_num` and `tip_num` in background
3. **BACKFILL** — Syncs historical blocks from `backfill_num` toward genesis

### When Gaps Occur

Gaps between `synced_num` and `tip_num` happen when:

| Scenario | What Happens |
|----------|--------------|
| **Indexer restart after downtime** | Chain moved ahead while stopped. Realtime jumps to head, gap-fill backfills the missed blocks. |
| **Slow RPC or network issues** | Realtime temporarily falls behind, then jumps ahead when connection recovers. |
| **Initial sync** | Realtime starts near head immediately, gap-fill works backwards to meet backfill. |
| **Rate-limited RPC** | Realtime prioritizes head, gap-fill catches up at sustainable rate. |

### Multiple Gaps (Repeated Restarts)

If the indexer stops and restarts multiple times, multiple non-contiguous gaps can form:

```
Scenario: Indexer stops twice while chain advances

  Run 1: Sync 0-100, stop
  Run 2: Chain at 250, jump to 240-250, stop  
  Run 3: Chain at 400, jump to 390-400

Block Numbers:  0          100    240  250          390  400
                │           │      │    │            │    │
                ▼           ▼      ▼    ▼            ▼    ▼
    ════════════╪═══════════╪══════╪════╪════════════╪════╪═══▶ time
                │           │      │    │            │    │
    INDEXED:    ████████████       █████             ██████
                │           │      │    │            │    │
                ▼           ▼      ▼    ▼            ▼    ▼
              genesis    synced  gap1  gap1        gap2  tip/head
               (0)        (100)  start  end        start  (400)
                                 (101) (239)       (251)

Gaps detected: [(101, 239), (251, 389)]
```

The gap-fill loop uses SQL to detect **all** gaps in the `blocks` table:

```sql
SELECT prev_num + 1 as gap_start, num - 1 as gap_end
FROM (SELECT num, LAG(num) OVER (ORDER BY num) as prev_num FROM blocks)
WHERE num - prev_num > 1
```

This returns all gaps regardless of how many exist. Gap-fill processes them sequentially until `synced_num` catches up to `tip_num`.

This architecture ensures **new blocks are always indexed immediately**, even if historical sync is incomplete.

## Development

### Prerequisites

- [Rust 1.75+](https://rustup.rs/)
- [Docker](https://docs.docker.com/get-docker/)
- [PostgreSQL](https://www.postgresql.org/download/)

### Make Commands

```bash
make bench               # Run benchmarks
make clean               # Stop services + clean build
make down                # Stop services
make logs                # Tail indexer logs
make seed                # Generate test transactions
make seed-heavy          # Generate ~1M+ transactions
make test                # Run tests
make up                  # Start devnet (PostgreSQL + Tempo)
```

## License

[LICENSE](./LICENSE)

## Acknowledgments

- [golden-axe](https://github.com/indexsupply/golden-axe) — Inspiration for everything.
