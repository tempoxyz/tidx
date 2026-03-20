<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset=".github/banner-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset=".github/banner-light.svg">
    <img alt="tidx" src=".github/banner-light.svg" width="100%">
  </picture>
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

**tidx** indexes [Tempo](https://tempo.xyz) chain data into a hybrid PostgreSQL + ClickHouse architecture for fast point lookups (OLTP) and lightning-fast analytics (OLAP). 

## Features

- **Dual Storage** — PostgreSQL (OLTP) + ClickHouse (OLAP), written in parallel
- **Event/Function Decoding** — Query decoded events or function calldata by ABI signature (no pre-registration)
- **HTTP API + CLI** — Query data via REST, SQL, or command line

## Table of Contents

- [Quickstart](#quickstart)
- [Overview](#overview)
- [Installation](#installation)
- [Configuration](#configuration)
- [CLI](#cli)
- [HTTP API](#http-api)
- [Database Schema](#database-schema)
- [Sync Architecture](#sync-architecture)
- [Development](#development)
- [License](#license)

## Quickstart

```bash
curl -L https://tidx.vercel.app/docker | bash
```

## Overview

The sync engine writes to both PostgreSQL and ClickHouse in parallel. Use the `engine` query parameter to choose which backend to query:

```
                                              ┌─────────────────────┐
                                              │      /query         │
                                              │                     │
                                              │  ?signature=...     │◄─── Lazy event decoding
                                              │  ?engine=...        │     (no pre-registration)
                                              └──────────┬──────────┘
                                                         │
              ┌──────────────────────────────────────────┼──────────────────────────────────────────┐
              │                                          │                                          │
              ▼                                          ▼                                          ▼
┌─────────────────────┐                    ┌─────────────────────┐                    ┌─────────────────────┐
│    PostgreSQL       │                    │     ClickHouse      │                    │  Materialized Views │
│    (OLTP)           │                    │      (OLAP)         │ ─────────────────► │  (auto-updated)     │
│                     │                    │                     │                    │                     │
└─────────┬───────────┘                    └─────────┬───────────┘                    └─────────────────────┘
          │                                          │
          └──────────────────┬───────────────────────┘
                             │
                     ┌───────┴───────┐
                     │  Dual Sink    │
                     └───────┬───────┘
                             │
                     ┌───────┴───────┐
                     │  Sync Engine  │
                     └───────────────┘
```

```bash
# PostgreSQL (OLTP) - last 10 transfers from an address
curl "https://tidx.example.com/query \
  ?chainId=4217 \
  &signature=Transfer(address,address,uint256) \
  &sql=SELECT * FROM Transfer WHERE from = '0x...' ORDER BY block_num DESC LIMIT 10"

# ClickHouse (OLAP) - same query, faster for large scans
curl "https://tidx.example.com/query \
  ?chainId=4217 \
  &engine=clickhouse \
  &signature=Transfer(address,address,uint256) \
  &sql=SELECT * FROM Transfer WHERE from = '0x...' ORDER BY block_num DESC LIMIT 10"

# ClickHouse (OLAP) - query pre-computed views
curl "https://tidx.example.com/views?chainId=4217"
> {"ok":true,"views":[{"name":"top_holders","columns":[{"name":"token","type":"String"},{"name":"holder","type":"String"},{"name":"balance","type":"UInt256"}]}]}

curl "https://tidx.example.com/query \
  ?chainId=4217 \
  &engine=clickhouse \
  &sql=SELECT * FROM top_holders WHERE token = '0x...' LIMIT 10"
```

## Installation

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
```

## Configuration

tidx uses a `config.toml` file to configure the indexer.

### Example

```toml
# config.toml

[http]
enabled = true
port = 8080
bind = "0.0.0.0"
trusted_cidrs = ["100.64.0.0/10"]   # Optional: trusted IPs for admin operations (e.g., Tailscale)

[prometheus]
enabled = true
port = 9090

[[chains]]
name = "mainnet"
chain_id = 4217
rpc_url = "https://rpc.tempo.xyz"
pg_url = "postgres://user@tidx.example.com:5432/tidx_mainnet"
pg_password_env = "TIDX_PG_PASSWORD"  # Password from environment variable
batch_size = 100

# Optional: ClickHouse for OLAP queries
[chains.clickhouse]
enabled = true
url = "http://clickhouse:8123"

[[chains]]
name = "moderato"
chain_id = 42431
rpc_url = "https://rpc.moderato.tempo.xyz"
pg_url = "postgres://user@tidx.example.com:5432/tidx_moderato"
pg_password_env = "TIDX_PG_PASSWORD"
```

### Reference

```
[http]                                             HTTP server configuration
├── enabled                 bool      = true         Enable HTTP API server
├── port                    u16       = 8080         HTTP server port
├── bind                    string    = "0.0.0.0"    Bind address
└── trusted_cidrs           string[]  = []           Trusted CIDRs for admin ops (e.g., Tailscale)

[prometheus]                                       Prometheus metrics server
├── enabled                 bool      = true         Enable metrics endpoint
└── port                    u16       = 9090         Metrics server port

[[chains]]                                         Chain configuration 
├── name                    string    (required)     Display name for logging
├── chain_id                u64       (required)     Chain ID
├── rpc_url                 string    (required)     JSON-RPC endpoint URL
├── pg_url                  string    (required)     PostgreSQL connection string
├── pg_password_env         string    (optional)     Env var name for PostgreSQL password
├── api_pg_url              string    (optional)     Separate PostgreSQL URL for API (e.g., read replica)
├── api_pg_password_env     string    (optional)     Env var name for API PostgreSQL password
├── batch_size              u64       = 100          Blocks per RPC batch request
└── [clickhouse]                                     ClickHouse OLAP settings
    ├── enabled             bool      = false        Enable ClickHouse OLAP queries
    └── url                 string    = "http://clickhouse:8123"  ClickHouse HTTP URL
```

## CLI

```
Usage: tidx <COMMAND>

Commands:
  init         Initialize a new config.toml
  up           Start syncing blocks from the chain (continuous) and serve HTTP API
  status       Show sync status
  query        Run a SQL query (use --signature to decode event logs)
  views        Manage ClickHouse materialized views
  upgrade      Update tidx to the latest version
  help         Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```

### `tidx init`

```
Initialize a new config.toml

Usage: tidx init [OPTIONS]

Options:
  -o, --output <OUTPUT>  Output path for config file [default: config.toml]
      --force            Overwrite existing config file
  -h, --help             Print help
```

### `tidx up`

```
Start syncing blocks from the chain (continuous) and serve HTTP API

Usage: tidx up [OPTIONS]

Options:
  -c, --config <CONFIG>  Path to config file [default: config.toml]
  -h, --help             Print help
```

### `tidx status`

```
Show sync status

Usage: tidx status [OPTIONS]

Options:
  -c, --config <CONFIG>  Path to config file [default: config.toml]
  -w, --watch            Watch mode - continuously update status
      --json             Output as JSON
  -h, --help             Print help
```

### `tidx query`

```
Run a SQL query (use --signature to decode event logs)

Usage: tidx query [OPTIONS] <SQL>

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

### `tidx views`

```
Manage ClickHouse materialized views

Usage: tidx views --url <URL> <COMMAND>

Commands:
  list    List all views for a chain
  get     Get view details
  create  Create a new materialized view
  delete  Delete a view

Options:
      --url <URL>  TIDX HTTP API URL [env: TIDX_URL]
  -h, --help       Print help
```

### `tidx upgrade`

```
Update tidx to the latest version

Usage: tidx upgrade

Downloads and replaces the current binary from GitHub releases.
```

### Examples

```bash
# Start with config
tidx up --config config.toml

# Watch sync status (updates every second)
tidx status --watch

# Run SQL query
tidx query "SELECT COUNT(*) FROM txs"

# Query with event decoding
tidx query \
  --signature "Transfer(address indexed from, address indexed to, uint256 value)" \
  "SELECT * FROM Transfer LIMIT 10"

# List views
tidx views --url https://tidx.example.com list --chain-id 4217

# Create a view (must be run from trusted IP)
tidx views --url https://tidx.example.com create \
  --chain-id 4217 \
  --name top_holders \
  --sql "SELECT holder, SUM(balance) as total FROM balances GROUP BY holder" \
  --order-by holder

# Self-update
tidx upgrade
```

## HTTP API

tidx exposes a HTTP API for querying the indexer.

### Examples

```bash
# Point lookup (auto-routed to PostgreSQL)
curl "https://tidx.example.com/query?chainId=4217&sql=SELECT * FROM blocks WHERE num = 12345"
> {"columns":["num","hash","timestamp"],"rows":[[12345,"0xabc...","2024-01-01T00:00:00Z"]],"row_count":1,"engine":"postgres","ok":true}

# Aggregation (auto-routed to ClickHouse)
curl "https://tidx.example.com/query?chainId=4217&sql=SELECT type, COUNT(*) FROM txs GROUP BY type"
> {"columns":["type","count"],"rows":[[0,50000],[2,120000]],"row_count":2,"engine":"clickhouse","ok":true}

# Status
curl https://tidx.example.com/status
> {"ok":true,"chains":[{"chain_id":4217,"synced_num":567890,"head_num":567890,"lag":0}]}
```

### Reference

```
GET  /health                                             Health check
GET  /status                                             Sync status for all chains
GET  /query                                              Execute SQL query
     ?sql                   string    (required)         SQL query (SELECT only)
     ?chainId               number    (required)         Chain ID to query
     ?signature             string                       Event signature for CTE generation
     ?engine                string    = postgres         Query engine: postgres or clickhouse
     ?live                  bool      = false            Enable SSE streaming (postgres only)
GET  /views?chainId=                                     List materialized views
GET  /views/{name}?chainId=                              Get view details
POST /views                                              Create view (trusted IP only)
DELETE /views/{name}?chainId=                            Delete view (trusted IP only)
GET  /metrics                                            Prometheus metrics
```

### Views API

Manage ClickHouse materialized views for pre-computed analytics. Views are stored in `analytics_{chainId}` database and auto-update on new data.

**Note:** POST and DELETE require connection from a trusted IP (configured via `trusted_cidrs`).

#### List Views

```bash
curl "https://tidx.example.com/views?chainId=42431"
```

```json
{
  "ok": true,
  "views": [
    {
      "name": "token_holders",
      "engine": "MaterializedView",
      "database": "analytics_42431",
      "columns": [
        {"name": "token", "type": "String"},
        {"name": "holder", "type": "String"},
        {"name": "balance", "type": "UInt256"}
      ]
    }
  ]
}
```

#### Create View (trusted IP only)

```bash
curl -X POST "https://tidx.example.com/views" \
  -H "Content-Type: application/json" \
  -d '{
    "chainId": 42431,
    "name": "token_holders",
    "sql": "SELECT token, holder, sum(balance) AS balance FROM token_balances GROUP BY token, holder HAVING balance > 0",
    "orderBy": ["token", "holder"]
  }'
```

| Field | Required | Description |
|-------|----------|-------------|
| `chainId` | yes | Target chain ID |
| `name` | yes | View name (alphanumeric + underscore) |
| `sql` | yes | SELECT statement for the view |
| `orderBy` | yes | Primary key columns for table sorting |
| `engine` | no | ClickHouse engine (default: `SummingMergeTree()`) |

This creates:
1. Target table `analytics_{chainId}.{name}` with inferred schema
2. Materialized view `analytics_{chainId}.{name}_mv` that auto-populates on inserts
3. Backfills existing data from the source query

#### Get View Details

```bash
curl "https://tidx.example.com/views/token_holders?chainId=42431"
```

```json
{
  "ok": true,
  "view": {"name": "token_holders", "engine": "View", "database": "analytics_42431"},
  "definition": "CREATE VIEW analytics_42431.token_holders AS SELECT ...",
  "row_count": 1234567
}
```

#### Delete View (trusted IP only)

```bash
curl -X DELETE "https://tidx.example.com/views/token_holders?chainId=42431"
```

```json
{
  "ok": true,
  "deleted": ["token_holders_mv", "token_holders"]
}
```

#### Query Views

Views are auto-prefixed with `analytics_{chainId}` when using `engine=clickhouse`:

```bash
# Query the view (auto-prefixed)
curl "https://tidx.example.com/query?chainId=42431&engine=clickhouse&sql=SELECT * FROM token_holders WHERE token = '0x...' ORDER BY balance DESC LIMIT 10"
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

tidx uses two concurrent sync operations: **Realtime** follows the chain head, while **Gap Sync** fills all missing blocks from most recent to earliest.

```
Block Numbers:  0                                                              HEAD
                │                                                                │
                ▼                                                                ▼
    ════════════╪════════════════════════════════════════════════════════════════╪═══▶ time
                │                                                                │
    INDEXED:    ░░░░░░░░░░░████████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░██████████
                │          │               │                           │        │
                ▼          ▼               ▼                           ▼        ▼
              genesis    gap 2           gap 1                      tip_num   head_num
               (0)     (fills 2nd)    (fills 1st)                   (1900)    (2000)
                │                                                              │
                │◄─────────────────── GAP SYNC ───────────────────────────────►│
                │           Fills ALL gaps, most recent first                  │
                │                                                    └─────────┘
                │                                                     REALTIME
                │                                                  (following head)
                │
                └─── Eventually reaches genesis (block 0)

Legend:
  ████  = indexed blocks
  ░░░░  = gaps (missing blocks)
```

| Operation | Description |
|-----------|-------------|
| **Realtime** | Follows chain head immediately, maintains ~0 lag |
| **Gap Sync** | Detects all gaps, fills from most recent to earliest |

Gap sync finds discontinuities via SQL and adds the gap from genesis to the first synced block. Gaps are sorted by end block descending (most recent first) and filled one at a time. Recent gaps are prioritized so users can query recent data during initial sync.

## Development

### Prerequisites

- [Rust 1.75+](https://rustup.rs/)
- [Docker](https://docs.docker.com/get-docker/)
- [PostgreSQL](https://www.postgresql.org/download/)

### Make Commands

```bash
make up                Start services (use LOCALNET=1 for localnet)
make down              Stop all services
make logs              Tail indexer logs
make build             Build Docker image
make seed              Generate transactions

make bench             Run benchmarks
make check             Run clippy lints
make test              Run tests

make clean             Stop services and clean
```

## License

[LICENSE](./LICENSE)

## Acknowledgments

- [golden-axe](https://github.com/indexsupply/golden-axe) — Inspiration for everything.


