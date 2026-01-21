<p align="center">
  <h1 align="center">ak47</h1>
  <p align="center"><strong>High-throughput Tempo indexer in Rust</strong></p>
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
- **Dual Storage** — TimescaleDB for OLTP + DuckDB columnar for OLAP
- **Continuous Aggregates** — Materialized views that auto-refresh for instant analytics
- **Event Decoding** — Query decoded events by ABI signature (no pre-registration)
- **HTTP API + CLI** — Query data via REST, SQL, or command line
- **Tempo-Native** — Optimized for instant finality, TIP-20 tokens, and fast block times

## Table of Contents

- [Quickstart](#quickstart)
- [How It Works](#how-it-works)
- [Query Routing](#query-routing)
- [Installation](#installation)
- [Configuration](#configuration)
- [CLI Reference](#cli-reference)
- [HTTP API](#http-api)
- [Query Cookbook](#query-cookbook)
- [Database Schema](#database-schema)
- [Development](#development)
- [License](#license)

## Quickstart

### Requirements

- [TimescaleDB](https://docs.timescale.com/self-hosted/latest/install/) (Postgres with time-series extensions)

### Install

```bash
curl -L https://raw.githubusercontent.com/tempoxyz/ak47/main/scripts/install.sh | bash
```

### Run

```bash
# Create config
cat > config.toml << EOF
[[chains]]
name = "mainnet"
chain_id = 4217
rpc_url = "https://rpc.tempo.xyz"
database_url = "postgres://user:pass@localhost:5432/ak47"
EOF

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

## How It Works

ak47 uses **bidirectional sync** to give you realtime data immediately:

```
Chain:    [0]----[1]----[2]----...----[HEAD-1]----[HEAD]----[HEAD+1]
                   ◄── Backfill ──┘              └── Forward ──►
```

1. **Forward Sync** — Starts at chain head, follows new blocks in realtime
2. **Backfill** — Runs concurrently, filling history from head → genesis
3. **Compression** — Columnar compression for 10-20x storage savings + faster analytics

Both syncs persist progress to `sync_state`, so interrupted syncs resume automatically.

### Dual Database Architecture

ak47 uses a hybrid architecture with two databases working together:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│   PostgreSQL (TimescaleDB)                 DuckDB                           │
│  ┌─────────────────────────────┐          ┌─────────────────────────────┐   │
│  │  • System of record         │          │  • Analytical replica       │   │
│  │  • ACID transactions        │  ──────► │  • Columnar storage         │   │
│  │  • Point lookups (< 1ms)    │   sync   │  • Aggregations (10-100x)   │   │
│  │  • Indexed queries          │          │  • Window functions         │   │
│  └─────────────────────────────┘          └─────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**PostgreSQL/TimescaleDB** handles:
- All writes (system of record)
- Point lookups by hash, address, block number
- Recent data queries
- Transactions and ACID guarantees

**DuckDB** handles:
- Aggregations (`GROUP BY`, `COUNT`, `SUM`, `AVG`)
- Window functions (`ROW_NUMBER`, `RANK`, `OVER`)
- Large table scans and joins
- Analytical queries over millions of rows

## Query Routing

Queries are **automatically routed** to the optimal engine based on SQL patterns:

| Pattern | Engine | Why |
|---------|--------|-----|
| `WHERE hash = '0x...'` | PostgreSQL | Indexed point lookup |
| `WHERE address = '0x...'` | PostgreSQL | Indexed lookup |
| `WHERE block_num = 123` | PostgreSQL | Indexed lookup |
| `GROUP BY` / `HAVING` | DuckDB | Columnar aggregation |
| `COUNT(*)`, `SUM()`, `AVG()` | DuckDB | Vectorized execution |
| `ROW_NUMBER() OVER (...)` | DuckDB | Optimized window functions |
| Multiple `JOIN`s | DuckDB | Columnar join optimization |

### Explicit Engine Control

Force a specific engine via SQL comment or query parameter:

```sql
-- Force DuckDB
/* engine=duckdb */ SELECT COUNT(*) FROM txs;

-- Force PostgreSQL (for freshest data)
/* engine=postgres */ SELECT * FROM blocks ORDER BY num DESC LIMIT 1;
```

Via HTTP API:
```bash
# Force PostgreSQL
curl "/query?sql=SELECT...&engine=postgres"

# Force DuckDB
curl "/query?sql=SELECT...&engine=duckdb"
```

### Status Endpoint

The `/status` endpoint shows sync status for both engines:

```json
{
  "ok": true,
  "chains": [...],
  "duckdb": {
    "enabled": true,
    "latest_block": 999950,
    "lag_blocks": 50
  }
}
```

### Why This Architecture?

- **Best of both worlds** — Sub-millisecond point lookups AND fast analytics
- **Isolation** — Analytical queries don't impact OLTP latency
- **Simplicity** — Automatic routing, no manual query optimization
- **Consistency** — PostgreSQL is source of truth, DuckDB is derived

## Installation

### One-liner

```bash
curl -L https://raw.githubusercontent.com/tempoxyz/ak47/main/scripts/install.sh | bash
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

**Requirements:** TimescaleDB 2.x

## Configuration

ak47 uses a TOML config file. Each `[[chains]]` block defines a chain to index:

```toml
# config.toml

[http]
enabled = true
port = 8080
bind = "0.0.0.0"

[prometheus]
enabled = true
port = 9090

[[chains]]
name = "mainnet"
chain_id = 4217
rpc_url = "https://rpc.tempo.xyz"
database_url = "postgres://user:pass@localhost:5432/ak47_mainnet"
duckdb_path = "/data/mainnet.duckdb"  # Optional: enables OLAP queries
backfill = true
batch_size = 100

[[chains]]
name = "moderato"
chain_id = 42431
rpc_url = "https://rpc.moderato.tempo.xyz"
database_url = "postgres://user:pass@localhost:5432/ak47_moderato"
duckdb_path = "/data/moderato.duckdb"
```

### Configuration Reference

#### `[http]` — HTTP API Server

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Enable HTTP API server |
| `port` | u16 | `8080` | HTTP server port |
| `bind` | string | `"0.0.0.0"` | Bind address |

#### `[prometheus]` — Metrics Server

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Enable Prometheus metrics endpoint |
| `port` | u16 | `9090` | Metrics server port |

#### `[[chains]]` — Chain Configuration (one per chain)

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | ✓ | - | Display name for logging |
| `chain_id` | u64 | ✓ | - | Chain ID |
| `rpc_url` | string | ✓ | - | JSON-RPC endpoint URL |
| `database_url` | string | ✓ | - | PostgreSQL connection string |
| `duckdb_path` | string | - | - | Path to DuckDB file (enables OLAP). Omit to disable DuckDB for this chain |
| `backfill` | bool | - | `true` | Enable backfill to genesis |
| `batch_size` | u64 | - | `100` | Blocks per RPC batch request |

## CLI Reference

```
Usage: ak47 <COMMAND>

Commands:
  up           Start syncing blocks from the chain (continuous) and serve HTTP API
  status       Show sync status
  query        Run a SQL query (use --signature to decode event logs)
  help         Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
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

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sql` | string | required | SQL query (SELECT only) |
| `signature` | string | - | Event signature for CTE generation |
| `chainId` | number | first chain | Chain ID to query |
| `engine` | string | auto | Force engine: `postgres` or `duckdb` |
| `timeout_ms` | number | 5000 | Query timeout in milliseconds |
| `limit` | number | 10000 | Maximum rows to return |

### Examples

```bash
# Simple query (auto-routed to PostgreSQL - point lookup)
curl "http://localhost:8080/query?sql=SELECT * FROM blocks WHERE num = 12345"

# Aggregation query (auto-routed to DuckDB)
curl "http://localhost:8080/query?sql=SELECT COUNT(*) FROM txs GROUP BY type"

# Force PostgreSQL for freshest data
curl "http://localhost:8080/query?sql=SELECT * FROM blocks ORDER BY num DESC LIMIT 1&engine=postgres"

# Response includes which engine was used
{
  "columns": ["num", "hash", "timestamp", ...],
  "rows": [[123, "0x...", "2024-01-01T00:00:00Z", ...]],
  "row_count": 5,
  "engine": "duckdb",
  "ok": true
}
```

```bash
curl http://localhost:8080/status

# Response includes DuckDB sync status
{
  "ok": true,
  "chains": [{
    "chain_id": 4217,
    "synced_num": 567890,
    "head_num": 567890,
    "backfill_num": 123456,
    "lag": 0
  }],
  "duckdb": {
    "enabled": true,
    "latest_block": 567840,
    "lag_blocks": 50
  }
}
```

## Query Cookbook

### OLTP (Point Lookups)

```sql
-- Get block by number
SELECT * FROM blocks WHERE num = 12345;

-- Get transaction by hash
SELECT * FROM txs WHERE hash = '\x...';

-- Transactions for a specific block
SELECT hash, "from", "to", value 
FROM txs 
WHERE block_num = 12345;

-- Logs for a specific transaction
SELECT * FROM logs WHERE tx_hash = '\x...';

-- Recent transactions from an address
SELECT * FROM txs 
WHERE "from" = '\x...' 
ORDER BY block_timestamp DESC 
LIMIT 20;
```

### OLAP (Analytics)

These queries are automatically routed to DuckDB for fast columnar execution:

```sql
-- Transactions per hour (last 24h)
SELECT 
  DATE_TRUNC('hour', block_timestamp) AS hour,
  COUNT(*) AS tx_count
FROM txs
WHERE block_timestamp > NOW() - INTERVAL '24 hours'
GROUP BY hour
ORDER BY hour DESC;

-- Gas usage trend (last 30 days)
SELECT 
  DATE_TRUNC('day', timestamp) AS day,
  SUM(gas_used) AS total_gas,
  AVG(gas_used)::bigint AS avg_gas
FROM blocks
GROUP BY day
ORDER BY day DESC
LIMIT 30;

-- Top contracts by event count
SELECT 
  encode(address, 'hex') AS contract,
  COUNT(*) AS event_count
FROM logs
WHERE block_timestamp > NOW() - INTERVAL '7 days'
GROUP BY address
ORDER BY event_count DESC
LIMIT 20;

-- Unique active addresses per day
SELECT 
  DATE_TRUNC('day', block_timestamp) AS day,
  COUNT(DISTINCT "from") AS unique_senders
FROM txs
GROUP BY day
ORDER BY day DESC
LIMIT 30;
```

### Decoded Events (via CLI)

```bash
# Transfer events with decoded fields
ak47 query \
  --signature "Transfer(address indexed from, address indexed to, uint256 value)" \
  "SELECT block_timestamp, \"from\", \"to\", value FROM Transfer ORDER BY block_timestamp DESC LIMIT 10"
```

## Database Schema

All tables use composite primary keys with timestamps for efficient range queries:

### blocks

| Column | Type | Description |
|--------|------|-------------|
| `num` | `INT8` | Block number |
| `hash` | `BYTEA` | Block hash |
| `parent_hash` | `BYTEA` | Parent block hash |
| `timestamp` | `TIMESTAMPTZ` | Block timestamp |
| `gas_limit` | `INT8` | Gas limit |
| `gas_used` | `INT8` | Gas used |
| `miner` | `BYTEA` | Block producer |

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
| `fee_payer` | `BYTEA` | Fee payer (if sponsored) |
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
| `head_num` | `INT8` | Remote chain head |
| `synced_num` | `INT8` | Highest synced block |
| `backfill_num` | `INT8` | Lowest synced block |

## Development

### Prerequisites

- Rust 1.75+
- Docker & Docker Compose
- PostgreSQL client (optional)

### Make Commands

```bash
make up                  # Start devnet (PostgreSQL + Tempo)
make down                # Stop services
make test                # Run tests
make bench               # Run benchmarks
make logs                # Tail indexer logs
make seed                # Generate test transactions
make seed-heavy          # Generate ~1M+ transactions
make clean               # Stop services + clean build
```

### Project Structure

```
src/
├── api/          # HTTP API (axum)
├── cli/          # CLI commands
├── db/           # Database pools (PostgreSQL + DuckDB)
│   ├── pool.rs      # PostgreSQL connection pool
│   ├── duckdb.rs    # DuckDB pool + schema + ABI macros
│   └── schema.rs    # PostgreSQL migrations
├── query/        # Query parsing and routing
│   ├── parser.rs    # Event signature parser + CTE generation
│   └── router.rs    # OLTP/OLAP query routing logic
├── sync/         # Sync engine
│   ├── engine.rs    # Bidirectional sync orchestration
│   ├── fetcher.rs   # RPC data fetching
│   ├── writer.rs    # PostgreSQL batch writer
│   └── replicator.rs # DuckDB sync replication
├── service/      # Shared business logic
└── types.rs      # Core data types

db/               # PostgreSQL migrations
tests/            # Integration tests
benches/          # Benchmarks
```

### Running Tests

```bash
# Start test infrastructure
DEVNET=1 make up

# Run all tests
make test

# Run specific test
cargo test smoke_test -- --test-threads=1
```

Tests use real PostgreSQL and Tempo nodes (no mocks).

## License

MIT License — see [LICENSE](./LICENSE)

## Acknowledgments

- [golden-axe](https://github.com/indexsupply/golden-axe) — Inspiration for the indexing architecture
