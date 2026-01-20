<p align="center">
  <h1 align="center">ak47</h1>
  <p align="center"><strong>High-throughput Tempo blockchain indexer in Rust</strong></p>
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

**ak47** indexes [Tempo](https://tempo.xyz) blockchain data into Postgres (TimescaleDB) for fast point lookups (OLTP) and analytics (OLAP). 

## Features

- **Bidirectional Sync** — Follow chain head in realtime while backfilling history concurrently
- **Hybrid Storage** — Row format for recent OLTP, columnar compression for historical OLAP
- **Continuous Aggregates** — Materialized views that auto-refresh for instant analytics
- **Event Decoding** — Query decoded events by ABI signature (no pre-registration)
- **HTTP API + CLI** — Query data via REST, SQL, or command line
- **Tempo-Native** — Optimized for instant finality, TIP-20 tokens, and fast block times

## Table of Contents

- [Quickstart](#quickstart)
- [How It Works](#how-it-works)
- [Installation](#installation)
- [Configuration](#configuration)
- [CLI Reference](#cli-reference)
- [HTTP API](#http-api)
- [Query Cookbook](#query-cookbook)
- [Database Schema](#database-schema)
- [Continuous Aggregates](#continuous-aggregates)
- [Operations](#operations)
- [Performance](#performance)
- [Development](#development)
- [License](#license)

## Quickstart

### Requirements

- [TimescaleDB](https://docs.timescale.com/self-hosted/latest/install/) (Postgres with time-series extensions)

### Install

```bash
curl -L https://ak47.tempo.xyz/install.sh | bash
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
docker compose up -d

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

### Hybrid Storage

Recent data stays in row format for fast writes and point queries. Older chunks auto-compress to columnar format for analytics:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│   HOT DATA (row format)              COLD DATA (columnar, compressed)       │
│  ┌─────┬─────┬─────┬─────┐          ┌─────────────────────────────────┐     │
│  │ now │ -1h │ -2h │ ... │   ───►   │  -30d  │  -60d  │  -90d  │ ...  │     │
│  └─────┴─────┴─────┴─────┘          └─────────────────────────────────┘     │
│     Fast writes + OLTP                  10-20x smaller + fast OLAP          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Why TimescaleDB?

- **Hypertables** — Auto-partitioned by timestamp for efficient time-range queries
- **Compression** — Columnar storage reduces size 10-20x, speeds up aggregations
- **Continuous Aggregates** — Materialized views that auto-refresh on schedule
- **Full SQL** — No custom query language, just Postgres

## Installation

### One-liner

```bash
curl -L https://ak47.tempo.xyz/install.sh | bash
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

ak47 uses a TOML config file. Each `[[chains]]` block creates a separate database:

```toml
# config.toml

[http]
enabled = true
port = 8080

[prometheus]
enabled = true
port = 9090

[[chains]]
name = "mainnet"
chain_id = 4217
rpc_url = "https://rpc.tempo.xyz"
database_url = "postgres://user:pass@localhost:5432/ak47_mainnet"

[[chains]]
name = "moderato"
chain_id = 42429
rpc_url = "https://rpc.moderato.tempo.xyz"
database_url = "postgres://user:pass@localhost:5432/ak47_moderato"
```

## CLI Reference

```
Usage: ak47 <COMMAND>

Commands:
  up           Start syncing blocks from the chain (continuous) and serve HTTP API
  status       Show sync status
  query        Run a SQL query (use --signature to decode event logs)
  materialize  Manage continuous aggregates (materialized views)
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

# Create a continuous aggregate
ak47 materialize create tx_volume_hourly \
  --sql "SELECT time_bucket('1 hour', block_timestamp) AS hour, COUNT(*) as tx_count FROM txs GROUP BY hour"

# List aggregates
ak47 materialize list
```

## HTTP API

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/status` | GET | Sync status for all chains |
| `/query` | GET | Execute SQL query |
| `/metrics` | GET | Prometheus metrics (port 9090) |

### Examples

```bash
# Simple query
curl "http://localhost:8080/query?sql=SELECT * FROM blocks LIMIT 5"

# Response
{
  "columns": ["num", "hash", "timestamp", ...],
  "rows": [[123, "0x...", "2024-01-01T00:00:00Z", ...]],
  "row_count": 5,
  "ok": true
}
```

```bash
curl http://localhost:8080/status

# Response
{
  "chains": [{
    "name": "mainnet",
    "chain_id": 4217,
    "synced_num": 567890,
    "head_num": 567890,
    "backfill_num": 123456,
    "lag": 0
  }]
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

These queries scan raw data. For large datasets, create [continuous aggregates](#continuous-aggregates) to pre-compute results.

```sql
-- Transactions per hour (last 24h)
SELECT 
  time_bucket('1 hour', block_timestamp) AS hour,
  COUNT(*) AS tx_count
FROM txs
WHERE block_timestamp > NOW() - INTERVAL '24 hours'
GROUP BY hour
ORDER BY hour DESC;

-- Gas usage trend (last 30 days)
SELECT 
  time_bucket('1 day', timestamp) AS day,
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
  time_bucket('1 day', block_timestamp) AS day,
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

All tables are TimescaleDB hypertables partitioned by `timestamp`/`block_timestamp`:

### blocks

| Column | Type | Description |
|--------|------|-------------|
| `num` | INT8 | Block number |
| `hash` | BYTEA | Block hash |
| `parent_hash` | BYTEA | Parent block hash |
| `timestamp` | TIMESTAMPTZ | Block timestamp |
| `gas_limit` | INT8 | Gas limit |
| `gas_used` | INT8 | Gas used |
| `miner` | BYTEA | Block producer |

### txs

| Column | Type | Description |
|--------|------|-------------|
| `block_num` | INT8 | Block number |
| `block_timestamp` | TIMESTAMPTZ | Block timestamp |
| `idx` | INT4 | Transaction index |
| `hash` | BYTEA | Transaction hash |
| `type` | INT2 | Transaction type |
| `from` | BYTEA | Sender address |
| `to` | BYTEA | Recipient address |
| `value` | TEXT | Transfer value (wei) |
| `input` | BYTEA | Calldata |
| `gas_used` | INT8 | Gas consumed |

### logs

| Column | Type | Description |
|--------|------|-------------|
| `block_num` | INT8 | Block number |
| `block_timestamp` | TIMESTAMPTZ | Block timestamp |
| `log_idx` | INT4 | Log index |
| `tx_hash` | BYTEA | Transaction hash |
| `address` | BYTEA | Emitting contract |
| `selector` | BYTEA | Event selector (topic0) |
| `topics` | BYTEA[] | All topics |
| `data` | BYTEA | Event data |

### sync_state

| Column | Type | Description |
|--------|------|-------------|
| `chain_id` | INT8 | Chain identifier |
| `head_num` | INT8 | Remote chain head |
| `synced_num` | INT8 | Highest synced block |
| `backfill_num` | INT8 | Lowest synced block |

## Continuous Aggregates

Pre-compute analytics for instant queries on large datasets using TimescaleDB continuous aggregates.

### Creating Aggregates

Create with the CLI:

```bash
# Create aggregate
ak47 materialize create txs_daily \
  --sql "SELECT time_bucket('1 day', block_timestamp) AS day, COUNT(*) AS count FROM txs GROUP BY day" \
  --refresh-interval "1 hour" \
  --refresh-start "3 days"

# List all aggregates
ak47 materialize list

# Get info about an aggregate
ak47 materialize info txs_hourly

# Manually refresh
ak47 materialize refresh txs_hourly --full

# Drop an aggregate
ak47 materialize drop txs_daily
```

### Querying Aggregates

```sql
-- Recent daily transaction counts by type
SELECT * FROM txs_daily ORDER BY day DESC LIMIT 30;

-- Compare with previous period
SELECT 
  day,
  tx_count,
  LAG(tx_count) OVER (ORDER BY day) AS prev_day,
  tx_count - LAG(tx_count) OVER (ORDER BY day) AS change
FROM tx_volume_hourly
WHERE day > NOW() - INTERVAL '7 days';

-- Join aggregate with raw data for drill-down
SELECT 
  agg.day,
  agg.tx_count,
  COUNT(*) FILTER (WHERE t.type = 118) AS tempo_txs
FROM txs_daily agg
JOIN txs t ON time_bucket('1 day', t.block_timestamp) = agg.day
WHERE agg.day > NOW() - INTERVAL '7 days'
GROUP BY agg.day, agg.tx_count
ORDER BY agg.day DESC;

-- Aggregate of aggregates (weekly from daily)
SELECT 
  time_bucket('1 week', day) AS week,
  SUM(tx_count) AS weekly_txs
FROM txs_daily
GROUP BY week
ORDER BY week DESC;
```

## Development

### Prerequisites

- Rust 1.75+
- Docker & Docker Compose
- PostgreSQL client (optional)

### Make Commands

```bash
make up                  # Start devnet (TimescaleDB + Tempo)
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
├── db/           # Database pool + schema
├── sync/         # Sync engine, fetcher, writer
├── service/      # Shared business logic
└── types.rs      # Core data types

db/               # SQL migrations
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

Tests use real TimescaleDB and Tempo nodes (no mocks).

## License

MIT License — see [LICENSE](./LICENSE)

## Acknowledgments

- [golden-axe](https://github.com/indexsupply/golden-axe) — Inspiration for the indexing architecture
