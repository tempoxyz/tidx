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

**tidx** is [Tempo](https://tempo.xyz)'s chain indexer. It ingests blocks, transactions, and logs from a Tempo node, then makes them queryable through SQL, an HTTP API, and a CLI. Its hybrid PostgreSQL and ClickHouse architecture supports fast point lookups (OLTP) and large analytical queries (OLAP).

Use tidx when you need structured Tempo chain data without building an indexing pipeline from scratch. Common use cases include analytics dashboards, block explorers, wallet balance lookups, token analytics, and DEX activity tracking.

## Features

- **Tiered Storage:** hot window in PostgreSQL (OLTP), full archive in ClickHouse (OLAP), queried as one
- **Event/Function Decoding:** Query decoded events or function calldata by ABI signature (no pre-registration)
- **HTTP API + CLI:** Query data via REST, SQL, or command line

## Table of Contents

- [Quickstart](#quickstart)
- [Overview](#overview)
- [Learn more](#learn-more)
- [Installation](#installation)
- [Configuration](#configuration)
- [CLI](#cli)
- [HTTP API](#http-api)
- [Tables](#tables)
- [Sync Architecture](#sync-architecture)
- [Development](#development)
- [License](#license)

## Quickstart

```bash
curl -L https://tidx.vercel.app/docker | bash
```

## Overview

Tidx sits between a Tempo node and applications that need structured chain data. The Tempo block explorer in `tempo-apps` is a downstream example of an application that uses the same kind of indexed chain data. The same architecture can support explorers, analytics dashboards, and data services for other applications built on Tempo.

Realtime ingestion writes to both PostgreSQL and ClickHouse. With `[chains.retention]` enabled, historical backfill writes directly from RPC to ClickHouse, then a reconciler hydrates only the configured PostgreSQL hot window from checkpointed ClickHouse archive ranges (for example `pg_keep = "30d"`). Increasing `pg_keep` restores the newly requested range from ClickHouse before moving the tier boundary; decreasing it moves the boundary first and then drops expired partitions. PostgreSQL therefore never needs enough space for the initial full-chain sync, and historical blocks are fetched and decoded from RPC only once. Without retention, both stores hold full history. Queries route across the tiers via the `engine` and `source` parameters:

```
                        ┌───────────────────────────────────────┐
                        │                /query                 │
                        │                                       │
                        │  ?signature=...                       │
                        │  ?engine=...  &source=...             │
                        └───────────────────┬───────────────────┘
                                            │
                                            ▼
                        ┌───────────────────────────────────────┐
                        │             Query Router              │
                        └───────┬───────────────────────┬───────┘
                          hot   │                       │   cold / analytics
                                ▼                       ▼
          ┌──────────────────────────┐                ┌──────────────────────────┐
          │        PostgreSQL        │  pg_clickhouse │        ClickHouse        │
          │    hot window (OLTP)     │     (FDW)      │    full archive (OLAP)   │
          │   point reads ~1 ms †    │───────────────►│  90d rollups ~160 ms †   │
          │    30d hot ≈ 8 GB †      │                │   full chain ≈ 26 GB †   │
          └────────────┬─────────────┘                └────────────┬─────────────┘
                       ▲                                           ▲
                       │ hot-window reconcile                      │ full archive
                       │                                           │ backfill
                       └─────────────────────┬─────────────────────┘
                                             │
                                     ┌───────┴───────┐
                                     │  Sync Engine  │
                                     │ realtime→both │
                                     └───────────────┘
```

`†`: sampled from Tempo mainnet (chain `4217`): full archive ≈ 29.3M blocks, 30d hot window ≈ 5.8M blocks.

| `engine` | `source` | Route |
|----------|----------|-------|
| `postgres`* | `postgres-clickhouse`* | **Tiered:** hot reads on PostgreSQL, cold/analytical reads on native ClickHouse; provably-safe shapes split at the prune boundary, everything else falls back to `pg_clickhouse` (FDW) |
| `postgres`* | `postgres` | Plain PostgreSQL (hot window only when retention is enabled) |
| `postgres`* | `clickhouse` | PostgreSQL planner over the full ClickHouse archive via `pg_clickhouse` foreign tables |
| `clickhouse` | `clickhouse`* | Native ClickHouse (full archive + materialized views) |

`*`: default when omitted. `engine=tiered` is accepted as a legacy alias for `engine=postgres&source=postgres-clickhouse`.

> [!NOTE]
> FDW routes require a `pg_clickhouse` build with [ClickHouse/pg_clickhouse#296](https://github.com/ClickHouse/pg_clickhouse/pull/296) (unreleased 0.4): 0.3.2 crashes PostgreSQL on LATERAL / correlated subqueries over foreign tables. `ghcr.io/tempoxyz/pg_clickhouse:16-02505c4` ships a pinned build (see `.github/workflows/pg-clickhouse.yml`). Aggregates pushed to ClickHouse (e.g. `avg`) compute in Float64 and can differ from PostgreSQL `numeric` in the last decimal.

```bash
# Tiered (default) - last 10 transfers from an address; hot rows come from
# PostgreSQL, older rows from the ClickHouse archive
curl "https://indexer.tempo.xyz/query \
  ?chainId=4217 \
  &signature=Transfer(address,address,uint256) \
  &sql=SELECT * FROM Transfer WHERE from = '0x...' ORDER BY block_num DESC LIMIT 10"

# ClickHouse (OLAP) - same query, faster for large scans
curl "https://indexer.tempo.xyz/query \
  ?chainId=4217 \
  &engine=clickhouse \
  &signature=Transfer(address,address,uint256) \
  &sql=SELECT * FROM Transfer WHERE from = '0x...' ORDER BY block_num DESC LIMIT 10"

# ClickHouse (OLAP) - query pre-computed views
curl "https://indexer.tempo.xyz/views?chainId=4217"
> {"ok":true,"views":[{"name":"top_holders","columns":[{"name":"token","type":"String"},{"name":"holder","type":"String"},{"name":"balance","type":"UInt256"}]}]}

curl "https://indexer.tempo.xyz/query \
  ?chainId=4217 \
  &engine=clickhouse \
  &sql=SELECT * FROM top_holders WHERE token = '0x...' LIMIT 10"
```

## Learn more

- [Tempo developer documentation](https://tempo.xyz/developers/docs): guides for building on Tempo
- [tempoxyz/tempo](https://github.com/tempoxyz/tempo): Tempo node and protocol implementation
- [tempoxyz/tempo-apps](https://github.com/tempoxyz/tempo-apps): Tempo applications, including the block explorer

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
batch_size = 100

[chains.postgres]
url = "postgres://user@tidx.example.com:5432/tidx_mainnet"
password_env = "TIDX_PG_PASSWORD"  # Password from environment variable

# Optional: ClickHouse for OLAP queries
[chains.clickhouse]
enabled = true
url = "http://clickhouse:8123"
# Historical materialized-table repair runs after base ClickHouse backfill by default.
repair_derived_on_startup = true

# Optional: tiered storage. Backfills full history directly to ClickHouse, then
# materializes a reversible 30d PostgreSQL hot window from that archive.
[chains.retention]
pg_keep = "30d"

[[chains]]
name = "moderato"
chain_id = 42431
rpc_url = "https://rpc.testnet.tempo.xyz"

[chains.postgres]
url = "postgres://user@tidx.example.com:5432/tidx_moderato"
password_env = "TIDX_PG_PASSWORD"
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
├── batch_size              u64       = 100          Blocks per RPC batch request
├── [postgres]                                       PostgreSQL settings
│   ├── url                 string    (required)     PostgreSQL connection string
│   ├── password_env        string    (optional)     Env var name for PostgreSQL password
│   ├── api_url             string    (optional)     Separate PostgreSQL URL for API (e.g., read replica)
│   └── api_password_env    string    (optional)     Env var name for API PostgreSQL password
├── [clickhouse]                                     ClickHouse OLAP settings
│   ├── enabled             bool      = false        Enable ClickHouse OLAP queries
│   ├── url                 string    = "http://clickhouse:8123"  ClickHouse HTTP URL
│   ├── fdw_url             string    = url          ClickHouse URL as reachable from PostgreSQL (pg_clickhouse FDW)
│   ├── repair_derived_on_startup bool = true        Repair historical derived-table gaps on startup
│   └── replicated_database bool      = false        Create the database as ENGINE = Replicated with Replicated* table engines
└── [retention]                                      Tiered storage (PostgreSQL hot window)
    ├── pg_keep             string    (required)     Recent data kept in PostgreSQL, e.g. "30d" or "36h"
    ├── prune_interval      string    = "1h"         How often the pruner runs
    └── require_clickhouse  bool      = true         Only prune rows durably archived in ClickHouse
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
  -u, --url <URL>              TIDX HTTP API URL (e.g., http://localhost:8080)
  -n, --chain-id <CHAIN_ID>   Chain ID to query (uses first chain if not specified)
  -e, --engine <ENGINE>        Query engine (postgres, clickhouse)
      --source <SOURCE>        Data source (postgres, clickhouse, postgres-clickhouse) [default: postgres-clickhouse]
  -f, --format <FORMAT>        Output format (table, json, csv, toon) [default: table]
  -l, --limit <LIMIT>          Maximum rows to return [default: 10000]
  -s, --signature <SIGNATURE>  Event signature to create a CTE
  -t, --timeout <TIMEOUT>      Query timeout in milliseconds [default: 30000]
  -c, --config <CONFIG>        Path to config file [default: config.toml]
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

# List views on the hosted mainnet indexer
tidx views --url https://indexer.tempo.xyz list --chain-id 4217

# Create a view (must be run from trusted IP)
tidx views --url http://localhost:8080 create \
  --chain-id 4217 \
  --name top_holders \
  --sql "SELECT holder, SUM(balance) as total FROM balances GROUP BY holder" \
  --order-by holder

# Self-update
tidx upgrade
```

## HTTP API

tidx exposes a HTTP API for querying the indexer.

Hosted endpoints:

- Mainnet: `https://indexer.tempo.xyz` (`chainId=4217`)
- Testnet: `https://indexer.testnet.tempo.xyz` (`chainId=42431`)

### Examples

```bash
# Point lookup (tiered by default: hot rows from PostgreSQL, archived rows from ClickHouse)
curl "https://indexer.tempo.xyz/query?chainId=4217&sql=SELECT * FROM blocks WHERE num = 12345"
> {"columns":["num","hash","timestamp"],"rows":[[12345,"0xabc...","2024-01-01T00:00:00Z"]],"row_count":1,"engine":"tiered","ok":true}

# Aggregation (native ClickHouse)
curl "https://indexer.tempo.xyz/query?chainId=4217&engine=clickhouse&sql=SELECT type, COUNT(*) FROM txs GROUP BY type"
> {"columns":["type","count"],"rows":[[0,50000],[2,120000]],"row_count":2,"engine":"clickhouse","ok":true}

# Status
curl http://localhost:8080/status
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
     ?source                string    = postgres-clickhouse  Data source: postgres, clickhouse, or postgres-clickhouse
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
curl "https://indexer.testnet.tempo.xyz/views?chainId=42431"
```

```json
{
  "ok": true,
  "views": [
    {
      "name": "whale_holders",
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
curl -X POST "http://localhost:8080/views" \
  -H "Content-Type: application/json" \
  -d '{
    "chainId": 42431,
    "name": "whale_holders",
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
curl "https://indexer.testnet.tempo.xyz/views/whale_holders?chainId=42431"
```

```json
{
  "ok": true,
  "view": {"name": "whale_holders", "engine": "View", "database": "analytics_42431"},
  "definition": "CREATE VIEW analytics_42431.whale_holders AS SELECT ...",
  "row_count": 1234567
}
```

#### Delete View (trusted IP only)

```bash
curl -X DELETE "http://localhost:8080/views/whale_holders?chainId=42431"
```

```json
{
  "ok": true,
  "deleted": ["token_holders_mv", "whale_holders"]
}
```

#### Query Views

Views are auto-prefixed with `analytics_{chainId}` when using `engine=clickhouse`:

```bash
# Query the view (auto-prefixed)
curl "https://indexer.testnet.tempo.xyz/query?chainId=42431&engine=clickhouse&sql=SELECT * FROM whale_holders WHERE token = '0x...' ORDER BY balance DESC LIMIT 10"
```

## Tables

Three families of tables are queryable through `/query`:

- **[Base Tables](#base-tables):** raw chain data written by the sync engine, available in both PostgreSQL and ClickHouse.
- **[Event Tables](#event-tables):** virtual, decoded-at-query-time tables generated from `?signature=Event(...)`. Available in both engines.
- **[Materialized Tables](#materialized-tables):** precomputed token, holder, and DEX analytics maintained by ClickHouse. Available only with `engine=clickhouse`, alongside user-defined views registered through the [`/views` API](#views-api).

### Base Tables

Written by the sync engine to both PostgreSQL and ClickHouse. Schemas are identical across engines; the per-table column names below are the source of truth.

#### blocks

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
| `consensus_proposer` | `BYTEA` | Ed25519 consensus proposer pubkey (TIP-1031, NULL pre-fork) |

#### txs

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

#### logs

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

#### receipts

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

> The ClickHouse mirror of `receipts` additionally carries `type` (`Nullable(Int16)`) and `fee_token` (`Nullable(String)`), denormalized from the matching tx at ingest so receipt queries don't have to join `txs`. Rows written before the `receipts_20260604_type_fee_token` migration stay `NULL` until re-synced.

#### sync_state

| Column | Type | Description |
|--------|------|-------------|
| `chain_id` | `INT8` | Chain identifier |
| `head_num` | `INT8` | Remote chain head from RPC |
| `synced_num` | `INT8` | Highest contiguous block (no gaps from backfill_num to here) |
| `tip_num` | `INT8` | Highest block near chain head (realtime follows this) |
| `backfill_num` | `INT8` | Lowest synced block going backwards (NULL=not started, 0=complete) |
| `archive_tip_num` | `INT8` | Highest block in the contiguous ClickHouse archive interval |
| `archive_backfill_num` | `INT8` | Lowest block in the contiguous ClickHouse archive interval (NULL=not started) |
| `pruned_below` | `INT8` | Current PostgreSQL hot-tier boundary; rows at or below it route to ClickHouse |
| `pruned_below_ts` | `TIMESTAMPTZ` | Timestamp boundary used for tiered constraint exclusion |
| `started_at` | `TIMESTAMPTZ` | Sync start time |
| `updated_at` | `TIMESTAMPTZ` | Last update time |

### Event Tables

Pass `?signature=Event(type1,type2,...)` to `/query` and tidx exposes a virtual table named after the event with one column per parameter. The table is generated as a CTE at query time, so no schema registration is needed and any event signature works on demand. Works against both `engine=postgres` and `engine=clickhouse`:

```bash
curl -G "https://indexer.tempo.xyz/query" \
  --data-urlencode "chainId=4217" \
  --data-urlencode "signature=Transfer(address,address,uint256)" \
  --data-urlencode "sql=SELECT \"from\", \"to\", value
    FROM Transfer
    WHERE \"from\" = '0xabc…'
    ORDER BY block_num DESC
    LIMIT 10"
```

For Transfer logs specifically, [`token_transfers`](#token_transfers) is pre-decoded and cheaper.

### Materialized Tables

> [!NOTE]
> All tables in this section are ClickHouse-only. Query them with `engine=clickhouse`.

ClickHouse maintains insert-time derived tables and scheduled snapshots, pruning block-scoped rows on reorg. Token-first and holder-first balance data use separate sort keys for their inverse access patterns.

On startup, tidx verifies built-in materialized tables after base ClickHouse backfill is caught up. It compares each table against its source SELECT in bounded block ranges, logs detected gaps, and replays only the missing ranges in chunks while realtime sync continues. Failed derived repairs are retried with backoff. Set `repair_derived_on_startup = false` under `[chains.clickhouse]` only if an operator needs to suppress historical repair work temporarily.

For self-hosted multi-replica ClickHouse (coordinated by Keeper/ZooKeeper), set `replicated_database = true` under `[chains.clickhouse]`. The sink then creates the database with `ENGINE = Replicated` and rewrites MergeTree-family table engines to their `Replicated*` counterparts, so both DDL and data reach every replica. Leave it off for ClickHouse Cloud (replication is implicit) and single-node instances. The flag only affects newly created objects. Point it at a fresh database when enabling replication for an existing chain.

| Name | Purpose |
|------|---------|
| [`address_balances`](#address_balances) | Current positive balance per `(holder, token)`. |
| [`address_balances_snapshot`](#address_balances_snapshot) | Pre-aggregated `address_balances`, refreshed on a schedule. |
| [`address_holder_deltas`](#address_holder_deltas) | Holder-first mirror of `token_holder_deltas`. |
| [`dex_fills`](#dex_fills) | Decoded stablecoin-DEX `OrderFilled` events. |
| [`dex_ohlc_1m`](#dex_ohlc_1m) | 1-minute OHLC candles per pair, refreshed on a schedule. |
| [`dex_orders`](#dex_orders) | Decoded stablecoin-DEX `OrderPlaced` events. |
| [`dex_pair_liquidity`](#dex_pair_liquidity) | Pairs joined to their on-DEX base liquidity. |
| [`dex_pairs`](#dex_pairs) | Decoded stablecoin-DEX `PairCreated` events. |
| [`token_balances`](#token_balances) | Current positive balance per `(token, holder)`. |
| [`token_balances_snapshot`](#token_balances_snapshot) | Pre-aggregated `token_balances`, refreshed on a schedule. |
| [`token_holder_counts`](#token_holder_counts) | Holder count per token, refreshed on a schedule. |
| [`token_holder_deltas`](#token_holder_deltas) | Per-event ± balance change, two rows per transfer. |
| [`token_metadata`](#token_metadata) | Per-token first/last seen + lifetime transfer count. |
| [`token_supply`](#token_supply) | Per-token mints − burns (zero-address legs). |
| [`token_transfer_stats`](#token_transfer_stats) | Per-`(day, token)` count, volume, unique senders/recipients. |
| [`token_transfers`](#token_transfers) | Decoded `Transfer` events. |

User-defined views registered through the [`/views` API](#views-api) live alongside these in `analytics_{chainId}` and are queryable the same way.

#### address_balances

> [!NOTE]
> View over `address_holder_deltas FINAL`, grouped by `(holder, token)`.

Current positive balances grouped by holder first answer "what does this address hold?" in one sort-key range. `token_balances` answers the inverse, "who holds this token?" Both use the same underlying transfer events with different sort orders.

| Column | Type | Description |
|--------|------|-------------|
| `holder` | `String` | Holder address |
| `token` | `String` | Token contract |
| `balance` | `Int256` | Current balance (positive only) |

```bash
curl -G "https://indexer.testnet.tempo.xyz/query" \
  --data-urlencode "chainId=42431" \
  --data-urlencode "engine=clickhouse" \
  --data-urlencode "sql=SELECT token, toString(balance) AS balance
    FROM address_balances
    WHERE holder = '0x70997970c51812dc3a010c7d01b50e0d17dc79c8'
    ORDER BY balance DESC"
```

#### address_balances_snapshot

> [!NOTE]
> Refreshable materialized view over `address_holder_deltas FINAL`, recomputed on a schedule (every 15 minutes) rather than on insert.

Same `(holder, token, balance)` rollup as [`address_balances`](#address_balances), but stored in its own `MergeTree` ordered by `(holder, balance, token)` so account balance pages and counts resolve via a holder-keyed range instead of re-aggregating the full delta history on every read. Use this for hot addresses where the live `address_balances` view would time out; results are up to one refresh interval stale.

| Column | Type | Description |
|--------|------|-------------|
| `holder` | `String` | Holder address |
| `token` | `String` | Token contract |
| `balance` | `Int256` | Current balance (positive only) |

```bash
curl -G "https://indexer.testnet.tempo.xyz/query" \
  --data-urlencode "chainId=42431" \
  --data-urlencode "engine=clickhouse" \
  --data-urlencode "sql=SELECT token, toString(balance) AS balance
    FROM address_balances_snapshot
    WHERE holder = '0x70997970c51812dc3a010c7d01b50e0d17dc79c8'
    ORDER BY balance DESC
    LIMIT 5"
```

#### address_holder_deltas

> [!NOTE]
> Materialized view over `token_transfers`, kept in sync on reorg.

Same deltas as `token_holder_deltas` but ordered `(holder, token, block_num, …)` so per-holder balance reconstructions are a sort-key seek. Two rows per transfer (recipient `leg=+1`, sender `leg=-1`); skips zero-address legs.

| Column | Type | Description |
|--------|------|-------------|
| `block_num` | `Int64` | Block number |
| `block_timestamp` | `DateTime64(3, 'UTC')` | Block timestamp |
| `tx_hash` | `String` | Transaction hash |
| `log_idx` | `Int32` | Log index |
| `holder` | `String` | Holder address |
| `token` | `String` | Token contract |
| `leg` | `Int8` | `+1` for credit, `-1` for debit |
| `balance_delta` | `Int256` | Signed delta applied to `(holder, token)` |


#### dex_pairs

> [!NOTE]
> Materialized view over `logs`, kept in sync on reorg.

Decoded `PairCreated(bytes32 indexed key, address indexed base, address indexed quote)` events from the stablecoin DEX precompile. All three params are indexed, so the payload lives in topics. `ReplacingMergeTree` ordered by `(block_num, log_idx)` for the pair-creation feed, with bloom indexes on `base`/`quote`. Replaces the runtime `signature=PairCreated(...)` CTE.

| Column | Type | Description |
|--------|------|-------------|
| `block_num` | `Int64` | Block number |
| `block_timestamp` | `DateTime64(3, 'UTC')` | Block timestamp |
| `tx_idx` | `Int32` | Transaction index |
| `log_idx` | `Int32` | Log index |
| `tx_hash` | `String` | Transaction hash |
| `address` | `String` | Emitting contract (the DEX precompile) |
| `key` | `String` | Pair key (`bytes32`) |
| `base` | `String` | Base token address |
| `quote` | `String` | Quote token address |

#### dex_orders

> [!NOTE]
> Materialized view over `logs`, kept in sync on reorg.

Decoded `OrderPlaced(uint128 indexed orderId, address indexed maker, address indexed token, uint128 amount, bool isBid, int16 tick, bool isFlipOrder, int16 flipTick)` events. `int16` ticks decode from their sign-extended ABI word (trailing 2 bytes, little-endian) so negatives are correct. `ReplacingMergeTree` ordered by `(token, orderId)` so pair-scoped filters (`WHERE token = base`) seek by sort key and joins by `orderId` use the bloom index.

| Column | Type | Description |
|--------|------|-------------|
| `block_num` | `Int64` | Block number |
| `block_timestamp` | `DateTime64(3, 'UTC')` | Block timestamp |
| `tx_idx` | `Int32` | Transaction index |
| `log_idx` | `Int32` | Log index |
| `tx_hash` | `String` | Transaction hash |
| `address` | `String` | Emitting contract (the DEX precompile) |
| `orderId` | `UInt256` | Order id |
| `maker` | `String` | Order maker |
| `token` | `String` | Pair base token |
| `amount` | `UInt256` | Order amount |
| `isBid` | `UInt8` | 1 = bid, 0 = ask |
| `tick` | `Int16` | Order price tick |
| `isFlipOrder` | `UInt8` | 1 = flip order |
| `flipTick` | `Int16` | Flip order price tick |

#### dex_fills

> [!NOTE]
> Materialized view over `logs`, kept in sync on reorg.

Decoded `OrderFilled(uint128 indexed orderId, address indexed maker, address indexed taker, uint128 amountFilled, bool partialFill)` events. The pair (`token`/`isBid`/`tick`) is not on this event. Join `orderId` to [`dex_orders`](#dex_orders). `ReplacingMergeTree` ordered by `(orderId, block_num, log_idx)` so the join probes by sort key, with bloom indexes on `maker`/`taker`/`tx_hash`.

| Column | Type | Description |
|--------|------|-------------|
| `block_num` | `Int64` | Block number |
| `block_timestamp` | `DateTime64(3, 'UTC')` | Block timestamp |
| `tx_idx` | `Int32` | Transaction index |
| `log_idx` | `Int32` | Log index |
| `tx_hash` | `String` | Transaction hash |
| `address` | `String` | Emitting contract (the DEX precompile) |
| `orderId` | `UInt256` | Order id (join key to `dex_orders`) |
| `maker` | `String` | Fill maker |
| `taker` | `String` | Fill taker |
| `amountFilled` | `UInt256` | Amount filled |
| `partialFill` | `UInt8` | 1 = partial fill |

```bash
curl -G "https://indexer.testnet.tempo.xyz/query" \
  --data-urlencode "chainId=42431" \
  --data-urlencode "engine=clickhouse" \
  --data-urlencode "sql=SELECT f.block_num, f.amountFilled, o.token, o.isBid, o.tick
    FROM dex_fills f
    JOIN dex_orders o ON o.orderId = f.orderId
    WHERE o.token = '0x20c000000000000000000000b9537d11c60e8b50'
    ORDER BY f.block_num DESC, f.log_idx DESC
    LIMIT 10"
```

#### dex_ohlc_1m

> [!NOTE]
> Refreshable materialized view over `dex_fills ⋈ dex_orders`, recomputed on a schedule (every minute) over a rolling 30-day retention window.

1-minute OHLC (candlestick) buckets per trading pair, stored in a `MergeTree` ordered by `(token, bucket)`. Replaces scanning up to ~1000 raw fills and bucketing in memory on every chart request: a read becomes a primary-key range scan over candles, and long windows are no longer truncated. Coarser intervals (5m, 1h, …) re-bucket these 1m candles client-side (`high = max(high)`, `low = min(low)`, `open` = first bucket's open, `close` = last bucket's close, volumes summed); historical ranges beyond the retention window fall back to scanning `dex_fills`.

Refreshable (full recompute + atomic swap) because candles are keyed by `(token, bucket)` with no `block_num`, so an incremental aggregate couldn't be reorg-pruned. This uses the same reasoning as `token_balances_snapshot`. Price math mirrors the API: `priceScale = 100000`, rate = `(priceScale + tick) / priceScale` for bids and its inverse for asks. Tune `REFRESH EVERY` and the retention window to fill volume.

| Column | Type | Description |
|--------|------|-------------|
| `bucket` | `DateTime('UTC')` | Start of the 1-minute bucket |
| `token` | `String` | Pair base token |
| `open` | `Float64` | Rate of the first fill in the bucket |
| `high` | `Float64` | Highest fill rate in the bucket |
| `low` | `Float64` | Lowest fill rate in the bucket |
| `close` | `Float64` | Rate of the last fill in the bucket |
| `base_volume` | `UInt256` | Sum of base-token amounts filled |
| `quote_volume` | `UInt256` | Sum of quote-token amounts |
| `fill_count` | `UInt64` | Number of fills in the bucket |

```bash
curl -G "https://indexer.testnet.tempo.xyz/query" \
  --data-urlencode "chainId=42431" \
  --data-urlencode "engine=clickhouse" \
  --data-urlencode "sql=SELECT bucket, open, high, low, close, base_volume
    FROM dex_ohlc_1m
    WHERE token = '0x20c000000000000000000000b9537d11c60e8b50'
      AND bucket >= now() - INTERVAL 6 HOUR
    ORDER BY bucket"
```

#### dex_pair_liquidity

> [!NOTE]
> Plain view over `dex_pairs FINAL ⋈ token_balances_snapshot`.

Trading pairs joined to their on-DEX base-token liquidity. The DEX precompile escrows both base and quote tokens, but only base addresses map to a pair; this view pushes that intersection into ClickHouse so the "pairs by liquidity" endpoint reads ranked pairs directly (`ORDER BY liquidity DESC, base ASC LIMIT …`) instead of over-fetching DEX balances and intersecting with the pair set in memory. The DEX precompile address (`0xdec0…0000`) is fixed across Tempo chains and inlined.

| Column | Type | Description |
|--------|------|-------------|
| `key` | `String` | Pair key (`bytes32`) |
| `base` | `String` | Base token address |
| `quote` | `String` | Quote token address |
| `block_num` | `Int64` | Pair creation block |
| `log_idx` | `Int32` | Pair creation log index |
| `block_timestamp` | `DateTime64(3, 'UTC')` | Pair creation timestamp |
| `tx_hash` | `String` | Pair creation tx hash |
| `liquidity` | `Int256` | Base-token balance escrowed on the DEX |

```bash
curl -G "https://indexer.testnet.tempo.xyz/query" \
  --data-urlencode "chainId=42431" \
  --data-urlencode "engine=clickhouse" \
  --data-urlencode "sql=SELECT base, quote, liquidity
    FROM dex_pair_liquidity
    ORDER BY liquidity DESC, base ASC
    LIMIT 20"
```


#### token_balances

> [!NOTE]
> View over `token_holder_deltas FINAL`, always reflects the post-merge state.

Current positive balance per `(token, holder)`, rolled up from `token_holder_deltas`. Filtered to `balance > 0`.

| Column | Type | Description |
|--------|------|-------------|
| `token` | `String` | Token contract |
| `holder` | `String` | Holder address |
| `balance` | `Int256` | Current balance (positive only) |

```bash
curl -G "https://indexer.testnet.tempo.xyz/query" \
  --data-urlencode "chainId=42431" \
  --data-urlencode "engine=clickhouse" \
  --data-urlencode "sql=SELECT holder, toString(balance) AS balance
    FROM token_balances
    WHERE token = '0x20c000000000000000000000e65cb5a40b7885ae'
    ORDER BY balance DESC
    LIMIT 5"
```

```json
{"ok":true,"columns":["holder","balance"],"rows":[
  ["0x70997970c51812dc3a010c7d01b50e0d17dc79c8","68056473384187692692674921486353642324"],
  ["0x3c44cdddb6a900fa2b585dd299e03d12fa4293bc","68056473384187692692674921486353642370"],
  ["0x15d34aaf54267db7d7c367839aaf71a00a2c6a65","68056473384187692692674921486353642328"],
  ["0x90f79bf6eb2c4f870365e785982e1f101e93b906","68056473384187692692674921486353642286"],
  ["0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266","68056473384187692692674921486353642272"]
]}
```

#### token_balances_snapshot

> [!NOTE]
> Refreshable materialized view over `token_holder_deltas FINAL`, recomputed on a schedule (every 15 minutes) rather than on insert.

Same `(token, holder, balance)` rollup as [`token_balances`](#token_balances), but stored in its own `MergeTree` ordered by `(token, balance)` so holder counts and "top holders by balance" resolve via a sort-key seek instead of re-aggregating the full delta history on every read. Use this for high-cardinality tokens where the live `token_balances` view would time out; results are up to one refresh interval stale.

| Column | Type | Description |
|--------|------|-------------|
| `token` | `String` | Token contract |
| `holder` | `String` | Holder address |
| `balance` | `Int256` | Current balance (positive only) |

```bash
curl -G "https://indexer.testnet.tempo.xyz/query" \
  --data-urlencode "chainId=42431" \
  --data-urlencode "engine=clickhouse" \
  --data-urlencode "sql=SELECT count() AS holders
    FROM token_balances_snapshot
    WHERE token = '0x20c000000000000000000000e65cb5a40b7885ae'"
```

#### token_holder_counts

> [!NOTE]
> Refreshable materialized view over [`token_balances_snapshot`](#token_balances_snapshot), recomputed on a schedule (every 15 minutes) rather than on insert.

Holder count per token, collapsed to a single row per token and stored in its own `MergeTree` ordered by `(token)`. `count() … FROM token_balances_snapshot WHERE token = X` is a primary-key range scan that still touches one row per holder (millions for tokens like PathUSD); this turns the token-detail "holder count" into a single-row primary-key lookup. Derived from the already-deduped snapshot rather than the raw deltas, so each refresh is cheap; counts are at most one refresh interval staler than the snapshot.

| Column | Type | Description |
|--------|------|-------------|
| `token` | `String` | Token contract |
| `holder_count` | `UInt64` | Number of holders with a positive balance |

```bash
curl -G "https://indexer.testnet.tempo.xyz/query" \
  --data-urlencode "chainId=42431" \
  --data-urlencode "engine=clickhouse" \
  --data-urlencode "sql=SELECT holder_count
    FROM token_holder_counts
    WHERE token = '0x20c000000000000000000000e65cb5a40b7885ae'"
```

#### token_holder_deltas

> [!NOTE]
> Materialized view over `token_transfers`, kept in sync on reorg.

Two rows per transfer (recipient `leg=+1`, sender `leg=-1`); skips zero-address legs. `ReplacingMergeTree` deduplicates by `(token, holder, block_num, tx_hash, log_idx, leg)` so retried inserts collapse on merge.

| Column | Type | Description |
|--------|------|-------------|
| `block_num` | `Int64` | Block number |
| `block_timestamp` | `DateTime64(3, 'UTC')` | Block timestamp |
| `tx_hash` | `String` | Transaction hash |
| `log_idx` | `Int32` | Log index |
| `token` | `String` | Token contract |
| `holder` | `String` | Holder address (sender or recipient) |
| `leg` | `Int8` | `+1` for recipient credit, `-1` for sender debit |
| `balance_delta` | `Int256` | Signed delta applied to `holder` for `token` at this block |

```bash
curl -G "https://indexer.testnet.tempo.xyz/query" \
  --data-urlencode "chainId=42431" \
  --data-urlencode "engine=clickhouse" \
  --data-urlencode "sql=SELECT block_num, toString(balance_delta) AS delta, leg
    FROM token_holder_deltas
    WHERE token = '0x20c000000000000000000000e65cb5a40b7885ae'
      AND holder = '0x70997970c51812dc3a010c7d01b50e0d17dc79c8'
    ORDER BY block_num DESC, log_idx DESC
    LIMIT 5"
```

```json
{"ok":true,"columns":["block_num","delta","leg"],"rows":[
  [1062,"-17431",-1],
  [1059,"42",1],
  [1051,"-180",-1],
  [1043,"500",1],
  [1027,"-23",-1]
]}
```

To reconstruct a holder's balance at block `N`:

```sql
SELECT sum(balance_delta)
FROM token_holder_deltas FINAL
WHERE token = '0x…' AND holder = '0x…' AND block_num <= 1050
```

#### token_metadata

> [!NOTE]
> View over `token_transfers FINAL`, aggregated per `token`.

Discovery / activity rollup per token contract. Every token that has ever emitted a `Transfer` shows up here with first/last seen block and timestamp plus a lifetime transfer count. Pair with a verified-tokens allowlist at query time to power `/tokens` listings.

| Column | Type | Description |
|--------|------|-------------|
| `token` | `String` | Token contract |
| `first_seen_block` | `Int64` | Block of the first observed `Transfer` |
| `last_seen_block` | `Int64` | Block of the most recent `Transfer` |
| `first_seen_timestamp` | `DateTime64(3, 'UTC')` | Timestamp of the first `Transfer` |
| `last_seen_timestamp` | `DateTime64(3, 'UTC')` | Timestamp of the most recent `Transfer` |
| `transfer_count` | `UInt64` | Total `Transfer` events ever emitted by this token |

```bash
curl -G "https://indexer.testnet.tempo.xyz/query" \
  --data-urlencode "chainId=42431" \
  --data-urlencode "engine=clickhouse" \
  --data-urlencode "sql=SELECT token, transfer_count, first_seen_block, last_seen_block
    FROM token_metadata
    ORDER BY transfer_count DESC
    LIMIT 5"
```

#### token_supply

> [!NOTE]
> View over `token_transfers FINAL`, computes net mints minus burns from zero-address legs.

Outstanding supply per token, derived from `Transfer` events whose sender or recipient is `0x0`. Mints (`from = 0x0`) add to supply, burns (`to = 0x0`) subtract. Filtered to `supply > 0`.

| Column | Type | Description |
|--------|------|-------------|
| `token` | `String` | Token contract |
| `supply` | `Int256` | Cumulative mints − cumulative burns |

```bash
curl -G "https://indexer.testnet.tempo.xyz/query" \
  --data-urlencode "chainId=42431" \
  --data-urlencode "engine=clickhouse" \
  --data-urlencode "sql=SELECT token, toString(supply) AS supply
    FROM token_supply
    ORDER BY supply DESC
    LIMIT 5"
```

```json
{"ok":true,"columns":["token","supply"],"rows":[
  ["0x20c000000000000000000000e65cb5a40b7885ae","340282366920938463463374607431768211455"],
  ["0x20c0000000000000000000003f9a1b2c4d5e6f70","100000000000000000000000"]
]}
```

#### token_transfer_stats

> [!NOTE]
> View over `token_transfers FINAL`, aggregated per `(day, token)`.

Per-day per-token rollups of transfer activity. Aggregated at query time so reorgs and retries are inherited from the underlying `token_transfers` table.

| Column | Type | Description |
|--------|------|-------------|
| `day` | `Date` | Day (UTC) bucket from `block_timestamp` |
| `token` | `String` | Token contract |
| `transfer_count` | `UInt64` | Number of `Transfer` events on that day |
| `volume` | `UInt256` | Sum of `amount` across all transfers on that day |
| `unique_senders` | `UInt64` | Distinct `from` addresses on that day |
| `unique_recipients` | `UInt64` | Distinct `to` addresses on that day |

```bash
curl -G "https://indexer.testnet.tempo.xyz/query" \
  --data-urlencode "chainId=42431" \
  --data-urlencode "engine=clickhouse" \
  --data-urlencode "sql=SELECT day, transfer_count, toString(volume) AS volume, unique_senders, unique_recipients
    FROM token_transfer_stats
    WHERE token = '0x20c000000000000000000000e65cb5a40b7885ae'
    ORDER BY day DESC
    LIMIT 5"
```

```json
{"ok":true,"columns":["day","transfer_count","volume","unique_senders","unique_recipients"],"rows":[
  ["2026-05-27",18342,"984320012345678901234567",1024,987],
  ["2026-05-26",17211,"872100012345678901234567",1011,973],
  ["2026-05-25",16982,"851234012345678901234567",998,961]
]}
```

#### token_transfers

> [!NOTE]
> Materialized view over `logs`, kept in sync on reorg.

One row per canonical `Transfer(address,address,uint256)` log, decoded at insert time. `ReplacingMergeTree` keyed on `(token, block_num, log_idx, tx_hash)`.

| Column | Type | Description |
|--------|------|-------------|
| `block_num` | `Int64` | Block number |
| `block_timestamp` | `DateTime64(3, 'UTC')` | Block timestamp |
| `tx_idx` | `Int32` | Transaction index |
| `log_idx` | `Int32` | Log index |
| `tx_hash` | `String` | Transaction hash |
| `token` | `String` | Emitting token contract |
| `from` | `String` | Sender address |
| `to` | `String` | Recipient address |
| `amount` | `UInt256` | Transfer amount |
| `is_virtual_forward` | `UInt8` | 1 if this transfer was inserted via a virtual-forward path |

```bash
curl -G "https://indexer.testnet.tempo.xyz/query" \
  --data-urlencode "chainId=42431" \
  --data-urlencode "engine=clickhouse" \
  --data-urlencode "sql=SELECT token, \`from\`, \`to\`, toString(amount) AS amount, tx_hash
    FROM token_transfers
    WHERE token = '0x20c000000000000000000000e65cb5a40b7885ae'
    ORDER BY block_num DESC, log_idx DESC
    LIMIT 3"
```

```json
{"ok":true,"columns":["token","from","to","amount","tx_hash"],"rows":[
  ["0x20c000000000000000000000e65cb5a40b7885ae","0x70997970c51812dc3a010c7d01b50e0d17dc79c8","0xfeec000000000000000000000000000000000000","17431","0x9d…ab"],
  ["0x20c000000000000000000000e65cb5a40b7885ae","0x3c44cdddb6a900fa2b585dd299e03d12fa4293bc","0x70997970c51812dc3a010c7d01b50e0d17dc79c8","42","0x77…3c"],
  ["0x20c000000000000000000000e65cb5a40b7885ae","0x90f79bf6eb2c4f870365e785982e1f101e93b906","0x3c44cdddb6a900fa2b585dd299e03d12fa4293bc","99","0x12…ef"]
]}
```

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
- [Docker](https://docs.docker.com/get-started/get-docker/)
- [PostgreSQL](https://www.postgresql.org/download/)
- [ClickHouse 25.4+](https://clickhouse.com/docs/install) when ClickHouse OLAP is enabled

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

- [golden-axe](https://github.com/indexsupply/golden-axe): Inspiration for everything.
