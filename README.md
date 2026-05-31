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
- [Tables](#tables)
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
rpc_url = "https://rpc.testnet.tempo.xyz"
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
  -u, --url <URL>              TIDX HTTP API URL (e.g., http://localhost:8080)
  -n, --chain-id <CHAIN_ID>   Chain ID to query (uses first chain if not specified)
  -e, --engine <ENGINE>        Force query engine (postgres, clickhouse)
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
# Point lookup (auto-routed to PostgreSQL)
curl "https://indexer.tempo.xyz/query?chainId=4217&sql=SELECT * FROM blocks WHERE num = 12345"
> {"columns":["num","hash","timestamp"],"rows":[[12345,"0xabc...","2024-01-01T00:00:00Z"]],"row_count":1,"engine":"postgres","ok":true}

# Aggregation (auto-routed to ClickHouse)
curl "https://indexer.tempo.xyz/query?chainId=4217&sql=SELECT type, COUNT(*) FROM txs GROUP BY type"
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

- **[Base Tables](#base-tables)** — raw chain data written by the sync engine, available in both PostgreSQL and ClickHouse.
- **[Event Tables](#event-tables)** — virtual, decoded-at-query-time tables generated from `?signature=Event(...)`. Available in both engines.
- **[Materialized Tables](#materialized-tables)** — precomputed, address- and token-keyed views maintained by ClickHouse on insert. Available only with `engine=clickhouse`. Includes built-ins (token transfers/balances/supply/approvals/metadata, address transfers/balances/txs, contract creations) plus any user-defined views registered through the [`/views` API](#views-api).

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

#### sync_state

| Column | Type | Description |
|--------|------|-------------|
| `chain_id` | `INT8` | Chain identifier |
| `head_num` | `INT8` | Remote chain head from RPC |
| `synced_num` | `INT8` | Highest contiguous block (no gaps from backfill_num to here) |
| `tip_num` | `INT8` | Highest block near chain head (realtime follows this) |
| `backfill_num` | `INT8` | Lowest synced block going backwards (NULL=not started, 0=complete) |
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

ClickHouse maintains these on insert and prunes them on reorg. Token-keyed tables answer "for this token, …"; address-keyed tables answer "for this account, …". Both families read from the same underlying Transfer/tx/receipt streams — the duplication exists so that either filter resolves via a sort-key seek instead of a full scan.

| Name | Purpose |
|------|---------|
| [`address_balances`](#address_balances) | Current positive balance per `(holder, token)`. |
| [`address_holder_deltas`](#address_holder_deltas) | Holder-first mirror of `token_holder_deltas`. |
| [`address_transfers`](#address_transfers) | Transfer feed keyed by account; `'in'`/`'out'`. |
| [`address_txs`](#address_txs) | Tx feed keyed by account; `'from'`/`'to'`. |
| [`contract_creations`](#contract_creations) | One row per contract deployment. |
| [`token_approvals`](#token_approvals) | Decoded `Approval` events. |
| [`token_approvals_current`](#token_approvals_current) | Latest allowance per `(token, owner, spender)`. |
| [`token_balances`](#token_balances) | Current positive balance per `(token, holder)`. |
| [`token_balances_snapshot`](#token_balances_snapshot) | Pre-aggregated `token_balances`, refreshed on a schedule. |
| [`token_holder_deltas`](#token_holder_deltas) | Per-event ± balance change, two rows per transfer. |
| [`token_metadata`](#token_metadata) | Per-token first/last seen + lifetime transfer count. |
| [`token_supply`](#token_supply) | Per-token mints − burns (zero-address legs). |
| [`token_transfer_stats`](#token_transfer_stats) | Per-`(day, token)` count, volume, unique senders/recipients. |
| [`token_transfers`](#token_transfers) | Decoded `Transfer` events. |

User-defined views registered through the [`/views` API](#views-api) live alongside these in `analytics_{chainId}` and are queryable the same way.

#### address_balances

> [!NOTE]
> View over `address_holder_deltas FINAL`, grouped by `(holder, token)`.

Current positive balances grouped by holder first — answers "what does this address hold?" in one sort-key range. `token_balances` answers the inverse "who holds this token?" — same underlying transfer events, two different sort orders.

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

#### address_transfers

> [!NOTE]
> Materialized view over `token_transfers`, kept in sync on reorg.

Address-keyed Transfer feed. Each `Transfer` produces up to two rows (one per non-zero side): an `'in'` row for the recipient and an `'out'` row for the sender. `ReplacingMergeTree` ordered by `(address, block_num, log_idx, tx_hash, direction)` so per-address pagination is a sort-key seek instead of a full scan. Zero-address legs are dropped — mints show up only on the recipient's `'in'` side, burns only on the sender's `'out'` side.

| Column | Type | Description |
|--------|------|-------------|
| `block_num` | `Int64` | Block number |
| `block_timestamp` | `DateTime64(3, 'UTC')` | Block timestamp |
| `tx_idx` | `Int32` | Transaction index |
| `log_idx` | `Int32` | Log index |
| `tx_hash` | `String` | Transaction hash |
| `address` | `String` | The account this row is scoped to |
| `direction` | `LowCardinality(String)` | `'in'` or `'out'` from `address`'s perspective |
| `counterparty` | `String` | The other side of the transfer (may be `0x0`) |
| `token` | `String` | Emitting token contract |
| `amount` | `UInt256` | Transfer amount |

```bash
curl -G "https://indexer.testnet.tempo.xyz/query" \
  --data-urlencode "chainId=42431" \
  --data-urlencode "engine=clickhouse" \
  --data-urlencode "sql=SELECT block_num, direction, counterparty, token, toString(amount) AS amount
    FROM address_transfers
    WHERE address = '0x70997970c51812dc3a010c7d01b50e0d17dc79c8'
    ORDER BY block_num DESC, log_idx DESC
    LIMIT 5"
```

#### address_txs

> [!NOTE]
> Materialized view over `txs`, kept in sync on reorg.

Address-keyed transaction feed. Each tx produces one row per non-null side (sender always emits an `'from'` row; recipient emits a `'to'` row when `to` is non-null). `ReplacingMergeTree` ordered by `(address, block_num, tx_idx, direction)` so `/addresses/:address/transactions` and the `direction=from|to|both` filter resolve via a sort-key seek.

| Column | Type | Description |
|--------|------|-------------|
| `block_num` | `Int64` | Block number |
| `block_timestamp` | `DateTime64(3, 'UTC')` | Block timestamp |
| `tx_idx` | `Int32` | Transaction index |
| `tx_hash` | `String` | Transaction hash |
| `address` | `String` | The account this row is scoped to |
| `direction` | `LowCardinality(String)` | `'from'` (this address sent) or `'to'` (this address was the recipient) |
| `counterparty` | `Nullable(String)` | The other side, NULL when the source tx had no `to` (contract deploy) |

```bash
curl -G "https://indexer.testnet.tempo.xyz/query" \
  --data-urlencode "chainId=42431" \
  --data-urlencode "engine=clickhouse" \
  --data-urlencode "sql=SELECT block_num, direction, tx_hash, counterparty
    FROM address_txs
    WHERE address = '0x70997970c51812dc3a010c7d01b50e0d17dc79c8'
      AND direction = 'from'
    ORDER BY block_num DESC, tx_idx DESC
    LIMIT 5"
```

#### contract_creations

> [!NOTE]
> Materialized view over `receipts`, kept in sync on reorg.

One row per contract deployment, derived from receipts where `contract_address` is set. Ordered by `(creator, block_num, tx_idx)` so "what contracts did this address deploy?" is a sort-key seek.

| Column | Type | Description |
|--------|------|-------------|
| `block_num` | `Int64` | Block number |
| `block_timestamp` | `DateTime64(3, 'UTC')` | Block timestamp |
| `tx_idx` | `Int32` | Transaction index |
| `tx_hash` | `String` | Transaction hash |
| `creator` | `String` | Deployer address (`receipts.from`) |
| `contract` | `String` | Deployed contract address |

```bash
curl -G "https://indexer.testnet.tempo.xyz/query" \
  --data-urlencode "chainId=42431" \
  --data-urlencode "engine=clickhouse" \
  --data-urlencode "sql=SELECT contract, block_num, tx_hash
    FROM contract_creations
    WHERE creator = '0x70997970c51812dc3a010c7d01b50e0d17dc79c8'
    ORDER BY block_num DESC, tx_idx DESC
    LIMIT 5"
```
#### token_approvals

> [!NOTE]
> Materialized view over `logs`, kept in sync on reorg.

One row per canonical `Approval(address,address,uint256)` log, decoded at insert time. `ReplacingMergeTree` keyed on `(token, block_num, log_idx, tx_hash)`. Stores raw approval events — to get the latest allowance per `(token, owner, spender)`, query with `ORDER BY block_num DESC, log_idx DESC LIMIT 1`.

| Column | Type | Description |
|--------|------|-------------|
| `block_num` | `Int64` | Block number |
| `block_timestamp` | `DateTime64(3, 'UTC')` | Block timestamp |
| `tx_idx` | `Int32` | Transaction index |
| `log_idx` | `Int32` | Log index |
| `tx_hash` | `String` | Transaction hash |
| `token` | `String` | Emitting token contract |
| `owner` | `String` | Approving address (token holder) |
| `spender` | `String` | Approved spender |
| `amount` | `UInt256` | Allowance set on this event |

```bash
curl -G "https://indexer.testnet.tempo.xyz/query" \
  --data-urlencode "chainId=42431" \
  --data-urlencode "engine=clickhouse" \
  --data-urlencode "sql=SELECT owner, spender, toString(amount) AS amount, block_num
    FROM token_approvals
    WHERE token = '0x20c000000000000000000000e65cb5a40b7885ae'
    ORDER BY block_num DESC, log_idx DESC
    LIMIT 3"
```

```json
{"ok":true,"columns":["owner","spender","amount","block_num"],"rows":[
  ["0x70997970c51812dc3a010c7d01b50e0d17dc79c8","0x3c44cdddb6a900fa2b585dd299e03d12fa4293bc","115792089237316195423570985008687907853269984665640564039457584007913129639935",1083],
  ["0x70997970c51812dc3a010c7d01b50e0d17dc79c8","0x3c44cdddb6a900fa2b585dd299e03d12fa4293bc","0",1071],
  ["0x3c44cdddb6a900fa2b585dd299e03d12fa4293bc","0x15d34aaf54267db7d7c367839aaf71a00a2c6a65","500",1042]
]}
```

#### token_approvals_current

> [!NOTE]
> View over `token_approvals FINAL`, `argMax` per `(token, owner, spender)`.

Current allowance per `(token, owner, spender)` — collapses `token_approvals` history down to the last set value, filtered to `amount > 0`. Cheap lookup for "what can `spender` move on behalf of `owner`?"

| Column | Type | Description |
|--------|------|-------------|
| `token` | `String` | Token contract |
| `owner` | `String` | Approving address |
| `spender` | `String` | Approved spender |
| `amount` | `UInt256` | Latest allowance for the pair |
| `last_block_num` | `Int64` | Block of the latest `Approval` event |
| `last_block_timestamp` | `DateTime64(3, 'UTC')` | Timestamp of that event |
| `last_tx_hash` | `String` | Transaction hash of that event |

```bash
curl -G "https://indexer.testnet.tempo.xyz/query" \
  --data-urlencode "chainId=42431" \
  --data-urlencode "engine=clickhouse" \
  --data-urlencode "sql=SELECT spender, toString(amount) AS amount, last_block_num
    FROM token_approvals_current
    WHERE token = '0x20c000000000000000000000e65cb5a40b7885ae'
      AND owner = '0x70997970c51812dc3a010c7d01b50e0d17dc79c8'
    ORDER BY amount DESC"
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
