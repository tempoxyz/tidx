---
name: querying-tempo
description: "Query indexed Tempo chain data via tidx HTTP API and CLI. Covers SQL queries for blocks, txs, logs, receipts, event CTE decoding, engine routing (PostgreSQL OLTP vs ClickHouse OLAP), live streaming. Use when querying Tempo chain data or working with tidx."
---

# Querying tidx

tidx exposes indexed Tempo chain data through SQL. Queries run against four core tables: `blocks`, `txs`, `logs`, and `receipts`. Two query engines are available: **PostgreSQL** (OLTP) for point lookups and real-time queries, and **ClickHouse** (OLAP) for heavy analytics.

## Query Interfaces

### HTTP API

```
GET /query?sql&chainId&engine&signature&timeout_ms&limit&live
```

| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| `sql` | yes | — | SQL query (SELECT only) |
| `chainId` | yes | — | Chain ID (e.g., `4217` for Presto mainnet) |
| `engine` | no | auto | Force engine: `postgres` or `clickhouse` |
| `signature` | no | — | Event signature for CTE decoding (repeatable) |
| `timeout_ms` | no | `5000` | Query timeout in ms |
| `limit` | no | `10000` | Max rows (hard cap: 10,000) |
| `live` | no | `false` | Enable SSE live streaming (PostgreSQL only) |

**Response format:**
```json
{
  "ok": true,
  "columns": ["num", "hash", "gas_used"],
  "rows": [[1, "0xabc...", 21000]],
  "row_count": 1,
  "engine": "postgres",
  "query_time_ms": 12.5
}
```

### CLI

```bash
tidx query --url <url> --chain-id <chain_id> [OPTIONS] <sql>
```

| Flag | Short | Required | Default | Description |
|------|-------|----------|---------|-------------|
| `--url` | `-u` | yes | — | tidx HTTP API URL (e.g., `http://localhost:8080`) |
| `--chain-id` | `-n` | yes | — | Chain ID |
| `--engine` | `-e` | no | auto | Force engine: `postgres` or `clickhouse` |
| `--format` | `-f` | no | `table` | Output: `table`, `json`, `csv`, `toon` |
| `--limit` | `-l` | no | `10000` | Max rows |
| `--signature` | `-s` | no | — | Event signature (repeatable) |
| `--timeout` | `-t` | no | `30000` | Timeout in ms |

```bash
tidx query --url http://localhost:8080 --chain-id 4217 \
  "SELECT num FROM blocks ORDER BY num DESC LIMIT 5"

# With basic auth (credentials extracted from URL)
tidx query --url http://user:pass@localhost:8080 --chain-id 4217 \
  "SELECT num FROM blocks ORDER BY num DESC LIMIT 5"
```

## Tables & Schemas

### blocks
| Column | Type | Description |
|--------|------|-------------|
| `num` | INT8 | Block number |
| `hash` | BYTEA | Block hash |
| `parent_hash` | BYTEA | Parent block hash |
| `timestamp` | TIMESTAMPTZ | Block timestamp |
| `timestamp_ms` | INT8 | Timestamp in milliseconds |
| `gas_limit` | INT8 | Gas limit |
| `gas_used` | INT8 | Gas used |
| `miner` | BYTEA | Block producer |
| `extra_data` | BYTEA | Extra data (nullable) |

### txs
| Column | Type | Description |
|--------|------|-------------|
| `block_num` | INT8 | Block number |
| `block_timestamp` | TIMESTAMPTZ | Block timestamp |
| `idx` | INT4 | Transaction index |
| `hash` | BYTEA | Transaction hash |
| `type` | INT2 | Transaction type |
| `from` | BYTEA | Sender address |
| `to` | BYTEA | Recipient (nullable for contract creation) |
| `value` | TEXT | Value in wei (text for uint256) |
| `input` | BYTEA | Calldata |
| `gas_limit` | INT8 | Gas limit |
| `max_fee_per_gas` | TEXT | Max fee per gas |
| `max_priority_fee_per_gas` | TEXT | Max priority fee |
| `gas_used` | INT8 | Gas used (nullable) |
| `nonce_key` | BYTEA | Nonce key |
| `nonce` | INT8 | Nonce |
| `fee_token` | BYTEA | Fee token address (nullable, Tempo-specific) |
| `fee_payer` | BYTEA | Fee payer address (nullable, Tempo-specific) |
| `calls` | JSONB | Internal calls (nullable) |
| `call_count` | INT2 | Number of calls |

### logs
| Column | Type | Description |
|--------|------|-------------|
| `block_num` | INT8 | Block number |
| `block_timestamp` | TIMESTAMPTZ | Block timestamp |
| `log_idx` | INT4 | Log index |
| `tx_idx` | INT4 | Transaction index |
| `tx_hash` | BYTEA | Transaction hash |
| `address` | BYTEA | Contract address |
| `selector` | BYTEA | First 4 bytes of topic0 (event selector) |
| `topic0`–`topic3` | BYTEA | Event topics (nullable) |
| `data` | BYTEA | ABI-encoded event data |

### receipts
| Column | Type | Description |
|--------|------|-------------|
| `block_num` | INT8 | Block number |
| `block_timestamp` | TIMESTAMPTZ | Block timestamp |
| `tx_idx` | INT4 | Transaction index |
| `tx_hash` | BYTEA | Transaction hash |
| `from` | BYTEA | Sender |
| `to` | BYTEA | Recipient (nullable) |
| `contract_address` | BYTEA | Deployed contract address (nullable) |
| `gas_used` | INT8 | Gas used |
| `cumulative_gas_used` | INT8 | Cumulative gas |
| `effective_gas_price` | TEXT | Effective gas price (nullable) |
| `status` | INT2 | 1 = success, 0 = revert (nullable) |
| `fee_payer` | BYTEA | Fee payer (nullable, Tempo-specific) |

## Query Examples

### Blocks

```bash
# Latest 5 blocks
curl "http://localhost:8080/query?chainId=4217&sql=SELECT num, gas_used FROM blocks ORDER BY num DESC LIMIT 5"

# Block by number
tidx query "SELECT * FROM blocks WHERE num = 1000000"

# Blocks in a time range
tidx query "SELECT num, gas_used FROM blocks WHERE timestamp > now() - interval '1 hour' ORDER BY num DESC LIMIT 100"
```

### Transactions

```bash
# Lookup by hash
tidx query "SELECT * FROM txs WHERE hash = '0x1234...'"

# Transactions from an address (uses `from` index)
curl "http://localhost:8080/query?chainId=4217&sql=SELECT hash, value, gas_used FROM txs WHERE \"from\" = '0xdAC17F958D2ee523a2206206994597C13D831ec7' ORDER BY block_num DESC LIMIT 10"

# Contract creations
tidx query "SELECT hash, \"from\" FROM txs WHERE \"to\" IS NULL ORDER BY block_num DESC LIMIT 10"
```

### Receipts

```bash
# Failed transactions
tidx query "SELECT tx_hash, \"from\", gas_used FROM receipts WHERE status = 0 ORDER BY block_num DESC LIMIT 10"

# Contract deployments
tidx query "SELECT tx_hash, contract_address FROM receipts WHERE contract_address IS NOT NULL ORDER BY block_num DESC LIMIT 10"

# Transactions paid by a fee payer (Tempo-specific)
tidx query "SELECT tx_hash, \"from\", fee_payer FROM receipts WHERE fee_payer IS NOT NULL ORDER BY block_num DESC LIMIT 10"
```

### Raw Logs

```bash
# Logs by contract address
tidx query "SELECT block_num, tx_hash, data FROM logs WHERE address = '0xABC...' ORDER BY block_num DESC LIMIT 10"

# Logs by event selector
tidx query "SELECT * FROM logs WHERE selector = '0xddf252ad' ORDER BY block_num DESC LIMIT 10"
```

## Event CTE Queries

Event CTEs let you decode raw log data into typed columns using an ABI event signature. Pass the signature via `--signature` / `&signature=` and query the event name as a virtual table.

**Signature format:** `EventName(type [indexed] name, ...)`

### Basic Transfer query

```bash
# CLI
tidx query \
  -s "Transfer(address indexed from, address indexed to, uint256 value)" \
  'SELECT "from", "to", "value" FROM Transfer ORDER BY block_num DESC LIMIT 10'

# HTTP
curl "http://localhost:8080/query?chainId=4217&signature=Transfer(address%20indexed%20from,%20address%20indexed%20to,%20uint256%20value)&sql=SELECT%20%22from%22,%20%22to%22,%20%22value%22%20FROM%20Transfer%20ORDER%20BY%20block_num%20DESC%20LIMIT%2010"
```

### Filter by address (predicate pushdown)

Filters on **indexed** params are automatically pushed down to topic-level WHERE clauses for index utilization:

```bash
tidx query \
  -s "Transfer(address indexed from, address indexed to, uint256 value)" \
  'SELECT "to", "value" FROM Transfer WHERE "from" = '\''0xdAC17F958D2ee523a2206206994597C13D831ec7'\'' ORDER BY block_num DESC LIMIT 10'
```

The `"from" = '0x...'` filter is rewritten to `topic1 = '\x000...dac17f...'` at the raw logs level.

### Filter by contract address

```bash
tidx query \
  -s "Transfer(address indexed from, address indexed to, uint256 value)" \
  'SELECT "from", "to", "value" FROM Transfer WHERE address = '\''0xABC...'\'' ORDER BY block_num DESC LIMIT 10'
```

`address` and `block_num` are **raw columns** that are always pushed down into the CTE.

### Multiple signatures

```bash
tidx query \
  -s "Transfer(address indexed from, address indexed to, uint256 value)" \
  -s "Approval(address indexed owner, address indexed spender, uint256 value)" \
  'SELECT * FROM Transfer LIMIT 5'
```

### Available decoded columns

Each CTE always includes these raw columns: `block_num`, `block_timestamp`, `log_idx`, `tx_idx`, `tx_hash`, `address`, `selector`, `topic1`, `topic2`, `topic3`, `data`.

Plus decoded columns from the signature params (e.g., `"from"`, `"to"`, `"value"` for Transfer).

### Supported ABI types

`address`, `uint8`–`uint256`, `int8`–`int256`, `bool`, `bytes`, `bytes1`–`bytes32`, `string`

### PostgreSQL helper functions

Available for manual decoding in raw queries (not needed with CTEs):
- `abi_uint(bytea) → NUMERIC` — Decode unsigned integer
- `abi_int(bytea) → NUMERIC` — Decode signed integer
- `abi_address(bytea) → BYTEA` — Extract 20-byte address from 32-byte slot
- `abi_bool(bytea) → BOOLEAN` — Decode boolean
- `abi_bytes(bytea, offset) → BYTEA` — Decode dynamic bytes
- `abi_string(bytea, offset) → TEXT` — Decode dynamic string
- `format_address(bytea) → TEXT` — Format as `0x...` hex string

## Hex Literals

In PostgreSQL queries, `'0x...'` hex literals (40+ chars) are automatically converted to `'\x...'` bytea format. You can write:
```sql
WHERE "from" = '0xdAC17F958D2ee523a2206206994597C13D831ec7'
```
No need to manually use `'\x...'` syntax.

## Engine Routing: PostgreSQL vs ClickHouse

### PostgreSQL (OLTP) — default

Best for:
- **Point lookups** — tx by hash, block by number, address history
- **Real-time queries** — latest blocks, recent activity, live streaming
- **Low-latency** — sub-second responses for indexed lookups
- **Small result sets** — WHERE on indexed columns with LIMIT

```bash
# Point lookup (fast, uses hash index)
curl "http://localhost:8080/query?chainId=4217&sql=SELECT * FROM txs WHERE hash = '0x1234...'&engine=postgres"

# Recent activity (fast, uses block_num DESC index)
curl "http://localhost:8080/query?chainId=4217&sql=SELECT * FROM blocks ORDER BY num DESC LIMIT 10&engine=postgres"
```

### ClickHouse (OLAP) — `engine=clickhouse`

Best for:
- **Aggregations** — COUNT, SUM, AVG over millions of rows
- **Full table scans** — analytics without narrow WHERE clauses
- **Time-series** — GROUP BY hour/day/week over large ranges
- **Heavy JOINs** — cross-table analytics

```bash
# Daily gas usage (heavy aggregation → ClickHouse)
curl "http://localhost:8080/query?chainId=4217&engine=clickhouse&sql=SELECT toDate(block_timestamp) as day, SUM(gas_used) as total_gas, COUNT(*) as tx_count FROM txs GROUP BY day ORDER BY day DESC LIMIT 30"

# Transfer volume by token (full scan with decode → ClickHouse)
curl "http://localhost:8080/query?chainId=4217&engine=clickhouse&signature=Transfer(address%20indexed%20from,%20address%20indexed%20to,%20uint256%20value)&sql=SELECT address as token, COUNT(*) as transfers, SUM(\"value\") as volume FROM Transfer GROUP BY token ORDER BY transfers DESC LIMIT 20"
```

### When to use which

| Use case | Engine | Why |
|----------|--------|-----|
| Tx by hash | PostgreSQL | B-tree index, O(1) |
| Address tx history | PostgreSQL | Indexed, small result |
| Latest N blocks | PostgreSQL | Index scan on `num DESC` |
| Live streaming (SSE) | PostgreSQL | Only engine that supports `live=true` |
| Daily/hourly aggregations | ClickHouse | Columnar scan, fast GROUP BY |
| Token holder snapshots | ClickHouse | Full scan + SUM |
| Top contracts by gas | ClickHouse | Full scan + aggregation |
| Time-series charts | ClickHouse | Columnar, time functions |

**Rule of thumb:** If your query has a tight WHERE on an indexed column → PostgreSQL. If it scans many rows with GROUP BY → ClickHouse.

## Live Streaming (SSE)

Add `live=true` to get real-time updates via Server-Sent Events. PostgreSQL only.

```bash
curl -N "http://localhost:8080/query?chainId=4217&live=true&sql=SELECT num, gas_used FROM blocks ORDER BY num DESC LIMIT 1"
```

Events:
- `result` — Query results for each new block
- `lagged` — Client fell behind, some blocks skipped
- `error` — Query error
