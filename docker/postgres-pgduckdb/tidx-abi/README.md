# tidx_abi - DuckDB Extension for Ethereum ABI Decoding

A DuckDB extension that provides vectorized scalar functions for decoding Ethereum event log data from Parquet files.

## Functions

| Function | Input | Output | Description |
|----------|-------|--------|-------------|
| `abi_address(blob)` | BLOB (32 bytes) | VARCHAR | Decode topic/data word to 0x-prefixed address |
| `abi_address(blob, offset)` | BLOB, BIGINT | VARCHAR | Decode address at byte offset |
| `abi_uint(blob)` | BLOB (32 bytes) | HUGEINT | Decode to uint128 (truncates uint256) |
| `abi_uint(blob, offset)` | BLOB, BIGINT | HUGEINT | Decode uint at byte offset |
| `abi_uint256(blob)` | BLOB (32 bytes) | VARCHAR | Decode to uint256 as hex string |
| `abi_uint256(blob, offset)` | BLOB, BIGINT | VARCHAR | Decode uint256 at offset as hex |
| `abi_bool(blob)` | BLOB (32 bytes) | BOOLEAN | Decode to boolean |
| `abi_bool(blob, offset)` | BLOB, BIGINT | BOOLEAN | Decode bool at byte offset |
| `abi_bytes32(blob)` | BLOB (32 bytes) | VARCHAR | Return 32 bytes as 0x-prefixed hex |
| `abi_bytes32(blob, offset)` | BLOB, BIGINT | VARCHAR | Extract 32 bytes at offset as hex |

## Usage

```sql
-- Load the extension
LOAD 'tidx_abi.duckdb_extension';

-- Decode Transfer event logs from a Parquet file
SELECT 
    abi_address(topic1) AS "from",
    abi_address(topic2) AS "to",
    abi_uint(data) AS value
FROM read_parquet('/data/logs_*.parquet')
WHERE selector = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';

-- Decode Swap event with multiple data fields
SELECT
    abi_address(topic1) AS sender,
    abi_uint(data, 0) AS amount0In,
    abi_uint(data, 32) AS amount1In,
    abi_uint(data, 64) AS amount0Out,
    abi_uint(data, 96) AS amount1Out
FROM read_parquet('/data/logs_*.parquet')
WHERE selector = '\xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822';
```

## Building

```bash
# Clone with submodules
git clone --recurse-submodules https://github.com/tempoxyz/tidx-duckdb-ext.git
cd tidx-duckdb-ext

# Configure and build
make configure
make release

# The extension will be in build/release/tidx_abi.duckdb_extension
```

## Testing

```bash
make test
```

## Architecture

This extension is designed to work with [tidx](https://github.com/tempoxyz/tidx), a high-throughput blockchain indexer. 

The architecture:
1. **PostgreSQL heap tables**: Store recent blocks/logs for fast writes
2. **Parquet files**: Export old data for efficient OLAP queries
3. **pg_duckdb + tidx_abi**: Query Parquet files with native ABI decoding

This enables sub-second analytical queries over billions of event logs.

## License

MIT
