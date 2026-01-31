-- Initialize pg_duckdb extension with tidx_abi for Ethereum ABI decoding
-- This is run automatically when the PostgreSQL container starts

-- Create the pg_duckdb extension
CREATE EXTENSION IF NOT EXISTS pg_duckdb;

-- Load tidx_abi extension into DuckDB for ABI decoding functions
-- This enables: abi_address(), abi_uint(), abi_uint256(), abi_bool(), abi_bytes32()
SELECT duckdb.raw_query($$ LOAD '/usr/share/duckdb/extensions/tidx_abi.duckdb_extension' $$);

-- Configure pg_duckdb defaults for analytical workloads
-- These can be overridden per-session via SET commands

-- Memory limit for DuckDB operations (default 16GB for 64GB server)
-- ALTER SYSTEM SET duckdb.max_memory = '16GB';

-- Number of threads for DuckDB operations (default uses all cores)
-- ALTER SYSTEM SET duckdb.threads = 8;

-- Note: pg_duckdb settings are applied per-session
-- The indexer sets these via SET commands in execute_query_pg_duckdb()
