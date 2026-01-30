-- Initialize pg_duckdb extension
-- This is run automatically when the PostgreSQL container starts

-- Create the extension
CREATE EXTENSION IF NOT EXISTS pg_duckdb;

-- Configure pg_duckdb defaults for analytical workloads
-- These can be overridden per-session via SET commands

-- Memory limit for DuckDB operations (default 16GB for 64GB server)
-- ALTER SYSTEM SET duckdb.max_memory = '16GB';

-- Number of threads for DuckDB operations (default uses all cores)
-- ALTER SYSTEM SET duckdb.threads = 8;

-- Temporary directory for large operations that spill to disk
-- ALTER SYSTEM SET duckdb.temporary_directory = '/tmp/duckdb';

-- Note: pg_duckdb settings are applied per-session
-- The indexer sets these via SET commands in execute_query_pg_duckdb()
