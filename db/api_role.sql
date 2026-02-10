-- Create a read-only role for API query connections.
-- The API should connect as this role to provide defense-in-depth
-- against SQL injection, even if the query validator is bypassed.
--
-- Modeled after golden-axe's uapi role:
-- https://github.com/indexsupply/golden-axe/blob/master/be/src/sql/roles.sql
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tidx_api') THEN
        CREATE ROLE tidx_api WITH LOGIN PASSWORD 'tidx_api' NOSUPERUSER NOCREATEDB NOCREATEROLE;
    END IF;
END $$;

-- Revoke all default privileges first (defense-in-depth)
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM tidx_api;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM tidx_api;

-- Grant read-only access to indexed tables only
GRANT USAGE ON SCHEMA public TO tidx_api;
GRANT SELECT ON blocks, txs, logs, receipts TO tidx_api;

-- Grant execute only on ABI decode helper functions
GRANT EXECUTE ON FUNCTION abi_uint(bytea) TO tidx_api;
GRANT EXECUTE ON FUNCTION abi_int(bytea) TO tidx_api;
GRANT EXECUTE ON FUNCTION abi_address(bytea) TO tidx_api;
GRANT EXECUTE ON FUNCTION abi_bool(bytea) TO tidx_api;
GRANT EXECUTE ON FUNCTION abi_bytes(bytea, int) TO tidx_api;
GRANT EXECUTE ON FUNCTION abi_string(bytea, int) TO tidx_api;
GRANT EXECUTE ON FUNCTION format_address(bytea) TO tidx_api;
GRANT EXECUTE ON FUNCTION format_uint(bytea) TO tidx_api;

-- Resource limits to prevent DoS even if validator is bypassed
ALTER ROLE tidx_api SET statement_timeout = '30s';
ALTER ROLE tidx_api SET work_mem = '256MB';
ALTER ROLE tidx_api SET temp_file_limit = '512MB';
ALTER ROLE tidx_api CONNECTION LIMIT 64;
