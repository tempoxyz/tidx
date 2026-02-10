-- Read-only API role for query connections.
-- Defense-in-depth: even if the query validator is bypassed,
-- this role cannot modify data, execute arbitrary functions,
-- or exhaust server resources.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tidx_api') THEN
        CREATE ROLE tidx_api WITH LOGIN PASSWORD 'tidx_api' NOSUPERUSER NOCREATEDB NOCREATEROLE;
    END IF;
END $$;

-- Revoke all privileges first (deny-by-default)
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM tidx_api;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM tidx_api;
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM tidx_api;

-- Grant read-only access to indexed tables only
GRANT USAGE ON SCHEMA public TO tidx_api;
GRANT SELECT ON blocks, txs, logs, receipts, token_holders, token_balances TO tidx_api;

-- Grant execute only on ABI decode helper functions
GRANT EXECUTE ON FUNCTION abi_uint(bytea) TO tidx_api;
GRANT EXECUTE ON FUNCTION abi_int(bytea) TO tidx_api;
GRANT EXECUTE ON FUNCTION abi_address(bytea) TO tidx_api;
GRANT EXECUTE ON FUNCTION abi_bool(bytea) TO tidx_api;
GRANT EXECUTE ON FUNCTION abi_bytes(bytea, int) TO tidx_api;
GRANT EXECUTE ON FUNCTION abi_string(bytea, int) TO tidx_api;
GRANT EXECUTE ON FUNCTION format_address(bytea) TO tidx_api;
GRANT EXECUTE ON FUNCTION format_uint(bytea) TO tidx_api;

-- Resource limits (prevent DoS)
ALTER ROLE tidx_api CONNECTION LIMIT 64;
ALTER ROLE tidx_api SET statement_timeout = '30s';
ALTER ROLE tidx_api SET work_mem = '64MB';
ALTER ROLE tidx_api SET temp_file_limit = '256MB';
