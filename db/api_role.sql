-- Create a read-only role for API query connections.
-- The API should connect as this role to provide defense-in-depth
-- against SQL injection, even if the query validator is bypassed.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tidx_api') THEN
        CREATE ROLE tidx_api WITH LOGIN PASSWORD 'tidx_api' NOSUPERUSER NOCREATEDB NOCREATEROLE;
    END IF;
END $$;

-- Grant read-only access to indexed tables only
GRANT USAGE ON SCHEMA public TO tidx_api;
GRANT SELECT ON blocks, txs, logs, receipts TO tidx_api;

-- Allow calling ABI decode helper functions
GRANT EXECUTE ON FUNCTION abi_uint(bytea) TO tidx_api;
GRANT EXECUTE ON FUNCTION abi_int(bytea) TO tidx_api;
GRANT EXECUTE ON FUNCTION abi_address(bytea) TO tidx_api;
GRANT EXECUTE ON FUNCTION abi_bool(bytea) TO tidx_api;
GRANT EXECUTE ON FUNCTION abi_bytes(bytea, int) TO tidx_api;
GRANT EXECUTE ON FUNCTION abi_string(bytea, int) TO tidx_api;
GRANT EXECUTE ON FUNCTION format_address(bytea) TO tidx_api;
GRANT EXECUTE ON FUNCTION format_uint(bytea) TO tidx_api;

-- Revoke everything else (defense-in-depth)
REVOKE ALL ON sync_state FROM tidx_api;
