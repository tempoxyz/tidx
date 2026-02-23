-- Create the tidx_api role if it doesn't already exist.
-- This may fail if the database user lacks CREATEROLE (e.g., on CNPG where
-- the role is managed declaratively). In that case, the error is caught in
-- Rust and the grants are still applied.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tidx_api') THEN
        CREATE ROLE tidx_api WITH LOGIN PASSWORD 'tidx_api' NOSUPERUSER NOCREATEDB NOCREATEROLE;
    END IF;
END $$;
