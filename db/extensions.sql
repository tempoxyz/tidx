-- pg_parquet: Server-side Parquet generation for fast DuckDB replication
-- This extension enables COPY TO STDOUT (FORMAT 'parquet') which is much faster
-- than streaming rows and building Parquet client-side.
--
-- Installation: https://github.com/CrunchyData/pg_parquet
-- The extension is optional - tidx falls back to Arrow-based export if not available.

CREATE EXTENSION IF NOT EXISTS pg_parquet;
