---
tidx: patch
---

Tiered storage bootstrap now creates a `tidx_clickhouse` user mapping for the PostgreSQL role in `postgres.api_url`, alongside the indexer role's mapping. API queries that fall back to the pg_clickhouse FDW (`ch.*` / `tiered.*`) no longer fail with `user mapping not found` when a separate API role is configured.
