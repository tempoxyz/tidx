---
tidx: minor
---

Moved PostgreSQL chain settings into a nested `[chains.postgres]` section (`url`, `password_env`, `api_url`, `api_password_env`), matching `[chains.clickhouse]`; the old root fields (`pg_url`, `pg_password_env`, `api_pg_url`, `api_pg_password_env`) still work but are deprecated.
