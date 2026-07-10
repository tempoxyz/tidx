---
tidx: major
---

**Breaking:** Moved PostgreSQL chain settings into a nested `[chains.postgres]` section (`url`, `password_env`, `api_url`, `api_password_env`), removing the root-level `pg_url`, `pg_password_env`, `api_pg_url`, and `api_pg_password_env` fields.

```diff
 [[chains]]
 name = "mainnet"
 chain_id = 4217
 rpc_url = "https://rpc.tempo.xyz"
-pg_url = "postgres://user@host:5432/tidx_mainnet"
-pg_password_env = "TIDX_PG_PASSWORD"
-api_pg_url = "postgres://user@host:5432/tidx_mainnet_r"
-api_pg_password_env = "TIDX_API_PG_PASSWORD"
+
+[chains.postgres]
+url = "postgres://user@host:5432/tidx_mainnet"
+password_env = "TIDX_PG_PASSWORD"
+api_url = "postgres://user@host:5432/tidx_mainnet_r"
+api_password_env = "TIDX_API_PG_PASSWORD"
```
