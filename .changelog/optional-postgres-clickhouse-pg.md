---
tidx: minor
---

Made both storage engines optional per chain (at least one required), moved the flat `pg_url`/`pg_password_env`/`api_pg_url`/`api_pg_password_env` options into a `[chains.postgres]` table, and added pg_clickhouse support via `clickhouse.pg_url` serving `engine=clickhouse_pg` queries, which `engine=postgres` aliases to on ClickHouse-only chains.

```diff
 [[chains]]
 name = "mainnet"
 chain_id = 4217
 rpc_url = "https://rpc.tempo.xyz"
-pg_url = "postgres://user@host:5432/tidx_mainnet"
-pg_password_env = "PG_PASSWORD"
-api_pg_url = "postgres://user@replica:5432/tidx_mainnet"
-api_pg_password_env = "PG_API_PASSWORD"
+
+[chains.postgres]
+url = "postgres://user@host:5432/tidx_mainnet"
+password_env = "PG_PASSWORD"
+api_url = "postgres://user@replica:5432/tidx_mainnet"
+api_password_env = "PG_API_PASSWORD"
```
