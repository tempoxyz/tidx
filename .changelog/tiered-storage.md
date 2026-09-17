---
tidx: minor
---

Added tiered storage: with `[chains.retention]`, PostgreSQL keeps a hot window of recent blocks and prunes the rest once durable in the ClickHouse archive.

```diff
 [[chains]]
 name = "mainnet"
 chain_id = 4217
 rpc_url = "https://rpc.tempo.xyz"

 [chains.clickhouse]
 enabled = true
 url = "http://clickhouse:8123"
+
+[chains.retention]
+pg_keep = "30d"
```
