---
tidx: minor
---

Added optional `chains.clickhouse.api_url` and `api_failover_urls` so `tidx serve` can query a ClickHouse replica dedicated to API reads while `tidx sync` writes to `url`; unset, serve keeps using `url` and `failover_urls`. The ClickHouse engine now also returns to its primary instance after a failover: it retries the primary every 30 seconds instead of staying on the failover instance until restart.
