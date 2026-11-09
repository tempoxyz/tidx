---
tidx: minor
---

Added optional API settings: `http.max_concurrent_queries` and `http.max_live_connections` (previously fixed at 32 and 20), `chains.postgres.api_pool_size` (previously fixed at 16) and, for `tidx serve`, `chains.postgres.api_pool_wait_ms` and a separate ClickHouse `api_user`/`api_password_env` for read-only query credentials. Every default matches the previous behavior.
