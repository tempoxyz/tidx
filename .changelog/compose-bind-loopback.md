---
tidx: patch
---

Bound the PostgreSQL, ClickHouse, Prometheus and Grafana ports in the production compose file to `127.0.0.1`, so their default credentials are no longer reachable from other hosts.
