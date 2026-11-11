---
tidx: patch
---

Realtime sync now recovers when PostgreSQL stops answering, such as after a failover: database connections time out instead of hanging, and `/health` returns 503 while a realtime sync tick has been stuck for more than 120 seconds, so the orchestrator restarts the indexer.
