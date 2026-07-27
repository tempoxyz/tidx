---
tidx: patch
---

Fixed duplicate or missing ClickHouse rows across retries, reorgs, partial writes, and startup backfills using canonical deduplication tokens, exact-key repair, and durable handoffs.
