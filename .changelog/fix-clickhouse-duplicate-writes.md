---
tidx: patch
---

Fixed duplicate, stale, or missing ClickHouse rows across retries, reorgs, partial writes, and startup backfills using bounded canonical-row repair, fresh deduplication generations, and durable handoffs.
