---
tidx: minor
---

Normalized `txs.calls` JSONB into a `tx_calls` table, dropped the column and GIN index, moved inner-call recipient filtering to an indexed `EXISTS`, and added an automatic legacy migration.
