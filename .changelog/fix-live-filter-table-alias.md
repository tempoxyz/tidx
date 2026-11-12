---
tidx: patch
---

Fixed live-mode SSE queries failing on every block when the source table is aliased. The injected block filter now uses the table alias as its column qualifier instead of the underlying table name.
