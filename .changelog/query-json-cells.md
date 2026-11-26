---
tidx: patch
---

Fixed PostgreSQL `/query` returning `null` for `json` and `jsonb` columns. They are now returned as JSON values, subject to the same 1 MiB cell limit as text and bytea.
