---
tidx: patch
---

PostgreSQL `/query` now refuses to send a row larger than 10 MiB as text, so a single huge value such as `string_agg` over a large table can no longer make the API buffer it before the result limits apply; such a query fails with `Query result row exceeded 10485760 bytes`.
