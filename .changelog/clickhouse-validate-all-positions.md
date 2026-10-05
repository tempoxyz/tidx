---
tidx: patch
---

Fixed ClickHouse query validation to check functions and tables in every clause of a query. `TOP` and `IN` with a single identifier are now rejected; use `LIMIT` and `=` or a subquery instead.
