---
tidx: patch
---

Fixed ClickHouse rejecting `/query` set operations (`UNION`, `INTERSECT`, `EXCEPT`) with a trailing `ORDER BY`/`LIMIT`/`OFFSET` by hoisting those clauses into a derived-table wrapper.
