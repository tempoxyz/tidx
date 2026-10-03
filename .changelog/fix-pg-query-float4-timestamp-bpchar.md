---
tidx: patch
---

Fixed PostgreSQL `/query` results returning `null` for `float4`, `timestamp` (without time zone), and `char(n)` columns. `timestamp` values are read as UTC and formatted as RFC 3339 like `timestamptz`.
