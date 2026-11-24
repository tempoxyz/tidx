---
tidx: patch
---

PostgreSQL queries now reject ClickHouse-only syntax that PostgreSQL cannot run: `ORDER BY ... WITH FILL`, `INTERPOLATE`, and parametric calls such as `f(a)(b)`. The query validator returns a clear error for them instead of passing the statement through to the database.
