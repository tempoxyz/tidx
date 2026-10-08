---
tidx: patch
---

Fixed PostgreSQL `/query` validation skipping the characters argument of `trim()`, which let a subquery there read tables the validator otherwise rejects, such as `pg_authid`. The argument is now validated like the rest of the expression.
