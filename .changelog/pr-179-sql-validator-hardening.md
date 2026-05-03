---
tidx: patch
---

Harden PostgreSQL SQL validation by fixing CTE scope handling, schema-qualified table checks, recursive depth accounting, LIMIT ALL rejection, and traversal of previously unchecked AST clauses.
