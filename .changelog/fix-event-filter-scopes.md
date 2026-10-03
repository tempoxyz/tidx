---
tidx: patch
---

Fixed unquoted and repeated indexed event equality filters. Topic pushdown now edits individual WHERE predicates on a directly named event table, including safe event scans inside subqueries and CTEs. Filters on joined or derived columns and grouped expressions keep their decoded references. PostgreSQL short fixed-byte comparisons outside this path still require explicit `\x` literals.
