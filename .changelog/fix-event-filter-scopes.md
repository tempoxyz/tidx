---
tidx: patch
---

Fixed unquoted and repeated indexed event equality filters in simple queries. Topic pushdown now edits individual WHERE predicates on a single event table; it leaves joins, CTEs, derived sources, subquery predicates, and grouped expressions alone instead of inserting out-of-scope topic references. PostgreSQL short fixed-byte comparisons outside this path still require explicit `\x` literals.
