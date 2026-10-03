---
tidx: patch
---

Fixed indexed `bytesN` equality filters losing matches when identifiers are unquoted or the same column is compared with multiple values. Event filters now respect table aliases and query scopes, so nested queries and unrelated CTE columns no longer get invalid topic references. PostgreSQL equality filters on projected or non-indexed fixed bytes also accept `0x` literals without changing text comparisons.
