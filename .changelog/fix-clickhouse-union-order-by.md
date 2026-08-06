---
tidx: patch
---

Fixed ClickHouse set operations with trailing ordering or limits, including case-insensitive aliases and unresolved compound outputs, by hoisting clauses into a derived-table wrapper.
