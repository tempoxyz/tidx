---
tidx: patch
---

Fixed ClickHouse set operations with trailing ordering or limits, including aliased relation-qualified outputs, by hoisting their clauses into a derived-table wrapper.
