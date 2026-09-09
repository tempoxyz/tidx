---
tidx: patch
---

Fixed event-query predicate pushdown dropping incremental Earn deposits by turning OR-based pagination cursors into contradictory AND filters. Only required WHERE conjuncts from a single, unambiguous reference to the matching event table are pushed down; shared event sources retain their consumer-specific filters.
