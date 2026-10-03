---
tidx: patch
---

Reduced PostgreSQL round trips per `/query` request from seven to four, and from fifteen to four for `engine=postgres-via-clickhouse`. Session settings are sent in one batch together with the query, which is no longer prepared separately, and the tiered prune-boundary lookup no longer prepares its statement.
