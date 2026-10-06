---
tidx: patch
---

Reduced PostgreSQL round trips per `/query` request from seven to five, and from fifteen to five for `engine=postgres-via-clickhouse`. Session settings are sent in one batch together with statement preparation, and the tiered prune-boundary lookup no longer prepares its statement. Result types are resolved before execution so composite columns cannot stall the result stream.
