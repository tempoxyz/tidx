---
tidx: patch
---

Fixed `/query` errors flattening PostgreSQL failures to `db error`; the server message and SQLSTATE code (e.g. `division by zero (22012)`) now surface, and statement timeouts classify as timeouts.
