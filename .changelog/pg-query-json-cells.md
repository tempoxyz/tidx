---
tidx: patch
---

Sped up converting PostgreSQL `/query` results to JSON, most noticeably for BYTEA-heavy results such as raw logs. Response output is unchanged.
