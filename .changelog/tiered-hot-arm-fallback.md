---
tidx: patch
---

Fixed tiered split queries erroring on PostgreSQL availability failures; they now degrade to the full-history ClickHouse archive while preserving PostgreSQL semantic errors.
