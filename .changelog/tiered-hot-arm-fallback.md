---
tidx: patch
---

Fixed tiered split queries erroring when the hot PostgreSQL arm fails; they now degrade to the ClickHouse archive, which holds full history.
