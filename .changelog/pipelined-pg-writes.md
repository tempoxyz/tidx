---
tidx: patch
---

Cut PostgreSQL write latency for small batches about fourfold. Batches are now written with one `INSERT ... SELECT FROM unnest(...)` per table instead of a temporary staging table, a COPY and an `INSERT ... SELECT`, and all statements of a batch are sent without waiting for each other, so a write takes about four round trips instead of about forty and no longer creates and drops temporary tables.
