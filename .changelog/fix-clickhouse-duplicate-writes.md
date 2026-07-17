---
tidx: patch
---

Fixed duplicate ClickHouse `ReplacingMergeTree` rows from concurrent writers, which surfaced as shifted `OFFSET` pages in `/query` results.
