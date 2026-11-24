---
tidx: patch
---

Without a ClickHouse sink, the PostgreSQL hot-window reconciler fills missing blocks only up to the tip realtime sync has reached, so a block it fetched above the tip can no longer be replaced by a different block at that height without a parent hash check. Gap detection also counts each block height once, so a duplicate row for a height no longer hides a missing one.
