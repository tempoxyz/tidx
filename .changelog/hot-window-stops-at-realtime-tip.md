---
tidx: patch
---

Without a ClickHouse sink, the PostgreSQL hot-window reconciler fills missing blocks only up to the tip realtime sync has reached, so a block it fetched above the tip can no longer be replaced by a different block at that height without a parent hash check. After a restart it waits until realtime sync is within its tail window of the head before checking gaps or moving the hot boundary, so the range realtime skips is filled on the next tick and the pruner cannot drop realtime's parent hash anchor. Gap detection also counts each block height once, so a duplicate row for a height no longer hides a missing one.
