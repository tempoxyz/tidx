---
tidx: patch
---

Fixed reorg handling skipping the replaced blocks: the sync pointers are now rewound to the fork point and realtime sync refetches the chain from there instead of writing the batch that exposed the reorg. The parent hash of a pipelined batch is now checked after the previous batch is committed, so two consecutive batches from different forks are no longer written.
