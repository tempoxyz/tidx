---
tidx: patch
---

Improved sync throughput against RPC endpoints that limit batch sizes. Oversized block and receipt batches are now split into halves that are fetched concurrently, and the accepted batch size is remembered for a minute so later ranges are requested in batches that fit instead of being rejected and split again.
