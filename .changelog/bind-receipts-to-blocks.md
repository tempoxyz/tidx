---
tidx: patch
---

Fixed receipts and logs being dropped or attached to the wrong block when the RPC node answers a block and its receipts inconsistently. Per-item batch errors and truncated batch responses now fail the fetch, and receipts are only written when they carry the hash and transactions of the block they are stored with. Batch responses are matched by request ID so out-of-order replies remain valid; missing, duplicate, and unexpected IDs are rejected.
