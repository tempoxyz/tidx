---
tidx: patch
---

Reduced PostgreSQL write latency on partitioned installs. The deletes that replace a batch's existing txs, logs and receipts are now bounded by the batch's block timestamps, so PostgreSQL plans and scans only the weekly partitions around the batch instead of every partition.
