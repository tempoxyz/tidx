---
tidx: minor
---

Range-partitioned the chain tables by block number on fresh installs (configurable `partition_blocks`, default 1M); the writer creates partitions on demand, and existing regular-table deployments were left unchanged.
