---
tidx: patch
---

Live `/query` streams now rewrite and validate their per-block query once per stream instead of for every new block. The block number is bound as a query parameter, which saves about 25 µs of CPU per block and subscriber for plain queries and about 130 µs for queries with event signatures.
