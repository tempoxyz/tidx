---
tidx: minor
---

Made the schema OrioleDB-ready: new partitions use orioledb with zstd compression when the extension is installed (heap otherwise), with C-collated text columns, heap-pinned staging/metadata tables, and AM-aware sealing. Docker images stay on stock Postgres pending an upstream correctness fix.
