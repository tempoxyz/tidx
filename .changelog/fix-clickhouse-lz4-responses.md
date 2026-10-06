---
tidx: patch
---

Fixed the ClickHouse sink failing against ClickHouse 26.9+ with `decompression error: incorrect magic number` by requesting LZ4-framed responses via `network_compression_method=lz4`.
