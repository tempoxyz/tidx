---
tidx: patch
---

Fixed `bytes1`–`bytes31` event parameters returning the whole zero-padded 32-byte ABI word instead of exactly N bytes on every query engine, so a `bytes4` value now reads `0xdeadbeef` rather than `0xdeadbeef000…000`. Signatures with a `bytesN` width outside 1–32 are now rejected.

Filters must now compare against the N-byte value: `WHERE "tag" = '0xdeadbeef'` matches, while the padded 32-byte literal no longer does. Equality filters on indexed `bytesN` params are pushed down to the topic column, so they match on every engine and use the topic index. ClickHouse materialized views created from a `bytesN` signature keep the padded values until they are recreated.
