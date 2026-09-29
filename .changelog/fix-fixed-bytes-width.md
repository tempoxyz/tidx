---
tidx: patch
---

Fixed `bytes1`–`bytes31` event parameters returning the whole zero-padded 32-byte ABI word instead of exactly N bytes on every query engine, so a `bytes4` value now reads `0xdeadbeef` rather than `0xdeadbeef000…000`. Signatures with a `bytesN` width outside 1–32 are now rejected.
