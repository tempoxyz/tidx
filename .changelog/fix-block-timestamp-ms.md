---
tidx: patch
---

Fixed `blocks.timestamp_ms` being rounded down to whole seconds. Block rows now store the header's `timestampMillis`, so blocks produced within the same second are distinguishable again, and archive range reads derive their partition bounds from the second-precision block `timestamp` that child rows carry.
