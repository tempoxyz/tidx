---
tidx: patch
---

Fixed ClickHouse error classification: client timeouts no longer burn failover retries, while connections reset or closed before a response still trigger failover.
