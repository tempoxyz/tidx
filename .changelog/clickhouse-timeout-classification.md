---
tidx: patch
---

Fixed ClickHouse client timeouts being misclassified as connection errors, which burned failover retries and suppressed tiered fallback on slow queries.
