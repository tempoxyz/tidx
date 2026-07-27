---
tidx: patch
---

Fixed ClickHouse failover classification so client timeouts stop immediately while non-timeout transport and protocol failures try a healthy secondary.
