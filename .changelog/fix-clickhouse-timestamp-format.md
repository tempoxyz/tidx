---
tidx: patch
---

Fixed `engine=clickhouse` returning timestamps without a timezone (`2026-09-11 22:38:08.000`), which JavaScript and Python parse as local time. Native ClickHouse timestamps now use the same RFC 3339 UTC format as PostgreSQL (`2026-09-11T22:38:08+00:00`).
