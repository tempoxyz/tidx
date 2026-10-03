---
tidx: patch
---

Fixed PostgreSQL-over-ClickHouse (`engine=postgres&source=clickhouse`) queries returning `columns: []` for an empty event result. Column names now come from the statement prepared inside the query transaction.
