---
tidx: patch
---

Fix ClickHouse DDL statement handling. DDL statements like `CREATE DATABASE` and `CREATE TABLE` return empty responses, which previously caused JSON parsing errors. Now handles empty responses gracefully.
