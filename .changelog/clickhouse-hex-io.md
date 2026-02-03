---
tidx: patch
---

Fix ClickHouse hex literal handling for MaterializedPostgreSQL.

**Input**: Use `concat(char(92), 'x...')` instead of `'\x...'` for WHERE clause comparisons because ClickHouse interprets `\x` as an escape sequence.

**Output**: Convert `\x...` to `0x...` in query results for standard Ethereum hex format, matching PostgreSQL behavior.
