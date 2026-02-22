---
tidx: minor
---

Added support for multiple event signatures per query. The HTTP API now accepts repeated `signature` query params (`?signature=Transfer(...)&signature=Approval(...)`) and the CLI accepts multiple `-s` flags. Each signature generates a separate CTE, enabling cross-event queries like `SELECT * FROM Transfer UNION ALL SELECT * FROM Approval`.
