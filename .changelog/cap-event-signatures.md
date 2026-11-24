---
tidx: patch
---

`/query` now rejects more than 16 `signature` params, and a query whose SQL and signatures together exceed 65,536 bytes, before rewriting it. Event signatures are applied to the SQL in a single pass, and the rewrite runs off the async worker threads, so a request with many signatures can no longer stall the API.
