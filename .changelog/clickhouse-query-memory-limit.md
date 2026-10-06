---
tidx: patch
---

Capped ClickHouse working memory at 1 GiB per public API query, so array-building functions can no longer allocate up to the server profile's limit before the result caps apply. Internal queries and the indexer's own writes are unaffected.
