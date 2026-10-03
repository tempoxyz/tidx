---
tidx: patch
---

Sped up decoding of ClickHouse query results about fourfold. Queries now request `JSONCompact`, whose rows are arrays in column order, and are deserialized directly instead of through a JSON tree that was cloned cell by cell. Result bodies are also about a fifth smaller.
