---
tidx: patch
---

Counted every statement of a live `/query` stream against the API query concurrency limit. The limit previously covered a live request only until its SSE response started, so open streams ran their statements on top of it.
