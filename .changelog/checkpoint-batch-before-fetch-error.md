---
tidx: patch
---

Realtime sync now saves its tip for a batch that was already written before returning an error from fetching the next batch, so a retry no longer starts below rows that are stored. Writing a batch also replaces the transactions, logs and receipts stored for every block in it, including blocks the new version has no rows for.
