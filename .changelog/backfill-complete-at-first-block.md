---
tidx: patch
---

Status now reports backfill as complete once block 1 is indexed on chains without a retention window, with no blocks remaining and no backfill ETA, and `tidx_synced` is reported as 0 for a gap-fill tick that cannot read the chain head instead of 1.
