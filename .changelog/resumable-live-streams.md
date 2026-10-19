---
tidx: minor
---

Live `/query?live=true` streams are now resumable. Each `result` event carries the last block it covers as its SSE `id`, and a client reconnecting with `Last-Event-ID` (or `lastEventId=`) gets the blocks it missed replayed from PostgreSQL instead of a new snapshot. When those blocks were pruned or more than 1000 were missed, the stream sends a `reset` event followed by a new snapshot. The initial snapshot now stops at the indexed tip, so its rows are no longer repeated by the per-block results that follow.
