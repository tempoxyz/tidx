---
tidx: patch
---

Fixed PostgreSQL `/query` result-size limits. Responses are now charged their exact JSON size (quotes, escapes and separators), so escape-heavy strings can no longer exceed the 10 MiB result limit. The data PostgreSQL sends is bounded separately at 40 MiB by wire length: cells returned as `null`, such as arrays or `txs.calls`, count only `null` towards the 10 MiB response limit but their full size towards the 40 MiB read limit. Cells of any type over 1 MiB are rejected, and a result may hold at most 1,000,000 cells.
