---
tidx: patch
---

Fixed PostgreSQL `/query` result-size limits. Responses are now charged their exact JSON size (quotes, escapes and separators), so escape-heavy strings can no longer exceed the 10 MiB result limit. The data PostgreSQL sends is bounded separately at 40 MiB by wire length, so cells that are dropped from the response, such as arrays, cannot be read without limit, while columns like `txs.calls` no longer count towards the response limit. Cells of any type over 1 MiB are rejected, and a result may hold at most 1,000,000 cells.
