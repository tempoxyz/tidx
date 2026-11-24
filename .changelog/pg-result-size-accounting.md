---
tidx: patch
---

Fixed PostgreSQL `/query` result-size limits. Results are now charged their exact JSON size (quotes, escapes and separators) or the wire size of each cell, whichever is larger, so escape-heavy strings and array cells can no longer exceed the 10 MiB result limit. Cells of any type over 1 MiB are rejected, and a result may hold at most 1,000,000 cells.
