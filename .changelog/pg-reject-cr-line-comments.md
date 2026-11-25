---
tidx: patch
---

PostgreSQL queries now reject a `--` comment that contains a carriage return not followed by a newline. PostgreSQL ends the comment at the carriage return but the query validator did not, so the rest of the line ran without being validated. Comments ending in `\r\n` are still accepted.
