---
tidx: patch
---

PostgreSQL queries now reject text that PostgreSQL lexes differently from the query validator: a `--` comment containing a carriage return not followed by a newline, raw string literals (`r'...'`, `r"""..."""`) and double-quoted byte strings (`b"..."`). PostgreSQL ends the comment at the carriage return and has no such literals, so part of the query ran without being validated. Comments ending in `\r\n` are still accepted, and tiered queries apply the same checks before any part runs on ClickHouse.
