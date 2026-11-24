---
tidx: patch
---

Event filters no longer change a quoted table alias that contains adjacent quotes or a backslash before a quote when rewriting a qualified column such as `"q""""r".amount`. The rewritten query now names the same relation as the `FROM` clause instead of failing or binding to another one.
