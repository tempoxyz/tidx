---
tidx: minor
---

Added `dex_ohlc_1m`, a refreshable ClickHouse materialized view that rolls `dex_fills` into per-minute OHLC candles keyed by `(token, bucket)`, removing the 1000-fill in-memory bucketing cap on the OHLC endpoint and enabling multi-month windows with the work moved off the request path.
