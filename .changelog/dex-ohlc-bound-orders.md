---
tidx: patch
---

Bounded the `dex_ohlc_1m` refresh: the join now only loads orders that were filled inside the retention window instead of every order ever placed, and the refresh carries explicit memory and thread limits.
