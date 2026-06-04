---
tidx: minor
---

Added `dex_pair_liquidity`, a ClickHouse view that joins `dex_pairs` to the DEX escrow balances in `token_balances_snapshot`, so the exchange pairs-by-liquidity endpoint can read ranked pairs directly instead of over-fetching escrow balances and intersecting base/quote pairs in memory.
