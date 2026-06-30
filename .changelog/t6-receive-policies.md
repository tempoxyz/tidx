---
tidx: minor
---

Added support for the Tempo T6 network upgrade (TIP-1028 receive policies, TIP-1049 admin keys). Excluded the `ReceivePolicyGuard` precompile (`0xb10c...`) from holder/balance derivation so blocked transfers no longer credit the guard as a fake holder, while leaving raw `token_transfers`/`token_supply` movement intact. Added a one-time post-derived migration that deletes any pre-existing guard rows from `token_holder_deltas`/`address_holder_deltas` (a no-op on fresh deployments) so historical balances and refreshable holder aggregates stop counting the guard. Pinned the Tempo crates to the T6 release commit.
