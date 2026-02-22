---
tidx: patch
---

Fixed array type parsing in event signatures (`uint256[]`, `uint256[N]`) which previously returned "Invalid uint size". Added missing SQL functions to query allowlist (`date`, `date_part`, `to_char`, `array_agg`, `string_agg`, etc.). Removed references to non-existent `token_holders`/`token_balances` tables.
