-- Wait for the refresh triggered by `..._refresh_address_balances_snapshot`
-- so address balance readers stop seeing stale guard-derived balances before
-- this migration is marked applied.
SYSTEM WAIT VIEW address_balances_snapshot
