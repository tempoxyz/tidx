-- Force a refresh of `token_holder_counts` after `token_balances_snapshot` has
-- been rebuilt (the preceding wait migration guarantees that), so the public
-- per-token holder count drops the stale ReceivePolicyGuard holder too. This MV
-- reads from `token_balances_snapshot`, so refreshing it without the snapshot
-- first would just recount the stale data. Paired with the following
-- `..._wait_token_holder_counts` migration.
SYSTEM REFRESH VIEW token_holder_counts
