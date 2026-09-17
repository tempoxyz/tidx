-- Wait for the refresh triggered by `..._refresh_token_balances_snapshot` to
-- complete before this migration is recorded as applied. `SYSTEM WAIT VIEW`
-- blocks until the running refresh finishes and reports an error if the latest
-- refresh failed, so a failing refresh keeps this migration unapplied (and thus
-- retried on the next startup) instead of silently leaving the public snapshot
-- stale. Must run before the `token_holder_counts` refresh, which reads from
-- this snapshot.
SYSTEM WAIT VIEW token_balances_snapshot
