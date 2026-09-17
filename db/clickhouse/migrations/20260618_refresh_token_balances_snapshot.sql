-- Force a refresh of `token_balances_snapshot` after the guard-row deletes so
-- the public holder-balance aggregate stops serving the stale ReceivePolicyGuard
-- holder. The refreshable MV otherwise only rebuilds every 15 minutes (longer if
-- a refresh is failing), so without this the fake holder lingers after the
-- delete is recorded as applied. Triggers an immediate out-of-schedule refresh;
-- the paired `..._wait_token_balances_snapshot` migration blocks until it
-- finishes. ClickHouse runs at most one refresh per view at a time, so this
-- coalesces with any in-flight scheduled refresh.
SYSTEM REFRESH VIEW token_balances_snapshot
