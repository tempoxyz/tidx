-- Force the refreshable address balance snapshot to rebuild after deleting
-- guard holder rows from address_holder_deltas. Pair this with the wait
-- migration before recording the cleanup as complete.
SYSTEM REFRESH VIEW address_balances_snapshot
