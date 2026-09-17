-- Wait for the refresh triggered by `..._refresh_token_holder_counts` to
-- complete before this migration is recorded as applied, mirroring the snapshot
-- wait migration. A failing refresh surfaces here and keeps the migration
-- unapplied (retried next startup) rather than leaving stale public counts.
SYSTEM WAIT VIEW token_holder_counts
