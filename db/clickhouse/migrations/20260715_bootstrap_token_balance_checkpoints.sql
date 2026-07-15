-- One-time full-history checkpoint. All recurring work after this migration
-- reads only post-checkpoint deltas, except for affected-key repair after an
-- out-of-order replay, historical backfill, or reorg.
INSERT INTO token_balance_checkpoints
SELECT
    token,
    holder,
    sumIf(balance_delta, leg = 1) AS credited,
    sumIf(balance_delta, leg = -1) AS debited,
    min(block_num) AS checkpoint_from_block,
    max(block_num) AS checkpoint_block,
    CAST([], 'Array(Tuple(UUID, Int64, Int64))') AS dirty_events,
    now64(9, 'UTC') AS version
FROM token_holder_deltas FINAL
GROUP BY token, holder
SETTINGS
    optimize_aggregation_in_order = 1,
    max_threads = 4,
    max_memory_usage = 34359738368,
    max_bytes_before_external_group_by = 2000000000
