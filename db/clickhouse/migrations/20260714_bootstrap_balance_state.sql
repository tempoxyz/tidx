-- One-time bootstrap for deployments that already have holder deltas.
--
-- This migration is ordered after the state table and its clean-marker MV but
-- before the public balance views are reconciled. Existing snapshots therefore
-- remain readable while this single full-history aggregation runs. Subsequent
-- updates use balance_state_refresh and touch only dirty pairs.
INSERT INTO balance_state
SELECT
    token,
    holder,
    balance,
    CAST(0 AS UInt8) AS is_deleted,
    now64(9, 'UTC') AS version
FROM (
    SELECT
        token,
        holder,
        if(
            sumIf(balance_delta, leg = 1) >= sumIf(balance_delta, leg = -1),
            toUInt256(sumIf(balance_delta, leg = 1) - sumIf(balance_delta, leg = -1)),
            toUInt256(0)
        ) AS balance
    FROM token_holder_deltas FINAL
    GROUP BY token, holder
    HAVING balance > 0
)
SETTINGS
    max_threads = 8,
    max_memory_usage = 34359738368,
    max_bytes_before_external_group_by = 2000000000
