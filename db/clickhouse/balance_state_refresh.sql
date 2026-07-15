-- Recompute only pairs dirtied since the previous successful refresh.
--
-- APPEND is intentional: balance_state is a ReplacingMergeTree keyed by the
-- pair, so each refresh publishes a newer absolute value. The LEFT JOIN emits
-- a tombstone when a reorg removed the last delta for a pair.
CREATE MATERIALIZED VIEW IF NOT EXISTS balance_state_refresh
REFRESH AFTER 1 MINUTE APPEND TO balance_state
AS
WITH
    dirty_keys AS (
        SELECT token, holder, version
        FROM balance_dirty_keys FINAL
        WHERE dirty = 1
    ),
    dirty_balances AS (
        SELECT
            token,
            holder,
            if(
                sumIf(balance_delta, leg = 1) >= sumIf(balance_delta, leg = -1),
                toUInt256(sumIf(balance_delta, leg = 1) - sumIf(balance_delta, leg = -1)),
                toUInt256(0)
            ) AS balance,
            CAST(1 AS UInt8) AS present
        FROM token_holder_deltas FINAL
        WHERE (token, holder) IN (SELECT token, holder FROM dirty_keys)
        GROUP BY token, holder
        HAVING balance > 0
    )
SELECT
    dirty_keys.token AS token,
    dirty_keys.holder AS holder,
    if(dirty_balances.present = 1, dirty_balances.balance, toUInt256(0)) AS balance,
    toUInt8(dirty_balances.present = 0) AS is_deleted,
    dirty_keys.version AS version
FROM dirty_keys
LEFT JOIN dirty_balances USING (token, holder)
SETTINGS
    max_threads = 8,
    max_memory_usage = 34359738368,
    max_bytes_before_external_group_by = 2000000000
