-- Net balance change of each holder for each token in each UTC hour.
--
-- The balance of (holder, token) at the end of hour H is
--   SELECT sum(delta) FROM address_balance_hours
--   WHERE holder = '<holder>' AND token = '<token>' AND hour <= H
-- Holders exclude the zero address and the TIP-1028 receive-policy guard,
-- exactly like address_holder_deltas.
--
-- Completed history comes from address_balance_hours_snapshot; hours at or
-- after its cutoff are aggregated live from address_holder_deltas FINAL, so
-- the view is always current, reflects reorg pruning of recent blocks at
-- once, and returns one row per (holder, token, hour).
CREATE VIEW IF NOT EXISTS address_balance_hours AS
WITH ifNull(
    (
        SELECT max(hour)
        FROM address_balance_hours_snapshot
        WHERE holder = '' AND token = ''
    ),
    toDateTime(0, 'UTC')
) AS cutoff
SELECT holder, token, hour, delta
FROM (
    SELECT holder, token, hour, delta
    FROM address_balance_hours_snapshot
    WHERE holder != '' AND hour < cutoff
    UNION ALL
    SELECT
        holder,
        token,
        toStartOfHour(toDateTime(block_timestamp, 'UTC')) AS hour,
        toInt256(sumIf(balance_delta, leg = 1)) - toInt256(sumIf(balance_delta, leg = -1)) AS delta
    FROM address_holder_deltas FINAL
    WHERE block_timestamp >= cutoff
    GROUP BY holder, token, hour
)
