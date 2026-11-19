-- One row per active (holder, token, UTC hour). Filter by holder to seek
-- into the canonical ledger's (holder, token, ...) sort key before grouping.
CREATE VIEW IF NOT EXISTS address_balance_hours AS
SELECT
    holder,
    token,
    toStartOfHour(toDateTime(block_timestamp, 'UTC')) AS hour,
    toInt256(sumIf(balance_delta, leg = 1)) - toInt256(sumIf(balance_delta, leg = -1)) AS delta
FROM address_holder_deltas FINAL
GROUP BY holder, token, hour
