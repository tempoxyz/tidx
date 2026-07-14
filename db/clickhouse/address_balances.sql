-- Holder-keyed facade over the canonical incremental balance state.
-- Dirty pairs retain exact read-after-write behavior by falling back to the
-- deduplicated delta ledger until the background reducer publishes them.
CREATE VIEW IF NOT EXISTS address_balances AS
WITH dirty_keys AS (
    SELECT token, holder
    FROM balance_dirty_keys FINAL
    WHERE dirty = 1
)
SELECT holder, token, balance
FROM balance_state FINAL
WHERE is_deleted = 0
  AND (token, holder) NOT IN (SELECT token, holder FROM dirty_keys)
UNION ALL
SELECT
    holder,
    token,
    if(
        sumIf(balance_delta, leg = 1) >= sumIf(balance_delta, leg = -1),
        toUInt256(sumIf(balance_delta, leg = 1) - sumIf(balance_delta, leg = -1)),
        toUInt256(0)
    ) AS balance
FROM token_holder_deltas FINAL
WHERE (token, holder) IN (SELECT token, holder FROM dirty_keys)
GROUP BY holder, token
HAVING balance > 0
