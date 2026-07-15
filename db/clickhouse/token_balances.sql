-- Exact current balances with an incremental-state fast path.
--
-- Clean pairs come directly from balance_state. Pairs changed since the last
-- background state refresh are recomputed from the canonical FINAL ledger, so
-- this view preserves the old immediate-consistency semantics without making
-- every query aggregate the full history.
CREATE VIEW IF NOT EXISTS token_balances AS
WITH dirty_keys AS (
    SELECT token, holder
    FROM balance_dirty_keys FINAL
    WHERE dirty = 1
)
SELECT token, holder, balance
FROM balance_state FINAL
WHERE is_deleted = 0
  AND (token, holder) NOT IN (SELECT token, holder FROM dirty_keys)
UNION ALL
SELECT
    token,
    holder,
    if(
        sumIf(balance_delta, leg = 1) >= sumIf(balance_delta, leg = -1),
        toUInt256(sumIf(balance_delta, leg = 1) - sumIf(balance_delta, leg = -1)),
        toUInt256(0)
    ) AS balance
FROM token_holder_deltas FINAL
WHERE (token, holder) IN (SELECT token, holder FROM dirty_keys)
GROUP BY token, holder
HAVING balance > 0
