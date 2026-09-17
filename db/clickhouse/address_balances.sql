CREATE VIEW IF NOT EXISTS address_balances AS
SELECT
    holder,
    token,
    if(
        sumIf(balance_delta, leg = 1) >= sumIf(balance_delta, leg = -1),
        toUInt256(sumIf(balance_delta, leg = 1) - sumIf(balance_delta, leg = -1)),
        toUInt256(0)
    ) AS balance
FROM address_holder_deltas FINAL
GROUP BY holder, token
HAVING balance > 0
