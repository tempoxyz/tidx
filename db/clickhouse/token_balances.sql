CREATE VIEW IF NOT EXISTS token_balances AS
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
