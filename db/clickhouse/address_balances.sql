CREATE VIEW IF NOT EXISTS address_balances AS
SELECT
    holder,
    token,
    sumIf(balance_delta, leg = 1) - sumIf(balance_delta, leg = -1) AS balance
FROM address_holder_deltas FINAL
GROUP BY holder, token
HAVING balance > 0
