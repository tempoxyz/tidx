CREATE VIEW IF NOT EXISTS address_balances AS
SELECT
    holder,
    token,
    sum(balance_delta) AS balance
FROM address_holder_deltas FINAL
GROUP BY holder, token
HAVING balance > 0
