CREATE VIEW IF NOT EXISTS token_balances AS
SELECT
    token,
    holder,
    sum(balance_delta) AS balance
FROM token_holder_deltas FINAL
GROUP BY token, holder
HAVING balance > 0
