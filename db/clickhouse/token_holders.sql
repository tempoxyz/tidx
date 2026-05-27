CREATE VIEW IF NOT EXISTS token_holders AS
SELECT
    token,
    holder,
    sum(balance_delta) AS balance
FROM token_holder_deltas
GROUP BY token, holder
HAVING balance > 0
