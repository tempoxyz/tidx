SELECT
    token,
    holder,
    CAST(1 AS UInt8) AS dirty,
    now64(9, 'UTC') AS version
FROM token_holder_deltas
GROUP BY token, holder
