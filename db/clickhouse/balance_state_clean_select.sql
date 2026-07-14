SELECT
    token,
    holder,
    CAST(0 AS UInt8) AS dirty,
    version
FROM balance_state
