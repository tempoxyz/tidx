SELECT
    block_num,
    block_timestamp,
    token,
    holder,
    sum(delta) AS balance_delta
FROM
(
    SELECT
        block_num,
        block_timestamp,
        token,
        `to` AS holder,
        CAST(amount AS Int256) AS delta
    FROM token_transfer_events

    UNION ALL

    SELECT
        block_num,
        block_timestamp,
        token,
        `from` AS holder,
        -CAST(amount AS Int256) AS delta
    FROM token_transfer_events
)
WHERE holder != '0x0000000000000000000000000000000000000000'
GROUP BY block_num, block_timestamp, token, holder
