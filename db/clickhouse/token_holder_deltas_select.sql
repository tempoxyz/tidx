SELECT
    block_num,
    block_timestamp,
    tx_hash,
    log_idx,
    token,
    tupleElement(leg_tuple, 1) AS holder,
    tupleElement(leg_tuple, 2) AS leg,
    tupleElement(leg_tuple, 3) AS balance_delta
FROM token_transfers
ARRAY JOIN
    [
        (`to`,   CAST(1 AS Int8),  CAST(amount AS Int256)),
        (`from`, CAST(-1 AS Int8), -CAST(amount AS Int256))
    ] AS leg_tuple
WHERE tupleElement(leg_tuple, 1) != '0x0000000000000000000000000000000000000000'
