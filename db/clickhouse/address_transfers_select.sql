SELECT
    block_num,
    block_timestamp,
    tx_idx,
    log_idx,
    tx_hash,
    tupleElement(leg_tuple, 1) AS address,
    tupleElement(leg_tuple, 2) AS direction,
    tupleElement(leg_tuple, 3) AS counterparty,
    token,
    amount
FROM token_transfers
ARRAY JOIN
    [
        (`to`,   CAST('in'  AS LowCardinality(String)), `from`),
        (`from`, CAST('out' AS LowCardinality(String)), `to`)
    ] AS leg_tuple
WHERE tupleElement(leg_tuple, 1) != '0x0000000000000000000000000000000000000000'
