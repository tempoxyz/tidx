INSERT INTO token_holder_deltas
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
        (`to`,   CAST(1 AS Int8),  amount),
        (`from`, CAST(-1 AS Int8), amount)
    ] AS leg_tuple
WHERE tupleElement(leg_tuple, 1) NOT IN (
    '0x0000000000000000000000000000000000000000',
    '0xb10c000000000000000000000000000000000000'
)
AND amount > toUInt256('57896044618658097711785492504343953926634992332820282019728792003956564819967')
