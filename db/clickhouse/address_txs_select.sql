SELECT
    block_num,
    block_timestamp,
    idx AS tx_idx,
    hash AS tx_hash,
    assumeNotNull(tupleElement(leg_tuple, 1)) AS address,
    tupleElement(leg_tuple, 2) AS direction,
    tupleElement(leg_tuple, 3) AS counterparty
FROM txs
ARRAY JOIN
    [
        (CAST(`from` AS Nullable(String)), CAST('from' AS LowCardinality(String)), `to`),
        (`to`,                              CAST('to'   AS LowCardinality(String)), CAST(`from` AS Nullable(String)))
    ] AS leg_tuple
WHERE tupleElement(leg_tuple, 1) IS NOT NULL
