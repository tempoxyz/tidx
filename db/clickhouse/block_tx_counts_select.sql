SELECT
    block_num,
    block_timestamp,
    count() AS tx_count
FROM txs
GROUP BY block_num, block_timestamp
