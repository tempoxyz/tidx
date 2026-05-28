SELECT
    token,
    block_num,
    block_timestamp,
    CAST(1 AS UInt64) AS transfer_count
FROM token_transfers
