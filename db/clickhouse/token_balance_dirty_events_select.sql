SELECT
    generateUUIDv4() AS event_id,
    token,
    holder,
    min(block_num) AS min_block,
    max(block_num) AS max_block,
    CAST(1 AS Int8) AS sign
FROM token_holder_deltas
GROUP BY token, holder
