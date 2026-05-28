CREATE VIEW IF NOT EXISTS token_metadata AS
SELECT
    token,
    min(block_num)                     AS first_seen_block,
    max(block_num)                     AS last_seen_block,
    min(block_timestamp)               AS first_seen_timestamp,
    max(block_timestamp)               AS last_seen_timestamp,
    count()                            AS transfer_count
FROM token_transfers FINAL
GROUP BY token
