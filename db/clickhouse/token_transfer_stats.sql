CREATE VIEW IF NOT EXISTS token_transfer_stats AS
SELECT
    toDate(block_timestamp) AS day,
    token,
    count() AS transfer_count,
    sum(amount) AS volume,
    uniqExact(`from`) AS unique_senders,
    uniqExact(`to`) AS unique_recipients
FROM token_transfers FINAL
GROUP BY day, token
