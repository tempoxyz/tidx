-- Decodes TIP-1049 `AdminKeyAuthorized(address indexed account,
-- address indexed publicKey)` emitted by the AccountKeychain precompile
-- alongside the unchanged `KeyAuthorized` event whenever an admin access key is
-- authorized. Both params are indexed, so the payload lives entirely in topics.
SELECT
    block_num,
    block_timestamp,
    tx_idx,
    log_idx,
    tx_hash,
    address,
    concat('0x', lower(substring(topic1, 27))) AS account,
    concat('0x', lower(substring(topic2, 27))) AS public_key
FROM logs
WHERE
    selector = '0x493bc0240c1da6c792754dc5247d39ed76c71c99a43e16777538687f8d05e88e'
    AND address = '0xaaaaaaaa00000000000000000000000000000000'
    AND topic1 IS NOT NULL
    AND topic2 IS NOT NULL
    AND length(topic1) >= 66
    AND length(topic2) >= 66
