SELECT
    block_num,
    block_timestamp,
    tx_idx,
    log_idx,
    tx_hash,
    token,
    `from`,
    `to`,
    amount,
    is_virtual_forward
FROM
(
    SELECT
        block_num,
        block_timestamp,
        tx_idx,
        log_idx,
        tx_hash,
        address AS token,
        concat('0x', lower(substring(topic1, 27))) AS `from`,
        concat('0x', lower(substring(topic2, 27))) AS `to`,
        reinterpretAsUInt256(reverse(unhex(substring(data, 3, 64)))) AS amount,
        is_virtual_forward
    FROM logs
    WHERE
        selector = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND topic1 IS NOT NULL
        AND topic2 IS NOT NULL
        AND length(topic1) >= 66
        AND length(topic2) >= 66
        AND length(data) >= 66
)
