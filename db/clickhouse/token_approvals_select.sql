SELECT
    block_num,
    block_timestamp,
    tx_idx,
    log_idx,
    tx_hash,
    token,
    owner,
    spender,
    amount
FROM
(
    SELECT
        block_num,
        block_timestamp,
        tx_idx,
        log_idx,
        tx_hash,
        address AS token,
        concat('0x', lower(substring(topic1, 27))) AS owner,
        concat('0x', lower(substring(topic2, 27))) AS spender,
        reinterpretAsUInt256(reverse(unhex(substring(data, 3, 64)))) AS amount
    FROM logs
    WHERE
        selector = '0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925'
        AND topic1 IS NOT NULL
        AND topic2 IS NOT NULL
        AND length(topic1) >= 66
        AND length(topic2) >= 66
        AND length(data) >= 66
)
