-- Decodes `PairCreated(bytes32 indexed key, address indexed base, address indexed quote)`
-- from the raw `logs` stream. All three params are indexed, so the payload
-- lives entirely in topics (no `data`). selector = keccak256 of the canonical
-- signature.
SELECT
    block_num,
    block_timestamp,
    tx_idx,
    log_idx,
    tx_hash,
    address,
    concat('0x', lower(substring(topic1, 3))) AS `key`,
    concat('0x', lower(substring(topic2, 27))) AS base,
    concat('0x', lower(substring(topic3, 27))) AS quote
FROM logs
WHERE
    selector = '0xaff90cfc97c741e6d1ffffa62656c16a763f41dc773055d7b0c36950a823babf'
    AND address = '0xdec0000000000000000000000000000000000000'
    AND topic1 IS NOT NULL
    AND topic2 IS NOT NULL
    AND topic3 IS NOT NULL
    AND length(topic1) >= 66
    AND length(topic2) >= 66
    AND length(topic3) >= 66
