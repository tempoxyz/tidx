SELECT
    block_num,
    block_timestamp,
    tx_idx,
    tx_hash,
    `from`                       AS creator,
    assumeNotNull(contract_address) AS contract
FROM receipts
WHERE contract_address IS NOT NULL
