ALTER TABLE receipts
    ADD INDEX IF NOT EXISTS idx_tx_hash   tx_hash   TYPE bloom_filter GRANULARITY 1,
    ADD INDEX IF NOT EXISTS idx_from      `from`    TYPE bloom_filter GRANULARITY 1,
    ADD INDEX IF NOT EXISTS idx_fee_payer fee_payer TYPE bloom_filter GRANULARITY 1;
