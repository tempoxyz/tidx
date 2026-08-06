ALTER TABLE txs
    ADD INDEX IF NOT EXISTS idx_fee_payer fee_payer TYPE bloom_filter GRANULARITY 1,
    ADD INDEX IF NOT EXISTS idx_fee_token fee_token TYPE bloom_filter GRANULARITY 1;
