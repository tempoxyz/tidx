ALTER TABLE receipts
    ADD INDEX IF NOT EXISTS idx_contract_address contract_address TYPE bloom_filter GRANULARITY 1;
