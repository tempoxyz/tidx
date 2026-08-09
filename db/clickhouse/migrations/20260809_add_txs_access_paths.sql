ALTER TABLE txs
    ADD INDEX IF NOT EXISTS idx_from_nonce_key_nonce (`from`, nonce_key, nonce)
        TYPE bloom_filter(0.01) GRANULARITY 1;
