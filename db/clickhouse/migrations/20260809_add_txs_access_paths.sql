ALTER TABLE txs
    ADD INDEX IF NOT EXISTS idx_from_nonce_key_nonce (`from`, nonce_key, nonce)
        TYPE bloom_filter(0.01) GRANULARITY 1,
    ADD PROJECTION IF NOT EXISTS prj_from_position (
        SELECT _part_offset ORDER BY `from`, block_num, idx
    ),
    ADD PROJECTION IF NOT EXISTS prj_to_position (
        SELECT _part_offset ORDER BY `to`, block_num, idx
    ),
    ADD PROJECTION IF NOT EXISTS prj_fee_payer_position (
        SELECT _part_offset ORDER BY fee_payer, block_num, idx
    ),
    ADD PROJECTION IF NOT EXISTS prj_fee_token_position (
        SELECT _part_offset ORDER BY fee_token, block_num, idx
    );
