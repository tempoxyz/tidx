ALTER TABLE receipts
    ADD INDEX IF NOT EXISTS idx_to `to`
        TYPE bloom_filter(0.01) GRANULARITY 1,
    ADD PROJECTION IF NOT EXISTS prj_tx_hash (
        SELECT _part_offset ORDER BY tx_hash
    ),
    ADD PROJECTION IF NOT EXISTS prj_fee_payer_position (
        SELECT _part_offset ORDER BY fee_payer, block_num, tx_idx
    );
