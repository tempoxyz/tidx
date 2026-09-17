ALTER TABLE logs
    ADD INDEX IF NOT EXISTS idx_selector_topic1 (selector, topic1)
        TYPE bloom_filter(0.01) GRANULARITY 1,
    ADD INDEX IF NOT EXISTS idx_selector_topic2 (selector, topic2)
        TYPE bloom_filter(0.01) GRANULARITY 1,
    ADD INDEX IF NOT EXISTS idx_selector_topic3 (selector, topic3)
        TYPE bloom_filter(0.01) GRANULARITY 1,
    ADD PROJECTION IF NOT EXISTS prj_address_position (
        SELECT _part_offset ORDER BY address, block_num, log_idx
    ),
    ADD PROJECTION IF NOT EXISTS prj_selector_address_position (
        SELECT _part_offset ORDER BY selector, address, block_num, log_idx
    ),
    ADD PROJECTION IF NOT EXISTS prj_selector_topic1_position (
        SELECT _part_offset ORDER BY selector, topic1, block_num, log_idx
    ),
    ADD PROJECTION IF NOT EXISTS prj_selector_topic2_position (
        SELECT _part_offset ORDER BY selector, topic2, block_num, log_idx
    ),
    ADD PROJECTION IF NOT EXISTS prj_selector_topic3_position (
        SELECT _part_offset ORDER BY selector, topic3, block_num, log_idx
    ),
    ADD PROJECTION IF NOT EXISTS prj_tx_hash (
        SELECT _part_offset ORDER BY tx_hash
    );
