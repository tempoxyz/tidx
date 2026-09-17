ALTER TABLE blocks
    ADD PROJECTION IF NOT EXISTS prj_hash (
        SELECT _part_offset ORDER BY hash
    ),
    ADD PROJECTION IF NOT EXISTS prj_timestamp_position (
        SELECT _part_offset ORDER BY timestamp, num
    );
