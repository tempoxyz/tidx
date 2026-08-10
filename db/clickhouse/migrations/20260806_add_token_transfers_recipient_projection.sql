ALTER TABLE token_transfers
    ADD PROJECTION IF NOT EXISTS by_recipient (
        SELECT _part_offset
        ORDER BY (`to`, block_num, log_idx)
    );
