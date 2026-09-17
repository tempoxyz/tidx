ALTER TABLE token_transfers
    MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
