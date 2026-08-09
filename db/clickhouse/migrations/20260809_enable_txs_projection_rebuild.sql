ALTER TABLE txs
    MODIFY SETTING
        deduplicate_merge_projection_mode = 'rebuild';
