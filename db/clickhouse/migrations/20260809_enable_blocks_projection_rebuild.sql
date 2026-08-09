ALTER TABLE blocks
    MODIFY SETTING
        deduplicate_merge_projection_mode = 'rebuild';
