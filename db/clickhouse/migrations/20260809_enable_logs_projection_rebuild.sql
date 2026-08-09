ALTER TABLE logs
    MODIFY SETTING
        deduplicate_merge_projection_mode = 'rebuild';
