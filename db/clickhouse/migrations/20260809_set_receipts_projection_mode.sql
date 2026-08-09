ALTER TABLE receipts
    MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild',
        allow_nullable_key = 1;
