ALTER TABLE tidx_schema_objects
    DELETE WHERE name = 'balance_state_20260714_bootstrap'
    SETTINGS mutations_sync = 1
