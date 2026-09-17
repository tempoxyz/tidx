-- Address-keyed counterpart of 20260618_delete_guard_token_holder_deltas.sql.
ALTER TABLE address_holder_deltas
    DELETE WHERE holder = '0xb10c000000000000000000000000000000000000'
    SETTINGS mutations_sync = 1
