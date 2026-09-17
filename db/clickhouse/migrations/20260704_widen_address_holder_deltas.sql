ALTER TABLE address_holder_deltas
    MODIFY COLUMN balance_delta UInt256
    SETTINGS mutations_sync = 1
