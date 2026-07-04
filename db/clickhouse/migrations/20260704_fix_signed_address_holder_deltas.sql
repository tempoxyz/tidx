ALTER TABLE address_holder_deltas
    UPDATE balance_delta = -balance_delta
    WHERE leg = -1 AND balance_delta < 0
    SETTINGS mutations_sync = 1
