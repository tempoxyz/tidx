-- Async background mutation; existing parts gain the indexes progressively.
ALTER TABLE receipts
    MATERIALIZE INDEX idx_tx_hash,
    MATERIALIZE INDEX idx_from,
    MATERIALIZE INDEX idx_fee_payer;
