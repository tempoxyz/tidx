-- Async background mutation; existing parts gain the indexes progressively.
ALTER TABLE txs
    MATERIALIZE INDEX idx_fee_payer,
    MATERIALIZE INDEX idx_fee_token;
