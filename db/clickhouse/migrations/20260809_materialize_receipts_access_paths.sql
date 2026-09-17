-- Async background mutations; existing parts gain the access paths progressively.
ALTER TABLE receipts
    MATERIALIZE INDEX idx_to,
    MATERIALIZE PROJECTION prj_tx_hash,
    MATERIALIZE PROJECTION prj_fee_payer_position;
