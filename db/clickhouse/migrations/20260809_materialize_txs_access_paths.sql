-- Async background mutation; existing parts gain the access paths progressively.
ALTER TABLE txs
    MATERIALIZE INDEX idx_from_nonce_key_nonce,
    MATERIALIZE PROJECTION prj_from_position,
    MATERIALIZE PROJECTION prj_to_position,
    MATERIALIZE PROJECTION prj_fee_payer_position,
    MATERIALIZE PROJECTION prj_fee_token_position;
