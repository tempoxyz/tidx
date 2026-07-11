-- Async background mutation; existing parts gain the index progressively.
ALTER TABLE receipts
    MATERIALIZE INDEX idx_contract_address;
