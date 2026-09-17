-- Async background mutation; existing parts gain the index progressively.
ALTER TABLE txs
    MATERIALIZE INDEX idx_hash;
