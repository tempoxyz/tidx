-- Async background mutation; existing parts gain the indexes progressively.
ALTER TABLE txs
    MATERIALIZE INDEX idx_from,
    MATERIALIZE INDEX idx_to;
