-- Async background mutation; existing parts gain the index progressively.
ALTER TABLE blocks
    MATERIALIZE INDEX idx_hash;
