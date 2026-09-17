-- Async background mutation; existing parts gain the index progressively.
ALTER TABLE logs
    MATERIALIZE INDEX idx_block_num;
