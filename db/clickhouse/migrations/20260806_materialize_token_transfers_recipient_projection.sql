-- Async background mutation; existing parts gain the projection progressively.
ALTER TABLE token_transfers
    MATERIALIZE PROJECTION by_recipient;
