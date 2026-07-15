SELECT
    tupleElement(event, 1) AS event_id,
    token,
    holder,
    tupleElement(event, 2) AS min_block,
    tupleElement(event, 3) AS max_block,
    CAST(-1 AS Int8) AS sign
FROM token_balance_checkpoints
ARRAY JOIN dirty_events AS event
