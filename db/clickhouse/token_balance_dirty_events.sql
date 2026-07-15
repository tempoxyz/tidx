-- Durable work queue for token/holder pairs whose checkpoint needs advancing.
--
-- Each source insert creates one positive event per affected pair. After a
-- checkpoint update is published, token_balance_dirty_events_clean_mv writes
-- the matching negative event. CollapsingMergeTree removes completed work
-- while preserving every concurrent/retried insert until it is acknowledged.
CREATE TABLE IF NOT EXISTS token_balance_dirty_events (
    event_id  UUID,
    token     String,
    holder    String,
    min_block Int64,
    max_block Int64,
    sign      Int8
) ENGINE = CollapsingMergeTree(sign)
ORDER BY (token, holder, event_id)
SETTINGS default_compression_codec = 'ZSTD(1)'
