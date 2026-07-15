-- Advance only holder checkpoints touched since the previous refresh.
--
-- The normal paths read deltas strictly above or below the checkpointed range,
-- which supports both realtime writes and reverse historical backfill. If a
-- retry, gap fill, or reorg overlaps that range, only the affected holder is
-- rebuilt from its canonical FINAL ledger. Publishing the acknowledged event
-- tuples causes token_balance_dirty_events_clean_mv to collapse completed work.
CREATE MATERIALIZED VIEW IF NOT EXISTS token_balance_checkpoint_refresh
REFRESH AFTER 1 MINUTE APPEND TO token_balance_checkpoints
AS
WITH
    pending AS (
        SELECT
            token,
            holder,
            min(min_block) AS min_changed_block,
            max(max_block) AS max_changed_block,
            groupArray((event_id, min_block, max_block)) AS dirty_events
        FROM token_balance_dirty_events FINAL
        WHERE sign = 1
        GROUP BY token, holder
    ),
    current AS (
        SELECT
            token,
            holder,
            credited,
            debited,
            checkpoint_from_block,
            checkpoint_block,
            CAST(1 AS UInt8) AS has_checkpoint
        FROM token_balance_checkpoints FINAL
        WHERE (token, holder) IN (SELECT token, holder FROM pending)
    ),
    work AS (
        SELECT
            pending.token AS token,
            pending.holder AS holder,
            pending.min_changed_block AS min_changed_block,
            pending.max_changed_block AS max_changed_block,
            pending.dirty_events AS dirty_events,
            current.credited AS checkpoint_credited,
            current.debited AS checkpoint_debited,
            current.checkpoint_from_block AS checkpoint_from_block,
            current.checkpoint_block AS checkpoint_block,
            current.has_checkpoint AS has_checkpoint,
            current.has_checkpoint = 1
                AND pending.min_changed_block > current.checkpoint_block AS extends_forward,
            current.has_checkpoint = 1
                AND pending.max_changed_block < current.checkpoint_from_block AS extends_backward,
            multiIf(
                current.has_checkpoint = 1
                    AND pending.min_changed_block > current.checkpoint_block,
                current.checkpoint_block,
                current.has_checkpoint = 1
                    AND pending.max_changed_block < current.checkpoint_from_block,
                pending.min_changed_block - 1,
                CAST(-1 AS Int64)
            ) AS scan_from_exclusive,
            multiIf(
                current.has_checkpoint = 1
                    AND pending.min_changed_block > current.checkpoint_block,
                pending.max_changed_block,
                current.has_checkpoint = 1
                    AND pending.max_changed_block < current.checkpoint_from_block,
                current.checkpoint_from_block - 1,
                greatest(current.checkpoint_block, pending.max_changed_block)
            ) AS scan_to_inclusive
        FROM pending
        LEFT JOIN current USING (token, holder)
    ),
    ledger AS (
        SELECT
            work.token AS token,
            work.holder AS holder,
            sumIf(token_holder_deltas.balance_delta, token_holder_deltas.leg = 1) AS credited,
            sumIf(token_holder_deltas.balance_delta, token_holder_deltas.leg = -1) AS debited
        FROM (
            SELECT token, holder, block_num, leg, balance_delta
            FROM token_holder_deltas FINAL
            WHERE (token, holder) IN (SELECT token, holder FROM pending)
        ) AS token_holder_deltas
        INNER JOIN work
            ON token_holder_deltas.token = work.token
            AND token_holder_deltas.holder = work.holder
            AND token_holder_deltas.block_num > work.scan_from_exclusive
            AND token_holder_deltas.block_num <= work.scan_to_inclusive
        GROUP BY work.token, work.holder
    )
SELECT
    work.token AS token,
    work.holder AS holder,
    if(
        work.extends_forward OR work.extends_backward,
        work.checkpoint_credited + ledger.credited,
        ledger.credited
    ) AS credited,
    if(
        work.extends_forward OR work.extends_backward,
        work.checkpoint_debited + ledger.debited,
        ledger.debited
    ) AS debited,
    multiIf(
        work.has_checkpoint = 0,
        work.min_changed_block,
        work.extends_backward,
        work.min_changed_block,
        least(work.checkpoint_from_block, work.min_changed_block)
    ) AS checkpoint_from_block,
    multiIf(
        work.has_checkpoint = 0,
        work.max_changed_block,
        work.extends_forward,
        work.max_changed_block,
        greatest(work.checkpoint_block, work.max_changed_block)
    ) AS checkpoint_block,
    work.dirty_events AS dirty_events,
    now64(9, 'UTC') AS version
FROM work
LEFT JOIN ledger USING (token, holder)
SETTINGS
    max_threads = 8,
    max_memory_usage = 34359738368,
    max_bytes_before_external_group_by = 2000000000
