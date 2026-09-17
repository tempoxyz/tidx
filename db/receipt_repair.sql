-- Durable, bounded work queue for transactions decoded without receipts.
--
-- New incomplete transaction batches populate this table in the same
-- PostgreSQL transaction as `txs`. A one-time bounded discovery cursor covers
-- legacy rows without a full-table anti-join or a heavyweight index build.
CREATE TABLE IF NOT EXISTS receipt_repair_queue (
    block_num          INT8 PRIMARY KEY,
    block_timestamp    TIMESTAMPTZ NOT NULL,
    attempts           INT4 NOT NULL DEFAULT 0,
    next_attempt_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error         TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_receipt_repair_queue_due
    ON receipt_repair_queue (next_attempt_at, block_num DESC);

CREATE INDEX IF NOT EXISTS idx_receipt_repair_queue_timestamp
    ON receipt_repair_queue (block_timestamp);

CREATE TABLE IF NOT EXISTS receipt_repair_discovery (
    chain_id           INT8 PRIMARY KEY,
    next_block         INT8,
    completed          BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
