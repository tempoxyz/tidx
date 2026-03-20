CREATE TABLE IF NOT EXISTS logs (
    block_num       INT8 NOT NULL,
    block_timestamp TIMESTAMPTZ NOT NULL,
    log_idx         INT4 NOT NULL,
    tx_idx          INT4 NOT NULL,
    tx_hash         BYTEA NOT NULL,
    address         BYTEA NOT NULL,
    selector        BYTEA,
    topic0          BYTEA,
    topic1          BYTEA,
    topic2          BYTEA,
    topic3          BYTEA,
    data            BYTEA NOT NULL,
    PRIMARY KEY (block_timestamp, block_num, log_idx)
);

CREATE INDEX IF NOT EXISTS idx_logs_block_num ON logs (block_num DESC);
DROP INDEX IF EXISTS idx_logs_block_num_asc;
CREATE INDEX IF NOT EXISTS idx_logs_tx_hash ON logs (tx_hash);
CREATE INDEX IF NOT EXISTS idx_logs_selector ON logs (selector, block_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_logs_address ON logs (address, block_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_logs_address_topic1 ON logs (topic1, address, block_num DESC);
DROP INDEX IF EXISTS idx_logs_topic1;
CREATE INDEX IF NOT EXISTS idx_logs_topic2 ON logs (topic2);
CREATE INDEX IF NOT EXISTS idx_logs_topic3 ON logs (topic3);
