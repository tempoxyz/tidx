CREATE TABLE IF NOT EXISTS receipts (
    block_num               INT8 NOT NULL,
    block_timestamp         TIMESTAMPTZ NOT NULL,
    tx_idx                  INT4 NOT NULL,
    tx_hash                 BYTEA NOT NULL,
    "from"                  BYTEA NOT NULL,
    "to"                    BYTEA,
    contract_address        BYTEA,
    gas_used                INT8 NOT NULL,
    cumulative_gas_used     INT8 NOT NULL,
    effective_gas_price     TEXT,
    status                  INT2,
    fee_payer               BYTEA,
    PRIMARY KEY (block_timestamp, block_num, tx_idx)
);

CREATE INDEX IF NOT EXISTS idx_receipts_tx_hash ON receipts (tx_hash);
CREATE INDEX IF NOT EXISTS idx_receipts_block_num ON receipts (block_num DESC);
DROP INDEX IF EXISTS idx_receipts_block_num_asc;
CREATE INDEX IF NOT EXISTS idx_receipts_from ON receipts ("from", block_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_receipts_fee_payer ON receipts (fee_payer, block_timestamp DESC) WHERE fee_payer IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_receipts_contract_address ON receipts (contract_address) WHERE contract_address IS NOT NULL;
