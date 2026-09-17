CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_receipts_fee_payer_block
ON receipts (fee_payer, block_num DESC, tx_idx DESC)
WHERE fee_payer IS NOT NULL;
