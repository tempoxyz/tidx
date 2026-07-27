CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_txs_fee_payer_block
ON txs (fee_payer, block_num DESC, idx DESC)
WHERE fee_payer IS NOT NULL;
