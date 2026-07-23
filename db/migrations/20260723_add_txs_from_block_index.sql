CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_txs_from_block
ON txs ("from", block_num DESC, idx DESC);
