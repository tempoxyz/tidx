-- Transactions decoded without receipts retain a NULL gas_used value until
-- receipt backfill enriches them. Keep that sparse repair queue indexable
-- without scanning all historical transactions.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_txs_missing_receipt
    ON txs (block_num DESC)
    WHERE gas_used IS NULL;
