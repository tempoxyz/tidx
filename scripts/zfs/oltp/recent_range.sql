-- Aggregate over the most recent 100 blocks.
SELECT count(*), sum(gas_used)
FROM txs
WHERE block_num > :max_block - 100;
