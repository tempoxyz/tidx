-- Keyset page of logs descending from a random block.
\set b random(:min_block, :max_block)
SELECT block_num, log_idx, address, topic0, data
FROM logs
WHERE block_num <= :b
ORDER BY block_num DESC, log_idx DESC
LIMIT 100;
