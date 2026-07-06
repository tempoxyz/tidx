-- Keyset page of transactions descending from a random block.
\set b random(:min_block, :max_block)
SELECT block_num, idx, hash, "from", "to", value, gas_used
FROM txs
WHERE block_num <= :b
ORDER BY block_num DESC, idx DESC
LIMIT 100;
