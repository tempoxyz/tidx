-- Point lookup: transaction by hash with a block bound (serving-layer contract
-- shape from the tiering evaluation; kept for cross-experiment comparability).
\set r random(1, :n_txh)
SELECT t.*
FROM bench.tx_hashes s
JOIN txs t
  ON t.hash = s.hash
 AND t.block_num BETWEEN s.block_num - 1000 AND s.block_num
WHERE s.id = :r;
