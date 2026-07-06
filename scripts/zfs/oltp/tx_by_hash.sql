-- Point lookup: transaction by hash (unbounded — pure idx_txs_hash probe).
\set r random(1, :n_txh)
SELECT t.*
FROM txs t
WHERE t.hash = (SELECT hash FROM bench.tx_hashes WHERE id = :r);
