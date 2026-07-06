-- Point lookup: receipt by tx hash (idx_receipts_tx_hash).
\set r random(1, :n_txh)
SELECT r.*
FROM receipts r
WHERE r.tx_hash = (SELECT hash FROM bench.tx_hashes WHERE id = :r);
