-- Recent logs for a random known address (idx_logs_address).
\set r random(1, :n_addr)
SELECT l.block_num, l.log_idx, l.topic0, l.data
FROM logs l
WHERE l.address = (SELECT addr FROM bench.addresses WHERE id = :r)
ORDER BY l.block_timestamp DESC
LIMIT 100;
