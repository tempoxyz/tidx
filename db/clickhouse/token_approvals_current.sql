CREATE VIEW IF NOT EXISTS token_approvals_current AS
SELECT
    token,
    owner,
    spender,
    argMax(amount,          (block_num, log_idx)) AS amount,
    argMax(block_num,       (block_num, log_idx)) AS last_block_num,
    argMax(block_timestamp, (block_num, log_idx)) AS last_block_timestamp,
    argMax(tx_hash,         (block_num, log_idx)) AS last_tx_hash
FROM token_approvals FINAL
GROUP BY token, owner, spender
HAVING amount > 0
