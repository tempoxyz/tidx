SELECT
    block_num,
    block_timestamp,
    tx_hash,
    log_idx,
    tupleElement(leg_tuple, 1) AS holder,
    token,
    tupleElement(leg_tuple, 2) AS leg,
    tupleElement(leg_tuple, 3) AS balance_delta
FROM token_transfers
ARRAY JOIN
    [
        (`to`,   CAST(1 AS Int8),  CAST(amount AS Int256)),
        (`from`, CAST(-1 AS Int8), -CAST(amount AS Int256))
    ] AS leg_tuple
WHERE tupleElement(leg_tuple, 1) NOT IN (
    '0x0000000000000000000000000000000000000000',
    -- TIP-1028 ReceivePolicyGuard: blocked transfers/mints credit this
    -- precompile instead of the intended receiver, and claims/burns debit it.
    -- It is not a real holder, so neither leg should affect holder balances.
    '0xb10c000000000000000000000000000000000000'
)
