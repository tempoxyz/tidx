-- Decodes TIP-1028 `ReceiptClaimed(address indexed token,
-- address indexed receiver, uint64 indexed blockedNonce, uint64 blockedAt,
-- uint8 receiptVersion, address originator, address recipient,
-- address recoveryAuthority, address caller, address to, uint256 amount)`
-- emitted by the ReceivePolicyGuard precompile when a held receipt is claimed.
--
-- token/receiver/blockedNonce are indexed (topics). The remaining params are
-- static ABI words in `data` (hex-char offsets, 1-indexed, skipping '0x'):
--   word0 blockedAt      uint64  (last 8 bytes, hex 51..66)
--   word1 receiptVersion uint8   (last byte,   hex 129..130)
--   word2 originator     address (last 20 bytes, hex 155..194)
--   word3 recipient      address (hex 219..258)
--   word4 recoveryAuth   address (hex 283..322)
--   word5 caller         address (hex 347..386)
--   word6 to             address (hex 411..450)
--   word7 amount         uint256 (hex 451..514)
SELECT
    block_num,
    block_timestamp,
    tx_idx,
    log_idx,
    tx_hash,
    address,
    concat('0x', lower(substring(topic1, 27))) AS token,
    concat('0x', lower(substring(topic2, 27))) AS receiver,
    reinterpretAsUInt64(reverse(unhex(substring(topic3, 51, 16)))) AS blocked_nonce,
    reinterpretAsUInt64(reverse(unhex(substring(data, 51, 16)))) AS blocked_at,
    reinterpretAsUInt8(unhex(substring(data, 129, 2))) AS receipt_version,
    concat('0x', lower(substring(data, 155, 40))) AS originator,
    concat('0x', lower(substring(data, 219, 40))) AS recipient,
    concat('0x', lower(substring(data, 283, 40))) AS recovery_authority,
    concat('0x', lower(substring(data, 347, 40))) AS caller,
    concat('0x', lower(substring(data, 411, 40))) AS `to`,
    reinterpretAsUInt256(reverse(unhex(substring(data, 451, 64)))) AS amount
FROM logs
WHERE
    selector = '0xdfa88f3774430fdb1d282219332a663236ccc8035ba8b9e0df856b374a5db085'
    AND address = '0xb10c000000000000000000000000000000000000'
    AND topic1 IS NOT NULL
    AND topic2 IS NOT NULL
    AND topic3 IS NOT NULL
    AND length(topic1) >= 66
    AND length(topic2) >= 66
    AND length(topic3) >= 66
    AND length(data) >= 514
