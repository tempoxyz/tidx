-- Decodes TIP-1028 `ReceivePolicyUpdated(address indexed account,
-- uint64 senderPolicyId, uint64 tokenFilterId, address recoveryAuthority)`
-- emitted by the TIP-403 Policy Registry precompile when an account changes its
-- receive policy.
--
-- account is indexed (topic1). The non-indexed params are static ABI words in
-- `data` (hex-char offsets, 1-indexed, skipping '0x'):
--   word0 senderPolicyId uint64  (last 8 bytes, hex 51..66)
--   word1 tokenFilterId  uint64  (last 8 bytes, hex 115..130)
--   word2 recoveryAuth   address (last 20 bytes, hex 155..194)
SELECT
    block_num,
    block_timestamp,
    tx_idx,
    log_idx,
    tx_hash,
    address,
    concat('0x', lower(substring(topic1, 27))) AS account,
    reinterpretAsUInt64(reverse(unhex(substring(data, 51, 16)))) AS sender_policy_id,
    reinterpretAsUInt64(reverse(unhex(substring(data, 115, 16)))) AS token_filter_id,
    concat('0x', lower(substring(data, 155, 40))) AS recovery_authority
FROM logs
WHERE
    selector = '0xf0d46e7e04f2bf4cc56ea683299f4145c2650ef690e276e069bc2b806d68b2ea'
    AND address = '0x403c000000000000000000000000000000000000'
    AND topic1 IS NOT NULL
    AND length(topic1) >= 66
    AND length(data) >= 194
