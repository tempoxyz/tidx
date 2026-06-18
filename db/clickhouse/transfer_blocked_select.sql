-- Decodes TIP-1028 `TransferBlocked(address indexed token,
-- address indexed receiver, uint64 indexed blockedNonce, uint256 amount,
-- uint8 receiptVersion, bytes receipt)` emitted by the ReceivePolicyGuard
-- precompile when an inbound transfer/mint is held instead of credited.
--
-- token/receiver/blockedNonce are indexed (topics). The non-indexed tail is
-- ABI-encoded in `data` (hex-char offsets below are 1-indexed and skip '0x'):
--   word0 amount         hex 3..66
--   word1 receiptVersion hex 67..130 (uint8 in last byte, 129..130)
--   word2 receipt offset hex 131..194 (byte offset of the dynamic `bytes`)
-- The `receipt` length word sits at that offset and the payload follows it, so
-- decode it from the offset/length words rather than assuming a fixed layout.
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
    reinterpretAsUInt256(reverse(unhex(substring(data, 3, 64)))) AS amount,
    reinterpretAsUInt8(unhex(substring(data, 129, 2))) AS receipt_version,
    concat('0x', lower(substring(
        data,
        -- payload starts one word (64 hex chars) past the length word, which
        -- itself sits at the byte offset encoded in word2.
        3 + reinterpretAsUInt64(reverse(unhex(substring(data, 131, 64)))) * 2 + 64,
        reinterpretAsUInt64(reverse(unhex(substring(
            data,
            3 + reinterpretAsUInt64(reverse(unhex(substring(data, 131, 64)))) * 2,
            64
        )))) * 2
    ))) AS receipt
FROM logs
WHERE
    selector = '0x361d86e46fd139dc3eac4148f16b53597f0f8ddd9aba772aae0034bda5531b1c'
    AND address = '0xb10c000000000000000000000000000000000000'
    AND topic1 IS NOT NULL
    AND topic2 IS NOT NULL
    AND topic3 IS NOT NULL
    AND length(topic1) >= 66
    AND length(topic2) >= 66
    AND length(topic3) >= 66
    AND length(data) >= 258
