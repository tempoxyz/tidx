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
    -- Fixed head (amount + receiptVersion + receipt offset word) plus the
    -- dynamic receipt length word must be present: '0x' (2) + 4 words * 64.
    AND length(data) >= 258
    -- `receipt` is the only dynamic field after three static head words, so a
    -- well-formed payload always encodes its ABI offset as 96 (0x60). Pin it:
    -- this rejects corrupt offset words that would otherwise push the substring
    -- reads below out of range, and lets us read the length word at its fixed
    -- position (hex 195..258 = 3 + 96*2) instead of chasing a hostile offset.
    -- The offset/length below are read as UInt64 (low 8 bytes of each 32-byte
    -- ABI word), so require the high 24 bytes (48 hex chars) to be zero; that
    -- makes the UInt64 read sound and rejects words whose high bits are set
    -- (e.g. a length of 2^64 + n that would otherwise slip through as n).
    AND substring(data, 131, 48) = repeat('0', 48)
    AND reinterpretAsUInt64(reverse(unhex(substring(data, 131, 64)))) = 96
    AND substring(data, 195, 48) = repeat('0', 48)
    -- ...and the full declared receipt payload must fit. The length word only
    -- proves how many bytes the receipt *claims*; without this a truncated row
    -- with a nonzero length would pass the checks above and store a receipt
    -- silently shortened by substring(). Required hex chars: '0x' (2) + head
    -- (192) + length word (64) = 258, plus payload (receipt_length * 2).
    -- UInt128 arithmetic so a large (but high-bits-zero) length can't overflow
    -- the comparison and wrap to a small value that lets a truncated row through.
    AND toUInt128(length(data)) >= 258
        + toUInt128(reinterpretAsUInt64(reverse(unhex(substring(data, 195, 64))))) * 2
