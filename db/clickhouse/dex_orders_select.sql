-- Decodes `OrderPlaced(uint128 indexed orderId, address indexed maker,
-- address indexed token, uint128 amount, bool isBid, int16 tick,
-- bool isFlipOrder, int16 flipTick)` from the raw `logs` stream.
--
-- orderId/maker/token are indexed (topics). The remaining five params are ABI
-- words in `data`, each right-aligned in a 32-byte (64-hex) slot after the
-- `0x` prefix:
--   word0 amount      hex 3..66    (value in the low 16 bytes, 35..66)
--   word1 isBid       hex 67..130  (bool in the last byte, 129..130)
--   word2 tick        hex 131..194 (int16, sign-extended; last 2 bytes 191..194)
--   word3 isFlipOrder hex 195..258 (bool in the last byte, 257..258)
--   word4 flipTick    hex 259..322 (int16; last 2 bytes 319..322)
--
-- UInt256 amounts reuse the full-word `reinterpretAsUInt256(reverse(unhex(...)))`
-- pattern from token_transfers (the high bytes of a uint128 are zero). int16s
-- read the trailing 2 bytes little-endian so two's-complement negatives decode
-- correctly. bool reads the single trailing byte.
SELECT
    block_num,
    block_timestamp,
    tx_idx,
    log_idx,
    tx_hash,
    address,
    reinterpretAsUInt256(reverse(unhex(substring(topic1, 3, 64)))) AS orderId,
    concat('0x', lower(substring(topic2, 27))) AS maker,
    concat('0x', lower(substring(topic3, 27))) AS token,
    reinterpretAsUInt256(reverse(unhex(substring(data, 3, 64)))) AS amount,
    reinterpretAsUInt8(unhex(substring(data, 129, 2))) AS isBid,
    reinterpretAsInt16(reverse(unhex(substring(data, 191, 4)))) AS tick,
    reinterpretAsUInt8(unhex(substring(data, 257, 2))) AS isFlipOrder,
    reinterpretAsInt16(reverse(unhex(substring(data, 319, 4)))) AS flipTick
FROM logs
WHERE
    selector = '0xc200d837816d02c5ee9bf081cba1a32ab1482de7a738b41c0b357186b0b998cd'
    AND topic1 IS NOT NULL
    AND topic2 IS NOT NULL
    AND topic3 IS NOT NULL
    AND length(topic1) >= 66
    AND length(topic2) >= 66
    AND length(topic3) >= 66
    AND length(data) >= 322
