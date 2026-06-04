-- Decodes `OrderFilled(uint128 indexed orderId, address indexed maker,
-- address indexed taker, uint128 amountFilled, bool partialFill)` from the raw
-- `logs` stream.
--
-- orderId/maker/taker are indexed (topics). The two remaining params are ABI
-- words in `data` after the `0x` prefix:
--   word0 amountFilled hex 3..66   (value in the low 16 bytes, 35..66)
--   word1 partialFill  hex 67..130 (bool in the last byte, 129..130)
--
-- The pair (`token`, `isBid`, `tick`) is not on this event; resolve it by
-- joining `orderId` to `dex_orders`.
SELECT
    block_num,
    block_timestamp,
    tx_idx,
    log_idx,
    tx_hash,
    address,
    reinterpretAsUInt256(reverse(unhex(substring(topic1, 3, 64)))) AS orderId,
    concat('0x', lower(substring(topic2, 27))) AS maker,
    concat('0x', lower(substring(topic3, 27))) AS taker,
    reinterpretAsUInt256(reverse(unhex(substring(data, 3, 64)))) AS amountFilled,
    reinterpretAsUInt8(unhex(substring(data, 129, 2))) AS partialFill
FROM logs
WHERE
    selector = '0x16c08f8f2c17b3c8879b3e3cf5efdbdcdfdbd0fcb3890f9d3086f470cd601ddd'
    AND topic1 IS NOT NULL
    AND topic2 IS NOT NULL
    AND topic3 IS NOT NULL
    AND length(topic1) >= 66
    AND length(topic2) >= 66
    AND length(topic3) >= 66
    AND length(data) >= 130
