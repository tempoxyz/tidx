-- Decodes the union of `OrderPlaced(uint128 indexed orderId,
-- address indexed maker, address indexed token, uint128 amount, bool isBid,
-- int16 tick, bool isFlipOrder, int16 flipTick)` (selector 0xc200d837…) and
-- `OrderFlipped(uint128 indexed orderId, address indexed maker,
-- address indexed token, uint128 amount, bool isBid, int16 tick,
-- int16 flipTick)` (selector 0x37a42d10…) from the raw `logs` stream.
--
-- Both events share the layout of the words read here: orderId/maker/token are
-- indexed topics; `data` word0 is `amount` (low 16 bytes, hex 35..66), word1 is
-- `isBid` (last byte, hex 129..130), word2 is `tick` (int16, sign-extended,
-- last 2 bytes, hex 191..194). int16s read the trailing 2 bytes little-endian
-- so two's-complement negatives decode correctly; the `OrderPlaced`-only tail
-- words (`isFlipOrder`, `flipTick`) are not read, so the shorter `OrderFlipped`
-- payload (4 data words) decodes through the same projection as `OrderPlaced`.
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
    if(
        selector = '0xc200d837816d02c5ee9bf081cba1a32ab1482de7a738b41c0b357186b0b998cd',
        'placed',
        'flipped'
    ) AS eventType
FROM logs
WHERE
    selector IN (
        '0xc200d837816d02c5ee9bf081cba1a32ab1482de7a738b41c0b357186b0b998cd',
        '0x37a42d10bbce3e94e109a6a44e4479f0ee45dd6ecc6ca902168ea58e01ba32fe'
    )
    AND address = '0xdec0000000000000000000000000000000000000'
    AND topic1 IS NOT NULL
    AND topic2 IS NOT NULL
    AND topic3 IS NOT NULL
    AND length(topic1) >= 66
    AND length(topic2) >= 66
    AND length(topic3) >= 66
    AND length(data) >= 194
