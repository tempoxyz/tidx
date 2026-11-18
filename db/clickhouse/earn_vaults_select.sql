-- Decodes Earn vault deployments from the raw `logs` stream.
--
-- Two deployment event layouts share the same indexed fields and leading data
-- words:
--   0x420632dc… EarnStackDeployed(address indexed earnVault,
--     address indexed earnShare, address indexed earnFees, address engine,
--     address asset, address owner, bytes32 deploymentId, …) — 11 data words
--   0x950acdb9… the later deployment event layout — 13 data words
-- topic1 earnVault, topic2 earnShare, topic3 earnFees; data word0 engine,
-- word1 asset, word2 owner. Addresses sit in the low 20 bytes of each word.
--
-- Any contract can emit these logs, so `factory` records the emitter and
-- readers filter on the factories they trust.
SELECT
    block_num AS deployed_block,
    block_timestamp AS deployed_at,
    tx_hash,
    log_idx,
    lower(address) AS factory,
    selector,
    concat('0x', lower(substring(ifNull(topic1, ''), 27, 40))) AS vault,
    concat('0x', lower(substring(ifNull(topic2, ''), 27, 40))) AS share_token,
    concat('0x', lower(substring(ifNull(topic3, ''), 27, 40))) AS fees,
    concat('0x', lower(substring(data, 27, 40))) AS engine,
    concat('0x', lower(substring(data, 91, 40))) AS asset,
    concat('0x', lower(substring(data, 155, 40))) AS owner
FROM logs
WHERE
    selector IN (
        '0x420632dca5c7b108ec3fe8f96f06644917f70c8682f8f66cadc3b03e1fe9301d',
        '0x950acdb981ae9ee0189b7bba6d347b4fb9b24f0e4f630a1b9a388bc5702fd67b'
    )
    AND length(ifNull(topic1, '')) = 66
    AND length(ifNull(topic2, '')) = 66
    AND length(ifNull(topic3, '')) = 66
    AND length(data) >= 194
