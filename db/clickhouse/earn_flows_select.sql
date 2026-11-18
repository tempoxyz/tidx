-- Decodes Earn vault deposit and redemption events from the raw `logs`
-- stream, one row per event. Amounts are the raw unsigned event values;
-- readers apply signs by `kind`.
--
-- kind                    event: topics | data words
--                         account, assets, shares
-- deposited               Deposited: caller, receiver | assets, earnShares
--                         receiver, assets, earnShares
-- redeemed                Redeemed: caller, receiver | earnShares, assets
--                         receiver, assets, earnShares
-- withdrew_exact          WithdrewExact: caller, receiver | assets, earnSharesBurned
--                         receiver, assets, earnSharesBurned
-- redeem_requested        RedeemRequested: requestId, requester, receiver | earnShares
--                         requester, 0, earnShares
-- redeem_finalized        RedeemFinalized: requestId, receiver | earnShares, asset, assets
--                         receiver, assets, earnShares
-- redeem_cancelled        RedeemCancelled: requestId, receiver | earnShares
--                         receiver, 0, earnShares
-- venue_shares_deposited  VenueSharesDeposited: caller, receiver |
--                           requestedVenueShares, receivedVenueShares, earnShares
--                         receiver, receivedVenueShares (venue share units), earnShares
--
-- `account` is topic2 for every event. `caller` is topic1 for the four
-- caller events and '' otherwise; `request_id` is the bytes32 topic1 of the
-- three request events and '' otherwise. Any contract can emit these logs:
-- readers filter `vault` to the vaults they trust (for example earn_vaults).
SELECT
    block_num,
    block_timestamp,
    tx_hash,
    log_idx,
    lower(address) AS vault,
    concat('0x', lower(substring(ifNull(topic2, ''), 27, 40))) AS account,
    multiIf(
        selector = '0xf5681f9d0db1b911ac18ee83d515a1cf1051853a9eae418316a2fdf7dea427c5', 'deposited',
        selector = '0x5cdf07ad0fc222442720b108e3ed4c4640f0fadc2ab2253e66f259a0fea83480', 'redeemed',
        selector = '0x667b1fd1a4d0290e6423170325223d7815bf90da8b978f2ad2001b314ac49c12', 'withdrew_exact',
        selector = '0xaa7dc873a7d37b94883eae5b185beafe0223de340014bc7a1690496363d5bb28', 'redeem_requested',
        selector = '0xd638999d611edc2680566b9afcd2c3ab1af17bf7438f625503928b75bd39e3cb', 'redeem_finalized',
        selector = '0xba481b3d46265a658a5106b2462d1fe3c6c50c858b33e5548f598073326b23ab', 'redeem_cancelled',
        'venue_shares_deposited'
    ) AS kind,
    if(
        kind IN ('deposited', 'redeemed', 'withdrew_exact', 'venue_shares_deposited'),
        concat('0x', lower(substring(ifNull(topic1, ''), 27, 40))),
        ''
    ) AS caller,
    if(
        kind IN ('redeem_requested', 'redeem_finalized', 'redeem_cancelled'),
        lower(ifNull(topic1, '')),
        ''
    ) AS request_id,
    toInt256(multiIf(
        kind IN ('deposited', 'withdrew_exact'), reinterpretAsUInt256(reverse(unhex(substring(data, 3, 64)))),
        kind IN ('redeemed', 'venue_shares_deposited'), reinterpretAsUInt256(reverse(unhex(substring(data, 67, 64)))),
        kind = 'redeem_finalized', reinterpretAsUInt256(reverse(unhex(substring(data, 131, 64)))),
        toUInt256(0)
    )) AS assets,
    toInt256(multiIf(
        kind IN ('deposited', 'withdrew_exact'), reinterpretAsUInt256(reverse(unhex(substring(data, 67, 64)))),
        kind = 'venue_shares_deposited', reinterpretAsUInt256(reverse(unhex(substring(data, 131, 64)))),
        reinterpretAsUInt256(reverse(unhex(substring(data, 3, 64))))
    )) AS shares
FROM logs
WHERE
    length(ifNull(topic1, '')) = 66
    AND length(ifNull(topic2, '')) = 66
    AND (
        (
            selector IN (
                '0xf5681f9d0db1b911ac18ee83d515a1cf1051853a9eae418316a2fdf7dea427c5',
                '0x5cdf07ad0fc222442720b108e3ed4c4640f0fadc2ab2253e66f259a0fea83480',
                '0x667b1fd1a4d0290e6423170325223d7815bf90da8b978f2ad2001b314ac49c12'
            )
            AND length(data) >= 130
        )
        OR (
            selector = '0xaa7dc873a7d37b94883eae5b185beafe0223de340014bc7a1690496363d5bb28'
            AND length(ifNull(topic3, '')) = 66
            AND length(data) >= 66
        )
        OR (
            selector IN (
                '0xd638999d611edc2680566b9afcd2c3ab1af17bf7438f625503928b75bd39e3cb',
                '0x35b15b3f988e6b5a7c3c72c7f44307862e2a93754541f8c81992b9365997bbc0'
            )
            AND length(data) >= 194
        )
        OR (
            selector = '0xba481b3d46265a658a5106b2462d1fe3c6c50c858b33e5548f598073326b23ab'
            AND length(data) >= 66
        )
    )
