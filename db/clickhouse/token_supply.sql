CREATE VIEW IF NOT EXISTS token_supply AS
SELECT
    token,
    sum(
        if(`from` = '0x0000000000000000000000000000000000000000', toInt256(amount), toInt256(0))
      - if(`to` = '0x0000000000000000000000000000000000000000', toInt256(amount), toInt256(0))
    ) AS supply
FROM token_transfers FINAL
GROUP BY token
HAVING supply > 0
