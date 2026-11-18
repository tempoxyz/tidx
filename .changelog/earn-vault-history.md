---
tidx: minor
---

Added ClickHouse tables for holder balance history and Earn vault positions: `address_balance_hours` (net balance change per holder, token and UTC hour), `earn_vaults` (vaults from their deployment events), `earn_flows` (decoded deposit and redemption events) and `earn_vault_price_hours` (hourly `previewRedeem(1e18)`, written and backfilled by a new RPC price sampler with lag metrics).
