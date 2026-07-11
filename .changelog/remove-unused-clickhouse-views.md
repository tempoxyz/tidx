---
tidx: major
---

Removed ClickHouse analytics unused by Tempo API to reclaim derived storage.

```diff
- Query address_transfers, address_txs, contract_creations, token_approvals, or token_approvals_current.
+ Query Transfer with signature=Transfer(address indexed from,address indexed to,uint256 value) through the tiered engine; reconstruct others from txs, receipts, and logs.
```
