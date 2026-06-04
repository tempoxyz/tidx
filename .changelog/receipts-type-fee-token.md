---
tidx: patch
---

Denormalized the tx-level `type` and `fee_token` onto the ClickHouse `receipts` table (populated from the matching tx at ingest, with a migration for existing deployments), so receipt-list queries no longer have to join `txs` to read those fields.
