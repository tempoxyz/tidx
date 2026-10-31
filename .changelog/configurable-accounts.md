---
tidx: minor
---

Indexed blocks that contain Tempo configurable-account transactions (TIP-1107). Such blocks previously failed to decode (`data did not match any variant of untagged enum BlockTransactions`) and stopped sync. Pinned the Tempo crates to tempoxyz/tempo `main` @ `b6d1615`, which includes configurable-account signatures (tempoxyz/tempo#8750), until a Tempo release contains them. `txs.signature_type` records configurable-account signatures, sent directly or as an access key, as `3`.
