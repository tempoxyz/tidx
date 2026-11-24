---
tidx: minor
---

Indexed blocks that contain Tempo configurable-account transactions (TIP-1107). Such blocks previously failed to decode (`data did not match any variant of untagged enum BlockTransactions`) and stopped sync. Pinned the Tempo crates to the `tempo/v1.19.1` release, which includes configurable-account signatures (tempoxyz/tempo#8750). `txs.signature_type` records configurable-account signatures, sent directly or as an access key, as `3`.
