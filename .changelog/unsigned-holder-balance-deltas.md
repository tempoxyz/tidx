---
tidx: patch
---

Store holder balance deltas as unsigned `UInt256` magnitude with the sign carried in `leg`, fixing balance corruption for transfers above `Int256::MAX`, and add post-derived migrations to repair existing historical data.
