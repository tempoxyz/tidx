---
tidx: patch
---

Fixed backfill-first mode skipping the blocks above the highest stored block after a lagging restart, and the blocks realtime sync jumped over after falling more than ten blocks behind. Both ranges stayed missing until the next restart; they are now backfilled before realtime sync starts and caught up in order afterwards.
