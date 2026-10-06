---
tidx: patch
---

Reduced PostgreSQL load of gap-fill on a synced chain. The check that runs every two seconds now counts only the blocks above `synced_num` instead of every block since the prune floor, and the full range is verified on start and every ten minutes.
