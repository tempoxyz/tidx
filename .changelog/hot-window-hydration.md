---
tidx: patch
---

Sped up PostgreSQL hot-window hydration in retention-enabled deployments. The first block of the hot window is now looked up once per weekly boundary instead of by a binary search over RPC on every tick, and each scan of the hot window for gaps now hydrates every gap it finds instead of stopping after one batch per worker.
