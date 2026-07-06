---
tidx: minor
---

Added an optional TimescaleDB columnstore cold tier (`[chains.timescale]`): fully-synced chunks behind the hot window are compressed in place, keeping cold data queryable in Postgres without an FDW.
