---
tidx: minor
---

Added an optional TimescaleDB columnstore cold tier (`[chains.timescale]`). Chain tables become hypertables chunked by block number; a maintenance loop compresses fully-populated chunks behind `tip - hot_window_blocks` into columnstore with bloom sparse indexes on the point-lookup columns (hashes, addresses, topics). Cold data stays local in Postgres — point lookups, joins and keyset pagination work without an FDW or query contract — while hot chunks remain plain rowstore with the full index set. Requires a `timescale/timescaledb` Postgres image and a fresh database at first enable; ClickHouse remains an independent OLAP option.
