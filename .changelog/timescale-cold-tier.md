---
tidx: minor
---

Added tiered storage on TimescaleDB (`[chains.timescale]`): chain tables partition into hot rowstore segments and highly compressed cold segments, keeping full history queryable in Postgres.
