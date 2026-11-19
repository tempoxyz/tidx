---
tidx: minor
---

Added optional OpenTelemetry tracing and index-freshness metrics. Setting `OTEL_EXPORTER_OTLP_ENDPOINT` (or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`) exports OTLP/HTTP traces; without it nothing changes. The HTTP API continues a caller's W3C `traceparent` and records spans for routing, parsing, PostgreSQL pool acquire and execution, ClickHouse execution and response serialization. New metrics: `tidx_index_latency_seconds` (histogram, block timestamp to rows committed, per chain and sink, realtime sync only) and `tidx_sync_head_timestamp_seconds`. `tidx_clickhouse_queries_total`, `tidx_clickhouse_query_duration_seconds` (now a bucketed histogram) and `tidx_clickhouse_query_rows` are now recorded on every ClickHouse query (`success="false"` only when ClickHouse is unreachable); they were defined but never written.
