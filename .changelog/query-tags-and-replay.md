---
tidx: minor
---

Tagged every database query an API request runs with `tidx/<route>/<caller>/<request_id>` (ClickHouse `log_comment`, PostgreSQL `application_name`), taking the caller from the `x-tidx-caller` header and the request ID from `x-request-id` (echoed on the response), and added `tidx_api_queries_total` / `tidx_api_query_duration_seconds` by route and status. Added `tidx query-log` to rank expensive API queries from ClickHouse's query_log and pg_stat_statements, and `tidx replay` to replay captured traffic against an endpoint and record a latency baseline.
