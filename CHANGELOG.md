# Changelog

## 0.2.0 (2026-02-06)

### Minor Changes

- Support PostgreSQL password via environment variable. Add `pg_password_env` config option to inject the password from an env var into `pg_url` at runtime, avoiding plaintext passwords in config files. Existing configs without `pg_password_env` work unchanged. (by @GeorgiosKonstantopoulos, [#72](https://github.com/tempoxyz/tidx/pull/72))
- Add ClickHouse failover support for multi-instance per chain. Reads go to the primary instance; connection-level errors (refused/timeout/DNS) trigger automatic failover to the next instance. Each instance runs its own MaterializedPostgreSQL replication. Configure with `failover_urls` in `[chains.clickhouse]`. Existing single-URL configs work unchanged. (by @GeorgiosKonstantopoulos, [#71](https://github.com/tempoxyz/tidx/pull/71))

### Patch Changes

- Handle SIGTERM for graceful container shutdown. Previously only SIGINT (ctrl-c) triggered graceful shutdown; now SIGTERM from Kubernetes/Docker also triggers the same broadcast for clean connection draining. (by @GeorgiosKonstantopoulos, [#71](https://github.com/tempoxyz/tidx/pull/71))

## 0.2.0 (2026-02-06)

### Minor Changes

- Add ClickHouse failover support for multi-instance per chain. Reads go to the primary instance; connection-level errors (refused/timeout/DNS) trigger automatic failover to the next instance. Each instance runs its own MaterializedPostgreSQL replication. Configure with `failover_urls` in `[chains.clickhouse]`. Existing single-URL configs work unchanged. (by @tempo-ai, [a74f174](https://github.com/tempoxyz/tidx/commit/a74f174))

## 0.1.3 (2026-02-03)

### Patch Changes

- Trigger release. (by @jxom, [4f89896](https://github.com/tempoxyz/tidx/commit/4f89896))

## 0.1.2 (2026-02-03)

### Patch Changes

- Fix ClickHouse DDL statement handling. DDL statements like `CREATE DATABASE` and `CREATE TABLE` return empty responses, which previously caused JSON parsing errors. Now handles empty responses gracefully. (by @jxom, [09573e1](https://github.com/tempoxyz/tidx/commit/09573e1))

## 0.1.1 (2026-02-03)

### Patch Changes

- Fix ClickHouse hex literal handling for MaterializedPostgreSQL.
**Input**: Use `concat(char(92), 'x...')` instead of `'\x...'` for WHERE clause comparisons because ClickHouse interprets `\x` as an escape sequence.
**Output**: Convert `\x...` to `0x...` in query results for standard Ethereum hex format, matching PostgreSQL behavior. (by @jxom, [e15afa5](https://github.com/tempoxyz/tidx/commit/e15afa5))

## 0.1.0 (2026-02-03)

### Minor Changes

- Add predicate pushdown for indexed event parameters.
- Rewrites SQL filters like `"from" = '0x...'` to `topic1 = '0x000...'` to enable index usage
- Add `signature` parameter to `/views` API for automatic CTE generation and decoding
- Support both PostgreSQL and ClickHouse query engines (by @jxom, [0bca021](https://github.com/tempoxyz/tidx/commit/0bca021))

### Patch Changes

- Fix hex literal conversion to preserve `0x` prefix in concat expressions.
- The naive `replace("'0x", "'\\x")` was incorrectly converting `concat('0x', ...)` to `concat('\x', ...)`, causing addresses to display as `\x...` instead of `0x...`. Now uses regex to only convert hex literals with 40+ characters. (by @jxom, [0bca021](https://github.com/tempoxyz/tidx/commit/0bca021))

## 0.0.37 (2026-02-03)

### Patch Changes

- Adds columns array to the /views?chainId=... response with column names and types. (by @jxom, [c2001e4](https://github.com/tempoxyz/tidx/commit/c2001e4))

## 0.0.36 (2026-02-03)

### Patch Changes

- Initial release. (by @jxom, [9bba8d5](https://github.com/tempoxyz/tidx/commit/9bba8d5))

