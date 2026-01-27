//! Benchmarks comparing PostgreSQL vs DuckDB query performance.
//!
//! Run with:
//! ```sh
//! DATABASE_URL=postgres://... cargo bench --bench engine_bench
//! ```

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;
use tokio::runtime::Runtime;

use tidx::db::{create_pool, DuckDbPool};

/// Setup shared test infrastructure.
fn setup() -> (Runtime, deadpool_postgres::Pool, Arc<DuckDbPool>) {
    let rt = Runtime::new().unwrap();

    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pg_pool = rt.block_on(async { create_pool(&db_url).await.expect("Failed to create pool") });

    // Create in-memory DuckDB and sync data from PostgreSQL
    let duckdb_pool = rt.block_on(async {
        let pool = DuckDbPool::in_memory().expect("Failed to create DuckDB pool");

        // Sync blocks from PostgreSQL to DuckDB
        let pg_conn = pg_pool.get().await.expect("Failed to get pg connection");

        // Copy blocks
        let rows = pg_conn
            .query(
                "SELECT num, encode(hash, 'hex'), encode(parent_hash, 'hex'), 
                        timestamp, timestamp_ms, gas_limit, gas_used, 
                        encode(miner, 'hex'), encode(extra_data, 'hex')
                 FROM blocks ORDER BY num LIMIT 50000",
                &[],
            )
            .await
            .expect("Failed to query blocks");

        // Build SQL statements from fetched rows
        let mut block_stmts = Vec::new();
        for row in &rows {
            let num: i64 = row.get(0);
            let hash: String = row.get(1);
            let parent_hash: String = row.get(2);
            let timestamp: chrono::DateTime<chrono::Utc> = row.get(3);
            let timestamp_ms: i64 = row.get(4);
            let gas_limit: i64 = row.get(5);
            let gas_used: i64 = row.get(6);
            let miner: String = row.get(7);
            let extra_data: Option<String> = row.get(8);

            block_stmts.push(format!(
                "INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner, extra_data)
                 VALUES ({num}, '0x{hash}', '0x{parent_hash}', '{timestamp}', {timestamp_ms}, {gas_limit}, {gas_used}, '0x{miner}', {})",
                extra_data.map_or("NULL".to_string(), |e| format!("'0x{e}'"))
            ));
        }

        // Execute block inserts
        if !block_stmts.is_empty() {
            let sql = block_stmts.join(";\n");
            pool.with_connection(move |conn| {
                for stmt in sql.split(";\n") {
                    let _ = conn.execute(stmt, []);
                }
                Ok(())
            }).await.ok();
        }

        // Copy transactions
        let rows = pg_conn
            .query(
                r#"SELECT block_num, block_timestamp, idx, encode(hash, 'hex'), type,
                          encode("from", 'hex'), encode("to", 'hex'), value, encode(input, 'hex'),
                          gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_used,
                          encode(nonce_key, 'hex'), nonce, encode(fee_token, 'hex'),
                          encode(fee_payer, 'hex'), calls::text, call_count, valid_before, valid_after,
                          signature_type
                   FROM txs ORDER BY block_num, idx LIMIT 100000"#,
                &[],
            )
            .await
            .expect("Failed to query txs");

        let mut tx_stmts = Vec::new();
        for row in &rows {
            let block_num: i64 = row.get(0);
            let block_timestamp: chrono::DateTime<chrono::Utc> = row.get(1);
            let idx: i32 = row.get(2);
            let hash: String = row.get(3);
            let tx_type: i16 = row.get(4);
            let from: String = row.get(5);
            let to: Option<String> = row.get(6);
            let value: String = row.get(7);
            let input: String = row.get(8);
            let gas_limit: i64 = row.get(9);
            let max_fee: String = row.get(10);
            let max_priority: String = row.get(11);
            let gas_used: Option<i64> = row.get(12);
            let nonce_key: String = row.get(13);
            let nonce: i64 = row.get(14);
            let fee_token: Option<String> = row.get(15);
            let fee_payer: Option<String> = row.get(16);
            let calls: Option<String> = row.get(17);
            let call_count: i16 = row.get(18);
            let valid_before: Option<i64> = row.get(19);
            let valid_after: Option<i64> = row.get(20);
            let sig_type: Option<i16> = row.get(21);

            tx_stmts.push(format!(
                r#"INSERT INTO txs (block_num, block_timestamp, idx, hash, type, "from", "to", value, input,
                                   gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_used,
                                   nonce_key, nonce, fee_token, fee_payer, calls, call_count,
                                   valid_before, valid_after, signature_type)
                   VALUES ({block_num}, '{block_timestamp}', {idx}, '0x{hash}', {tx_type}, '0x{from}',
                           {to}, '{value}', '0x{input}', {gas_limit}, '{max_fee}', '{max_priority}',
                           {gas_used}, '0x{nonce_key}', {nonce}, {fee_token}, {fee_payer}, {calls},
                           {call_count}, {valid_before}, {valid_after}, {sig_type})"#,
                to = to.map_or("NULL".to_string(), |t| format!("'0x{t}'")),
                gas_used = gas_used.map_or("NULL".to_string(), |g| g.to_string()),
                fee_token = fee_token.map_or("NULL".to_string(), |f| format!("'0x{f}'")),
                fee_payer = fee_payer.map_or("NULL".to_string(), |f| format!("'0x{f}'")),
                calls = calls.map_or("NULL".to_string(), |c| format!("'{}'", c.replace('\'', "''"))),
                valid_before = valid_before.map_or("NULL".to_string(), |v| v.to_string()),
                valid_after = valid_after.map_or("NULL".to_string(), |v| v.to_string()),
                sig_type = sig_type.map_or("NULL".to_string(), |s| s.to_string()),
            ));
        }

        // Execute tx inserts
        if !tx_stmts.is_empty() {
            let sql = tx_stmts.join(";\n");
            pool.with_connection(move |conn| {
                for stmt in sql.split(";\n") {
                    let _ = conn.execute(stmt, []);
                }
                Ok(())
            }).await.ok();
        }

        // Copy logs
        let rows = pg_conn
            .query(
                "SELECT block_num, block_timestamp, log_idx, tx_idx, encode(tx_hash, 'hex'),
                        encode(address, 'hex'), encode(selector, 'hex'), 
                        encode(topic0, 'hex'), encode(topic1, 'hex'), encode(topic2, 'hex'), encode(topic3, 'hex'),
                        encode(data, 'hex')
                 FROM logs ORDER BY block_num, log_idx LIMIT 100000",
                &[],
            )
            .await
            .expect("Failed to query logs");

        let mut log_stmts = Vec::new();
        for row in &rows {
            let block_num: i64 = row.get(0);
            let block_timestamp: chrono::DateTime<chrono::Utc> = row.get(1);
            let log_idx: i32 = row.get(2);
            let tx_idx: i32 = row.get(3);
            let tx_hash: String = row.get(4);
            let address: String = row.get(5);
            let selector: Option<String> = row.get(6);
            let topic0: Option<String> = row.get(7);
            let topic1: Option<String> = row.get(8);
            let topic2: Option<String> = row.get(9);
            let topic3: Option<String> = row.get(10);
            let data: String = row.get(11);

            log_stmts.push(format!(
                "INSERT INTO logs (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topic0, topic1, topic2, topic3, data)
                 VALUES ({block_num}, '{block_timestamp}', {log_idx}, {tx_idx}, '0x{tx_hash}', '0x{address}', {selector}, {topic0}, {topic1}, {topic2}, {topic3}, '0x{data}')",
                selector = selector.map_or("NULL".to_string(), |s| format!("'0x{s}'")),
                topic0 = topic0.map_or("NULL".to_string(), |t| format!("'0x{t}'")),
                topic1 = topic1.map_or("NULL".to_string(), |t| format!("'0x{t}'")),
                topic2 = topic2.map_or("NULL".to_string(), |t| format!("'0x{t}'")),
                topic3 = topic3.map_or("NULL".to_string(), |t| format!("'0x{t}'")),
            ));
        }

        // Execute log inserts
        if !log_stmts.is_empty() {
            let sql = log_stmts.join(";\n");
            pool.with_connection(move |conn| {
                for stmt in sql.split(";\n") {
                    let _ = conn.execute(stmt, []);
                }
                Ok(())
            }).await.ok();
        }

        Arc::new(pool)
    });

    (rt, pg_pool, duckdb_pool)
}

// ============================================================================
// OLTP Benchmarks (Point Lookups)
// ============================================================================

fn bench_oltp_postgres_vs_duckdb(c: &mut Criterion) {
    let (rt, pg_pool, duck_pool) = setup();

    let mut group = c.benchmark_group("oltp");
    group.significance_level(0.05);
    group.sample_size(100);

    // Block by primary key
    group.bench_function("block_by_num/postgres", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pg_pool.get().await.unwrap();
            let _rows = conn
                .query("SELECT * FROM blocks WHERE num = 100", &[])
                .await
                .unwrap();
        });
    });

    group.bench_function("block_by_num/duckdb", |b| {
        b.to_async(&rt).iter(|| async {
            let _result = duck_pool
                .query("SELECT * FROM blocks WHERE num = 100")
                .await
                .unwrap();
        });
    });

    // Transaction by hash (indexed)
    group.bench_function("tx_by_hash/postgres", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pg_pool.get().await.unwrap();
            let _rows = conn
                .query(
                    "SELECT * FROM txs WHERE hash = (SELECT hash FROM txs LIMIT 1)",
                    &[],
                )
                .await
                .unwrap();
        });
    });

    group.bench_function("tx_by_hash/duckdb", |b| {
        b.to_async(&rt).iter(|| async {
            let _result = duck_pool
                .query("SELECT * FROM txs WHERE hash = (SELECT hash FROM txs LIMIT 1)")
                .await
                .unwrap();
        });
    });

    // Recent blocks with LIMIT
    for limit in [1, 10, 100] {
        group.bench_with_input(
            BenchmarkId::new("recent_blocks/postgres", limit),
            &limit,
            |b, &n| {
                b.to_async(&rt).iter(|| async {
                    let conn = pg_pool.get().await.unwrap();
                    let _rows = conn
                        .query(
                            &format!("SELECT * FROM blocks ORDER BY num DESC LIMIT {n}"),
                            &[],
                        )
                        .await
                        .unwrap();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("recent_blocks/duckdb", limit),
            &limit,
            |b, &n| {
                b.to_async(&rt).iter(|| async {
                    let _result = duck_pool
                        .query(&format!("SELECT * FROM blocks ORDER BY num DESC LIMIT {n}"))
                        .await
                        .unwrap();
                });
            },
        );
    }

    // Logs by address (indexed)
    group.bench_function("logs_by_address/postgres", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pg_pool.get().await.unwrap();
            let _rows = conn
                .query(
                    "SELECT * FROM logs WHERE address = (SELECT address FROM logs LIMIT 1) LIMIT 100",
                    &[],
                )
                .await
                .unwrap();
        });
    });

    group.bench_function("logs_by_address/duckdb", |b| {
        b.to_async(&rt).iter(|| async {
            let _result = duck_pool
                .query(
                    "SELECT * FROM logs WHERE address = (SELECT address FROM logs LIMIT 1) LIMIT 100",
                )
                .await
                .unwrap();
        });
    });

    group.finish();
}

// ============================================================================
// OLAP Benchmarks (Aggregations, GROUP BY, Window Functions)
// ============================================================================

fn bench_olap_postgres_vs_duckdb(c: &mut Criterion) {
    let (rt, pg_pool, duck_pool) = setup();

    let mut group = c.benchmark_group("olap");
    group.significance_level(0.05);
    group.sample_size(50);

    // COUNT(*) - full table scan
    group.bench_function("count_blocks/postgres", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pg_pool.get().await.unwrap();
            let _: i64 = conn
                .query_one("SELECT COUNT(*) FROM blocks", &[])
                .await
                .unwrap()
                .get(0);
        });
    });

    group.bench_function("count_blocks/duckdb", |b| {
        b.to_async(&rt).iter(|| async {
            let _result = duck_pool.query("SELECT COUNT(*) FROM blocks").await.unwrap();
        });
    });

    group.bench_function("count_txs/postgres", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pg_pool.get().await.unwrap();
            let _: i64 = conn
                .query_one("SELECT COUNT(*) FROM txs", &[])
                .await
                .unwrap()
                .get(0);
        });
    });

    group.bench_function("count_txs/duckdb", |b| {
        b.to_async(&rt).iter(|| async {
            let _result = duck_pool.query("SELECT COUNT(*) FROM txs").await.unwrap();
        });
    });

    // GROUP BY aggregation
    group.bench_function("txs_by_type/postgres", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pg_pool.get().await.unwrap();
            let _rows = conn
                .query("SELECT type, COUNT(*) FROM txs GROUP BY type", &[])
                .await
                .unwrap();
        });
    });

    group.bench_function("txs_by_type/duckdb", |b| {
        b.to_async(&rt).iter(|| async {
            let _result = duck_pool
                .query("SELECT type, COUNT(*) FROM txs GROUP BY type")
                .await
                .unwrap();
        });
    });

    // SUM/AVG aggregation
    group.bench_function("gas_stats/postgres", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pg_pool.get().await.unwrap();
            let _rows = conn
                .query(
                    "SELECT AVG(gas_used), MAX(gas_used), MIN(gas_used), SUM(gas_used) FROM blocks",
                    &[],
                )
                .await
                .unwrap();
        });
    });

    group.bench_function("gas_stats/duckdb", |b| {
        b.to_async(&rt).iter(|| async {
            let _result = duck_pool
                .query("SELECT AVG(gas_used), MAX(gas_used), MIN(gas_used), SUM(gas_used) FROM blocks")
                .await
                .unwrap();
        });
    });

    // Top N with GROUP BY
    group.bench_function("top_senders/postgres", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pg_pool.get().await.unwrap();
            let _rows = conn
                .query(
                    r#"SELECT "from", COUNT(*) as cnt FROM txs GROUP BY "from" ORDER BY cnt DESC LIMIT 10"#,
                    &[],
                )
                .await
                .unwrap();
        });
    });

    group.bench_function("top_senders/duckdb", |b| {
        b.to_async(&rt).iter(|| async {
            let _result = duck_pool
                .query(
                    r#"SELECT "from", COUNT(*) as cnt FROM txs GROUP BY "from" ORDER BY cnt DESC LIMIT 10"#,
                )
                .await
                .unwrap();
        });
    });

    // COUNT DISTINCT
    group.bench_function("unique_senders/postgres", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pg_pool.get().await.unwrap();
            let _row = conn
                .query_one(r#"SELECT COUNT(DISTINCT "from") FROM txs"#, &[])
                .await
                .unwrap();
        });
    });

    group.bench_function("unique_senders/duckdb", |b| {
        b.to_async(&rt).iter(|| async {
            let _result = duck_pool
                .query(r#"SELECT COUNT(DISTINCT "from") FROM txs"#)
                .await
                .unwrap();
        });
    });

    // Top events by selector
    group.bench_function("top_events/postgres", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pg_pool.get().await.unwrap();
            let _rows = conn
                .query(
                    "SELECT selector, COUNT(*) as cnt FROM logs WHERE selector IS NOT NULL GROUP BY selector ORDER BY cnt DESC LIMIT 10",
                    &[],
                )
                .await
                .unwrap();
        });
    });

    group.bench_function("top_events/duckdb", |b| {
        b.to_async(&rt).iter(|| async {
            let _result = duck_pool
                .query(
                    "SELECT selector, COUNT(*) as cnt FROM logs WHERE selector IS NOT NULL GROUP BY selector ORDER BY cnt DESC LIMIT 10",
                )
                .await
                .unwrap();
        });
    });

    group.finish();
}

// ============================================================================
// ABI Decoding Benchmarks (CTE with event signature)
// ============================================================================

fn bench_abi_decoding(c: &mut Criterion) {
    let (rt, pg_pool, duck_pool) = setup();

    let mut group = c.benchmark_group("abi_decoding");
    group.significance_level(0.05);
    group.sample_size(30);

    // Transfer event selector (full 32-byte topic0 hash)
    let transfer_selector_pg = r"\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
    let transfer_selector_duck = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

    // PostgreSQL with SQL-based ABI decoding
    group.bench_function("transfer_decode_group_by/postgres", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pg_pool.get().await.unwrap();
            let _rows = conn
                .query(
                    &format!(
                        r#"WITH transfer AS (
                            SELECT block_num, block_timestamp, log_idx, tx_idx, tx_hash, address,
                                   abi_address(topic1) AS "from",
                                   abi_address(topic2) AS "to",
                                   abi_uint(substring(data FROM 1 FOR 32)) AS value
                            FROM logs
                            WHERE selector = '{transfer_selector_pg}'
                        )
                        SELECT "to", COUNT(*) as cnt
                        FROM transfer
                        GROUP BY "to"
                        ORDER BY cnt DESC
                        LIMIT 10"#
                    ),
                    &[],
                )
                .await
                .unwrap();
        });
    });

    // DuckDB with native Rust UDFs
    group.bench_function("transfer_decode_group_by/duckdb", |b| {
        b.to_async(&rt).iter(|| async {
            let _result = duck_pool
                .query(&format!(
                    r#"WITH transfer AS (
                        SELECT block_num, block_timestamp, log_idx, tx_idx, tx_hash, address,
                               topic_address_native(topic1) AS "from",
                               topic_address_native(topic2) AS "to",
                               abi_uint_native(data, 0) AS value
                        FROM logs
                        WHERE selector = '{transfer_selector_duck}'
                    )
                    SELECT "to", COUNT(*) as cnt
                    FROM transfer
                    GROUP BY "to"
                    ORDER BY cnt DESC
                    LIMIT 10"#
                ))
                .await
                .unwrap();
        });
    });

    // Sum of values (filter out huge values that overflow)
    group.bench_function("transfer_sum_values/postgres", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pg_pool.get().await.unwrap();
            let _rows = conn
                .query(
                    &format!(
                        r#"WITH transfer AS (
                            SELECT abi_uint(substring(data FROM 1 FOR 32)) AS value
                            FROM logs
                            WHERE selector = '{transfer_selector_pg}'
                        )
                        SELECT SUM(value) FROM transfer WHERE value < 1000000000000000000"#
                    ),
                    &[],
                )
                .await
                .unwrap();
        });
    });

    group.bench_function("transfer_sum_values/duckdb", |b| {
        b.to_async(&rt).iter(|| async {
            let _result = duck_pool
                .query(&format!(
                    r#"WITH transfer AS (
                        SELECT abi_uint_native(data, 0) AS value
                        FROM logs
                        WHERE selector = '{transfer_selector_duck}'
                    )
                    SELECT SUM(value) FROM transfer WHERE value < 1000000000000000000"#
                ))
                .await
                .unwrap();
        });
    });

    // Raw decode without aggregation (measures pure UDF performance)
    group.bench_function("transfer_raw_decode/postgres", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pg_pool.get().await.unwrap();
            let _rows = conn
                .query(
                    &format!(
                        r#"SELECT 
                               abi_address(topic1) AS "from",
                               abi_address(topic2) AS "to",
                               abi_uint(substring(data FROM 1 FOR 32)) AS value
                           FROM logs
                           WHERE selector = '{transfer_selector_pg}'
                           LIMIT 1000"#
                    ),
                    &[],
                )
                .await
                .unwrap();
        });
    });

    group.bench_function("transfer_raw_decode/duckdb", |b| {
        b.to_async(&rt).iter(|| async {
            let _result = duck_pool
                .query(&format!(
                    r#"SELECT 
                           topic_address_native(topic1) AS "from",
                           topic_address_native(topic2) AS "to",
                           abi_uint_native(data, 0) AS value
                       FROM logs
                       WHERE selector = '{transfer_selector_duck}'
                       LIMIT 1000"#
                ))
                .await
                .unwrap();
        });
    });

    group.finish();
}

// ============================================================================
// JOIN Benchmarks
// ============================================================================

fn bench_joins(c: &mut Criterion) {
    let (rt, pg_pool, duck_pool) = setup();

    let mut group = c.benchmark_group("joins");
    group.significance_level(0.05);
    group.sample_size(30);

    // Simple 2-table join
    group.bench_function("blocks_txs_join/postgres", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pg_pool.get().await.unwrap();
            let _rows = conn
                .query(
                    "SELECT b.num, b.hash, COUNT(t.hash) as tx_count
                     FROM blocks b
                     JOIN txs t ON b.num = t.block_num
                     GROUP BY b.num, b.hash
                     ORDER BY tx_count DESC
                     LIMIT 10",
                    &[],
                )
                .await
                .unwrap();
        });
    });

    group.bench_function("blocks_txs_join/duckdb", |b| {
        b.to_async(&rt).iter(|| async {
            let _result = duck_pool
                .query(
                    "SELECT b.num, b.hash, COUNT(t.hash) as tx_count
                     FROM blocks b
                     JOIN txs t ON b.num = t.block_num
                     GROUP BY b.num, b.hash
                     ORDER BY tx_count DESC
                     LIMIT 10",
                )
                .await
                .unwrap();
        });
    });

    // 3-table join
    group.bench_function("blocks_txs_logs_join/postgres", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pg_pool.get().await.unwrap();
            let _rows = conn
                .query(
                    "SELECT b.num, COUNT(DISTINCT t.hash) as tx_count, COUNT(l.log_idx) as log_count
                     FROM blocks b
                     JOIN txs t ON b.num = t.block_num
                     JOIN logs l ON t.hash = l.tx_hash
                     GROUP BY b.num
                     ORDER BY log_count DESC
                     LIMIT 10",
                    &[],
                )
                .await
                .unwrap();
        });
    });

    group.bench_function("blocks_txs_logs_join/duckdb", |b| {
        b.to_async(&rt).iter(|| async {
            let _result = duck_pool
                .query(
                    "SELECT b.num, COUNT(DISTINCT t.hash) as tx_count, COUNT(l.log_idx) as log_count
                     FROM blocks b
                     JOIN txs t ON b.num = t.block_num
                     JOIN logs l ON t.hash = l.tx_hash
                     GROUP BY b.num
                     ORDER BY log_count DESC
                     LIMIT 10",
                )
                .await
                .unwrap();
        });
    });

    group.finish();
}

// ============================================================================
// Window Function Benchmarks
// ============================================================================

fn bench_window_functions(c: &mut Criterion) {
    let (rt, pg_pool, duck_pool) = setup();

    let mut group = c.benchmark_group("window_functions");
    group.significance_level(0.05);
    group.sample_size(30);

    // ROW_NUMBER
    group.bench_function("row_number/postgres", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pg_pool.get().await.unwrap();
            let _rows = conn
                .query(
                    r#"SELECT block_num, "from", gas_limit,
                              ROW_NUMBER() OVER (PARTITION BY "from" ORDER BY block_num) as rn
                       FROM txs
                       LIMIT 1000"#,
                    &[],
                )
                .await
                .unwrap();
        });
    });

    group.bench_function("row_number/duckdb", |b| {
        b.to_async(&rt).iter(|| async {
            let _result = duck_pool
                .query(
                    r#"SELECT block_num, "from", gas_limit,
                              ROW_NUMBER() OVER (PARTITION BY "from" ORDER BY block_num) as rn
                       FROM txs
                       LIMIT 1000"#,
                )
                .await
                .unwrap();
        });
    });

    // Running totals with SUM OVER
    group.bench_function("running_sum/postgres", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pg_pool.get().await.unwrap();
            let _rows = conn
                .query(
                    "SELECT num, gas_used,
                            SUM(gas_used) OVER (ORDER BY num ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_gas
                     FROM blocks
                     LIMIT 1000",
                    &[],
                )
                .await
                .unwrap();
        });
    });

    group.bench_function("running_sum/duckdb", |b| {
        b.to_async(&rt).iter(|| async {
            let _result = duck_pool
                .query(
                    "SELECT num, gas_used,
                            SUM(gas_used) OVER (ORDER BY num ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_gas
                     FROM blocks
                     LIMIT 1000",
                )
                .await
                .unwrap();
        });
    });

    // LAG/LEAD
    group.bench_function("lag_lead/postgres", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pg_pool.get().await.unwrap();
            let _rows = conn
                .query(
                    "SELECT num, gas_used,
                            LAG(gas_used, 1) OVER (ORDER BY num) as prev_gas,
                            LEAD(gas_used, 1) OVER (ORDER BY num) as next_gas
                     FROM blocks
                     LIMIT 1000",
                    &[],
                )
                .await
                .unwrap();
        });
    });

    group.bench_function("lag_lead/duckdb", |b| {
        b.to_async(&rt).iter(|| async {
            let _result = duck_pool
                .query(
                    "SELECT num, gas_used,
                            LAG(gas_used, 1) OVER (ORDER BY num) as prev_gas,
                            LEAD(gas_used, 1) OVER (ORDER BY num) as next_gas
                     FROM blocks
                     LIMIT 1000",
                )
                .await
                .unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_oltp_postgres_vs_duckdb,
    bench_olap_postgres_vs_duckdb,
    bench_abi_decoding,
    bench_joins,
    bench_window_functions,
);
criterion_main!(benches);
