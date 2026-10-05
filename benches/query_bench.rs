use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use tidx::db::partitions::ensure_partitions_covering;
use tidx::db::{create_pool, run_migrations};
use tidx::query::apply_event_signature_ctes_postgres;
use tidx::service::{QueryOptions, execute_query_postgres, format_column_json};

fn bench_oltp_queries(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = rt.block_on(async { create_pool(&db_url).await.expect("Failed to create pool") });

    let mut group = c.benchmark_group("oltp");
    group.significance_level(0.05);
    group.sample_size(100);

    // Point lookup by primary key
    group.bench_function("block_by_num", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pool.get().await.unwrap();
            let _rows = conn
                .query("SELECT * FROM blocks WHERE num = 100", &[])
                .await
                .unwrap();
        });
    });

    // Point lookup by hash (indexed)
    group.bench_function("block_by_hash", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pool.get().await.unwrap();
            let _rows = conn
                .query(
                    "SELECT * FROM blocks WHERE hash = (SELECT hash FROM blocks WHERE num = 100)",
                    &[],
                )
                .await
                .unwrap();
        });
    });

    // Transaction lookup by block
    group.bench_function("txs_by_block", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pool.get().await.unwrap();
            let _rows = conn
                .query("SELECT * FROM txs WHERE block_num = 100", &[])
                .await
                .unwrap();
        });
    });

    // Transaction lookup by hash (indexed)
    group.bench_function("tx_by_hash", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pool.get().await.unwrap();
            let _rows = conn
                .query(
                    "SELECT * FROM txs WHERE hash = (SELECT hash FROM txs LIMIT 1)",
                    &[],
                )
                .await
                .unwrap();
        });
    });

    // Recent blocks (small LIMIT, ordered by index)
    for limit in [1, 10, 100] {
        group.bench_with_input(BenchmarkId::new("recent_blocks", limit), &limit, |b, &n| {
            b.to_async(&rt).iter(|| async {
                let conn = pool.get().await.unwrap();
                let _rows = conn
                    .query(
                        &format!("SELECT * FROM blocks ORDER BY num DESC LIMIT {n}"),
                        &[],
                    )
                    .await
                    .unwrap();
            });
        });
    }

    // Logs by selector (indexed, small result)
    group.bench_function("logs_by_selector_limit", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pool.get().await.unwrap();
            let _rows = conn
                .query(
                    "SELECT * FROM logs WHERE selector = (SELECT selector FROM logs LIMIT 1) LIMIT 100",
                    &[],
                )
                .await
                .unwrap();
        });
    });

    group.finish();
}

fn bench_olap_queries(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = rt.block_on(async { create_pool(&db_url).await.expect("Failed to create pool") });

    let mut group = c.benchmark_group("olap");
    group.significance_level(0.05);
    group.sample_size(50); // Fewer samples for slower queries

    // Full table counts (scans entire table)
    group.bench_function("count_blocks_full", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pool.get().await.unwrap();
            let _: i64 = conn
                .query_one("SELECT COUNT(*) FROM blocks", &[])
                .await
                .unwrap()
                .get(0);
        });
    });

    group.bench_function("count_txs_full", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pool.get().await.unwrap();
            let _: i64 = conn
                .query_one("SELECT COUNT(*) FROM txs", &[])
                .await
                .unwrap()
                .get(0);
        });
    });

    // Group by aggregation (full scan)
    group.bench_function("txs_by_type_full", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pool.get().await.unwrap();
            let _rows = conn
                .query("SELECT type, COUNT(*) FROM txs GROUP BY type", &[])
                .await
                .unwrap();
        });
    });

    // Time-range aggregation (partial scan)
    group.bench_function("gas_stats_last_1000_blocks", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pool.get().await.unwrap();
            let _rows = conn
                .query(
                    "SELECT AVG(gas_used), MAX(gas_used), MIN(gas_used), SUM(gas_used) 
                     FROM blocks 
                     WHERE num > (SELECT MAX(num) - 1000 FROM blocks)",
                    &[],
                )
                .await
                .unwrap();
        });
    });

    // Top senders (full scan with group by)
    group.bench_function("top_senders_full", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pool.get().await.unwrap();
            let _rows = conn
                .query(
                    "SELECT \"from\", COUNT(*) as cnt FROM txs GROUP BY \"from\" ORDER BY cnt DESC LIMIT 10",
                    &[],
                )
                .await
                .unwrap();
        });
    });

    // Unique senders (full scan)
    group.bench_function("unique_senders_full", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pool.get().await.unwrap();
            let _row = conn
                .query_one("SELECT COUNT(DISTINCT \"from\") FROM txs", &[])
                .await
                .unwrap();
        });
    });

    // Event analytics by selector (full scan)
    group.bench_function("top_events_full", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pool.get().await.unwrap();
            let _rows = conn
                .query(
                    "SELECT selector, COUNT(*) as cnt FROM logs WHERE selector IS NOT NULL GROUP BY selector ORDER BY cnt DESC LIMIT 10",
                    &[],
                )
                .await
                .unwrap();
        });
    });

    group.finish();
}

fn bench_olap_materialized(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = rt.block_on(async {
        let pool = create_pool(&db_url).await.expect("Failed to create pool");
        run_migrations(&pool)
            .await
            .expect("Failed to run migrations");

        // Create materialized views for benchmarking
        let conn = pool.get().await.expect("Failed to get connection");

        // Drop existing views first
        conn.batch_execute(
            r#"
            DROP MATERIALIZED VIEW IF EXISTS mv_block_count;
            DROP MATERIALIZED VIEW IF EXISTS mv_tx_count;
            DROP MATERIALIZED VIEW IF EXISTS mv_txs_by_type;
            DROP MATERIALIZED VIEW IF EXISTS mv_top_senders;
            DROP MATERIALIZED VIEW IF EXISTS mv_unique_senders;
            DROP MATERIALIZED VIEW IF EXISTS mv_top_events;
            "#,
        )
        .await
        .ok();

        // Create materialized views
        conn.batch_execute(
            r#"
            CREATE MATERIALIZED VIEW mv_block_count AS 
            SELECT COUNT(*) as cnt FROM blocks;

            CREATE MATERIALIZED VIEW mv_tx_count AS 
            SELECT COUNT(*) as cnt FROM txs;

            CREATE MATERIALIZED VIEW mv_txs_by_type AS 
            SELECT type, COUNT(*) as cnt FROM txs GROUP BY type;

            CREATE MATERIALIZED VIEW mv_top_senders AS 
            SELECT "from", COUNT(*) as cnt FROM txs GROUP BY "from" ORDER BY cnt DESC LIMIT 100;

            CREATE MATERIALIZED VIEW mv_unique_senders AS 
            SELECT COUNT(DISTINCT "from") as cnt FROM txs;

            CREATE MATERIALIZED VIEW mv_top_events AS 
            SELECT selector, COUNT(*) as cnt FROM logs 
            WHERE selector IS NOT NULL 
            GROUP BY selector ORDER BY cnt DESC LIMIT 100;
            "#,
        )
        .await
        .expect("Failed to create materialized views");

        pool
    });

    let mut group = c.benchmark_group("olap_materialized");
    group.significance_level(0.05);
    group.sample_size(100); // More samples - these are fast!

    // Materialized view queries (instant lookups)
    group.bench_function("count_blocks_mv", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pool.get().await.unwrap();
            let _: i64 = conn
                .query_one("SELECT cnt FROM mv_block_count", &[])
                .await
                .unwrap()
                .get(0);
        });
    });

    group.bench_function("count_txs_mv", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pool.get().await.unwrap();
            let _: i64 = conn
                .query_one("SELECT cnt FROM mv_tx_count", &[])
                .await
                .unwrap()
                .get(0);
        });
    });

    group.bench_function("txs_by_type_mv", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pool.get().await.unwrap();
            let _rows = conn
                .query("SELECT * FROM mv_txs_by_type", &[])
                .await
                .unwrap();
        });
    });

    group.bench_function("top_senders_mv", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pool.get().await.unwrap();
            let _rows = conn
                .query("SELECT * FROM mv_top_senders LIMIT 10", &[])
                .await
                .unwrap();
        });
    });

    group.bench_function("unique_senders_mv", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pool.get().await.unwrap();
            let _: i64 = conn
                .query_one("SELECT cnt FROM mv_unique_senders", &[])
                .await
                .unwrap()
                .get(0);
        });
    });

    group.bench_function("top_events_mv", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pool.get().await.unwrap();
            let _rows = conn
                .query("SELECT * FROM mv_top_events LIMIT 10", &[])
                .await
                .unwrap();
        });
    });

    group.finish();
}

fn bench_comparison(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = rt.block_on(async { create_pool(&db_url).await.expect("Failed to create pool") });

    let mut group = c.benchmark_group("oltp_vs_olap");
    group.significance_level(0.05);

    // Direct comparison: point lookup vs scan
    group.bench_function("single_block/by_pk", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pool.get().await.unwrap();
            let _rows = conn
                .query("SELECT * FROM blocks WHERE num = 50", &[])
                .await
                .unwrap();
        });
    });

    group.bench_function("single_block/full_scan", |b| {
        b.to_async(&rt).iter(|| async {
            let conn = pool.get().await.unwrap();
            let _rows = conn
                .query(
                    "SELECT * FROM blocks WHERE gas_used = (SELECT gas_used FROM blocks WHERE num = 50)",
                    &[],
                )
                .await
                .unwrap();
        });
    });

    group.finish();
}

const PG_JSON_LOG_ROWS: i64 = 10_000;
const PG_JSON_LOGS_PER_BLOCK: i64 = 20;
const PG_JSON_START_TS: i64 = 1_767_225_600;

/// ERC-20 Transfer-shaped logs: 6 non-null BYTEA columns per row plus a NULL
/// topic3, ints and a timestamp.
const PG_JSON_SEED_LOGS: &str = r#"
    TRUNCATE logs;
    INSERT INTO logs (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address,
                      selector, topic0, topic1, topic2, topic3, data)
    SELECT 1000000 + g / 20,
           to_timestamp(1767225600 + g / 20),
           g % 20,
           g % 20 / 2,
           sha256(int8send(g / 2)),
           substring(sha256(int8send(g % 50)) FROM 1 FOR 20),
           t.topic0,
           t.topic0,
           '\x000000000000000000000000'::bytea || substring(sha256(int8send(g * 7)) FROM 1 FOR 20),
           '\x000000000000000000000000'::bytea || substring(sha256(int8send(g * 13)) FROM 1 FOR 20),
           NULL,
           decode(lpad(to_hex(g * 1000000000007), 64, '0'), 'hex')
    FROM generate_series(0::int8, 9999) AS g,
         (SELECT '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'::bytea AS topic0) AS t;
"#;

/// PostgreSQL row → JSON conversion on the `/query` path. Seeds its own
/// database (DATABASE_URL with the database name replaced).
fn bench_pg_json(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut db_url =
        url::Url::parse(&std::env::var("DATABASE_URL").expect("DATABASE_URL must be set"))
            .expect("Invalid DATABASE_URL");
    db_url.set_path("/tidx_bench_pg_json");

    let pool = rt.block_on(async {
        let pool = create_pool(db_url.as_str())
            .await
            .expect("Failed to create pool");
        run_migrations(&pool)
            .await
            .expect("Failed to run migrations");
        let start = chrono::DateTime::from_timestamp(PG_JSON_START_TS, 0).unwrap();
        let end = start + chrono::Duration::seconds(PG_JSON_LOG_ROWS / PG_JSON_LOGS_PER_BLOCK);
        ensure_partitions_covering(&pool, start, end)
            .await
            .expect("Failed to create partitions");
        pool.get()
            .await
            .unwrap()
            .batch_execute(PG_JSON_SEED_LOGS)
            .await
            .expect("Failed to seed logs");
        pool
    });

    let transfer_sql = apply_event_signature_ctes_postgres(
        "SELECT * FROM transfer",
        &["Transfer(address indexed from, address indexed to, uint256 value)"],
    )
    .unwrap();

    let mut group = c.benchmark_group("pg_json");
    group.sample_size(30);

    // Pre-fetched rows isolate the per-cell conversion from query execution.
    for (name, sql) in [
        ("logs_10k", "SELECT * FROM logs ORDER BY block_num, log_idx"),
        ("transfer_10k", transfer_sql.as_str()),
    ] {
        let rows = rt.block_on(async { pool.get().await.unwrap().query(sql, &[]).await.unwrap() });
        assert_eq!(rows.len() as i64, PG_JSON_LOG_ROWS);
        group.bench_function(BenchmarkId::new("convert", name), |b| {
            b.iter(|| {
                rows.iter()
                    .map(|row| {
                        (0..row.len())
                            .map(|i| format_column_json(row, i))
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>()
            });
        });
    }

    let options = QueryOptions {
        timeout_ms: 30_000,
        limit: PG_JSON_LOG_ROWS,
    };
    group.bench_function(BenchmarkId::new("execute", "logs_10k"), |b| {
        b.to_async(&rt).iter(|| async {
            execute_query_postgres(&pool, "SELECT * FROM logs", &[], &options)
                .await
                .unwrap()
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_oltp_queries,
    bench_olap_queries,
    bench_olap_materialized,
    bench_comparison,
    bench_pg_json
);
criterion_main!(benches);
