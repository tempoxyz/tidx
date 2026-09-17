use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use tidx::db::{create_pool, run_migrations};

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

criterion_group!(
    benches,
    bench_oltp_queries,
    bench_olap_queries,
    bench_olap_materialized,
    bench_comparison
);
criterion_main!(benches);
