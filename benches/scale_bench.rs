//! Extrapolation-based benchmarks for OLTP and OLAP query performance.
//!
//! Runs queries on small datasets (100K-800K rows) and extrapolates to 100M.
//! Compares DuckDB vs PostgreSQL performance.
//!
//! Run with:
//! ```sh
//! cargo bench --bench scale_bench -- extrapolate
//! ```

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;
use std::time::Instant;
use tokio::runtime::Runtime;

use tidx::db::{create_pool, run_migrations, DuckDbPool, Pool};

/// Extrapolation target size
const TARGET_ROWS: usize = 100_000_000;

/// Test sizes for extrapolation (larger for accuracy, ~2-3 min setup total)
const EXTRAPOLATE_SIZES: [usize; 4] = [1_000_000, 2_000_000, 4_000_000, 8_000_000];

// ============================================================================
// DATA GENERATION
// ============================================================================

/// Generate synthetic blockchain data in DuckDB.
/// Creates blocks, txs, and logs with realistic distributions.
/// Uses a temp file for compression (more realistic than in-memory).
fn setup_duckdb(log_count: usize) -> Arc<DuckDbPool> {
    // Use temp file for compression benefits (mirrors production behavior)
    let temp_path = format!("/tmp/tidx_bench_{}.duckdb", log_count);
    let _ = std::fs::remove_file(&temp_path); // Clean up any existing file
    let pool = DuckDbPool::new(&temp_path).expect("Failed to create DuckDB pool");

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let conn = pool.conn().await;

        // Derive counts: 1 block per 10 logs, 1 tx per 2 logs
        let block_count = log_count / 10;
        let tx_count = log_count / 2;

        // Generate blocks
        if block_count > 0 {
            conn.execute(
                &format!(
                    r#"INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
                       SELECT 
                           i as num,
                           '0x' || lpad(printf('%x', i), 64, '0') as hash,
                           '0x' || lpad(printf('%x', i - 1), 64, '0') as parent_hash,
                           make_timestamptz(2024, 1, 1, 0, 0, 0) + to_seconds(i) as timestamp,
                           1704067200000 + i * 1000 as timestamp_ms,
                           30000000 as gas_limit,
                           15000000 + (i % 10000000) as gas_used,
                           '0x' || lpad(printf('%x', i % 1000), 40, '0') as miner
                       FROM generate_series(1, {block_count}) as t(i)"#
                ),
                [],
            )
            .expect("Failed to insert blocks");
        }

        // Generate transactions (1 tx per block to avoid PK conflicts)
        if tx_count > 0 {
            conn.execute(
                &format!(
                    r#"INSERT INTO txs (block_num, block_timestamp, idx, hash, type, "from", "to", value, input,
                                       gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_used,
                                       nonce_key, nonce, call_count)
                       SELECT 
                           i as block_num,
                           make_timestamptz(2024, 1, 1, 0, 0, 0) + to_seconds(i) as block_timestamp,
                           0 as idx,
                           '0x' || lpad(printf('%x', i), 64, '0') as hash,
                           (i % 3)::smallint as type,
                           '0x' || lpad(printf('%x', i % 10000), 40, '0') as "from",
                           '0x' || lpad(printf('%x', (i + 1) % 10000), 40, '0') as "to",
                           (i % 1000000000)::text as value,
                           '0x' as input,
                           21000 + (i % 100000) as gas_limit,
                           '1000000000' as max_fee_per_gas,
                           '100000000' as max_priority_fee_per_gas,
                           21000 as gas_used,
                           '0x' || lpad(printf('%x', i % 10000), 40, '0') as nonce_key,
                           (i / 10000)::bigint as nonce,
                           1::smallint as call_count
                       FROM generate_series(1, {tx_count}) as t(i)"#
                ),
                [],
            )
            .expect("Failed to insert txs");
        }

        // Generate logs (80% Transfer events from specific contract)
        // This simulates real-world distribution where one contract dominates
        // PK is (block_num, log_idx) so we use i as block_num with log_idx=0
        let transfer_topic0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
        let target_contract = "0x20c0000000000000000000000000000000000001";
        let hot_sender = "0x5bc1473610754a5ca10749552b119df90c1a1877";

        conn.execute(
            &format!(
                r#"INSERT INTO logs (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topic0, topic1, topic2, topic3, data)
                   SELECT 
                       i as block_num,
                       make_timestamptz(2024, 1, 1, 0, 0, 0) + to_seconds(i) as block_timestamp,
                       0 as log_idx,
                       0 as tx_idx,
                       '0x' || lpad(printf('%x', i), 64, '0') as tx_hash,
                       -- 80% from target contract, 20% random
                       CASE WHEN i % 5 < 4 THEN '{target_contract}'
                            ELSE '0x' || lpad(printf('%x', i % 1000), 40, '0')
                       END as address,
                       '{transfer_topic0}' as selector,
                       '{transfer_topic0}' as topic0,
                       -- 1% from hot sender (for filtered query testing)
                       CASE WHEN i % 100 = 0 
                            THEN '0x000000000000000000000000' || substr('{hot_sender}', 3)
                            ELSE '0x' || lpad(printf('%x', i % 10000), 64, '0')
                       END as topic1,
                       '0x' || lpad(printf('%x', (i + 1) % 10000), 64, '0') as topic2,
                       NULL as topic3,
                       '0x' || lpad(printf('%x', i % 1000000000000), 64, '0') as data
                   FROM generate_series(1, {log_count}) as t(i)"#
            ),
            [],
        )
        .expect("Failed to insert logs");

        // Force checkpoint to compress data (simulates production behavior)
        conn.execute("CHECKPOINT", []).expect("Failed to checkpoint");
    });

    Arc::new(pool)
}

// ============================================================================
// QUERY DEFINITIONS
// ============================================================================

/// OLTP queries - optimized for point lookups and small result sets
mod oltp {
    pub const BLOCK_BY_NUM: &str = "SELECT * FROM blocks WHERE num = 500";

    pub const BLOCK_BY_HASH: &str =
        "SELECT * FROM blocks WHERE hash = (SELECT hash FROM blocks WHERE num = 500)";

    pub const TX_BY_HASH: &str = "SELECT * FROM txs WHERE hash = (SELECT hash FROM txs LIMIT 1)";

    pub const TXS_BY_BLOCK: &str = "SELECT * FROM txs WHERE block_num = 50";

    pub const LOGS_BY_TX: &str =
        "SELECT * FROM logs WHERE tx_hash = (SELECT tx_hash FROM logs LIMIT 1)";

    pub const RECENT_BLOCKS: &str = "SELECT * FROM blocks ORDER BY num DESC LIMIT 10";

    pub const LOGS_BY_SELECTOR_LIMIT: &str =
        "SELECT * FROM logs WHERE selector = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' LIMIT 100";
}

/// OLAP queries - analytics requiring full/partial scans
mod olap {
    pub const COUNT_BLOCKS: &str = "SELECT COUNT(*) FROM blocks";

    pub const COUNT_TXS: &str = "SELECT COUNT(*) FROM txs";

    pub const COUNT_LOGS: &str = "SELECT COUNT(*) FROM logs";

    pub const SUM_GAS_USED: &str = "SELECT SUM(gas_used) FROM blocks";

    pub const AVG_GAS_PER_BLOCK: &str =
        "SELECT AVG(gas_used), MIN(gas_used), MAX(gas_used) FROM blocks";

    pub const GROUP_BY_TX_TYPE: &str = "SELECT type, COUNT(*) FROM txs GROUP BY type";

    pub const TOP_SENDERS: &str =
        r#"SELECT "from", COUNT(*) as cnt FROM txs GROUP BY "from" ORDER BY cnt DESC LIMIT 10"#;

    pub const COUNT_DISTINCT_SENDERS: &str = r#"SELECT COUNT(DISTINCT "from") FROM txs"#;

    pub const TOP_CONTRACTS: &str =
        "SELECT address, COUNT(*) as cnt FROM logs GROUP BY address ORDER BY cnt DESC LIMIT 10";

    pub const COUNT_DISTINCT_ADDRESSES: &str = "SELECT COUNT(DISTINCT address) FROM logs";

    /// The problematic Transfer query that was hanging on 80M rows
    pub const TRANSFER_BY_ADDRESS_AND_FROM: &str = r#"
        WITH transfer AS (
            SELECT 
                tx_hash,
                address,
                topic_address_native(topic1) AS "from",
                topic_address_native(topic2) AS "to",
                abi_uint_native(data, 0) AS tokens
            FROM logs
            WHERE selector = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        )
        SELECT COUNT(tx_hash) FROM transfer 
        WHERE address = '0x20c0000000000000000000000000000000000001' 
        AND "from" = '0x5bc1473610754a5ca10749552b119df90c1a1877'"#;

    /// Transfer with address filter pushed into CTE (optimized version)
    pub const TRANSFER_ADDRESS_FIRST: &str = r#"
        WITH transfer AS (
            SELECT 
                tx_hash,
                topic_address_native(topic1) AS "from"
            FROM logs
            WHERE selector = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
            AND address = '0x20c0000000000000000000000000000000000001'
        )
        SELECT COUNT(tx_hash) FROM transfer 
        WHERE "from" = '0x5bc1473610754a5ca10749552b119df90c1a1877'"#;

    pub const TRANSFER_SUM_BY_TO: &str = r#"
        SELECT 
            topic_address_native(topic2) AS "to",
            SUM(abi_uint_native(data, 0)) AS total
        FROM logs
        WHERE selector = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        GROUP BY 1
        ORDER BY total DESC
        LIMIT 10"#;

    pub const HOURLY_GAS_STATS: &str = r#"
        SELECT 
            date_trunc('hour', timestamp) as hour,
            COUNT(*) as block_count,
            SUM(gas_used) as total_gas,
            AVG(gas_used) as avg_gas
        FROM blocks
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 24"#;
}

// ============================================================================
// EXTRAPOLATION UTILITIES
// ============================================================================

struct ExtrapolationResult {
    name: String,
    data_points: Vec<(usize, f64)>,
    exponent: f64,        // Power-law exponent (b in time = a * n^b)
    coefficient: f64,     // Power-law coefficient (a)
    extrapolated_100m: f64,
}

impl ExtrapolationResult {
    /// Uses power-law regression: time = a * n^b
    /// In log-log space: log(time) = log(a) + b * log(n)
    fn from_points(name: &str, points: Vec<(usize, f64)>, target: usize) -> Self {
        // Transform to log-log space for linear regression
        let log_points: Vec<(f64, f64)> = points
            .iter()
            .filter(|(_, y)| *y > 0.0) // Avoid log(0)
            .map(|(x, y)| ((*x as f64).ln(), y.ln()))
            .collect();

        let n = log_points.len() as f64;
        if n < 2.0 {
            return Self {
                name: name.to_string(),
                data_points: points,
                exponent: 1.0,
                coefficient: 1.0,
                extrapolated_100m: 0.0,
            };
        }

        let sum_x: f64 = log_points.iter().map(|(x, _)| *x).sum();
        let sum_y: f64 = log_points.iter().map(|(_, y)| *y).sum();
        let sum_xy: f64 = log_points.iter().map(|(x, y)| x * y).sum();
        let sum_xx: f64 = log_points.iter().map(|(x, _)| x.powi(2)).sum();

        // Linear regression in log-log space: log(y) = log(a) + b * log(x)
        let exponent = (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x.powi(2));
        let log_coefficient = (sum_y - exponent * sum_x) / n;
        let coefficient = log_coefficient.exp();

        // Extrapolate: time = a * n^b
        let extrapolated = coefficient * (target as f64).powf(exponent);

        Self {
            name: name.to_string(),
            data_points: points,
            exponent,
            coefficient,
            extrapolated_100m: extrapolated.max(0.0),
        }
    }
}

fn print_extrapolation_report(results: &[ExtrapolationResult]) {
    println!("\n");
    println!("╔═══════════════════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                      EXTRAPOLATION REPORT (Power-law: time = a × n^b)                             ║");
    println!("╠═══════════════════════════════════════════════════════════════════════════════════════════════════╣");
    println!("║ {:^35} │ {:^8} │ {:^8} │ {:^10} │ {:^10} │ {:^6} ║", "Query", "1M", "8M", "→ 100M", "→ 80M", "exp");
    println!("╠═══════════════════════════════════════════════════════════════════════════════════════════════════╣");

    for r in results {
        let t_1m = r.data_points.iter().find(|(s, _)| *s == 1_000_000).map(|(_, t)| *t).unwrap_or(0.0);
        let t_8m = r.data_points.iter().find(|(s, _)| *s == 8_000_000).map(|(_, t)| *t).unwrap_or(0.0);

        let name = if r.name.len() > 35 {
            format!("{}...", &r.name[..32])
        } else {
            r.name.clone()
        };

        let extrapolated_str = if r.extrapolated_100m >= 1000.0 {
            format!("{:.1}s", r.extrapolated_100m / 1000.0)
        } else {
            format!("{:.0}ms", r.extrapolated_100m)
        };

        // Estimate 80M using power-law: time = a * n^b
        let est_80m = r.coefficient * (80_000_000_f64).powf(r.exponent);
        let est_80m_str = if est_80m >= 1000.0 {
            format!("{:.1}s", est_80m / 1000.0)
        } else {
            format!("{:.0}ms", est_80m)
        };

        println!(
            "║ {:<35} │ {:>6.1}ms │ {:>6.1}ms │ {:>10} │ {:>10} │ {:>6.2} ║",
            name, t_1m, t_8m, extrapolated_str, est_80m_str, r.exponent
        );
    }

    println!("╚═══════════════════════════════════════════════════════════════════════════════════════════════════╝");
    println!("\nExponent guide: 1.0 = linear, <1.0 = sub-linear (faster at scale), >1.0 = super-linear");
    println!("Actual 80M benchmark: transfer_by_address_and_from = 496ms");
}

// ============================================================================
// BENCHMARK FUNCTIONS
// ============================================================================

fn measure_query(pool: &DuckDbPool, rt: &Runtime, query: &str, iterations: usize) -> f64 {
    let mut times = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        rt.block_on(async {
            pool.query(query).await.unwrap();
        });
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    times.iter().sum::<f64>() / times.len() as f64
}

/// Main extrapolation benchmark - runs OLTP and OLAP queries on DuckDB and Postgres, extrapolates to 100M
fn bench_extrapolate(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Default Postgres URL from docker-compose
    let pg_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://tidx:tidx@localhost:5433/tidx".to_string());

    // Try to connect to Postgres
    let pg_pool = rt.block_on(async {
        match create_pool(&pg_url).await {
            Ok(pool) => {
                if let Err(e) = run_migrations(&pool).await {
                    eprintln!("Postgres migrations failed: {}", e);
                    return None;
                }
                Some(pool)
            }
            Err(e) => {
                eprintln!("Postgres not available: {} (start with: docker compose -f docker/local/docker-compose.yml up -d postgres)", e);
                None
            }
        }
    });

    // Queries that work on both engines
    // (name, duckdb_query, postgres_query)
    let queries: Vec<(&str, &str, &str)> = vec![
        // OLTP
        ("oltp/block_by_num", oltp::BLOCK_BY_NUM, "SELECT * FROM blocks WHERE num = 500"),
        ("oltp/logs_by_selector_limit", oltp::LOGS_BY_SELECTOR_LIMIT, "SELECT * FROM logs WHERE selector = '\\xddf252ad' LIMIT 100"),
        // OLAP
        ("olap/count_logs", olap::COUNT_LOGS, "SELECT COUNT(*) FROM logs"),
        ("olap/sum_gas_used", olap::SUM_GAS_USED, "SELECT SUM(gas_used) FROM blocks"),
        ("olap/group_by_tx_type", olap::GROUP_BY_TX_TYPE, "SELECT type, COUNT(*) FROM txs GROUP BY type"),
        ("olap/top_senders", olap::TOP_SENDERS, r#"SELECT "from", COUNT(*) as cnt FROM txs GROUP BY "from" ORDER BY cnt DESC LIMIT 10"#),
        ("olap/count_distinct_addresses", olap::COUNT_DISTINCT_ADDRESSES, "SELECT COUNT(DISTINCT address) FROM logs"),
    ];

    // Initialize results storage: engine/query_name -> [(size, time)]
    let mut duck_results: std::collections::HashMap<String, Vec<(usize, f64)>> =
        std::collections::HashMap::new();
    let mut pg_results: std::collections::HashMap<String, Vec<(usize, f64)>> =
        std::collections::HashMap::new();
    for (name, _, _) in &queries {
        duck_results.insert(name.to_string(), Vec::new());
        pg_results.insert(name.to_string(), Vec::new());
    }

    let mut group = c.benchmark_group("extrapolate");
    group.sample_size(10);

    // Generate data ONCE per size, run ALL queries on both engines
    for size in EXTRAPOLATE_SIZES {
        // Setup DuckDB
        println!("\n=== Setting up DuckDB with {} logs ===", size);
        let start = Instant::now();
        let duck_pool = setup_duckdb(size);
        println!("  DuckDB ready in {:?}", start.elapsed());

        // Setup Postgres (if available)
        if let Some(ref pool) = pg_pool {
            println!("=== Setting up Postgres with {} logs ===", size);
            let start = Instant::now();
            setup_postgres(&rt, pool, size);
            println!("  Postgres ready in {:?}", start.elapsed());
        }

        for (name, duck_query, pg_query) in &queries {
            // DuckDB benchmark
            group.bench_with_input(
                BenchmarkId::new(format!("{}/duckdb", name), size),
                &size,
                |b, _| {
                    b.to_async(&rt).iter(|| async {
                        duck_pool.query(duck_query).await.unwrap()
                    });
                },
            );
            let duck_time = measure_query(&duck_pool, &rt, duck_query, 5);
            duck_results.get_mut(*name).unwrap().push((size, duck_time));

            // Postgres benchmark (if available)
            if let Some(ref pool) = pg_pool {
                group.bench_with_input(
                    BenchmarkId::new(format!("{}/postgres", name), size),
                    &size,
                    |b, _| {
                        b.to_async(&rt).iter(|| async {
                            let conn = pool.get().await.unwrap();
                            conn.query(*pg_query, &[]).await.unwrap()
                        });
                    },
                );
                let pg_time = measure_query_pg(pool, &rt, pg_query, 5);
                pg_results.get_mut(*name).unwrap().push((size, pg_time));
            }
        }
    }

    group.finish();

    // Build extrapolation results for both engines
    let duck_extrapolations: Vec<ExtrapolationResult> = queries
        .iter()
        .map(|(name, _, _)| {
            let points = duck_results.remove(*name).unwrap();
            ExtrapolationResult::from_points(&format!("duckdb/{}", name), points, TARGET_ROWS)
        })
        .collect();

    let pg_extrapolations: Vec<ExtrapolationResult> = if pg_pool.is_some() {
        queries
            .iter()
            .map(|(name, _, _)| {
                let points = pg_results.remove(*name).unwrap();
                ExtrapolationResult::from_points(&format!("postgres/{}", name), points, TARGET_ROWS)
            })
            .collect()
    } else {
        Vec::new()
    };

    print_comparison_report(&queries, &duck_extrapolations, &pg_extrapolations);
}

fn measure_query_pg(pool: &Pool, rt: &Runtime, query: &str, iterations: usize) -> f64 {
    let mut times = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        rt.block_on(async {
            let conn = pool.get().await.unwrap();
            conn.query(query, &[]).await.unwrap();
        });
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    times.iter().sum::<f64>() / times.len() as f64
}

fn print_comparison_report(
    queries: &[(&str, &str, &str)],
    duck_results: &[ExtrapolationResult],
    pg_results: &[ExtrapolationResult],
) {
    println!("\n");
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║            Extrapolated Query Times (80M rows)               ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║ {:^35} │ {:^10} │ {:^10} ║", "Query", "DuckDB", "Postgres");
    println!("╠══════════════════════════════════════════════════════════════╣");

    for (i, (name, _, _)) in queries.iter().enumerate() {
        let duck = &duck_results[i];
        let duck_80m = duck.coefficient * (80_000_000_f64).powf(duck.exponent);

        let pg_80m = if i < pg_results.len() {
            let pg = &pg_results[i];
            pg.coefficient * (80_000_000_f64).powf(pg.exponent)
        } else {
            0.0
        };

        let duck_str = format_time(duck_80m);
        let pg_str = if pg_80m > 0.0 { format_time(pg_80m) } else { "N/A".to_string() };

        let name_short = if name.len() > 35 { &name[..32] } else { *name };

        println!("║ {:<35} │ {:>10} │ {:>10} ║", name_short, duck_str, pg_str);
    }

    println!("╚══════════════════════════════════════════════════════════════╝");
}

fn format_time(ms: f64) -> String {
    if ms >= 1000.0 {
        format!("{:.1}s", ms / 1000.0)
    } else {
        format!("{:.0}ms", ms)
    }
}

/// Generate synthetic data in PostgreSQL (same schema as DuckDB).
/// PostgreSQL uses BYTEA for hashes/addresses.
fn setup_postgres(rt: &Runtime, pool: &Pool, log_count: usize) {
    rt.block_on(async {
        let conn = pool.get().await.expect("Failed to get Postgres connection");

        // Clear existing data
        conn.batch_execute("TRUNCATE blocks, txs, logs CASCADE")
            .await
            .ok();

        let block_count = log_count / 10;
        let tx_count = log_count / 2;

        // Generate blocks (BYTEA columns)
        if block_count > 0 {
            conn.execute(
                &format!(
                    r#"INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
                       SELECT 
                           i as num,
                           decode(lpad(to_hex(i), 64, '0'), 'hex') as hash,
                           decode(lpad(to_hex(i - 1), 64, '0'), 'hex') as parent_hash,
                           '2024-01-01'::timestamptz + (i || ' seconds')::interval as timestamp,
                           1704067200000 + i * 1000 as timestamp_ms,
                           30000000 as gas_limit,
                           15000000 + (i % 10000000) as gas_used,
                           decode(lpad(to_hex(i % 1000), 40, '0'), 'hex') as miner
                       FROM generate_series(1, {block_count}) as t(i)"#
                ),
                &[],
            )
            .await
            .expect("Failed to insert blocks");
        }

        // Generate transactions (BYTEA columns)
        if tx_count > 0 {
            conn.execute(
                &format!(
                    r#"INSERT INTO txs (block_num, block_timestamp, idx, hash, type, "from", "to", value, input,
                                       gas_limit, max_fee_per_gas, max_priority_fee_per_gas, gas_used,
                                       nonce_key, nonce, call_count)
                       SELECT 
                           i as block_num,
                           '2024-01-01'::timestamptz + (i || ' seconds')::interval as block_timestamp,
                           0 as idx,
                           decode(lpad(to_hex(i), 64, '0'), 'hex') as hash,
                           (i % 3)::smallint as type,
                           decode(lpad(to_hex(i % 10000), 40, '0'), 'hex') as "from",
                           decode(lpad(to_hex((i + 1) % 10000), 40, '0'), 'hex') as "to",
                           (i % 1000000000)::text as value,
                           '\x00'::bytea as input,
                           21000 + (i % 100000) as gas_limit,
                           '1000000000' as max_fee_per_gas,
                           '100000000' as max_priority_fee_per_gas,
                           21000 as gas_used,
                           decode(lpad(to_hex(i % 10000), 40, '0'), 'hex') as nonce_key,
                           (i / 10000)::bigint as nonce,
                           1::smallint as call_count
                       FROM generate_series(1, {tx_count}) as t(i)"#
                ),
                &[],
            )
            .await
            .expect("Failed to insert txs");
        }

        // Generate logs (BYTEA columns, topics is an array)
        let target_contract = "20c0000000000000000000000000000000000001"; // no 0x prefix
        let hot_sender = "5bc1473610754a5ca10749552b119df90c1a1877";

        conn.execute(
            &format!(
                r#"INSERT INTO logs (block_num, block_timestamp, log_idx, tx_idx, tx_hash, address, selector, topics, data)
                   SELECT 
                       i as block_num,
                       '2024-01-01'::timestamptz + (i || ' seconds')::interval as block_timestamp,
                       0 as log_idx,
                       0 as tx_idx,
                       decode(lpad(to_hex(i), 64, '0'), 'hex') as tx_hash,
                       CASE WHEN i % 5 < 4 THEN decode('{target_contract}', 'hex')
                            ELSE decode(lpad(to_hex(i % 1000), 40, '0'), 'hex')
                       END as address,
                       decode('ddf252ad', 'hex') as selector,
                       ARRAY[
                           decode('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef', 'hex'),
                           CASE WHEN i % 100 = 0 
                                THEN decode('000000000000000000000000{hot_sender}', 'hex')
                                ELSE decode(lpad(to_hex(i % 10000), 64, '0'), 'hex')
                           END,
                           decode(lpad(to_hex((i + 1) % 10000), 64, '0'), 'hex')
                       ] as topics,
                       decode(lpad(to_hex(i % 1000000000), 64, '0'), 'hex') as data
                   FROM generate_series(1, {log_count}) as t(i)"#
            ),
            &[],
        )
        .await
        .expect("Failed to insert logs");

        // Analyze tables for query planning
        conn.batch_execute("ANALYZE blocks; ANALYZE txs; ANALYZE logs;")
            .await
            .ok();
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_extrapolate
);
criterion_main!(benches);
