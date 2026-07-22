use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use serde::Serialize;
use serde_json::Value;
use sha3::{Digest, Keccak256};
use tidx::db::{Pool, create_pool};
use tokio::runtime::Runtime;

const SCHEMA: &str = "log_index_bench";
const LOGS_TABLE: &str = "logs";
const WRITE_TABLE: &str = "write_logs";
const HOT_HIT_ADDRESS: &str = "1111111111111111111111111111111111111111";
const HOT_MISS_ADDRESS: &str = "2222222222222222222222222222222222222222";
const TOKEN_HIT_ADDRESS: &str = "0000000000000000000000000000000000000001";
const TOKEN_MISS_ADDRESS: &str = "ffffffffffffffffffffffffffffffffffffffff";
const DEFAULT_ROWS: u64 = 5_000_000;
const DEFAULT_SAMPLES: usize = 20;
const DEFAULT_WRITE_ROWS: u64 = 10_000;
const ROLE_EVENT_EVERY: u64 = 997;
const TOKEN_CREATED_EVERY: u64 = 1_000;

#[derive(Clone, Copy)]
enum IndexSet {
    Baseline,
    Exact,
    Generic,
}

impl IndexSet {
    const ALL: [Self; 3] = [Self::Baseline, Self::Exact, Self::Generic];

    const fn name(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::Exact => "exact",
            Self::Generic => "generic",
        }
    }
}

#[derive(Serialize)]
struct BenchmarkReport {
    rows: u64,
    samples: usize,
    write_rows: u64,
    scenarios: Vec<ScenarioReport>,
}

#[derive(Serialize)]
struct ScenarioReport {
    name: &'static str,
    index_build_ms: f64,
    candidate_index_bytes: i64,
    write_p50_ms: f64,
    write_p95_ms: f64,
    queries: Vec<QueryReport>,
}

#[derive(Serialize)]
struct QueryReport {
    name: &'static str,
    sql: String,
    rows: usize,
    p50_ms: f64,
    p95_ms: f64,
    plan: Value,
}

struct QueryCase {
    name: &'static str,
    sql: String,
    expected_rows: usize,
}

fn bench_log_indexes(c: &mut Criterion) {
    let runtime = Runtime::new().expect("create Tokio runtime");
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let rows = env_value("LOG_INDEX_BENCH_ROWS", DEFAULT_ROWS).max(TOKEN_CREATED_EVERY);
    let samples = env_value("LOG_INDEX_BENCH_SAMPLES", DEFAULT_SAMPLES).max(10);
    let write_rows = env_value("LOG_INDEX_BENCH_WRITE_ROWS", DEFAULT_WRITE_ROWS);

    let pool = runtime
        .block_on(create_pool(&database_url))
        .expect("connect to PostgreSQL");
    let topics = Topics::new();
    runtime
        .block_on(prepare_corpus(&pool, rows, &topics))
        .expect("prepare log benchmark corpus");

    let mut reports = Vec::new();
    for index_set in IndexSet::ALL {
        let (build_time, index_bytes) = runtime
            .block_on(configure_indexes(&pool, LOGS_TABLE, index_set))
            .expect("configure query indexes");
        let queries = queries(index_set, &topics);
        let query_reports = runtime
            .block_on(profile_queries(&pool, &queries, samples))
            .expect("profile log queries");
        let write_times = runtime
            .block_on(profile_writes(
                &pool, index_set, write_rows, samples, &topics,
            ))
            .expect("profile log inserts");

        let mut group = c.benchmark_group(format!("log_indexes/{}", index_set.name()));
        group.sample_size(samples);
        group.throughput(Throughput::Elements(1));
        for query in &queries {
            let pool = pool.clone();
            group.bench_function(query.name, |b| {
                b.to_async(&runtime).iter(|| async {
                    let conn = pool.get().await.expect("get PostgreSQL connection");
                    conn.query(query.sql.as_str(), &[])
                        .await
                        .expect("run benchmark query")
                });
            });
        }
        group.finish();

        reports.push(ScenarioReport {
            name: index_set.name(),
            index_build_ms: duration_ms(build_time),
            candidate_index_bytes: index_bytes,
            write_p50_ms: percentile_ms(&write_times, 50),
            write_p95_ms: percentile_ms(&write_times, 95),
            queries: query_reports,
        });
    }

    let report = BenchmarkReport {
        rows,
        samples,
        write_rows,
        scenarios: reports,
    };
    write_report(&report).expect("write benchmark report");
}

fn env_value<T>(name: &str, default: T) -> T
where
    T: std::str::FromStr,
{
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

struct Topics {
    role_membership_updated: String,
    token_created: String,
    transfer: String,
}

impl Topics {
    fn new() -> Self {
        Self {
            role_membership_updated: event_topic(
                "RoleMembershipUpdated(bytes32,address,address,bool)",
            ),
            token_created: event_topic(
                "TokenCreated(address,string,string,string,address,address,bytes32)",
            ),
            transfer: event_topic("Transfer(address,address,uint256)"),
        }
    }
}

fn event_topic(signature: &str) -> String {
    hex::encode(Keccak256::digest(signature.as_bytes()))
}

async fn prepare_corpus(pool: &Pool, rows: u64, topics: &Topics) -> Result<()> {
    let conn = pool.get().await?;
    let database: String = conn
        .query_one("SELECT current_database()", &[])
        .await?
        .get(0);
    anyhow::ensure!(
        database == "tidx_log_index_bench",
        "log index benchmark requires the tidx_log_index_bench database"
    );
    conn.batch_execute(&format!(
        r#"
        DROP SCHEMA IF EXISTS {SCHEMA} CASCADE;
        CREATE SCHEMA {SCHEMA};
        CREATE FUNCTION {SCHEMA}.abi_address(input BYTEA) RETURNS BYTEA AS $$
            SELECT substring(input FROM 13 FOR 20)
        $$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
        {};
        {};
        "#,
        create_table_sql(LOGS_TABLE),
        baseline_indexes_sql(LOGS_TABLE),
    ))
    .await?;

    let start = Instant::now();
    conn.batch_execute("SET synchronous_commit = off").await?;
    conn.batch_execute(&seed_sql(LOGS_TABLE, rows, topics))
        .await?;
    conn.batch_execute(&format!("VACUUM ANALYZE {SCHEMA}.{LOGS_TABLE}"))
        .await?;
    eprintln!(
        "seeded {rows} log rows in {:.1}s",
        start.elapsed().as_secs_f64()
    );
    Ok(())
}

fn create_table_sql(table: &str) -> String {
    format!(
        r#"
        CREATE TABLE {SCHEMA}.{table} (
            block_num BIGINT NOT NULL,
            block_timestamp TIMESTAMPTZ NOT NULL,
            log_idx INTEGER NOT NULL,
            tx_idx INTEGER NOT NULL,
            tx_hash BYTEA NOT NULL,
            address BYTEA NOT NULL,
            selector BYTEA,
            topic0 BYTEA,
            topic1 BYTEA,
            topic2 BYTEA,
            topic3 BYTEA,
            data BYTEA NOT NULL,
            is_virtual_forward BOOLEAN NOT NULL DEFAULT FALSE,
            PRIMARY KEY (block_timestamp, block_num, log_idx)
        )
        "#
    )
}

fn baseline_indexes_sql(table: &str) -> String {
    format!(
        r#"
        CREATE INDEX {table}_selector ON {SCHEMA}.{table} (selector, block_timestamp DESC);
        CREATE INDEX {table}_address ON {SCHEMA}.{table} (address, block_timestamp DESC)
        "#
    )
}

fn seed_sql(table: &str, rows: u64, topics: &Topics) -> String {
    format!(
        r#"
        INSERT INTO {SCHEMA}.{table} (
            block_num, block_timestamp, log_idx, tx_idx, tx_hash, address,
            selector, topic0, topic1, topic2, topic3, data, is_virtual_forward
        )
        SELECT
            g,
            TIMESTAMPTZ '2026-07-20 00:00:00+00' + (g % 604800) * INTERVAL '1 second',
            ((g - 1) % 100)::INTEGER,
            ((g - 1) % 100)::INTEGER,
            decode(lpad(to_hex(g), 64, '0'), 'hex'),
            CASE
                WHEN g <= 3 THEN decode('{HOT_HIT_ADDRESS}', 'hex')
                WHEN g % {ROLE_EVENT_EVERY} = 0
                    THEN decode(lpad(to_hex(65536 + g % 10000), 40, '0'), 'hex')
                WHEN g % 10 < 4 THEN decode('{HOT_HIT_ADDRESS}', 'hex')
                WHEN g % 10 < 8 THEN decode('{HOT_MISS_ADDRESS}', 'hex')
                ELSE decode(lpad(to_hex(256 + g % 10000), 40, '0'), 'hex')
            END,
            CASE
                WHEN g <= 3 OR g % {ROLE_EVENT_EVERY} = 0 THEN decode('{role_topic}', 'hex')
                WHEN g % {TOKEN_CREATED_EVERY} = 0 THEN decode('{token_topic}', 'hex')
                ELSE decode('{transfer_topic}', 'hex')
            END,
            CASE
                WHEN g <= 3 OR g % {ROLE_EVENT_EVERY} = 0 THEN decode('{role_topic}', 'hex')
                WHEN g % {TOKEN_CREATED_EVERY} = 0 THEN decode('{token_topic}', 'hex')
                ELSE decode('{transfer_topic}', 'hex')
            END,
            CASE
                WHEN g % {TOKEN_CREATED_EVERY} = 0
                    THEN decode(lpad(to_hex(g / {TOKEN_CREATED_EVERY}), 64, '0'), 'hex')
                ELSE decode(lpad(to_hex(g % 10000), 64, '0'), 'hex')
            END,
            decode(lpad(to_hex(g % 10000), 64, '0'), 'hex'),
            NULL,
            decode(repeat('00', 32), 'hex'),
            FALSE
        FROM generate_series(1, {rows}) AS series(g)
        "#,
        role_topic = topics.role_membership_updated,
        token_topic = topics.token_created,
        transfer_topic = topics.transfer,
    )
}

async fn configure_indexes(
    pool: &Pool,
    table: &str,
    index_set: IndexSet,
) -> Result<(Duration, i64)> {
    let conn = pool.get().await?;
    for suffix in ["exact_role", "exact_token", "generic_role", "generic_token"] {
        conn.batch_execute(&format!(
            "DROP INDEX IF EXISTS {SCHEMA}.{}_{}",
            table, suffix
        ))
        .await?;
    }

    let start = Instant::now();
    conn.batch_execute(&candidate_indexes_sql(table, index_set))
        .await?;
    let build_time = start.elapsed();
    conn.batch_execute(&format!("ANALYZE {SCHEMA}.{table}"))
        .await?;

    let mut bytes = 0i64;
    for name in candidate_index_names(table, index_set) {
        bytes += conn
            .query_one(
                &format!("SELECT pg_relation_size('{SCHEMA}.{name}'::regclass)::BIGINT"),
                &[],
            )
            .await?
            .get::<_, i64>(0);
    }
    Ok((build_time, bytes))
}

fn candidate_indexes_sql(table: &str, index_set: IndexSet) -> String {
    match index_set {
        IndexSet::Baseline => String::new(),
        IndexSet::Exact => format!(
            r#"
            CREATE INDEX {table}_exact_role
                ON {SCHEMA}.{table} (address, topic0, block_num, log_idx);
            CREATE INDEX {table}_exact_token
                ON {SCHEMA}.{table} (selector, {SCHEMA}.abi_address(topic1))
            "#
        ),
        IndexSet::Generic => format!(
            r#"
            CREATE INDEX {table}_generic_role
                ON {SCHEMA}.{table} (address, selector, block_num, log_idx);
            CREATE INDEX {table}_generic_token
                ON {SCHEMA}.{table} (selector, topic1, block_num, log_idx)
            "#
        ),
    }
}

fn candidate_index_names(table: &str, index_set: IndexSet) -> Vec<String> {
    let suffixes: &[&str] = match index_set {
        IndexSet::Baseline => &[],
        IndexSet::Exact => &["exact_role", "exact_token"],
        IndexSet::Generic => &["generic_role", "generic_token"],
    };
    suffixes
        .iter()
        .map(|suffix| format!("{table}_{suffix}"))
        .collect()
}

fn queries(index_set: IndexSet, topics: &Topics) -> Vec<QueryCase> {
    let role_column = if matches!(index_set, IndexSet::Generic) {
        "selector"
    } else {
        "topic0"
    };
    let token_filter = |address: &str| {
        if matches!(index_set, IndexSet::Generic) {
            format!("topic1 = decode('000000000000000000000000{address}', 'hex')")
        } else {
            format!("{SCHEMA}.abi_address(topic1) = decode('{address}', 'hex')")
        }
    };

    vec![
        QueryCase {
            name: "roles_hit",
            sql: role_query(
                role_column,
                HOT_HIT_ADDRESS,
                &topics.role_membership_updated,
            ),
            expected_rows: 3,
        },
        QueryCase {
            name: "roles_miss",
            sql: role_query(
                role_column,
                HOT_MISS_ADDRESS,
                &topics.role_membership_updated,
            ),
            expected_rows: 0,
        },
        QueryCase {
            name: "token_created_hit",
            sql: token_query(&topics.token_created, &token_filter(TOKEN_HIT_ADDRESS)),
            expected_rows: 1,
        },
        QueryCase {
            name: "token_created_miss",
            sql: token_query(&topics.token_created, &token_filter(TOKEN_MISS_ADDRESS)),
            expected_rows: 0,
        },
    ]
}

fn role_query(column: &str, address: &str, topic: &str) -> String {
    format!(
        r#"
        SELECT topic0, topic1, topic2, data, block_timestamp, tx_hash, block_num, log_idx
        FROM {SCHEMA}.{LOGS_TABLE}
        WHERE address = decode('{address}', 'hex')
          AND {column} = decode('{topic}', 'hex')
          AND block_num > 0
        ORDER BY block_num ASC, log_idx ASC
        LIMIT 10000
        "#
    )
}

fn token_query(topic: &str, filter: &str) -> String {
    format!(
        r#"
        WITH tokencreated AS (
            SELECT block_num, block_timestamp, log_idx, tx_idx, tx_hash, address,
                   selector, topic1, topic2, topic3, data,
                   {SCHEMA}.abi_address(topic1) AS token
            FROM {SCHEMA}.{LOGS_TABLE}
            WHERE selector = decode('{topic}', 'hex')
              AND block_num > 0
        )
        SELECT token, block_timestamp
        FROM tokencreated
        WHERE {filter}
        LIMIT 1000
        "#
    )
}

async fn profile_queries(
    pool: &Pool,
    queries: &[QueryCase],
    samples: usize,
) -> Result<Vec<QueryReport>> {
    let conn = pool.get().await?;
    let mut reports = Vec::with_capacity(queries.len());
    for query in queries {
        let warm_rows = conn.query(query.sql.as_str(), &[]).await?;
        anyhow::ensure!(
            warm_rows.len() == query.expected_rows,
            "{} returned {} rows, expected {}",
            query.name,
            warm_rows.len(),
            query.expected_rows
        );

        let mut timings = Vec::with_capacity(samples);
        for _ in 0..samples {
            let start = Instant::now();
            let rows = conn.query(query.sql.as_str(), &[]).await?;
            timings.push(start.elapsed());
            anyhow::ensure!(rows.len() == query.expected_rows);
        }

        let explain = format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {}", query.sql);
        let plan = conn.query_one(explain.as_str(), &[]).await?.get(0);
        reports.push(QueryReport {
            name: query.name,
            sql: query.sql.trim().to_string(),
            rows: query.expected_rows,
            p50_ms: percentile_ms(&timings, 50),
            p95_ms: percentile_ms(&timings, 95),
            plan,
        });
    }
    Ok(reports)
}

async fn profile_writes(
    pool: &Pool,
    index_set: IndexSet,
    rows: u64,
    samples: usize,
    topics: &Topics,
) -> Result<Vec<Duration>> {
    let conn = pool.get().await?;
    conn.batch_execute(&format!(
        r#"
        DROP TABLE IF EXISTS {SCHEMA}.{WRITE_TABLE};
        {};
        {}
        "#,
        create_table_sql(WRITE_TABLE),
        baseline_indexes_sql(WRITE_TABLE),
    ))
    .await?;
    configure_indexes(pool, WRITE_TABLE, index_set).await?;

    let insert = seed_sql(WRITE_TABLE, rows, topics);
    conn.batch_execute(&insert).await?;
    conn.batch_execute(&format!("TRUNCATE {SCHEMA}.{WRITE_TABLE}"))
        .await?;
    let mut timings = Vec::with_capacity(samples);
    for _ in 0..samples {
        let start = Instant::now();
        conn.batch_execute(&insert).await?;
        timings.push(start.elapsed());
        conn.batch_execute(&format!("TRUNCATE {SCHEMA}.{WRITE_TABLE}"))
            .await?;
    }
    Ok(timings)
}

fn percentile_ms(values: &[Duration], percentile: usize) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let index = (sorted.len() * percentile).div_ceil(100).saturating_sub(1);
    duration_ms(sorted[index])
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn write_report(report: &BenchmarkReport) -> Result<()> {
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target"));
    let directory = target.join("log-index-bench");
    fs::create_dir_all(&directory).context("create benchmark report directory")?;
    let path = directory.join("report.json");
    fs::write(&path, serde_json::to_vec_pretty(report)?)
        .with_context(|| format!("write {}", path.display()))?;
    eprintln!("benchmark report: {}", path.display());
    Ok(())
}

criterion_group!(benches, bench_log_indexes);
criterion_main!(benches);
