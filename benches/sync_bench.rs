use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tokio::runtime::Runtime;

use tidx::db::{create_pool, run_migrations};
use tidx::sync::writer::{write_blocks, write_logs, write_txs};
use tidx::types::{BlockRow, LogRow, TxRow};

fn generate_blocks(count: usize, offset: usize) -> Vec<BlockRow> {
    (0..count)
        .map(|i| {
            let n = (offset + i) as i64;
            BlockRow {
                num: n,
                hash: vec![(n % 256) as u8; 32],
                parent_hash: vec![((n - 1) % 256) as u8; 32],
                timestamp: chrono::Utc::now(),
                timestamp_ms: chrono::Utc::now().timestamp_millis(),
                gas_limit: 30_000_000,
                gas_used: 15_000_000,
                miner: vec![0u8; 20],
                extra_data: Some(vec![0u8; 32]),
            }
        })
        .collect()
}

fn generate_txs(count: usize, block_num: i64) -> Vec<TxRow> {
    (0..count)
        .map(|i| TxRow {
            block_num,
            block_timestamp: chrono::Utc::now(),
            idx: i as i32,
            hash: vec![i as u8; 32],
            tx_type: 2,
            from: vec![1u8; 20],
            to: Some(vec![2u8; 20]),
            value: "0".to_string(),
            input: vec![0u8; 100],
            gas_limit: 21000,
            max_fee_per_gas: "1000000000".to_string(),
            max_priority_fee_per_gas: "100000000".to_string(),
            gas_used: Some(21000),
            nonce_key: vec![1u8; 20],
            nonce: i as i64,
            fee_token: None,
            fee_payer: None,
            calls: None,
            call_count: 1,
            valid_before: None,
            valid_after: None,
            signature_type: Some(0),
        })
        .collect()
}

fn generate_logs(count: usize, block_num: i64) -> Vec<LogRow> {
    (0..count)
        .map(|i| LogRow {
            block_num,
            block_timestamp: chrono::Utc::now(),
            log_idx: i as i32,
            tx_idx: (i % 100) as i32,
            tx_hash: vec![i as u8; 32],
            address: vec![3u8; 20],
            selector: Some(vec![0xddu8; 32]), // Transfer selector (full 32-byte topic0)
            topic0: Some(vec![0xddu8; 32]),
            topic1: Some(vec![1u8; 32]),
            topic2: Some(vec![2u8; 32]),
            topic3: None,
            data: vec![0u8; 64],
            is_virtual_forward: false,
        })
        .collect()
}

fn bench_batch_writes(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = rt.block_on(async {
        let pool = create_pool(&db_url).await.expect("Failed to create pool");
        run_migrations(&pool).await.expect("Failed to run migrations");
        pool
    });

    let mut group = c.benchmark_group("batch_writes");
    group.sample_size(50);

    // Benchmark block batch sizes
    for batch_size in [10, 50, 100] {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("blocks", batch_size),
            &batch_size,
            |b, &size| {
                let mut offset = 1_000_000; // Start high to avoid conflicts
                b.to_async(&rt).iter(|| {
                    let blocks = generate_blocks(size, offset);
                    offset += size;
                    let pool = pool.clone();
                    async move {
                        write_blocks(&pool, &blocks).await.unwrap();
                    }
                });
            },
        );
    }

    // Benchmark tx batch sizes
    for batch_size in [100, 500, 1000] {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("txs", batch_size),
            &batch_size,
            |b, &size| {
                let mut block_num = 2_000_000i64;
                b.to_async(&rt).iter(|| {
                    let txs = generate_txs(size, block_num);
                    block_num += 1;
                    let pool = pool.clone();
                    async move {
                        write_txs(&pool, &txs).await.unwrap();
                    }
                });
            },
        );
    }

    // Benchmark log batch sizes
    for batch_size in [100, 1000, 5000] {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("logs", batch_size),
            &batch_size,
            |b, &size| {
                let mut block_num = 3_000_000i64;
                b.to_async(&rt).iter(|| {
                    let logs = generate_logs(size, block_num);
                    block_num += 1;
                    let pool = pool.clone();
                    async move {
                        write_logs(&pool, &logs).await.unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_mixed_workload(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = rt.block_on(async {
        let pool = create_pool(&db_url).await.expect("Failed to create pool");
        run_migrations(&pool).await.expect("Failed to run migrations");
        pool
    });

    let mut group = c.benchmark_group("sync_batch");
    group.sample_size(30);

    // Simulate syncing a batch of blocks with realistic tx/log counts
    // 10 blocks, 500 txs each, 1000 logs each
    group.throughput(Throughput::Elements(10)); // 10 blocks
    group.bench_function("10_blocks_realistic", |b| {
        let mut base_block = 4_000_000i64;
        b.to_async(&rt).iter(|| {
            let blocks = generate_blocks(10, base_block as usize);
            let txs: Vec<_> = (0..10)
                .flat_map(|i| generate_txs(500, base_block + i64::from(i)))
                .collect();
            let logs: Vec<_> = (0..10)
                .flat_map(|i| generate_logs(1000, base_block + i64::from(i)))
                .collect();
            base_block += 10;

            let pool = pool.clone();
            async move {
                write_blocks(&pool, &blocks).await.unwrap();
                write_txs(&pool, &txs).await.unwrap();
                write_logs(&pool, &logs).await.unwrap();
            }
        });
    });

    // High TPS simulation: 10 blocks, 1000 txs each, 3000 logs each
    group.throughput(Throughput::Elements(10));
    group.bench_function("10_blocks_high_tps", |b| {
        let mut base_block = 5_000_000i64;
        b.to_async(&rt).iter(|| {
            let blocks = generate_blocks(10, base_block as usize);
            let txs: Vec<_> = (0..10)
                .flat_map(|i| generate_txs(1000, base_block + i64::from(i)))
                .collect();
            let logs: Vec<_> = (0..10)
                .flat_map(|i| generate_logs(3000, base_block + i64::from(i)))
                .collect();
            base_block += 10;

            let pool = pool.clone();
            async move {
                write_blocks(&pool, &blocks).await.unwrap();
                write_txs(&pool, &txs).await.unwrap();
                write_logs(&pool, &logs).await.unwrap();
            }
        });
    });

    group.finish();
}

fn bench_copy_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = rt.block_on(async {
        let pool = create_pool(&db_url).await.expect("Failed to create pool");
        run_migrations(&pool).await.expect("Failed to run migrations");
        pool
    });

    let mut group = c.benchmark_group("copy_throughput");
    group.sample_size(20);

    // Large batch COPY benchmark for txs
    for batch_size in [1000, 5000, 10000] {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("txs_copy", batch_size),
            &batch_size,
            |b, &size| {
                let mut block_num = 100_000_000i64;
                b.to_async(&rt).iter(|| {
                    let blocks = generate_blocks(1, block_num as usize);
                    let txs = generate_txs(size, block_num);
                    block_num += 1;
                    let pool = pool.clone();
                    async move {
                        write_blocks(&pool, &blocks).await.unwrap();
                        write_txs(&pool, &txs).await.unwrap();
                    }
                });
            },
        );
    }

    // Large batch COPY benchmark for logs
    for batch_size in [1000, 5000, 10000, 30000] {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("logs_copy", batch_size),
            &batch_size,
            |b, &size| {
                let mut block_num = 200_000_000i64;
                b.to_async(&rt).iter(|| {
                    let blocks = generate_blocks(1, block_num as usize);
                    let logs = generate_logs(size, block_num);
                    block_num += 1;
                    let pool = pool.clone();
                    async move {
                        write_blocks(&pool, &blocks).await.unwrap();
                        write_logs(&pool, &logs).await.unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_batch_writes, bench_mixed_workload, bench_copy_throughput);
criterion_main!(benches);
