use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use tidx::db::{create_pool, partitions, run_migrations};
use tidx::sync::writer::{write_batch, write_blocks, write_logs, write_txs};
use tidx::types::{BlockRow, LogRow, ReceiptRow, TxRow};

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
                consensus_proposer: None,
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
        run_migrations(&pool)
            .await
            .expect("Failed to run migrations");
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
        run_migrations(&pool)
            .await
            .expect("Failed to run migrations");
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
        run_migrations(&pool)
            .await
            .expect("Failed to run migrations");
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

/// Rows of `blocks` consecutive blocks with 8 txs, 8 receipts and 24 logs each,
/// with distinct hashes and addresses like real blocks.
fn generate_batch(
    first: i64,
    blocks: usize,
) -> (Vec<BlockRow>, Vec<TxRow>, Vec<LogRow>, Vec<ReceiptRow>) {
    fn bytes(seed: i64, tag: u8, len: usize) -> Vec<u8> {
        let mut out = vec![tag; len];
        out[..8].copy_from_slice(&seed.to_be_bytes());
        out
    }

    let now = chrono::Utc::now();
    let (mut block_rows, mut txs, mut logs, mut receipts) = (vec![], vec![], vec![], vec![]);
    for num in first..first + blocks as i64 {
        block_rows.extend(generate_blocks(1, num as usize));
        for idx in 0..8 {
            let id = num * 8 + i64::from(idx);
            let from = bytes(id % 5000, 1, 20);
            txs.push(TxRow {
                block_num: num,
                block_timestamp: now,
                idx,
                hash: bytes(id, 2, 32),
                tx_type: 118,
                from: from.clone(),
                to: Some(bytes(id % 300, 3, 20)),
                value: "0".to_string(),
                input: vec![0u8; 68],
                gas_limit: 100_000,
                max_fee_per_gas: "1000".to_string(),
                max_priority_fee_per_gas: "0".to_string(),
                gas_used: Some(50_000),
                nonce_key: vec![0u8; 32],
                nonce: id,
                fee_token: Some(vec![4u8; 20]),
                fee_payer: Some(from.clone()),
                calls: None,
                call_count: 1,
                valid_before: None,
                valid_after: None,
                signature_type: Some(0),
            });
            receipts.push(ReceiptRow {
                block_num: num,
                block_timestamp: now,
                tx_idx: idx,
                tx_hash: bytes(id, 2, 32),
                from: from.clone(),
                to: Some(bytes(id % 300, 3, 20)),
                contract_address: None,
                gas_used: 50_000,
                cumulative_gas_used: 50_000 * i64::from(idx + 1),
                effective_gas_price: Some("1000".to_string()),
                status: Some(1),
                fee_payer: Some(from.clone()),
                tx_type: Some(118),
                fee_token: Some(vec![4u8; 20]),
            });
            for i in 0..3 {
                logs.push(LogRow {
                    block_num: num,
                    block_timestamp: now,
                    log_idx: idx * 3 + i,
                    tx_idx: idx,
                    tx_hash: bytes(id, 2, 32),
                    address: bytes(i64::from(i), 5, 20),
                    selector: Some(vec![0xdd; 32]),
                    topic0: Some(vec![0xdd; 32]),
                    topic1: Some(bytes(id % 5000, 6, 32)),
                    topic2: Some(bytes(id % 300, 7, 32)),
                    topic3: None,
                    data: bytes(id, 8, 32),
                    is_virtual_forward: false,
                });
            }
        }
    }
    (block_rows, txs, logs, receipts)
}

/// `write_batch` as called by every sync path, on a database holding a year of
/// weekly partitions like a long-running deployment.
fn bench_write_batch(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let (pool, first_block) = rt.block_on(async {
        let pool = create_pool(&db_url).await.expect("Failed to create pool");
        run_migrations(&pool)
            .await
            .expect("Failed to run migrations");
        let now = chrono::Utc::now();
        partitions::ensure_partitions_covering(&pool, now - chrono::Duration::weeks(52), now)
            .await
            .expect("Failed to create partitions");
        // Write new blocks on every run instead of replacing earlier runs' rows.
        let first_block: i64 = pool
            .get()
            .await
            .unwrap()
            .query_one("SELECT GREATEST(MAX(num) + 1, 300000000) FROM blocks", &[])
            .await
            .unwrap()
            .get(0);
        (pool, first_block)
    });

    let mut group = c.benchmark_group("write_batch");
    group.sample_size(50);

    for blocks in [1, 10, 100] {
        group.throughput(Throughput::Elements(blocks as u64));
        group.bench_with_input(BenchmarkId::new("blocks", blocks), &blocks, |b, &blocks| {
            let mut next_block = first_block + blocks as i64 * 1_000_000;
            b.to_async(&rt).iter_batched(
                || {
                    let batch = generate_batch(next_block, blocks);
                    next_block += blocks as i64;
                    batch
                },
                |(block_rows, txs, logs, receipts)| {
                    let pool = pool.clone();
                    async move {
                        write_batch(&pool, &block_rows, &txs, &logs, &receipts)
                            .await
                            .unwrap();
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_batch_writes,
    bench_mixed_workload,
    bench_copy_throughput,
    bench_write_batch
);
criterion_main!(benches);
