use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use tidx::db::{create_pool, run_migrations};
use tidx::types::BlockRow;

fn generate_blocks(count: usize) -> Vec<BlockRow> {
    (0..count)
        .map(|i| BlockRow {
            num: i as i64,
            hash: vec![i as u8; 32],
            parent_hash: vec![(i.wrapping_sub(1)) as u8; 32],
            timestamp: chrono::Utc::now(),
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            gas_limit: 30_000_000,
            gas_used: 15_000_000,
            miner: vec![0u8; 20],
            extra_data: Some(vec![0u8; 32]),
        })
        .collect()
}

fn bench_block_insert(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = rt.block_on(async {
        let pool = create_pool(&db_url).await.expect("Failed to create pool");
        run_migrations(&pool)
            .await
            .expect("Failed to run migrations");
        pool
    });

    let mut group = c.benchmark_group("block_insert");

    for batch_size in [10, 100, 1000] {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("individual", batch_size),
            &batch_size,
            |b, &size| {
                let blocks = generate_blocks(size);
                b.to_async(&rt).iter(|| async {
                    for block in &blocks {
                        tidx::sync::writer::write_block(&pool, block).await.unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_block_insert);
criterion_main!(benches);
