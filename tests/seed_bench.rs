//! Standalone binary for seeding benchmark data.
//! Run with: cargo test --test seed_bench -- --ignored --nocapture

mod common;

use anyhow::Result;

#[tokio::test]
#[ignore = "Only run explicitly with --ignored"]
async fn seed_benchmark_data() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("tidx=info,seed=info")
        .init();

    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let txs: u64 = std::env::var("SEED_TXS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5_000_000);

    let txs_per_block: u64 = std::env::var("SEED_TXS_PER_BLOCK")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let pool = tidx::db::create_pool(&db_url).await?;
    tidx::db::run_migrations(&pool).await?;

    let config = common::seed::SeedConfig::new(txs, txs_per_block);
    common::seed::seed(&pool, &config).await?;

    Ok(())
}
