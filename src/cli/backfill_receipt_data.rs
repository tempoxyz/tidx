use anyhow::Result;
use clap::Args as ClapArgs;
use std::path::PathBuf;
use tracing::info;

use tidx::config::Config;
use tidx::db;

#[derive(ClapArgs)]
pub struct Args {
    /// Path to config file
    #[arg(short, long, default_value = "config.toml")]
    pub config: PathBuf,

    /// Chain ID (uses first chain if not specified)
    #[arg(long)]
    pub chain_id: Option<u64>,

    /// Number of blocks per batch
    #[arg(long, default_value = "10000")]
    pub batch_size: i64,
}

pub async fn run(args: Args) -> Result<()> {
    let config = Config::load(&args.config)?;

    let chain = if let Some(id) = args.chain_id {
        config
            .chains
            .iter()
            .find(|c| c.chain_id == id)
            .ok_or_else(|| anyhow::anyhow!("Chain ID {} not found in config", id))?
    } else {
        config
            .chains
            .first()
            .ok_or_else(|| anyhow::anyhow!("No chains configured"))?
    };

    let pg_url = chain.resolved_pg_url()?;
    let pool = db::create_pool(&pg_url).await?;
    let conn = pool.get().await?;

    // Find the range of blocks that need backfilling
    let row = conn
        .query_one(
            "SELECT COALESCE(MIN(block_num), 0), COALESCE(MAX(block_num), 0) FROM txs WHERE gas_used IS NULL",
            &[],
        )
        .await?;

    let min_block: i64 = row.get(0);
    let max_block: i64 = row.get(1);

    if min_block == 0 && max_block == 0 {
        info!("No rows to backfill");
        return Ok(());
    }

    let total_range = max_block - min_block + 1;
    info!(min_block, max_block, total_range, batch_size = args.batch_size, "Starting gas_used backfill");

    let mut lo = min_block;
    let mut total_updated: u64 = 0;

    while lo <= max_block {
        let hi = (lo + args.batch_size).min(max_block + 1);

        let updated = conn
            .execute(
                "UPDATE txs SET gas_used = r.gas_used, fee_payer = r.fee_payer \
                 FROM receipts r \
                 WHERE txs.block_num = r.block_num AND txs.idx = r.tx_idx \
                   AND txs.block_num >= $1 AND txs.block_num < $2 \
                   AND txs.gas_used IS NULL",
                &[&lo, &hi],
            )
            .await?;

        total_updated += updated;
        let progress = ((hi - min_block) as f64 / total_range as f64 * 100.0).min(100.0);
        info!(blocks = %format!("{lo}..{hi}"), updated, total_updated, progress = %format!("{progress:.1}%"), "Backfilled batch");

        lo = hi;
    }

    info!(total_updated, "Backfill complete");
    Ok(())
}
