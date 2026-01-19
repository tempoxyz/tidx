use anyhow::Result;
use chrono::{TimeZone, Utc};
use clap::{Args as ClapArgs, Subcommand};
use std::collections::HashMap;
use tracing::info;

use ak47::db::{self, PartitionManager};
use ak47::sync::decoder::{decode_block, decode_log, decode_transaction};
use ak47::sync::fetcher::RpcClient;
use ak47::sync::writer::{write_block, write_logs, write_txs};

#[derive(ClapArgs)]
pub struct Args {
    /// RPC endpoint URL
    #[arg(long, env = "AK47_RPC_URL")]
    pub rpc: String,

    /// Database URL
    #[arg(long, env = "AK47_DATABASE_URL")]
    pub db: String,

    /// Skip running migrations
    #[arg(long)]
    pub skip_migrations: bool,

    #[command(subcommand)]
    pub command: SyncCommands,
}

#[derive(Subcommand)]
pub enum SyncCommands {
    /// Sync blocks forward from a range
    Forward {
        /// Start block number
        #[arg(long)]
        from: u64,

        /// End block number
        #[arg(long)]
        to: u64,

        /// Batch size for RPC requests
        #[arg(long, default_value = "100")]
        batch_size: u64,
    },
}

pub async fn run(args: Args) -> Result<()> {
    let pool = db::create_pool(&args.db).await?;
    if !args.skip_migrations {
        db::run_migrations(&pool).await?;
    }

    let rpc = RpcClient::new(&args.rpc);
    let partitions = PartitionManager::new(pool.clone());

    match args.command {
        SyncCommands::Forward {
            from,
            to,
            batch_size,
        } => {
            run_forward(&pool, &rpc, &partitions, from, to, batch_size).await?;
        }
    }

    Ok(())
}

async fn run_forward(
    pool: &db::Pool,
    rpc: &RpcClient,
    partitions: &PartitionManager,
    from: u64,
    to: u64,
    batch_size: u64,
) -> Result<()> {
    info!(from, to, batch_size, "Starting forward sync");

    let mut synced = 0u64;
    let mut total_logs = 0u64;
    let total = to - from + 1;

    for chunk_start in (from..=to).step_by(batch_size as usize) {
        let chunk_end = (chunk_start + batch_size - 1).min(to);

        partitions.ensure_partition(chunk_end).await?;

        let (blocks, receipts) = tokio::try_join!(
            rpc.get_blocks_batch(chunk_start..=chunk_end),
            rpc.get_receipts_batch(chunk_start..=chunk_end)
        )?;

        let block_timestamps: HashMap<u64, _> = blocks
            .iter()
            .map(|b| {
                let ts = Utc.timestamp_opt(b.timestamp_u64() as i64, 0).unwrap();
                (b.number_u64(), ts)
            })
            .collect();

        for block in &blocks {
            let block_row = decode_block(block);
            write_block(pool, &block_row).await?;

            let txs: Vec<_> = block
                .transactions()
                .enumerate()
                .map(|(i, tx)| decode_transaction(tx, block, i as u32))
                .collect();

            write_txs(pool, &txs).await?;
        }

        let all_logs: Vec<_> = receipts
            .iter()
            .flatten()
            .flat_map(|receipt| {
                let block_num = receipt.block_number.to::<u64>();
                block_timestamps
                    .get(&block_num)
                    .map(|&ts| receipt.logs.iter().map(move |log| decode_log(log, ts)))
                    .into_iter()
                    .flatten()
            })
            .collect();

        let log_count = all_logs.len();
        write_logs(pool, &all_logs).await?;
        total_logs += log_count as u64;

        synced += blocks.len() as u64;
        let pct = (synced as f64 / total as f64) * 100.0;
        info!(
            chunk_start,
            chunk_end,
            synced,
            total,
            logs = log_count,
            pct = format!("{:.1}%", pct),
            "Synced batch"
        );
    }

    info!(synced, total_logs, "Forward sync complete");
    Ok(())
}
