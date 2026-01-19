pub mod compress;
pub mod query;
pub mod query_logs;
pub mod status;
pub mod sync;
pub mod up;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "ak47")]
#[command(about = "High-throughput Tempo blockchain indexer")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start syncing blocks from the chain (continuous)
    Up(up::Args),
    /// Show sync status
    Status(status::Args),
    /// Sync a specific block range
    Sync(sync::Args),
    /// Run a SQL query
    Query(query::Args),
    /// Query logs by event signature (e.g., "Transfer(address,address,uint256)")
    QueryLogs(query_logs::Args),
    /// Compress chunks and refresh aggregates
    Compress(compress::Args),
}
