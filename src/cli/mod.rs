pub mod api_key;
pub mod init;
pub mod query;
pub mod status;
pub mod up;
pub mod upgrade;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "tidx")]
#[command(about = "High-throughput Tempo blockchain indexer")]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Initialize a new config.toml
    Init(init::Args),
    /// Start syncing blocks from the chain (continuous) and serve HTTP API
    Up(up::Args),
    /// Show sync status
    Status(status::Args),
    /// Run a SQL query (use --signature to decode event logs)
    Query(query::Args),
    /// Generate a new API key and add it to config
    ApiKey(api_key::Args),
    /// Update tidx to the latest version
    Upgrade,
}
