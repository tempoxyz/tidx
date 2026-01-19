use anyhow::Result;
use clap::Parser;

mod cli;
mod config;
mod db;
mod sync;
mod tempo;
mod types;

use cli::{Cli, Commands};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("ak47=info".parse().unwrap()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Up(args) => cli::up::run(args).await,
        Commands::Status(args) => cli::status::run(args).await,
        Commands::Sync(args) => cli::sync::run(args).await,
        Commands::Query(args) => cli::query::run(args).await,
        Commands::Compress(args) => cli::compress::run(args).await,
    }
}
