use anyhow::Result;
use clap::Parser;

mod cli;

use cli::{Cli, Commands};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("tidx=info".parse().unwrap()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Init(args) => cli::init::run(args),
        Commands::Up(args) => cli::up::run(args).await,
        Commands::Status(args) => cli::status::run(args).await,
        Commands::Query(args) => cli::query::run(args).await,
    }
}
