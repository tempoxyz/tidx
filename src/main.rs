use anyhow::Result;
use clap::Parser;

mod cli;

use cli::{Cli, Commands};

#[tokio::main]
async fn main() -> Result<()> {
    let filter = tracing_subscriber::EnvFilter::from_default_env()
        .add_directive("tidx=info".parse().unwrap());

    // Use JSON format if RUST_LOG_FORMAT=json
    if std::env::var("RUST_LOG_FORMAT").as_deref() == Ok("json") {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .init();
    }

    let cli = Cli::parse();

    match cli.command {
        Commands::Init(args) => cli::init::run(args),
        Commands::Up(args) => cli::up::run(args).await,
        Commands::Status(args) => cli::status::run(args).await,
        Commands::Query(args) => cli::query::run(args).await,
        Commands::Views(args) => cli::views::run(args).await,
        Commands::Upgrade => cli::upgrade::run(),
    }
}
