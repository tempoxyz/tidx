use anyhow::Result;
use clap::Parser;

mod cli;

use cli::{Cli, Commands};

#[tokio::main]
async fn main() -> Result<()> {
    // Logs, plus OTLP trace export when OTEL_EXPORTER_OTLP_*ENDPOINT is set.
    let _telemetry = tidx::telemetry::init()?;

    let cli = Cli::parse();

    match cli.command {
        Commands::Init(args) => cli::init::run(args),
        Commands::Up(args) => cli::up::run(args).await,
        Commands::Sync(args) => cli::up::run_sync(args).await,
        Commands::Serve(args) => cli::serve::run(args).await,
        Commands::Status(args) => cli::status::run(args).await,
        Commands::Query(args) => cli::query::run(args).await,
        Commands::Views(args) => cli::views::run(args).await,
        Commands::BackfillReceiptData(args) => cli::backfill_receipt_data::run(args).await,
        Commands::Upgrade => cli::upgrade::run(),
    }
}
