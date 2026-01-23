use anyhow::Result;
use clap::Args as ClapArgs;
use std::path::PathBuf;

use tidx::config::Config;
use tidx::mcp;

#[derive(ClapArgs)]
pub struct Args {
    /// Path to config file
    #[arg(short, long, default_value = "config.toml")]
    pub config: PathBuf,
}

pub async fn run(args: Args) -> Result<()> {
    let config = Config::load(&args.config)?;
    mcp::serve_stdio(&config).await
}
