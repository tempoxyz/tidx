use anyhow::Result;
use clap::Args as ClapArgs;
use std::path::PathBuf;

use tidx::mcp;

#[derive(ClapArgs)]
pub struct Args {
    /// Path to config file (for direct database access)
    #[arg(short, long, default_value = "config.toml")]
    pub config: PathBuf,

    /// TIDX HTTP API URL to proxy requests to (e.g., http://localhost:8080)
    /// If provided, queries go through HTTP instead of direct DB access.
    #[arg(long)]
    pub url: Option<String>,
}

pub async fn run(args: Args) -> Result<()> {
    if let Some(url) = args.url {
        mcp::serve_stdio_http(&url).await
    } else {
        let config = tidx::config::Config::load(&args.config)?;
        mcp::serve_stdio(&config).await
    }
}
