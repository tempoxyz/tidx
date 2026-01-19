use anyhow::Result;
use clap::Args as ClapArgs;
use std::path::PathBuf;

use ak47::config::Config;
use ak47::db;
use ak47::sync::fetcher::RpcClient;
use ak47::sync::writer::load_sync_state;

#[derive(ClapArgs)]
pub struct Args {
    /// Path to config file
    #[arg(short, long, default_value = "config.toml")]
    pub config: PathBuf,

    /// Watch mode - continuously update status
    #[arg(long, short)]
    pub watch: bool,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,
}

pub async fn run(args: Args) -> Result<()> {
    let config = Config::load(&args.config)?;
    let pool = db::create_pool(&config.database_url).await?;

    loop {
        if args.watch {
            print!("\x1B[2J\x1B[1;1H");
        }

        if args.json {
            print_json_status(&config, &pool).await?;
        } else {
            print_status(&config, &pool).await?;
        }

        if !args.watch {
            break;
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    Ok(())
}

async fn print_status(config: &Config, pool: &db::Pool) -> Result<()> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║                     AK47 Indexer Status                   ║");
    println!("╚═══════════════════════════════════════════════════════════╝");
    println!();

    let state = load_sync_state(pool).await?.unwrap_or_default();

    for chain in &config.chains {
        let rpc = RpcClient::new(&chain.rpc_url);
        let live_head = rpc.latest_block_number().await.ok();

        println!("┌─ {} (chain_id: {}) ─────────────────────", chain.name, chain.chain_id);
        println!("│");

        if state.chain_id == chain.chain_id {
            let head = live_head.unwrap_or(state.head_num);
            let lag = head.saturating_sub(state.synced_num);

            // Forward sync status
            println!("│  Forward Sync");
            println!("│  ├─ Head:      {} {}", head, if live_head.is_some() { "(live)" } else { "" });
            println!("│  ├─ Synced:    {}", state.synced_num);
            println!("│  └─ Lag:       {} blocks", lag);
            println!("│");

            // Backfill status
            println!("│  Backfill");
            match state.backfill_num {
                None => {
                    println!("│  └─ Status:   Not started");
                }
                Some(0) => {
                    println!("│  └─ Status:   ✓ Complete (genesis reached)");
                }
                Some(n) => {
                    let total = state.synced_num;
                    let done = state.synced_num.saturating_sub(n);
                    let pct = if total > 0 {
                        (done as f64 / total as f64 * 100.0) as u64
                    } else {
                        0
                    };
                    println!("│  ├─ Status:   In progress");
                    println!("│  ├─ Position: block {}", n);
                    println!("│  ├─ Remaining: {} blocks", n);
                    println!("│  └─ Progress: {}%", pct);
                }
            }

            // Coverage
            let (low, high) = state.indexed_range();
            let total_indexed = if high >= low { high - low + 1 } else { 0 };
            println!("│");
            println!("│  Coverage");
            println!("│  ├─ Range:    {} - {}", low, high);
            println!("│  └─ Total:    {} blocks", format_number(total_indexed));
        } else {
            println!("│  Status: Not syncing");
            if let Some(head) = live_head {
                println!("│  Head: {} (live)", head);
            }
        }

        println!("└───────────────────────────────────────────────────────────");
        println!();
    }

    Ok(())
}

async fn print_json_status(config: &Config, pool: &db::Pool) -> Result<()> {
    let state = load_sync_state(pool).await?.unwrap_or_default();
    let mut chains = Vec::new();

    for chain in &config.chains {
        let rpc = RpcClient::new(&chain.rpc_url);
        let live_head = rpc.latest_block_number().await.ok();

        let chain_status = serde_json::json!({
            "name": chain.name,
            "chain_id": chain.chain_id,
            "rpc_url": chain.rpc_url,
            "head": live_head,
            "synced": if state.chain_id == chain.chain_id { Some(state.synced_num) } else { None },
            "lag": live_head.map(|h| h.saturating_sub(state.synced_num)),
            "backfill_block": state.backfill_num,
            "backfill_complete": state.backfill_complete(),
        });
        chains.push(chain_status);
    }

    let output = serde_json::json!({ "chains": chains });
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn format_number(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}
