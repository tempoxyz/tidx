use anyhow::Result;
use clap::Args as ClapArgs;
use std::path::PathBuf;

use ak47::config::Config;
use ak47::db;
use ak47::sync::fetcher::RpcClient;
use ak47::sync::writer::{detect_gaps, load_sync_state};

#[derive(ClapArgs)]
pub struct Args {
    /// Path to config file
    #[arg(short, long, default_value = "config.toml")]
    pub config: PathBuf,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,

    /// Watch mode - continuously update status
    #[arg(long, short)]
    pub watch: bool,
}

pub async fn run(args: Args) -> Result<()> {
    let config = Config::load(&args.config)?;

    loop {
        if args.watch {
            print!("\x1B[2J\x1B[1;1H");
        }

        if args.json {
            print_json_status(&config).await?;
        } else {
            print_status(&config).await?;
        }

        if !args.watch {
            break;
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    Ok(())
}

async fn print_status(config: &Config) -> Result<()> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║                     AK47 Indexer Status                   ║");
    println!("╚═══════════════════════════════════════════════════════════╝");
    println!();

    for chain in &config.chains {
        let rpc = RpcClient::new(&chain.rpc_url);
        let live_head = rpc.latest_block_number().await.ok();

        println!("┌─ {} (chain_id: {}) ─────────────────────", chain.name, chain.chain_id);
        println!("│");

        // Connect to this chain's database
        let pool = match db::create_pool(&chain.pg_url).await {
            Ok(p) => p,
            Err(e) => {
                println!("│  Status: Database connection failed");
                println!("│  Error: {e}");
                println!("└───────────────────────────────────────────────────────────");
                println!();
                continue;
            }
        };

        if let Some(state) = load_sync_state(&pool, chain.chain_id).await? {
            let head = live_head.unwrap_or(state.head_num);
            let realtime_lag = head.saturating_sub(state.tip_num);

            // Realtime sync status
            println!("│  Realtime Sync");
            println!("│  ├─ Head:      {} {}", head, if live_head.is_some() { "(live)" } else { "" });
            println!("│  ├─ Tip:       {}", state.tip_num);
            println!("│  └─ Lag:       {realtime_lag} blocks");
            println!("│");

            // Gap-fill status
            let gaps = detect_gaps(&pool).await.unwrap_or_default();
            println!("│  Gap-Fill");
            println!("│  ├─ Synced:    {}", state.synced_num);
            if gaps.is_empty() {
                println!("│  └─ Gaps:      ✓ No gaps");
            } else {
                let total_gap_blocks: u64 = gaps.iter().map(|(s, e)| e - s + 1).sum();
                println!("│  ├─ Gaps:      {} ({} blocks total)", gaps.len(), format_number(total_gap_blocks));
                for (i, (start, end)) in gaps.iter().enumerate() {
                    let size = end - start + 1;
                    let prefix = if i == gaps.len() - 1 { "└" } else { "├" };
                    println!("│  │  {prefix}─ {} → {} ({} blocks)", format_number(*start), format_number(*end), format_number(size));
                }
            }
            println!("│");

            // Backfill status
            let remaining = state.backfill_remaining();
            println!("│  Backfill");
            match state.backfill_num {
                None if remaining > 0 => {
                    println!("│  ├─ Status:   Pending");
                    println!("│  └─ Needed:   {} blocks (0 → {})", format_number(remaining), state.synced_num.saturating_sub(1));
                }
                None => {
                    println!("│  └─ Status:   Not needed");
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
                    println!("│  ├─ Position: block {}", format_number(n));
                    println!("│  ├─ Remaining: {} blocks", format_number(n));
                    println!("│  ├─ Progress: {pct}%");
                    if let Some(rate) = state.sync_rate() {
                        println!("│  ├─ Rate:     {:.0} blk/s", rate);
                    }
                    if let Some(eta) = state.backfill_eta_secs() {
                        println!("│  └─ ETA:      {}", format_eta(eta));
                    } else {
                        println!("│  └─ ETA:      calculating...");
                    }
                }
            }

            // Coverage
            let (low, high) = state.indexed_range();
            println!("│");
            println!("│  Coverage");
            println!("│  ├─ Range:    {} → {}", format_number(low), format_number(high));
            println!("│  └─ Total:    {} blocks", format_number(state.total_indexed()));
        } else {
            println!("│  Status: Not syncing");
            if let Some(head) = live_head {
                println!("│  Head: {head} (live)");
            }
        }

        println!("└───────────────────────────────────────────────────────────");
        println!();
    }

    Ok(())
}

async fn print_json_status(config: &Config) -> Result<()> {
    let mut chains = Vec::new();

    for chain in &config.chains {
        let rpc = RpcClient::new(&chain.rpc_url);
        let live_head = rpc.latest_block_number().await.ok();

        let (state, gaps) = if let Ok(pool) = db::create_pool(&chain.pg_url).await {
            let state = load_sync_state(&pool, chain.chain_id).await.ok().flatten();
            let gaps = detect_gaps(&pool).await.unwrap_or_default();
            (state, gaps)
        } else {
            (None, vec![])
        };

        let gaps_json: Vec<_> = gaps.iter().map(|(s, e)| serde_json::json!({"start": s, "end": e, "size": e - s + 1})).collect();
        let total_gap_blocks: u64 = gaps.iter().map(|(s, e)| e - s + 1).sum();

        let chain_status = serde_json::json!({
            "name": chain.name,
            "chain_id": chain.chain_id,
            "rpc_url": chain.rpc_url,
            "pg_url": chain.pg_url,
            "head": live_head,
            "tip_num": state.as_ref().map(|s| s.tip_num),
            "synced_num": state.as_ref().map(|s| s.synced_num),
            "realtime_lag": state.as_ref().and_then(|s| live_head.map(|h| h.saturating_sub(s.tip_num))),
            "gap_count": gaps.len(),
            "gap_blocks": total_gap_blocks,
            "gaps": gaps_json,
            "backfill_block": state.as_ref().and_then(|s| s.backfill_num),
            "backfill_complete": state.as_ref().map(|s| s.backfill_complete()).unwrap_or(false),
            "sync_rate": state.as_ref().and_then(|s| s.sync_rate()),
            "backfill_eta_secs": state.as_ref().and_then(|s| s.backfill_eta_secs()),
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

fn format_eta(secs: f64) -> String {
    if secs <= 0.0 || secs.is_nan() || secs.is_infinite() {
        return "unknown".to_string();
    }

    let secs = secs as u64;
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else if secs < 86400 {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    } else {
        format!("{}d {}h", secs / 86400, (secs % 86400) / 3600)
    }
}
