use anyhow::Result;
use clap::Args as ClapArgs;
use std::path::PathBuf;
use std::sync::Arc;

use tidx::config::Config;
use tidx::db::{self, DuckDbPool};
use tidx::sync::fetcher::RpcClient;
use tidx::sync::writer::{detect_all_gaps, load_sync_state};

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

    /// TIDX HTTP API URL to proxy requests to (e.g., http://localhost:8080)
    #[arg(long)]
    pub url: Option<String>,
}

pub async fn run(args: Args) -> Result<()> {
    if let Some(url) = &args.url {
        return run_via_http(url, &args).await;
    }

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

async fn run_via_http(base_url: &str, args: &Args) -> Result<()> {
    let client = reqwest::Client::new();
    let url = format!("{}/status", base_url.trim_end_matches('/'));

    loop {
        if args.watch {
            print!("\x1B[2J\x1B[1;1H");
        }

        let resp = client.get(&url).send().await?;
        let status = resp.status();
        let body = resp.text().await?;

        if !status.is_success() {
            anyhow::bail!("HTTP {}: {}", status, body);
        }

        if args.json {
            println!("{body}");
        } else {
            let parsed: serde_json::Value = serde_json::from_str(&body)?;
            print_http_status(&parsed)?;
        }

        if !args.watch {
            break;
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    Ok(())
}

fn print_http_status(resp: &serde_json::Value) -> Result<()> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║                     TIDX Indexer Status                   ║");
    println!("╚═══════════════════════════════════════════════════════════╝");
    println!();

    let empty = vec![];
    let chains = resp["chains"].as_array().unwrap_or(&empty);
    for chain in chains {
        let name = chain["name"].as_str().unwrap_or("unknown");
        let chain_id = chain["chain_id"].as_i64().unwrap_or(0);
        let head = chain["head"].as_i64();
        let synced_num = chain["synced_num"].as_i64().unwrap_or(0);
        let realtime_lag = chain["realtime_lag"].as_i64().unwrap_or(0);
        let backfill_complete = chain["backfill_complete"].as_bool().unwrap_or(false);
        let gap_blocks = chain["gap_blocks"].as_i64().unwrap_or(0);
        let gap_count = chain["gap_count"].as_i64().unwrap_or(0);
        let sync_rate = chain["sync_rate"].as_f64();

        println!("┌─ {} (chain_id: {}) ─────────────────────", name, chain_id);
        println!("│");
        println!("│  Realtime Sync");
        if let Some(h) = head {
            println!("│  ├─ Head:      {} (live)", format_number(h as u64));
        }
        println!("│  ├─ Tip:       {}", format_number(synced_num as u64));
        println!("│  └─ Lag:       {} blocks", realtime_lag);
        println!("│");
        println!("│  Backfill");
        if backfill_complete {
            println!("│  └─ Status:   ✓ Complete");
        } else {
            println!("│  ├─ Remaining: {} blocks in {} gap(s)", format_number(gap_blocks as u64), gap_count);
            if let Some(rate) = sync_rate {
                println!("│  ├─ Rate:     {:.0} blk/s", rate);
                if rate > 0.0 {
                    let eta_secs = gap_blocks as f64 / rate;
                    println!("│  └─ ETA:      {}", format_eta(eta_secs));
                }
            }
        }
        println!("└───────────────────────────────────────────────────────────");
        println!();
    }

    Ok(())
}

async fn print_status(config: &Config) -> Result<()> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║                     TIDX Indexer Status                   ║");
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

            // Gap sync status - show ALL gaps (sorted by end desc, most recent first)
            let gaps = detect_all_gaps(&pool, state.tip_num).await.unwrap_or_default();
            let total_gap_blocks: u64 = gaps.iter().map(|(s, e)| e - s + 1).sum();
            
            // Get actual block count from database for accurate stats
            let actual_block_count: u64 = {
                let conn = pool.get().await.ok();
                if let Some(conn) = conn {
                    conn.query_one("SELECT COUNT(*) FROM blocks", &[])
                        .await
                        .ok()
                        .map(|row| row.get::<_, i64>(0) as u64)
                        .unwrap_or(0)
                } else {
                    0
                }
            };
            
            // Get PG min for DuckDB backfill calculation
            let pg_min: i64 = {
                let conn = pool.get().await.ok();
                if let Some(conn) = conn {
                    conn.query_one("SELECT COALESCE(MIN(num), 0) FROM blocks", &[])
                        .await
                        .ok()
                        .map(|row| row.get::<_, i64>(0))
                        .unwrap_or(0)
                } else {
                    0
                }
            };
            
            println!("│  Backfill");
            if gaps.is_empty() {
                println!("│  └─ Status:   ✓ Complete (1 → {})", format_number(state.tip_num));
            } else {
                let total_needed = state.tip_num; // blocks 1 to tip_num
                let pct = if total_needed > 0 {
                    (actual_block_count as f64 / total_needed as f64 * 100.0) as u64
                } else {
                    0
                };
                
                // Use the rolling sync rate stored in the database
                let (rate_str, eta_str) = if let Some(rate) = state.sync_rate {
                    if rate > 0.0 {
                        let eta_secs = total_gap_blocks as f64 / rate;
                        (format!("{:.0} blk/s", rate), format_eta(eta_secs))
                    } else {
                        ("--".to_string(), "calculating...".to_string())
                    }
                } else {
                    // Fallback to overall rate if rolling rate unavailable
                    if let Some(started) = state.started_at {
                        let elapsed = chrono::Utc::now().signed_duration_since(started);
                        let secs = elapsed.num_seconds() as f64;
                        if secs > 10.0 && actual_block_count > 0 {
                            let rate = actual_block_count as f64 / secs;
                            let eta_secs = if rate > 0.0 { total_gap_blocks as f64 / rate } else { 0.0 };
                            (format!("{:.0} blk/s", rate), format_eta(eta_secs))
                        } else {
                            ("--".to_string(), "calculating...".to_string())
                        }
                    } else {
                        ("--".to_string(), "calculating...".to_string())
                    }
                };
                
                println!("│  ├─ Synced:   {} / {} ({pct}%)", format_number(actual_block_count), format_number(total_needed));
                println!("│  ├─ Remaining: {} blocks in {} gap(s)", format_number(total_gap_blocks), gaps.len());
                println!("│  ├─ Rate:     {rate_str}");
                println!("│  └─ ETA:      {eta_str}");
            }

            // DuckDB status
            if let Some(ref duckdb_path) = chain.duckdb_path {
                println!("│");
                println!("│  DuckDB (OLAP)");
                match DuckDbPool::open_readonly(duckdb_path) {
                    Ok(duckdb) => {
                        let duckdb = Arc::new(duckdb);
                        if let Ok((duck_min, duck_max)) = duckdb.block_range().await {
                            let duck_min = duck_min.unwrap_or(0);
                            let duck_max = duck_max.unwrap_or(0);
                            
                            if duck_max == 0 {
                                println!("│  └─ Status:   Empty");
                            } else {
                                let tip_lag = (state.tip_num as i64) - duck_max;
                                let backfill_remaining = (duck_min - pg_min).max(0);
                                
                                // Get DuckDB block count
                                let duck_count: i64 = duckdb.query("SELECT COUNT(*) FROM blocks").await
                                    .ok()
                                    .and_then(|(_, rows)| rows.first().and_then(|r| r.first().and_then(|v| v.as_i64())))
                                    .unwrap_or(0);
                                
                                // Total needed = PG range that DuckDB should have
                                let total_needed = (state.tip_num as i64) - pg_min + 1;
                                let pct = if total_needed > 0 {
                                    (duck_count as f64 / total_needed as f64 * 100.0) as u64
                                } else {
                                    0
                                };
                                
                                let total_remaining = backfill_remaining + tip_lag;
                                
                                // Estimate rate from PG sync rate
                                let (rate_str, eta_str) = if let Some(rate) = state.sync_rate {
                                    if rate > 0.0 {
                                        (format!("{:.0} blk/s", rate), format_eta(total_remaining as f64 / rate))
                                    } else {
                                        ("--".to_string(), "calculating...".to_string())
                                    }
                                } else {
                                    ("--".to_string(), "calculating...".to_string())
                                };
                                
                                if total_remaining <= 0 {
                                    println!("│  └─ Status:   ✓ Complete ({} → {})", format_number(duck_min as u64), format_number(duck_max as u64));
                                } else {
                                    println!("│  ├─ Synced:   {} / {} ({}%)", format_number(duck_count as u64), format_number(total_needed as u64), pct);
                                    println!("│  ├─ Remaining: {} blocks (tip: {}, backfill: {})", format_number(total_remaining as u64), tip_lag, format_number(backfill_remaining as u64));
                                    println!("│  ├─ Rate:     {}", rate_str);
                                    println!("│  └─ ETA:      {}", eta_str);
                                }
                            }
                        } else {
                            println!("│  └─ Status:   Empty");
                        }
                    }
                    Err(e) => {
                        println!("│  └─ Error:    {}", e);
                    }
                }
            }

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
            let tip = state.as_ref().map(|s| s.tip_num).unwrap_or(0);
            let gaps = detect_all_gaps(&pool, tip).await.unwrap_or_default();
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
            "sync_rate": state.as_ref().and_then(|s| s.current_rate()),
            "backfill_eta_secs": state.as_ref().and_then(|s| s.backfill_eta_secs()),
        });
        chains.push(chain_status);
    }

    let output = serde_json::json!({ "chains": chains });
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn format_number(n: u64) -> String {
    // Format with thousand separators for readability
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
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


