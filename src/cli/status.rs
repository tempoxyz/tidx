use anyhow::Result;
use clap::Args as ClapArgs;
use std::path::PathBuf;

use tidx::config::Config;
use tidx::db;
use tidx::sync::ch_sink::ClickHouseSink;
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

    // Auto-detect running HTTP API for instant status (uses in-memory watermarks)
    if config.http.enabled {
        let url = format!("http://127.0.0.1:{}", config.http.port);
        let client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_millis(200))
            .timeout(std::time::Duration::from_millis(500))
            .build()?;
        if client.get(format!("{url}/health")).send().await.is_ok() {
            return run_via_http(&url, &args).await;
        }
    }

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
        let head_num = chain["head_num"].as_i64().unwrap_or(0);
        let synced_num = chain["synced_num"].as_i64().unwrap_or(0);
        let lag = chain["lag"].as_i64().unwrap_or(0);

        println!("┌─ {} (chain_id: {}) ─────────────────────", name, chain_id);
        println!("│");
        if head_num > 0 {
            println!("│  Head:          {} (live)", format_number(head_num as u64));
        }
        println!("│  Lag:           {} blocks", lag);

        // Backfill ETA (from sync_rate or store write rate)
        if synced_num > 0 && synced_num < head_num {
            let remaining = head_num - synced_num;
            // Prefer store write rate (real-time), fall back to sync_rate from DB
            let rate = chain
                .get("postgres")
                .and_then(|pg| pg["rate"].as_f64())
                .or_else(|| chain["sync_rate"].as_f64());
            if let Some(r) = rate {
                if r > 0.0 {
                    println!("│  ETA:           {}", format_eta(remaining as f64 / r));
                }
            }
        }

        // PostgreSQL per-table status
        if let Some(pg) = chain.get("postgres") {
            println!("│");
            if let Some(rate) = pg["rate"].as_f64() {
                println!("│  PostgreSQL ({:.0} blk/s)", rate);
            } else {
                println!("│  PostgreSQL");
            }
            print_store_rows(pg, synced_num, head_num);
        }

        // ClickHouse per-table status
        if let Some(ch) = chain.get("clickhouse") {
            println!("│");
            if let Some(rate) = ch["rate"].as_f64() {
                println!("│  ClickHouse ({:.0} blk/s)", rate);
            } else {
                println!("│  ClickHouse");
            }
            print_store_rows(ch, synced_num, head_num);
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
        let pg_url = match chain.resolved_pg_url() {
            Ok(url) => url,
            Err(e) => {
                println!("│  Status: Failed to resolve pg_url");
                println!("│  Error: {e}");
                println!("└───────────────────────────────────────────────────────────");
                println!();
                continue;
            }
        };
        let pool = match db::create_pool(&pg_url).await {
            Ok(p) => p,
            Err(e) => {
                println!("│  Status: Database connection failed");
                println!("│  Error: {e}");
                println!("└───────────────────────────────────────────────────────────");
                println!();
                continue;
            }
        };

        let head = if let Some(state) = load_sync_state(&pool, chain.chain_id).await? {
            let head = live_head.unwrap_or(state.head_num);
            let lag = head.saturating_sub(state.tip_num);
            println!("│  Head:          {} {}", format_number(head as u64), if live_head.is_some() { "(live)" } else { "" });
            println!("│  Lag:           {} blocks", lag);
            head
        } else {
            let head = live_head.unwrap_or(0);
            if head > 0 {
                println!("│  Head:          {} (live)", format_number(head as u64));
            }
            println!("│  Status:        Not syncing");
            head
        };

        // PostgreSQL per-table status (from in-memory watermarks)
        println!("│");
        print_store_status_from_watermarks("PostgreSQL", "postgres", head as i64);

        // ClickHouse per-table status (from in-memory watermarks)
        if let Some(ref ch_config) = chain.clickhouse {
            if ch_config.enabled {
                println!("│");
                print_store_status_from_watermarks("ClickHouse", "clickhouse", head as i64);
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

        let (state, gaps) = match chain.resolved_pg_url() {
            Ok(pg_url) => match db::create_pool(&pg_url).await {
                Ok(pool) => {
                    let state = load_sync_state(&pool, chain.chain_id).await.ok().flatten();
                    let tip = state.as_ref().map(|s| s.tip_num).unwrap_or(0);
                    let gaps = detect_all_gaps(&pool, tip).await.unwrap_or_default();
                    (state, gaps)
                }
                Err(_) => (None, vec![]),
            },
            Err(_) => (None, vec![]),
        };

        let gaps_json: Vec<_> = gaps.iter().map(|(s, e)| serde_json::json!({"start": s, "end": e, "size": e - s + 1})).collect();
        let total_gap_blocks: u64 = gaps.iter().map(|(s, e)| e - s + 1).sum();

        let mut chain_status = serde_json::json!({
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

        // Add ClickHouse status if configured
        if let Some(ref ch_config) = chain.clickhouse {
            if ch_config.enabled {
                let database = ch_config
                    .database
                    .clone()
                    .unwrap_or_else(|| format!("tidx_{}", chain.chain_id));
                if let Ok(sink) = ClickHouseSink::new(&ch_config.url, &database) {
                    let blocks = sink.max_block_in_table("blocks").await.ok().flatten();
                    let txs = sink.max_block_in_table("txs").await.ok().flatten();
                    let logs = sink.max_block_in_table("logs").await.ok().flatten();
                    let receipts = sink.max_block_in_table("receipts").await.ok().flatten();
                    chain_status["clickhouse"] = serde_json::json!({
                        "blocks": blocks,
                        "txs": txs,
                        "logs": logs,
                        "receipts": receipts,
                    });
                }
            }
        }

        chains.push(chain_status);
    }

    let output = serde_json::json!({ "chains": chains });
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

/// Print per-table status using in-memory watermarks (instant, no DB queries).
fn print_store_status_from_watermarks(label: &str, sink_name: &str, head: i64) {
    let rate = tidx::metrics::get_sink_block_rate(sink_name);
    if let Some(r) = rate {
        println!("│  {label} ({:.0} blk/s)", r);
    } else {
        println!("│  {label}");
    }
    let (_, txs_count, logs_count, receipts_count) =
        tidx::metrics::get_sink_row_counts(sink_name);

    // blocks: show watermark / head progress
    let blocks_wm = tidx::metrics::get_sink_watermark(sink_name, "blocks");
    print_table_row("├", "blocks", blocks_wm, head);

    // txs, logs, receipts: show row counts
    for (i, (table, count)) in [("txs", txs_count), ("logs", logs_count), ("receipts", receipts_count)]
        .iter()
        .enumerate()
    {
        let prefix = if i == 2 { "└" } else { "├" };
        if *count > 0 {
            println!("│  {prefix}─ {:<10} {} rows", table, format_number(*count));
        } else {
            println!("│  {prefix}─ {:<10} empty", table);
        }
    }
}

/// Print per-table rows for a store (blocks as progress, others as row counts).
fn print_store_rows(store: &serde_json::Value, synced_num: i64, head: i64) {
    // blocks: show backfill progress (synced_num / head)
    let blocks_count = store["blocks_count"].as_u64();
    if synced_num > 0 && head > 0 {
        let pct = (synced_num as f64 / head as f64 * 100.0).min(100.0) as u64;
        if pct >= 100 {
            println!("│  ├─ {:<10} {} ✓", "blocks", format_number(head as u64));
        } else {
            println!(
                "│  ├─ {:<10} {} / {} ({pct}%)",
                "blocks",
                format_number(synced_num as u64),
                format_number(head as u64)
            );
        }
    } else if let Some(count) = blocks_count {
        if count > 0 {
            println!("│  ├─ {:<10} {}", "blocks", format_number(count));
        } else {
            println!("│  ├─ {:<10} empty", "blocks");
        }
    } else {
        println!("│  ├─ {:<10} empty", "blocks");
    }

    // txs, logs, receipts: show row counts
    for (i, table) in ["txs", "logs", "receipts"].iter().enumerate() {
        let prefix = if i == 2 { "└" } else { "├" };
        let count_key = format!("{table}_count");
        let count = store[&count_key].as_u64();
        match count {
            Some(n) if n > 0 => {
                println!("│  {prefix}─ {:<10} {} rows", table, format_number(n));
            }
            _ => {
                println!("│  {prefix}─ {:<10} empty", table);
            }
        }
    }
}

/// Print a single table row with optional percentage vs head.
fn print_table_row(prefix: &str, table: &str, max_block: Option<i64>, head: i64) {
    match max_block {
        Some(n) if head > 0 => {
            let pct = (n as f64 / head as f64 * 100.0).min(100.0) as u64;
            if pct >= 100 {
                println!("│  {prefix}─ {:<10} {} ✓", table, format_number(n as u64));
            } else {
                println!(
                    "│  {prefix}─ {:<10} {} / {} ({pct}%)",
                    table,
                    format_number(n as u64),
                    format_number(head as u64)
                );
            }
        }
        Some(n) => {
            println!("│  {prefix}─ {:<10} {}", table, format_number(n as u64));
        }
        None => {
            println!("│  {prefix}─ {:<10} empty", table);
        }
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




