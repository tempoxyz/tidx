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

        let head_num = head.unwrap_or(synced_num);

        println!("┌─ {} (chain_id: {}) ─────────────────────", name, chain_id);
        println!("│");
        if let Some(h) = head {
            println!("│  Head:          {} (live)", format_number(h as u64));
        }
        println!("│  Lag:           {} blocks", realtime_lag);

        // PostgreSQL per-table status
        if let Some(pg) = chain.get("postgres") {
            println!("│");
            if let Some(rate) = pg["rate"].as_f64() {
                println!("│  PostgreSQL ({:.0} blk/s)", rate);
            } else {
                println!("│  PostgreSQL");
            }
            for (i, table) in ["blocks", "txs", "logs", "receipts"].iter().enumerate() {
                let prefix = if i == 3 { "└" } else { "├" };
                print_table_row(prefix, table, pg[*table].as_i64(), head_num);
            }
        }

        // ClickHouse per-table status
        if let Some(ch) = chain.get("clickhouse") {
            println!("│");
            if let Some(rate) = ch["rate"].as_f64() {
                println!("│  ClickHouse ({:.0} blk/s)", rate);
            } else {
                println!("│  ClickHouse");
            }
            for (i, table) in ["blocks", "txs", "logs", "receipts"].iter().enumerate() {
                let prefix = if i == 3 { "└" } else { "├" };
                print_table_row(prefix, table, ch[*table].as_i64(), head_num);
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

        // PostgreSQL per-table status
        println!("│");
        print_store_status("PostgreSQL", &pool, head as i64, &["blocks", "txs", "logs", "receipts"], "postgres").await;

        // ClickHouse per-table status
        if let Some(ref ch_config) = chain.clickhouse {
            if ch_config.enabled {
                let database = ch_config
                    .database
                    .clone()
                    .unwrap_or_else(|| format!("tidx_{}", chain.chain_id));
                println!("│");
                match ClickHouseSink::new(&ch_config.url, &database) {
                    Ok(sink) => {
                        print_ch_store_status("ClickHouse", &sink, head as i64).await;
                    }
                    Err(_) => {
                        println!("│  ClickHouse");
                        println!("│  └─ ✗ Connection failed");
                    }
                }
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

/// Print per-table status for PostgreSQL.
async fn print_store_status(label: &str, pool: &tidx::db::Pool, head: i64, tables: &[&str], sink_name: &str) {
    let rate = tidx::metrics::get_sink_block_rate(sink_name);
    if let Some(r) = rate {
        println!("│  {label} ({:.0} blk/s)", r);
    } else {
        println!("│  {label}");
    }
    let conn = pool.get().await.ok();
    for (i, table) in tables.iter().enumerate() {
        let prefix = if i == tables.len() - 1 { "└" } else { "├" };
        let col = if *table == "blocks" { "num" } else { "block_num" };
        let max_block: Option<i64> = if let Some(ref c) = conn {
            c.query_one(&format!("SELECT MAX({col}) FROM {table}"), &[])
                .await
                .ok()
                .and_then(|r| r.get(0))
        } else {
            None
        };
        print_table_row(prefix, table, max_block, head);
    }
}

/// Print per-table status for ClickHouse.
async fn print_ch_store_status(label: &str, sink: &ClickHouseSink, head: i64) {
    let rate = tidx::metrics::get_sink_block_rate("clickhouse");
    if let Some(r) = rate {
        println!("│  {label} ({:.0} blk/s)", r);
    } else {
        println!("│  {label}");
    }
    let tables = ["blocks", "txs", "logs", "receipts"];
    for (i, table) in tables.iter().enumerate() {
        let prefix = if i == tables.len() - 1 { "└" } else { "├" };
        let max_block = sink.max_block_in_table(table).await.ok().flatten();
        print_table_row(prefix, table, max_block, head);
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




