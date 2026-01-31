use anyhow::Result;
use clap::Args as ClapArgs;
use std::path::PathBuf;

use tidx::config::Config;
use tidx::db;
use tidx::service::{self, execute_query, PgDuckdbConfig, QueryOptions};

#[derive(ClapArgs)]
pub struct Args {
    /// Chain ID to query (uses first chain if not specified)
    #[arg(long)]
    pub chain_id: Option<u64>,

    /// Path to config file
    #[arg(short, long, default_value = "config.toml")]
    pub config: PathBuf,

    /// Force query engine (postgres, duckdb). Auto-routes if not specified.
    #[arg(long)]
    pub engine: Option<String>,

    /// Output format (table, json, csv)
    #[arg(long, default_value = "table")]
    pub format: String,

    /// Maximum rows to return
    #[arg(long, default_value = "10000")]
    pub limit: i64,

    /// Event signature to create a CTE (e.g., "Transfer(address indexed from, address indexed to, uint256 value)")
    #[arg(long, short)]
    pub signature: Option<String>,

    /// SQL query (SELECT only). Use event name from --signature as table.
    pub sql: String,

    /// Query timeout in milliseconds
    #[arg(long, default_value = "30000")]
    pub timeout: u64,

    /// TIDX HTTP API URL to proxy requests to (e.g., http://localhost:8080)
    #[arg(long)]
    pub url: Option<String>,
}

pub async fn run(args: Args) -> Result<()> {
    if let Some(url) = &args.url {
        return run_via_http(url, &args).await;
    }

    let config = Config::load(&args.config)?;

    // Find chain by ID, or use first chain
    let chain = if let Some(id) = args.chain_id {
        config
            .chains
            .iter()
            .find(|c| c.chain_id == id)
            .ok_or_else(|| anyhow::anyhow!("Chain ID {} not found in config", id))?
    } else {
        config
            .chains
            .first()
            .ok_or_else(|| anyhow::anyhow!("No chains configured"))?
    };

    let pool = db::create_pool(&chain.pg_url).await?;

    let options = QueryOptions {
        timeout_ms: args.timeout,
        limit: args.limit,
    };

    let pg_duckdb_config = PgDuckdbConfig {
        memory_limit: chain.pg_duckdb_memory_limit.clone(),
        threads: chain.pg_duckdb_threads,
    };
    let result = execute_query(
        &pool,
        &args.sql,
        args.signature.as_deref(),
        &options,
        &pg_duckdb_config,
        args.engine.as_deref(),
    )
    .await?;

    if result.row_count == 0 {
        println!("No results");
        return Ok(());
    }

    match args.format.as_str() {
        "json" => print_json(&result)?,
        "csv" => print_csv(&result)?,
        _ => print_table(&result)?,
    }

    Ok(())
}

fn print_table(result: &service::QueryResult) -> Result<()> {
    println!("{}", result.columns.join(" | "));
    println!("{}", "-".repeat(result.columns.len() * 20));

    for row in &result.rows {
        let values: Vec<String> = row.iter().map(json_to_string).collect();
        println!("{}", values.join(" | "));
    }

    println!("\n({} rows)", result.row_count);
    Ok(())
}

fn print_json(result: &service::QueryResult) -> Result<()> {
    let mut output = Vec::new();

    for row in &result.rows {
        let mut obj = serde_json::Map::new();
        for (i, col) in result.columns.iter().enumerate() {
            obj.insert(col.clone(), row[i].clone());
        }
        output.push(serde_json::Value::Object(obj));
    }

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn print_csv(result: &service::QueryResult) -> Result<()> {
    println!("{}", result.columns.join(","));

    for row in &result.rows {
        let values: Vec<String> = row.iter().map(json_to_string).collect();
        println!("{}", values.join(","));
    }

    Ok(())
}

fn json_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

async fn run_via_http(base_url: &str, args: &Args) -> Result<()> {
    let chain_id = args
        .chain_id
        .ok_or_else(|| anyhow::anyhow!("--chain-id required when using --url"))?;

    let client = reqwest::Client::new();
    let mut url = reqwest::Url::parse(&format!("{}/query", base_url.trim_end_matches('/')))?;

    url.query_pairs_mut()
        .append_pair("sql", &args.sql)
        .append_pair("chainId", &chain_id.to_string())
        .append_pair("timeout_ms", &args.timeout.to_string())
        .append_pair("limit", &args.limit.to_string());

    if let Some(sig) = &args.signature {
        url.query_pairs_mut().append_pair("signature", sig);
    }
    if let Some(engine) = &args.engine {
        url.query_pairs_mut().append_pair("engine", engine);
    }

    let resp = client.get(url).send().await?;
    let status = resp.status();
    let body = resp.text().await?;

    if !status.is_success() {
        anyhow::bail!("HTTP {}: {}", status, body);
    }

    let parsed: serde_json::Value = serde_json::from_str(&body)?;

    if !parsed["ok"].as_bool().unwrap_or(false) {
        let err = parsed["error"].as_str().unwrap_or("Unknown error");
        anyhow::bail!("{}", err);
    }

    let result = service::QueryResult {
        columns: parsed["columns"]
            .as_array()
            .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
            .unwrap_or_default(),
        rows: parsed["rows"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|row| row.as_array().cloned())
                    .collect()
            })
            .unwrap_or_default(),
        row_count: parsed["row_count"].as_i64().unwrap_or(0) as usize,
        engine: parsed["engine"].as_str().map(String::from),
        query_time_ms: parsed["query_time_ms"].as_f64(),
    };

    if result.row_count == 0 {
        println!("No results");
        return Ok(());
    }

    match args.format.as_str() {
        "json" => print_json(&result)?,
        "csv" => print_csv(&result)?,
        _ => print_table(&result)?,
    }

    Ok(())
}
