use anyhow::Result;
use clap::Args as ClapArgs;
use std::path::PathBuf;

use tidx::config::Config;
use tidx::db;
use tidx::service::{self, execute_query_with_engine, QueryOptions};

#[derive(ClapArgs)]
pub struct Args {
    /// Chain name to query (uses first chain if not specified)
    #[arg(long)]
    pub chain: Option<String>,

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
}

pub async fn run(args: Args) -> Result<()> {
    let config = Config::load(&args.config)?;

    // Find chain by name, or use first chain
    let chain = if let Some(name) = &args.chain {
        config
            .chains
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| anyhow::anyhow!("Chain '{}' not found in config", name))?
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

    let result = execute_query_with_engine(
        &pool,
        None, // DuckDB pool not available in CLI yet
        &args.sql,
        args.signature.as_deref(),
        &options,
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
