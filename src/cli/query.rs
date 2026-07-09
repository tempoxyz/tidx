use anyhow::Result;
use clap::Args as ClapArgs;
use std::path::PathBuf;

use tidx::config::Config;
use tidx::db;
use tidx::service::{self, QueryOptions, execute_query_postgres};

#[derive(ClapArgs)]
pub struct Args {
    /// Chain ID to query (uses first chain if not specified)
    #[arg(short = 'n', long)]
    pub chain_id: Option<u64>,

    /// Path to config file
    #[arg(short, long, default_value = "config.toml")]
    pub config: PathBuf,

    /// Query executor: postgres (default) or clickhouse.
    /// "tiered" is a legacy alias for --engine postgres --source postgres-clickhouse.
    #[arg(short, long)]
    pub engine: Option<String>,

    /// Output format (table, json, csv)
    #[arg(short, long, default_value = "table")]
    pub format: String,

    /// Maximum rows to return
    #[arg(short, long, default_value = "10000")]
    pub limit: i64,

    /// Event signature(s) to create CTEs (e.g., "Transfer(address indexed from, address indexed to, uint256 value)")
    #[arg(short, long)]
    pub signature: Vec<String>,

    /// Where the data lives: postgres (hot window), clickhouse (full archive
    /// via pg_clickhouse), or postgres-clickhouse (tiered: hot PG window +
    /// cold ClickHouse archive). Defaults to the engine's native store.
    #[arg(long)]
    pub source: Option<String>,

    /// SQL query (SELECT only). Use event name from --signature as table.
    pub sql: String,

    /// Query timeout in milliseconds
    #[arg(short, long, default_value = "30000")]
    pub timeout: u64,

    /// TIDX HTTP API URL to proxy requests to (e.g., http://localhost:8080)
    #[arg(short, long)]
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

    let pg_url = chain.resolved_pg_url()?;
    let pool = db::create_pool(&pg_url).await?;

    let options = QueryOptions {
        timeout_ms: args.timeout,
        limit: args.limit,
    };

    let route = tidx::query::QueryRoute::resolve(args.engine.as_deref(), args.source.as_deref())
        .map_err(|e| anyhow::anyhow!(e))?;

    let sig_strs: Vec<&str> = args.signature.iter().map(String::as_str).collect();
    let result = match route {
        tidx::query::QueryRoute::ClickHouse => {
            // CLI executes through PostgreSQL only.
            anyhow::bail!(
                "ClickHouse engine not available in CLI. Use HTTP API with engine=clickhouse for OLAP queries."
            );
        }
        tidx::query::QueryRoute::PostgresViaClickHouse => {
            // PostgreSQL over ch.* pg_clickhouse foreign tables (full archive).
            service::execute_query_postgres_via_clickhouse(&pool, &args.sql, &sig_strs, &options)
                .await?
        }
        tidx::query::QueryRoute::Tiered => {
            // Best-effort ClickHouse engine for the tiered fast path's cold arm;
            // without it, eligible queries still fall back to the FDW views.
            let clickhouse = chain
                .clickhouse
                .as_ref()
                .filter(|c| c.enabled)
                .and_then(|c| tidx::clickhouse::ClickHouseEngine::new(c, chain.chain_id).ok());
            service::execute_query_tiered(
                &pool,
                clickhouse.as_ref(),
                chain.chain_id,
                &args.sql,
                &sig_strs,
                &options,
            )
            .await?
        }
        tidx::query::QueryRoute::Postgres => {
            execute_query_postgres(&pool, &args.sql, &sig_strs, &options).await?
        }
    };

    if result.row_count == 0 {
        println!("No results");
        return Ok(());
    }

    match args.format.as_str() {
        "json" => print_json(&result)?,
        "csv" => print_csv(&result)?,
        "toon" => print_toon(&result)?,
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

fn print_toon(result: &service::QueryResult) -> Result<()> {
    let mut output = Vec::new();
    for row in &result.rows {
        let mut obj = serde_json::Map::new();
        for (i, col) in result.columns.iter().enumerate() {
            obj.insert(col.clone(), row[i].clone());
        }
        output.push(serde_json::Value::Object(obj));
    }
    let toon = toon_format::encode_default(&output)?;
    println!("{toon}");
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

    // Extract basic auth credentials from URL before building the request
    let username = if !url.username().is_empty() {
        Some(url.username().to_string())
    } else {
        None
    };
    let password = url.password().map(String::from);
    let _ = url.set_username("");
    let _ = url.set_password(None);

    url.query_pairs_mut()
        .append_pair("sql", &args.sql)
        .append_pair("chainId", &chain_id.to_string())
        .append_pair("timeout_ms", &args.timeout.to_string())
        .append_pair("limit", &args.limit.to_string());

    for sig in &args.signature {
        url.query_pairs_mut().append_pair("signature", sig);
    }
    if let Some(engine) = &args.engine {
        url.query_pairs_mut().append_pair("engine", engine);
    }
    if let Some(source) = &args.source {
        url.query_pairs_mut().append_pair("source", source);
    }

    let mut req = client.get(url);
    if let Some(user) = &username {
        req = req.basic_auth(user, password.as_deref());
    }

    let resp = req.send().await?;
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
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
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
        "toon" => print_toon(&result)?,
        _ => print_table(&result)?,
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_result() -> service::QueryResult {
        service::QueryResult {
            columns: vec!["num".into(), "hash".into(), "gas_used".into()],
            rows: vec![
                vec![json!(1), json!("0xabc"), json!(21000)],
                vec![json!(2), json!("0xdef"), json!(42000)],
            ],
            row_count: 2,
            engine: Some("postgres".into()),
            query_time_ms: Some(1.5),
        }
    }

    #[test]
    fn test_print_toon_does_not_panic() {
        print_toon(&sample_result()).unwrap();
    }

    #[test]
    fn test_print_toon_roundtrip() {
        let result = service::QueryResult {
            columns: vec!["num".into(), "name".into(), "gas_used".into()],
            rows: vec![
                vec![json!(1), json!("alice"), json!(21000)],
                vec![json!(2), json!("bob"), json!(42000)],
            ],
            row_count: 2,
            engine: None,
            query_time_ms: None,
        };

        let mut output = Vec::new();
        for row in &result.rows {
            let mut obj = serde_json::Map::new();
            for (i, col) in result.columns.iter().enumerate() {
                obj.insert(col.clone(), row[i].clone());
            }
            output.push(serde_json::Value::Object(obj));
        }

        let toon = toon_format::encode_default(&output).unwrap();
        let decoded: serde_json::Value = toon_format::decode_default(&toon).unwrap();

        assert_eq!(decoded, serde_json::Value::Array(output));
    }

    #[test]
    fn test_print_toon_empty() {
        let result = service::QueryResult {
            columns: vec!["num".into()],
            rows: vec![],
            row_count: 0,
            engine: None,
            query_time_ms: None,
        };
        print_toon(&result).unwrap();
    }

    #[test]
    fn test_print_toon_null_values() {
        let result = service::QueryResult {
            columns: vec!["to".into(), "contract_address".into()],
            rows: vec![vec![json!(null), json!(null)]],
            row_count: 1,
            engine: None,
            query_time_ms: None,
        };
        print_toon(&result).unwrap();
    }

    #[test]
    fn test_print_json_does_not_panic() {
        print_json(&sample_result()).unwrap();
    }

    #[test]
    fn test_print_csv_does_not_panic() {
        print_csv(&sample_result()).unwrap();
    }

    #[test]
    fn test_print_table_does_not_panic() {
        print_table(&sample_result()).unwrap();
    }
}
