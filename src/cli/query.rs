use anyhow::Result;
use clap::Args as ClapArgs;

use ak47::db;
use ak47::service::{self, QueryOptions};

#[derive(ClapArgs)]
pub struct Args {
    /// Database URL
    #[arg(long, env = "AK47_DATABASE_URL")]
    pub db: String,

    /// SQL query (SELECT only). Use event name from --signature as table.
    pub sql: String,

    /// Event signature to create a CTE (e.g., "Transfer(address indexed from, address indexed to, uint256 value)")
    #[arg(long, short)]
    pub signature: Option<String>,

    /// Output format (table, json, csv)
    #[arg(long, default_value = "table")]
    pub format: String,

    /// Query timeout in milliseconds
    #[arg(long, default_value = "30000")]
    pub timeout: u64,

    /// Maximum rows to return
    #[arg(long, default_value = "10000")]
    pub limit: i64,
}

pub async fn run(args: Args) -> Result<()> {
    let pool = db::create_pool(&args.db).await?;

    let options = QueryOptions {
        timeout_ms: args.timeout,
        limit: args.limit,
    };

    let result = service::execute_query(
        &pool,
        &args.sql,
        args.signature.as_deref(),
        &options,
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
