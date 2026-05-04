use anyhow::Result;
use clap::{Args as ClapArgs, Subcommand};
use serde::{Deserialize, Serialize};

#[derive(ClapArgs)]
pub struct Args {
    /// TIDX HTTP API URL (required for views commands)
    #[arg(long, env = "TIDX_URL")]
    pub url: String,

    #[command(subcommand)]
    pub command: ViewsCommand,
}

#[derive(Subcommand)]
pub enum ViewsCommand {
    /// List all views for a chain
    List {
        /// Chain ID
        #[arg(long)]
        chain_id: u64,
    },
    /// Get view details
    Get {
        /// Chain ID
        #[arg(long)]
        chain_id: u64,
        /// View name
        name: String,
    },
    /// Create a new materialized view
    Create {
        /// Chain ID
        #[arg(long)]
        chain_id: u64,
        /// View name
        #[arg(long)]
        name: String,
        /// SELECT statement for the view
        #[arg(long)]
        sql: String,
        /// Primary key columns (comma-separated)
        #[arg(long, value_delimiter = ',')]
        order_by: Vec<String>,
        /// ClickHouse engine (default: SummingMergeTree())
        #[arg(long, default_value = "SummingMergeTree()")]
        engine: String,
        /// Event signature for automatic CTE generation (e.g., "Transfer(address indexed from, address indexed to, uint256 value)")
        #[arg(long)]
        signature: Option<String>,
    },
    /// Delete a view
    Delete {
        /// Chain ID
        #[arg(long)]
        chain_id: u64,
        /// View name
        name: String,
    },
}

#[derive(Serialize)]
struct CreateViewRequest {
    #[serde(rename = "chainId")]
    chain_id: u64,
    engine: String,
    name: String,
    #[serde(rename = "orderBy")]
    order_by: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    signature: Option<String>,
    sql: String,
}

#[derive(Deserialize)]
struct ViewInfo {
    database: String,
    engine: String,
    name: String,
}

#[derive(Deserialize)]
struct ListViewsResponse {
    #[serde(default)]
    error: Option<String>,
    ok: bool,
    views: Vec<ViewInfo>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct CreateViewResponse {
    #[serde(default)]
    backfill_rows: Option<u64>,
    #[serde(default)]
    error: Option<String>,
    ok: bool,
    #[serde(default)]
    view: Option<ViewInfo>,
}

#[derive(Deserialize)]
struct GetViewResponse {
    #[serde(default)]
    definition: Option<String>,
    #[serde(default)]
    error: Option<String>,
    ok: bool,
    #[serde(default)]
    row_count: Option<u64>,
    #[serde(default)]
    view: Option<ViewInfo>,
}

#[derive(Deserialize)]
struct DeleteViewResponse {
    #[serde(default)]
    deleted: Vec<String>,
    #[serde(default)]
    error: Option<String>,
    ok: bool,
}

pub async fn run(args: Args) -> Result<()> {
    let client = reqwest::Client::new();
    let base_url = args.url.trim_end_matches('/');

    match args.command {
        ViewsCommand::List { chain_id } => {
            let url = format!("{}/views?chainId={}", base_url, chain_id);
            let resp: ListViewsResponse = client.get(&url).send().await?.json().await?;

            if !resp.ok {
                anyhow::bail!(
                    "{}",
                    resp.error.unwrap_or_else(|| "Unknown error".to_string())
                );
            }

            if resp.views.is_empty() {
                println!("No views found for chain {}", chain_id);
                return Ok(());
            }

            println!("{:<30} {:<20} DATABASE", "NAME", "ENGINE");
            println!("{}", "-".repeat(70));
            for view in resp.views {
                println!("{:<30} {:<20} {}", view.name, view.engine, view.database);
            }
        }

        ViewsCommand::Get { chain_id, name } => {
            let url = format!("{}/views/{}?chainId={}", base_url, name, chain_id);
            let resp: GetViewResponse = client.get(&url).send().await?.json().await?;

            if !resp.ok {
                anyhow::bail!(
                    "{}",
                    resp.error.unwrap_or_else(|| "Unknown error".to_string())
                );
            }

            if let Some(view) = resp.view {
                println!("Name:       {}", view.name);
                println!("Database:   {}", view.database);
                println!("Engine:     {}", view.engine);
                println!("Row Count:  {}", resp.row_count.unwrap_or(0));
                if let Some(def) = resp.definition {
                    println!("\nDefinition:\n{}", def);
                }
            }
        }

        ViewsCommand::Create {
            chain_id,
            name,
            sql,
            order_by,
            engine,
            signature,
        } => {
            let url = format!("{}/views", base_url);
            let req = CreateViewRequest {
                chain_id,
                name: name.clone(),
                sql,
                order_by,
                engine,
                signature,
            };

            let resp: CreateViewResponse =
                client.post(&url).json(&req).send().await?.json().await?;

            if !resp.ok {
                anyhow::bail!(
                    "{}",
                    resp.error.unwrap_or_else(|| "Unknown error".to_string())
                );
            }

            println!("Created view: {}", name);
            if let Some(rows) = resp.backfill_rows {
                println!("Backfilled {} rows", rows);
            }
        }

        ViewsCommand::Delete { chain_id, name } => {
            let url = format!("{}/views/{}?chainId={}", base_url, name, chain_id);
            let resp: DeleteViewResponse = client.delete(&url).send().await?.json().await?;

            if !resp.ok {
                anyhow::bail!(
                    "{}",
                    resp.error.unwrap_or_else(|| "Unknown error".to_string())
                );
            }

            println!("Deleted: {}", resp.deleted.join(", "));
        }
    }

    Ok(())
}
