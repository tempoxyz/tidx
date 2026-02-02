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
    name: String,
    sql: String,
    #[serde(rename = "orderBy")]
    order_by: Vec<String>,
    engine: String,
}

#[derive(Deserialize)]
struct ViewInfo {
    name: String,
    engine: String,
    database: String,
}

#[derive(Deserialize)]
struct ListViewsResponse {
    ok: bool,
    views: Vec<ViewInfo>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct CreateViewResponse {
    ok: bool,
    #[serde(default)]
    view: Option<ViewInfo>,
    #[serde(default)]
    backfill_rows: Option<u64>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Deserialize)]
struct GetViewResponse {
    ok: bool,
    #[serde(default)]
    view: Option<ViewInfo>,
    #[serde(default)]
    definition: Option<String>,
    #[serde(default)]
    row_count: Option<u64>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Deserialize)]
struct DeleteViewResponse {
    ok: bool,
    #[serde(default)]
    deleted: Vec<String>,
    #[serde(default)]
    error: Option<String>,
}

pub async fn run(args: Args) -> Result<()> {
    let client = reqwest::Client::new();
    let base_url = args.url.trim_end_matches('/');

    match args.command {
        ViewsCommand::List { chain_id } => {
            let url = format!("{}/views?chainId={}", base_url, chain_id);
            let resp: ListViewsResponse = client.get(&url).send().await?.json().await?;
            
            if !resp.ok {
                anyhow::bail!("{}", resp.error.unwrap_or_else(|| "Unknown error".to_string()));
            }

            if resp.views.is_empty() {
                println!("No views found for chain {}", chain_id);
                return Ok(());
            }

            println!("{:<30} {:<20} {}", "NAME", "ENGINE", "DATABASE");
            println!("{}", "-".repeat(70));
            for view in resp.views {
                println!("{:<30} {:<20} {}", view.name, view.engine, view.database);
            }
        }

        ViewsCommand::Get { chain_id, name } => {
            let url = format!("{}/views/{}?chainId={}", base_url, name, chain_id);
            let resp: GetViewResponse = client.get(&url).send().await?.json().await?;

            if !resp.ok {
                anyhow::bail!("{}", resp.error.unwrap_or_else(|| "Unknown error".to_string()));
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

        ViewsCommand::Create { chain_id, name, sql, order_by, engine } => {
            let url = format!("{}/views", base_url);
            let req = CreateViewRequest {
                chain_id,
                name: name.clone(),
                sql,
                order_by,
                engine,
            };

            let resp: CreateViewResponse = client
                .post(&url)
                .json(&req)
                .send()
                .await?
                .json()
                .await?;

            if !resp.ok {
                anyhow::bail!("{}", resp.error.unwrap_or_else(|| "Unknown error".to_string()));
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
                anyhow::bail!("{}", resp.error.unwrap_or_else(|| "Unknown error".to_string()));
            }

            println!("Deleted: {}", resp.deleted.join(", "));
        }
    }

    Ok(())
}
