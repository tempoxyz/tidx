mod tools;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;

use rmcp::transport::stdio;
use rmcp::ServiceExt;
use tokio::sync::RwLock;

use crate::config::Config;
use crate::db::{self, DuckDbPool, Pool};

pub use tools::TidxMcp;

/// Shared state for MCP tools
#[derive(Clone)]
pub struct McpState {
    /// Map of chain_id -> PostgreSQL pool
    pub pools: Arc<RwLock<HashMap<u64, Pool>>>,
    /// Map of chain_id -> DuckDB pool
    pub duckdb_pools: Arc<RwLock<HashMap<u64, Arc<DuckDbPool>>>>,
    /// Chain name -> chain_id mapping
    pub chain_names: Arc<HashMap<String, u64>>,
    /// Default chain ID (first configured chain)
    pub default_chain_id: u64,
}

impl McpState {
    pub async fn from_config(config: &Config) -> Result<Self> {
        let mut pools = HashMap::new();
        let mut duckdb_pools = HashMap::new();
        let mut chain_names = HashMap::new();
        let mut default_chain_id = 0;

        for (i, chain) in config.chains.iter().enumerate() {
            let pool = db::create_pool(&chain.pg_url).await?;
            pools.insert(chain.chain_id, pool);
            chain_names.insert(chain.name.to_lowercase(), chain.chain_id);

            if i == 0 {
                default_chain_id = chain.chain_id;
            }

            // Create DuckDB pool if configured
            if let Some(ref duckdb_path) = chain.duckdb_path {
                let duckdb_pool = Arc::new(DuckDbPool::new(duckdb_path)?);
                duckdb_pools.insert(chain.chain_id, duckdb_pool);
            }
        }

        Ok(Self {
            pools: Arc::new(RwLock::new(pools)),
            duckdb_pools: Arc::new(RwLock::new(duckdb_pools)),
            chain_names: Arc::new(chain_names),
            default_chain_id,
        })
    }

    pub async fn get_pool(&self, chain_id: u64) -> Option<Pool> {
        self.pools.read().await.get(&chain_id).cloned()
    }

    pub async fn get_duckdb_pool(&self, chain_id: u64) -> Option<Arc<DuckDbPool>> {
        self.duckdb_pools.read().await.get(&chain_id).cloned()
    }

    pub fn resolve_chain_id(&self, chain: Option<&str>) -> u64 {
        chain
            .and_then(|name| self.chain_names.get(&name.to_lowercase()).copied())
            .unwrap_or(self.default_chain_id)
    }
}

/// Run the MCP server over stdio
pub async fn serve_stdio(config: &Config) -> Result<()> {
    let state = McpState::from_config(config).await?;
    let service = TidxMcp::new(state);

    let server = service.serve(stdio()).await?;
    server.waiting().await?;

    Ok(())
}
