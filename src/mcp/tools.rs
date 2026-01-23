use rmcp::{Error as McpError, ServerHandler, model::*, schemars, tool};
use serde::{Deserialize, Serialize};

use super::McpState;
use crate::service::{self, QueryOptions};

/// TIDX MCP Server - exposes blockchain indexer query and status tools
#[derive(Clone)]
pub struct TidxMcp {
    state: McpState,
}

impl TidxMcp {
    pub fn new(state: McpState) -> Self {
        Self { state }
    }
}

/// Query parameters for tidx_query tool
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct QueryParams {
    /// SQL query (SELECT only). Tables: blocks, txs, logs
    pub sql: String,
    /// Chain ID or name (e.g., 4217 or "presto"). Uses default if not specified.
    #[serde(default)]
    pub chain: Option<String>,
    /// Event signature to decode logs (e.g., "Transfer(address indexed from, address indexed to, uint256 value)")
    #[serde(default)]
    pub signature: Option<String>,
    /// Query timeout in milliseconds (default: 5000, max: 30000)
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    /// Maximum rows to return (default: 1000, max: 10000)
    #[serde(default)]
    pub limit: Option<i64>,
    /// Force query engine: "postgres" or "duckdb"
    #[serde(default)]
    pub engine: Option<String>,
}

/// Status parameters for tidx_status tool
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct StatusParams {
    /// Chain ID or name to filter (returns all chains if not specified)
    #[serde(default)]
    pub chain: Option<String>,
}

/// Parameters for querying Tempo documentation
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DocsParams {
    /// Search query for Tempo documentation
    pub query: String,
}

#[derive(Debug, Serialize)]
struct QueryResult {
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
    row_count: usize,
    engine: Option<String>,
}

#[tool(tool_box)]
impl TidxMcp {
    /// Query indexed blockchain data using SQL. Available tables: blocks, txs, logs.
    /// Use the signature parameter to decode event logs into typed columns.
    #[tool(
        name = "tidx_query",
        description = "Execute a SQL query on indexed Tempo blockchain data. Tables available: blocks (num, hash, timestamp, tx_count, gas_used), txs (hash, block_num, from_addr, to_addr, value, gas_used, status), logs (block_num, tx_hash, address, topic0-3, data). Use signature param to decode logs."
    )]
    async fn query(&self, #[tool(aggr)] params: QueryParams) -> Result<CallToolResult, McpError> {
        let chain_id = match &params.chain {
            Some(c) => c.parse::<u64>().unwrap_or_else(|_| self.state.resolve_chain_id(Some(c))),
            None => self.state.default_chain_id,
        };

        let pool = self
            .state
            .get_pool(chain_id)
            .await
            .ok_or_else(|| McpError::invalid_params(format!("Unknown chain: {chain_id}"), None))?;

        let duckdb_pool = self.state.get_duckdb_pool(chain_id).await;

        let options = QueryOptions {
            timeout_ms: params.timeout_ms.unwrap_or(5000).clamp(100, 30000),
            limit: params.limit.unwrap_or(1000).clamp(1, 10000),
        };

        let result = service::execute_query_with_engine(
            &pool,
            duckdb_pool.as_ref(),
            &params.sql,
            params.signature.as_deref(),
            &options,
            params.engine.as_deref(),
        )
        .await
        .map_err(|e| McpError::internal_error(e.to_string(), None))?;

        let response = QueryResult {
            columns: result.columns,
            rows: result.rows,
            row_count: result.row_count,
            engine: result.engine,
        };

        Ok(CallToolResult::success(vec![Content::json(&response)?]))
    }

    /// Get sync status for all indexed chains including block height, lag, and backfill progress.
    #[tool(
        name = "tidx_status", 
        description = "Get sync status for indexed Tempo chains. Returns current block height, sync lag, backfill progress, and ETA."
    )]
    async fn status(&self, #[tool(aggr)] params: StatusParams) -> Result<CallToolResult, McpError> {
        let filter_chain_id = params.chain.as_ref().map(|c| {
            c.parse::<u64>()
                .unwrap_or_else(|_| self.state.resolve_chain_id(Some(c)))
        });

        let mut all_statuses = Vec::new();
        let pools = self.state.pools.read().await;

        for (&chain_id, pool) in pools.iter() {
            if let Some(filter) = filter_chain_id {
                if chain_id != filter {
                    continue;
                }
            }

            match service::get_all_status(pool).await {
                Ok(statuses) => all_statuses.extend(statuses),
                Err(e) => {
                    tracing::warn!(chain_id, error = %e, "Failed to get status");
                }
            }
        }

        Ok(CallToolResult::success(vec![Content::json(&all_statuses)?]))
    }

    /// Search Tempo documentation for protocol specs, integration guides, and API reference.
    #[tool(
        name = "tempo_docs",
        description = "Search Tempo blockchain documentation. Covers protocol specs (TIP-20 tokens, fees, transactions), integration guides, and SDK reference."
    )]
    async fn docs(&self, #[tool(aggr)] params: DocsParams) -> Result<CallToolResult, McpError> {
        // Forward to docs.tempo.xyz MCP endpoint via HTTP
        let client = reqwest::Client::new();
        
        // Connect to SSE endpoint to get session
        let sse_resp = client
            .get("https://docs.tempo.xyz/api/mcp")
            .header("Accept", "text/event-stream")
            .send()
            .await
            .map_err(|e| McpError::internal_error(format!("Failed to connect to docs: {e}"), None))?;

        let text = sse_resp
            .text()
            .await
            .map_err(|e| McpError::internal_error(format!("Failed to read SSE: {e}"), None))?;

        // Parse session ID from SSE response
        let session_id = text
            .lines()
            .find(|l| l.starts_with("data:"))
            .and_then(|l| l.split("sessionId=").nth(1))
            .map(|s| s.trim())
            .ok_or_else(|| McpError::internal_error("Failed to get session ID", None))?;

        // Call the docs search tool
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "searchDocs",
                "arguments": {
                    "query": params.query
                }
            }
        });

        let resp = client
            .post(format!("https://docs.tempo.xyz/api/mcp/messages?sessionId={session_id}"))
            .json(&request)
            .send()
            .await
            .map_err(|e| McpError::internal_error(format!("Docs request failed: {e}"), None))?;

        let result: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| McpError::internal_error(format!("Failed to parse response: {e}"), None))?;

        // Extract content from MCP response
        if let Some(content) = result.get("result").and_then(|r| r.get("content")) {
            Ok(CallToolResult::success(vec![Content::json(content)?]))
        } else if let Some(error) = result.get("error") {
            Err(McpError::internal_error(error.to_string(), None))
        } else {
            Ok(CallToolResult::success(vec![Content::json(&result)?]))
        }
    }
}

#[tool(tool_box)]
impl ServerHandler for TidxMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some(
                "TIDX is a Tempo blockchain indexer. Use tidx_query to run SQL queries on blocks, transactions, and logs. Use tidx_status to check sync progress. Use tempo_docs to search documentation.".into()
            ),
            capabilities: ServerCapabilities::builder()
                .enable_tools()
                .build(),
            ..Default::default()
        }
    }
}
