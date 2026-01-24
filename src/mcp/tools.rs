use rmcp::{Error as McpError, RoleServer, ServerHandler, model::*, schemars, service::RequestContext, tool};
use serde::{Deserialize, Serialize};

use super::McpState;
use crate::service::{self, QueryOptions};

// ============================================================================
// Schema Documentation (used as context for query generation)
// ============================================================================

const SCHEMA_DOCS: &str = r#"
# TIDX Database Schema

## Tables

### blocks
Indexed blockchain blocks.
| Column | Type | Description |
|--------|------|-------------|
| num | bigint | Block number (primary key) |
| hash | bytea | Block hash (0x-prefixed hex) |
| parent_hash | bytea | Parent block hash |
| timestamp | timestamptz | Block timestamp |
| tx_count | int | Number of transactions |
| gas_used | bigint | Total gas used |
| gas_limit | bigint | Block gas limit |
| base_fee | bigint | Base fee per gas (wei) |

### txs
Indexed transactions.
| Column | Type | Description |
|--------|------|-------------|
| hash | bytea | Transaction hash (primary key) |
| block_num | bigint | Block number |
| tx_index | int | Transaction index in block |
| from_addr | bytea | Sender address |
| to_addr | bytea | Recipient address (null for contract creation) |
| value | numeric | Value transferred (wei) |
| gas_used | bigint | Gas used |
| gas_price | bigint | Gas price (wei) |
| status | smallint | 1 = success, 0 = failure |
| input | bytea | Transaction input data |

### logs
Indexed event logs.
| Column | Type | Description |
|--------|------|-------------|
| block_num | bigint | Block number |
| tx_hash | bytea | Transaction hash |
| log_index | int | Log index in transaction |
| address | bytea | Contract address that emitted the event |
| topic0 | bytea | Event signature hash |
| topic1 | bytea | Indexed parameter 1 |
| topic2 | bytea | Indexed parameter 2 |
| topic3 | bytea | Indexed parameter 3 |
| data | bytea | Non-indexed event data (ABI-encoded) |

## Query Examples

### Get recent blocks
```sql
SELECT num, timestamp, tx_count, gas_used FROM blocks ORDER BY num DESC LIMIT 10
```

### Get transactions for an address
```sql
SELECT hash, block_num, value, status FROM txs 
WHERE from_addr = '0x...' OR to_addr = '0x...'
ORDER BY block_num DESC LIMIT 100
```

### Decode Transfer events (use signature parameter)
With signature: `Transfer(address indexed from, address indexed to, uint256 value)`
```sql
SELECT block_num, "from", "to", value FROM Transfer ORDER BY block_num DESC LIMIT 100
```

### Count events by contract
```sql
SELECT address, COUNT(*) as event_count FROM logs 
GROUP BY address ORDER BY event_count DESC LIMIT 20
```

## Tempo-Specific Notes

- Chain IDs: Presto (mainnet) = 4217, Andantino (testnet) = 42429
- TIP-20 tokens use standard ERC20 events (Transfer, Approval)
- Addresses are 20 bytes, stored as bytea, query with '0x...' prefix
- Values are in wei (1 TEMPO = 10^18 wei)
"#;

const QUERY_PATTERNS: &str = r#"
# Common Query Patterns

## Filtering by address
Always use hex format with '0x' prefix:
```sql
WHERE address = '0x1234...'
```

## Time-based queries
Use timestamp column on blocks table:
```sql
SELECT * FROM blocks WHERE timestamp > NOW() - INTERVAL '1 hour'
```

## Joining logs with transactions
```sql
SELECT l.*, t.from_addr, t.status 
FROM logs l 
JOIN txs t ON l.tx_hash = t.hash 
WHERE l.topic0 = '0xddf252ad...'  -- Transfer event
```

## Decoding events with signature parameter
Pass the event signature to automatically decode logs:
- signature: "Transfer(address indexed from, address indexed to, uint256 value)"
- This creates a CTE named after the event that decodes topic1/topic2/data

## Aggregations (uses DuckDB for better performance)
```sql
SELECT DATE_TRUNC('day', timestamp) as day, COUNT(*) as tx_count
FROM blocks b JOIN txs t ON b.num = t.block_num
GROUP BY day ORDER BY day DESC
```
"#;

// ============================================================================
// Direct DB Access Implementation
// ============================================================================

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
            instructions: Some(format!(
                "TIDX is a Tempo blockchain indexer. Use tidx_query to run SQL queries on blocks, transactions, and logs. Use tidx_status to check sync progress. Use tempo_docs to search Tempo protocol documentation.\n\n{SCHEMA_DOCS}\n\n{QUERY_PATTERNS}"
            )),
            capabilities: ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .build(),
            ..Default::default()
        }
    }

    async fn list_resources(
        &self,
        _request: PaginatedRequestParam,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        let schema = RawResource {
            uri: "tidx://schema".to_string(),
            name: "TIDX Database Schema".to_string(),
            description: Some("Database schema for blocks, txs, and logs tables".to_string()),
            mime_type: Some("text/markdown".to_string()),
            size: None,
        };
        let patterns = RawResource {
            uri: "tidx://patterns".to_string(),
            name: "Query Patterns".to_string(),
            description: Some("Common SQL query patterns for blockchain data".to_string()),
            mime_type: Some("text/markdown".to_string()),
            size: None,
        };
        Ok(ListResourcesResult {
            resources: vec![
                Annotated { raw: schema, annotations: None },
                Annotated { raw: patterns, annotations: None },
            ],
            next_cursor: None,
        })
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParam,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, McpError> {
        let content = match request.uri.as_str() {
            "tidx://schema" => SCHEMA_DOCS,
            "tidx://patterns" => QUERY_PATTERNS,
            _ => return Err(McpError::resource_not_found(
                format!("Unknown resource: {}", request.uri),
                None,
            )),
        };

        Ok(ReadResourceResult {
            contents: vec![ResourceContents::TextResourceContents {
                uri: request.uri,
                mime_type: Some("text/markdown".to_string()),
                text: content.to_string(),
            }],
        })
    }
}

// ============================================================================
// HTTP Proxy Implementation
// ============================================================================

/// TIDX MCP Server that proxies to a remote TIDX HTTP API
#[derive(Clone)]
pub struct TidxMcpHttp {
    base_url: String,
    client: reqwest::Client,
}

impl TidxMcpHttp {
    pub fn new(base_url: String) -> Self {
        Self {
            base_url,
            client: reqwest::Client::new(),
        }
    }
}

#[tool(tool_box)]
impl TidxMcpHttp {
    /// Query indexed blockchain data using SQL. Available tables: blocks, txs, logs.
    #[tool(
        name = "tidx_query",
        description = "Execute a SQL query on indexed Tempo blockchain data. Tables available: blocks (num, hash, timestamp, tx_count, gas_used), txs (hash, block_num, from_addr, to_addr, value, gas_used, status), logs (block_num, tx_hash, address, topic0-3, data). Use signature param to decode logs."
    )]
    async fn query(&self, #[tool(aggr)] params: QueryParams) -> Result<CallToolResult, McpError> {
        let chain_id = params
            .chain
            .as_ref()
            .and_then(|c| c.parse::<u64>().ok())
            .unwrap_or(4217); // Default to Presto mainnet

        let mut url = reqwest::Url::parse(&format!("{}/query", self.base_url))
            .map_err(|e| McpError::internal_error(format!("Invalid URL: {e}"), None))?;

        url.query_pairs_mut()
            .append_pair("sql", &params.sql)
            .append_pair("chainId", &chain_id.to_string())
            .append_pair("timeout_ms", &params.timeout_ms.unwrap_or(5000).to_string())
            .append_pair("limit", &params.limit.unwrap_or(1000).to_string());

        if let Some(sig) = &params.signature {
            url.query_pairs_mut().append_pair("signature", sig);
        }
        if let Some(engine) = &params.engine {
            url.query_pairs_mut().append_pair("engine", engine);
        }

        let resp = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| McpError::internal_error(format!("HTTP request failed: {e}"), None))?;

        let status = resp.status();
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| McpError::internal_error(format!("Failed to parse response: {e}"), None))?;

        if !status.is_success() {
            let error = body["error"].as_str().unwrap_or("Unknown error");
            return Err(McpError::internal_error(error.to_string(), None));
        }

        Ok(CallToolResult::success(vec![Content::json(&body)?]))
    }

    /// Get sync status for all indexed chains.
    #[tool(
        name = "tidx_status",
        description = "Get sync status for indexed Tempo chains. Returns current block height, sync lag, backfill progress, and ETA."
    )]
    async fn status(&self, #[tool(aggr)] _params: StatusParams) -> Result<CallToolResult, McpError> {
        let url = format!("{}/status", self.base_url);

        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| McpError::internal_error(format!("HTTP request failed: {e}"), None))?;

        let status = resp.status();
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| McpError::internal_error(format!("Failed to parse response: {e}"), None))?;

        if !status.is_success() {
            let error = body["error"].as_str().unwrap_or("Unknown error");
            return Err(McpError::internal_error(error.to_string(), None));
        }

        Ok(CallToolResult::success(vec![Content::json(&body)?]))
    }

    /// Search Tempo documentation for protocol specs, integration guides, and API reference.
    #[tool(
        name = "tempo_docs",
        description = "Search Tempo blockchain documentation. Covers protocol specs (TIP-20 tokens, fees, transactions), integration guides, and SDK reference."
    )]
    async fn docs(&self, #[tool(aggr)] params: DocsParams) -> Result<CallToolResult, McpError> {
        // Same implementation as TidxMcp - forward to docs.tempo.xyz
        let sse_resp = self
            .client
            .get("https://docs.tempo.xyz/api/mcp")
            .header("Accept", "text/event-stream")
            .send()
            .await
            .map_err(|e| McpError::internal_error(format!("Failed to connect to docs: {e}"), None))?;

        let text = sse_resp
            .text()
            .await
            .map_err(|e| McpError::internal_error(format!("Failed to read SSE: {e}"), None))?;

        let session_id = text
            .lines()
            .find(|l| l.starts_with("data:"))
            .and_then(|l| l.split("sessionId=").nth(1))
            .map(|s| s.trim())
            .ok_or_else(|| McpError::internal_error("Failed to get session ID", None))?;

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

        let resp = self
            .client
            .post(format!("https://docs.tempo.xyz/api/mcp/messages?sessionId={session_id}"))
            .json(&request)
            .send()
            .await
            .map_err(|e| McpError::internal_error(format!("Docs request failed: {e}"), None))?;

        let result: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| McpError::internal_error(format!("Failed to parse response: {e}"), None))?;

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
impl ServerHandler for TidxMcpHttp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some(format!(
                "TIDX is a Tempo blockchain indexer (HTTP proxy mode). Use tidx_query to run SQL queries on blocks, transactions, and logs. Use tidx_status to check sync progress. Use tempo_docs to search Tempo protocol documentation.\n\n{SCHEMA_DOCS}\n\n{QUERY_PATTERNS}"
            )),
            capabilities: ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .build(),
            ..Default::default()
        }
    }

    async fn list_resources(
        &self,
        _request: PaginatedRequestParam,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        let schema = RawResource {
            uri: "tidx://schema".to_string(),
            name: "TIDX Database Schema".to_string(),
            description: Some("Database schema for blocks, txs, and logs tables".to_string()),
            mime_type: Some("text/markdown".to_string()),
            size: None,
        };
        let patterns = RawResource {
            uri: "tidx://patterns".to_string(),
            name: "Query Patterns".to_string(),
            description: Some("Common SQL query patterns for blockchain data".to_string()),
            mime_type: Some("text/markdown".to_string()),
            size: None,
        };
        Ok(ListResourcesResult {
            resources: vec![
                Annotated { raw: schema, annotations: None },
                Annotated { raw: patterns, annotations: None },
            ],
            next_cursor: None,
        })
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParam,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, McpError> {
        let content = match request.uri.as_str() {
            "tidx://schema" => SCHEMA_DOCS,
            "tidx://patterns" => QUERY_PATTERNS,
            _ => return Err(McpError::resource_not_found(
                format!("Unknown resource: {}", request.uri),
                None,
            )),
        };

        Ok(ReadResourceResult {
            contents: vec![ResourceContents::TextResourceContents {
                uri: request.uri,
                mime_type: Some("text/markdown".to_string()),
                text: content.to_string(),
            }],
        })
    }
}
