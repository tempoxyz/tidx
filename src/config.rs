mod watcher;

pub use watcher::{ConfigWatcher, NewChainEvent, SharedHttpConfig};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// HTTP API settings
    #[serde(default)]
    pub http: HttpConfig,

    /// Prometheus metrics settings
    #[serde(default)]
    pub prometheus: PrometheusConfig,

    /// Chains to index
    pub chains: Vec<ChainConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpConfig {
    /// Enable HTTP API (default: true)
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// HTTP API port (default: 8080)
    #[serde(default = "default_http_port")]
    pub port: u16,

    /// Bind address (default: 0.0.0.0)
    #[serde(default = "default_bind")]
    pub bind: String,

    /// API keys that bypass rate limiting (optional)
    #[serde(default)]
    pub api_keys: Vec<String>,

    /// Rate limiting configuration
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 8080,
            bind: "0.0.0.0".to_string(),
            api_keys: Vec::new(),
            rate_limit: RateLimitConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Enable rate limiting (default: true)
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Maximum requests per window for unauthenticated requests (default: 100)
    #[serde(default = "default_rate_limit_requests")]
    pub requests_per_window: u32,

    /// Rate limit window in seconds (default: 60)
    #[serde(default = "default_rate_limit_window")]
    pub window_secs: u64,

    /// Maximum concurrent SSE connections per IP (default: 5)
    #[serde(default = "default_max_sse_connections")]
    pub max_sse_connections: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            requests_per_window: 100,
            window_secs: 60,
            max_sse_connections: 5,
        }
    }
}

fn default_rate_limit_requests() -> u32 {
    100
}

fn default_rate_limit_window() -> u64 {
    60
}

fn default_max_sse_connections() -> u32 {
    5
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrometheusConfig {
    /// Enable Prometheus metrics (default: true)
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Metrics port (default: 9090)
    #[serde(default = "default_metrics_port")]
    pub port: u16,
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 9090,
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_http_port() -> u16 {
    8080
}

fn default_bind() -> String {
    "0.0.0.0".to_string()
}

fn default_metrics_port() -> u16 {
    9090
}

/// Analytics engine to use for OLAP queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnalyticsEngine {
    /// Use DuckDB file with pg_parquet replication (legacy)
    DuckdbFile,
    /// Use pg_duckdb extension for direct PostgreSQL queries (recommended)
    #[default]
    PgDuckdb,
    /// Use PostgreSQL only (no DuckDB acceleration)
    Postgres,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainConfig {
    /// Chain name (for display/logging)
    pub name: String,

    /// Chain ID
    pub chain_id: u64,

    /// RPC URL
    pub rpc_url: String,

    /// Database connection URL for this chain
    pub pg_url: String,

    /// DuckDB path for this chain (optional, only used with analytics_engine = "duckdb_file")
    pub duckdb_path: Option<String>,

    /// Analytics engine for OLAP queries (default: "pg_duckdb")
    /// - "pg_duckdb": Query PostgreSQL using DuckDB engine (no replication needed)
    /// - "duckdb_file": Replicate to DuckDB file via pg_parquet (legacy)
    /// - "postgres": Use PostgreSQL only (no DuckDB acceleration)
    #[serde(default)]
    pub analytics_engine: AnalyticsEngine,

    /// Enable backfill to genesis (default: true)
    #[serde(default = "default_backfill")]
    pub backfill: bool,

    /// Batch size for RPC requests (default: 100)
    #[serde(default = "default_batch_size")]
    pub batch_size: u64,

    /// Number of concurrent gap-fill workers (default: 4)
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,

    /// Complete backfill before starting realtime sync (default: false)
    /// When true, syncs all gaps to genesis before following chain head.
    /// When false (default), runs realtime and backfill concurrently.
    #[serde(default)]
    pub backfill_first: bool,

    /// Trust RPC data without validating parent hashes (default: false)
    /// When true, skips reorg detection for faster sync on trusted RPCs.
    /// Use for chains with frequent shallow reorgs where RPC is authoritative.
    #[serde(default)]
    pub trust_rpc: bool,

    /// DuckDB gap-fill batch sizes (blocks per batch, only used with analytics_engine = "duckdb_file")
    #[serde(default)]
    pub duckdb_batch_sizes: Option<DuckDbBatchSizes>,

    /// pg_duckdb memory limit (e.g., "16GB", only used with analytics_engine = "pg_duckdb")
    #[serde(default)]
    pub pg_duckdb_memory_limit: Option<String>,

    /// pg_duckdb thread count (only used with analytics_engine = "pg_duckdb")
    #[serde(default)]
    pub pg_duckdb_threads: Option<u32>,
}

/// DuckDB gap-fill batch sizes per table type.
/// Larger batches = faster backfill but more memory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuckDbBatchSizes {
    /// Blocks table batch size (default: 50000)
    #[serde(default = "default_blocks_batch")]
    pub blocks: i64,
    /// Transactions table batch size (default: 50000)
    #[serde(default = "default_txs_batch")]
    pub txs: i64,
    /// Logs table batch size (default: 20000)
    #[serde(default = "default_logs_batch")]
    pub logs: i64,
    /// Receipts table batch size (default: 50000)
    #[serde(default = "default_receipts_batch")]
    pub receipts: i64,
}

impl Default for DuckDbBatchSizes {
    fn default() -> Self {
        Self {
            blocks: 50_000,
            txs: 50_000,
            logs: 20_000,
            receipts: 50_000,
        }
    }
}

fn default_blocks_batch() -> i64 { 50_000 }
fn default_txs_batch() -> i64 { 50_000 }
fn default_logs_batch() -> i64 { 20_000 }
fn default_receipts_batch() -> i64 { 50_000 }

fn default_backfill() -> bool {
    true
}

fn default_batch_size() -> u64 {
    100
}

fn default_concurrency() -> usize {
    4
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;

        let config: Config = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;

        if config.chains.is_empty() {
            anyhow::bail!("No chains configured. Add at least one [[chains]] section.");
        }

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_analytics_engine_default_is_pg_duckdb() {
        assert_eq!(AnalyticsEngine::default(), AnalyticsEngine::PgDuckdb);
    }

    #[test]
    fn test_analytics_engine_serialization() {
        // Test pg_duckdb
        let engine = AnalyticsEngine::PgDuckdb;
        let json = serde_json::to_string(&engine).unwrap();
        assert_eq!(json, "\"pg_duckdb\"");
        
        // Test duckdb_file
        let engine = AnalyticsEngine::DuckdbFile;
        let json = serde_json::to_string(&engine).unwrap();
        assert_eq!(json, "\"duckdb_file\"");
        
        // Test postgres
        let engine = AnalyticsEngine::Postgres;
        let json = serde_json::to_string(&engine).unwrap();
        assert_eq!(json, "\"postgres\"");
    }

    #[test]
    fn test_analytics_engine_deserialization() {
        let engine: AnalyticsEngine = serde_json::from_str("\"pg_duckdb\"").unwrap();
        assert_eq!(engine, AnalyticsEngine::PgDuckdb);
        
        let engine: AnalyticsEngine = serde_json::from_str("\"duckdb_file\"").unwrap();
        assert_eq!(engine, AnalyticsEngine::DuckdbFile);
        
        let engine: AnalyticsEngine = serde_json::from_str("\"postgres\"").unwrap();
        assert_eq!(engine, AnalyticsEngine::Postgres);
    }

    #[test]
    fn test_chain_config_with_pg_duckdb() {
        let toml_str = r#"
            name = "test"
            chain_id = 1
            rpc_url = "http://localhost:8545"
            pg_url = "postgres://localhost/test"
            analytics_engine = "pg_duckdb"
            pg_duckdb_memory_limit = "16GB"
            pg_duckdb_threads = 8
        "#;
        
        let config: ChainConfig = toml::from_str(toml_str).unwrap();
        
        assert_eq!(config.analytics_engine, AnalyticsEngine::PgDuckdb);
        assert_eq!(config.pg_duckdb_memory_limit, Some("16GB".to_string()));
        assert_eq!(config.pg_duckdb_threads, Some(8));
        assert!(config.duckdb_path.is_none());
    }

    #[test]
    fn test_chain_config_with_duckdb_file() {
        let toml_str = r#"
            name = "test"
            chain_id = 1
            rpc_url = "http://localhost:8545"
            pg_url = "postgres://localhost/test"
            analytics_engine = "duckdb_file"
            duckdb_path = "/data/chain.db"
        "#;
        
        let config: ChainConfig = toml::from_str(toml_str).unwrap();
        
        assert_eq!(config.analytics_engine, AnalyticsEngine::DuckdbFile);
        assert_eq!(config.duckdb_path, Some("/data/chain.db".to_string()));
    }

    #[test]
    fn test_chain_config_defaults_to_pg_duckdb() {
        let toml_str = r#"
            name = "test"
            chain_id = 1
            rpc_url = "http://localhost:8545"
            pg_url = "postgres://localhost/test"
        "#;
        
        let config: ChainConfig = toml::from_str(toml_str).unwrap();
        
        // Default should be pg_duckdb
        assert_eq!(config.analytics_engine, AnalyticsEngine::PgDuckdb);
    }

    #[test]
    fn test_chain_config_with_postgres_only() {
        let toml_str = r#"
            name = "test"
            chain_id = 1
            rpc_url = "http://localhost:8545"
            pg_url = "postgres://localhost/test"
            analytics_engine = "postgres"
        "#;
        
        let config: ChainConfig = toml::from_str(toml_str).unwrap();
        
        assert_eq!(config.analytics_engine, AnalyticsEngine::Postgres);
    }

    #[test]
    fn test_full_config_with_multiple_chains() {
        let toml_str = r#"
            [http]
            enabled = true
            port = 8080
            
            [prometheus]
            enabled = true
            port = 9090
            
            [[chains]]
            name = "chain1"
            chain_id = 1
            rpc_url = "http://localhost:8545"
            pg_url = "postgres://localhost/chain1"
            analytics_engine = "pg_duckdb"
            pg_duckdb_memory_limit = "8GB"
            
            [[chains]]
            name = "chain2"
            chain_id = 2
            rpc_url = "http://localhost:8546"
            pg_url = "postgres://localhost/chain2"
            analytics_engine = "duckdb_file"
            duckdb_path = "/data/chain2.db"
        "#;
        
        let config: Config = toml::from_str(toml_str).unwrap();
        
        assert_eq!(config.chains.len(), 2);
        
        assert_eq!(config.chains[0].analytics_engine, AnalyticsEngine::PgDuckdb);
        assert_eq!(config.chains[0].pg_duckdb_memory_limit, Some("8GB".to_string()));
        
        assert_eq!(config.chains[1].analytics_engine, AnalyticsEngine::DuckdbFile);
        assert_eq!(config.chains[1].duckdb_path, Some("/data/chain2.db".to_string()));
    }

    #[test]
    fn test_duckdb_batch_sizes_default() {
        let sizes = DuckDbBatchSizes::default();
        assert_eq!(sizes.blocks, 50_000);
        assert_eq!(sizes.txs, 50_000);
        assert_eq!(sizes.logs, 20_000);
        assert_eq!(sizes.receipts, 50_000);
    }

    #[test]
    fn test_rate_limit_config_default() {
        let config = RateLimitConfig::default();
        assert!(config.enabled);
        assert_eq!(config.requests_per_window, 100);
        assert_eq!(config.window_secs, 60);
        assert_eq!(config.max_sse_connections, 5);
    }
}
