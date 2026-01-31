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

    /// pg_duckdb memory limit (e.g., "16GB")
    #[serde(default)]
    pub pg_duckdb_memory_limit: Option<String>,

    /// pg_duckdb thread count
    #[serde(default)]
    pub pg_duckdb_threads: Option<u32>,

    /// Parquet compression/export settings
    #[serde(default)]
    pub compress: Option<CompressConfig>,
}

/// Configuration for Parquet export/compression of old data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressConfig {
    /// Enable Parquet export (default: false)
    #[serde(default)]
    pub enabled: bool,

    /// Export when this many contiguous blocks are ready (default: 100000)
    #[serde(default = "default_threshold_blocks")]
    pub threshold_blocks: u64,

    /// Keep this many recent blocks in PostgreSQL (default: 10000)
    #[serde(default = "default_retention_blocks")]
    pub retention_blocks: u64,

    /// Check interval in seconds (default: 600 = 10 minutes)
    #[serde(default = "default_check_interval")]
    pub check_interval_secs: u64,

    /// Directory to store Parquet files (default: /data)
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
}

impl Default for CompressConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            threshold_blocks: 100_000,
            retention_blocks: 10_000,
            check_interval_secs: 600,
            data_dir: "/data".to_string(),
        }
    }
}

fn default_threshold_blocks() -> u64 {
    100_000
}

fn default_retention_blocks() -> u64 {
    10_000
}

fn default_check_interval() -> u64 {
    600
}

fn default_data_dir() -> String {
    "/data".to_string()
}

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
    fn test_chain_config_with_pg_duckdb_settings() {
        let toml_str = r#"
            name = "test"
            chain_id = 1
            rpc_url = "http://localhost:8545"
            pg_url = "postgres://localhost/test"
            pg_duckdb_memory_limit = "16GB"
            pg_duckdb_threads = 8
        "#;
        
        let config: ChainConfig = toml::from_str(toml_str).unwrap();
        
        assert_eq!(config.pg_duckdb_memory_limit, Some("16GB".to_string()));
        assert_eq!(config.pg_duckdb_threads, Some(8));
    }

    #[test]
    fn test_chain_config_defaults() {
        let toml_str = r#"
            name = "test"
            chain_id = 1
            rpc_url = "http://localhost:8545"
            pg_url = "postgres://localhost/test"
        "#;
        
        let config: ChainConfig = toml::from_str(toml_str).unwrap();
        
        assert!(config.backfill);
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.concurrency, 4);
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
            pg_duckdb_memory_limit = "8GB"
            
            [[chains]]
            name = "chain2"
            chain_id = 2
            rpc_url = "http://localhost:8546"
            pg_url = "postgres://localhost/chain2"
        "#;
        
        let config: Config = toml::from_str(toml_str).unwrap();
        
        assert_eq!(config.chains.len(), 2);
        assert_eq!(config.chains[0].pg_duckdb_memory_limit, Some("8GB".to_string()));
        assert_eq!(config.chains[1].pg_duckdb_memory_limit, None);
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
