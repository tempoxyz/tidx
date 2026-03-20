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

    /// Trusted CIDRs for admin operations (e.g., `100.64.0.0/10` for Tailscale)
    #[serde(default)]
    pub trusted_cidrs: Vec<String>,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 8080,
            bind: "0.0.0.0".to_string(),
            trusted_cidrs: Vec::new(),
        }
    }
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

    /// Database connection URL for this chain.
    /// If `pg_password_env` is set, the password in this URL will be replaced
    /// with the value from that environment variable.
    pub pg_url: String,

    /// Environment variable name containing the PostgreSQL password.
    /// When set, the password portion of `pg_url` is replaced with this value.
    #[serde(default)]
    pub pg_password_env: Option<String>,

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

    /// Separate PostgreSQL URL for the HTTP API (e.g., a CNPG `-r` read replica).
    /// When set, the API connection pool connects to this URL instead of `pg_url`.
    /// If `api_pg_password_env` is also set, the password is injected into this URL.
    #[serde(default)]
    pub api_pg_url: Option<String>,

    /// Environment variable name containing the API PostgreSQL password.
    /// When set, replaces the password in `api_pg_url` with the env var value.
    /// Has no effect without `api_pg_url`.
    #[serde(default)]
    pub api_pg_password_env: Option<String>,

    /// ClickHouse OLAP settings (for analytical queries)
    #[serde(default)]
    pub clickhouse: Option<ClickHouseConfig>,
}

/// Configuration for ClickHouse OLAP engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickHouseConfig {
    /// Enable ClickHouse OLAP queries (default: false)
    #[serde(default)]
    pub enabled: bool,

    /// Primary ClickHouse HTTP URL (default: http://clickhouse:8123)
    #[serde(default = "default_clickhouse_url")]
    pub url: String,

    /// Additional ClickHouse instance URLs for failover.
    /// Queries go to the primary `url`; failover instances are tried
    /// in order if the primary is unavailable.
    #[serde(default)]
    pub failover_urls: Vec<String>,

    /// Database name override (default: tidx_{chain_id})
    #[serde(default)]
    pub database: Option<String>,

    /// ClickHouse username for HTTP basic auth.
    #[serde(default)]
    pub user: Option<String>,

    /// Environment variable name containing the ClickHouse password.
    /// When set, the password is read from this env var at startup.
    #[serde(default)]
    pub password_env: Option<String>,
}

impl ClickHouseConfig {
    /// Returns all URLs: primary first, then failover instances.
    pub fn all_urls(&self) -> Vec<&str> {
        let mut urls = vec![self.url.as_str()];
        urls.extend(self.failover_urls.iter().map(|u| u.as_str()));
        urls
    }

    /// Resolve the ClickHouse password from the environment variable specified by `password_env`.
    pub fn resolved_password(&self) -> Result<Option<String>> {
        match &self.password_env {
            Some(env_var) => {
                let password = std::env::var(env_var).with_context(|| {
                    format!(
                        "clickhouse password_env '{env_var}' is set but environment variable not found"
                    )
                })?;
                Ok(Some(password))
            }
            None => Ok(None),
        }
    }
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            url: "http://clickhouse:8123".to_string(),
            failover_urls: Vec::new(),
            database: None,
            user: None,
            password_env: None,
        }
    }
}

fn default_clickhouse_url() -> String {
    "http://clickhouse:8123".to_string()
}

fn default_backfill() -> bool {
    true
}

impl ChainConfig {
    /// Returns the PostgreSQL connection URL with password resolved from environment if configured.
    /// If `pg_password_env` is set, replaces the password in `pg_url` with the env var value.
    pub fn resolved_pg_url(&self) -> Result<String> {
        match &self.pg_password_env {
            Some(env_var) => {
                let password = std::env::var(env_var).with_context(|| {
                    format!("pg_password_env '{env_var}' is set but environment variable not found")
                })?;

                let mut url = url::Url::parse(&self.pg_url)
                    .with_context(|| format!("Invalid pg_url: {}", self.pg_url))?;

                url.set_password(Some(&password))
                    .map_err(|()| anyhow::anyhow!("Failed to set password in pg_url"))?;

                Ok(url.to_string())
            }
            None => Ok(self.pg_url.clone()),
        }
    }

    /// Returns a separate API database URL for read-only queries.
    /// Returns `None` if `api_pg_url` is not set (API uses the main pool).
    pub fn resolved_api_pg_url(&self) -> Result<Option<String>> {
        let api_url = match &self.api_pg_url {
            Some(url) => url,
            None => return Ok(None),
        };

        match &self.api_pg_password_env {
            Some(pass_env) => {
                let password = std::env::var(pass_env).with_context(|| {
                    format!(
                        "api_pg_password_env '{pass_env}' is set but environment variable not found"
                    )
                })?;
                let mut url = url::Url::parse(api_url)
                    .with_context(|| format!("Invalid api_pg_url: {api_url}"))?;
                url.set_password(Some(&password))
                    .map_err(|()| anyhow::anyhow!("Failed to set password in api_pg_url"))?;
                Ok(Some(url.to_string()))
            }
            None => Ok(Some(api_url.clone())),
        }
    }
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
            
            [[chains]]
            name = "chain2"
            chain_id = 2
            rpc_url = "http://localhost:8546"
            pg_url = "postgres://localhost/chain2"
        "#;

        let config: Config = toml::from_str(toml_str).unwrap();

        assert_eq!(config.chains.len(), 2);
    }

    #[test]
    fn test_clickhouse_config_with_failover() {
        let toml_str = r#"
            name = "test"
            chain_id = 1
            rpc_url = "http://localhost:8545"
            pg_url = "postgres://localhost/test"

            [clickhouse]
            enabled = true
            url = "http://clickhouse-1:8123"
            failover_urls = ["http://clickhouse-2:8123", "http://clickhouse-3:8123"]
        "#;

        let config: ChainConfig = toml::from_str(toml_str).unwrap();
        let ch = config.clickhouse.unwrap();

        assert!(ch.enabled);
        assert_eq!(ch.url, "http://clickhouse-1:8123");
        assert_eq!(ch.failover_urls.len(), 2);
        assert_eq!(
            ch.all_urls(),
            vec![
                "http://clickhouse-1:8123",
                "http://clickhouse-2:8123",
                "http://clickhouse-3:8123",
            ]
        );
    }

    #[test]
    fn test_clickhouse_config_without_failover() {
        let toml_str = r#"
            name = "test"
            chain_id = 1
            rpc_url = "http://localhost:8545"
            pg_url = "postgres://localhost/test"

            [clickhouse]
            enabled = true
            url = "http://clickhouse:8123"
        "#;

        let config: ChainConfig = toml::from_str(toml_str).unwrap();
        let ch = config.clickhouse.unwrap();

        assert!(ch.failover_urls.is_empty());
        assert_eq!(ch.all_urls(), vec!["http://clickhouse:8123"]);
    }

    #[test]
    fn test_resolved_pg_url_without_env() {
        let config = ChainConfig {
            name: "test".to_string(),
            chain_id: 1,
            rpc_url: "http://localhost:8545".to_string(),
            pg_url: "postgres://user:pass@localhost/db".to_string(),
            pg_password_env: None,
            backfill: true,
            batch_size: 100,
            concurrency: 4,
            backfill_first: false,
            trust_rpc: false,
            api_pg_url: None,
            api_pg_password_env: None,
            clickhouse: None,
        };

        assert_eq!(
            config.resolved_pg_url().unwrap(),
            "postgres://user:pass@localhost/db"
        );
    }

    #[test]
    fn test_resolved_pg_url_with_env() {
        // PATH is always set, use it to test env var substitution
        let config = ChainConfig {
            name: "test".to_string(),
            chain_id: 1,
            rpc_url: "http://localhost:8545".to_string(),
            pg_url: "postgres://user:placeholder@localhost/db".to_string(),
            pg_password_env: Some("PATH".to_string()),
            backfill: true,
            batch_size: 100,
            concurrency: 4,
            backfill_first: false,
            trust_rpc: false,
            api_pg_url: None,
            api_pg_password_env: None,
            clickhouse: None,
        };

        let resolved = config.resolved_pg_url().unwrap();
        assert!(resolved.starts_with("postgres://user:"));
        assert!(resolved.ends_with("@localhost/db"));
        assert!(!resolved.contains("placeholder"));
    }

    #[test]
    fn test_resolved_pg_url_missing_env() {
        let config = ChainConfig {
            name: "test".to_string(),
            chain_id: 1,
            rpc_url: "http://localhost:8545".to_string(),
            pg_url: "postgres://user:placeholder@localhost/db".to_string(),
            pg_password_env: Some("NONEXISTENT_VAR_XYZ_999".to_string()),
            backfill: true,
            batch_size: 100,
            concurrency: 4,
            backfill_first: false,
            trust_rpc: false,
            api_pg_url: None,
            api_pg_password_env: None,
            clickhouse: None,
        };

        assert!(config.resolved_pg_url().is_err());
    }
}
