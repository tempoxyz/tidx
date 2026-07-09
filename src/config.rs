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
    #[serde(default = "default_trusted_cidrs")]
    pub trusted_cidrs: Vec<String>,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 8080,
            bind: "0.0.0.0".to_string(),
            trusted_cidrs: default_trusted_cidrs(),
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

fn default_trusted_cidrs() -> Vec<String> {
    vec!["127.0.0.1/32".to_string(), "::1/128".to_string()]
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

    /// Environment variable name containing RPC Basic Auth credentials as `username:password`.
    /// When set, credentials are injected into `rpc_url` at startup.
    #[serde(default)]
    pub rpc_auth_env: Option<String>,

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

    /// PostgreSQL OLTP settings. Optional; at least one of `postgres` or an
    /// enabled `clickhouse` must be configured per chain.
    #[serde(default)]
    pub postgres: Option<PostgresConfig>,

    /// ClickHouse OLAP settings (for analytical queries)
    #[serde(default)]
    pub clickhouse: Option<ClickHouseConfig>,
}

/// Configuration for the PostgreSQL engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    /// Database connection URL for this chain.
    /// If `password_env` is set, the password in this URL will be replaced
    /// with the value from that environment variable.
    pub url: String,

    /// Environment variable name containing the PostgreSQL password.
    /// When set, the password portion of `url` is replaced with this value.
    #[serde(default)]
    pub password_env: Option<String>,

    /// Separate PostgreSQL URL for the HTTP API (e.g., a CNPG `-r` read replica).
    /// When set, the API connection pool connects to this URL instead of `url`.
    /// If `api_password_env` is also set, the password is injected into this URL.
    #[serde(default)]
    pub api_url: Option<String>,

    /// Environment variable name containing the API PostgreSQL password.
    /// When set, replaces the password in `api_url` with the env var value.
    /// Has no effect without `api_url`.
    #[serde(default)]
    pub api_password_env: Option<String>,
}

impl PostgresConfig {
    /// Returns the connection URL with password resolved from environment if configured.
    pub fn resolved_url(&self) -> Result<String> {
        match &self.password_env {
            Some(env_var) => {
                let password = std::env::var(env_var).with_context(|| {
                    format!(
                        "postgres password_env '{env_var}' is set but environment variable not found"
                    )
                })?;

                let mut url = url::Url::parse(&self.url)
                    .with_context(|| format!("Invalid postgres url: {}", self.url))?;

                url.set_password(Some(&password))
                    .map_err(|()| anyhow::anyhow!("Failed to set password in postgres url"))?;

                Ok(url.to_string())
            }
            None => Ok(self.url.clone()),
        }
    }

    /// Returns a separate API database URL for read-only queries.
    /// Returns `None` if `api_url` is not set (API uses the main pool).
    pub fn resolved_api_url(&self) -> Result<Option<String>> {
        let api_url = match &self.api_url {
            Some(url) => url,
            None => return Ok(None),
        };

        match &self.api_password_env {
            Some(pass_env) => {
                let password = std::env::var(pass_env).with_context(|| {
                    format!(
                        "postgres api_password_env '{pass_env}' is set but environment variable not found"
                    )
                })?;
                let mut url = url::Url::parse(api_url)
                    .with_context(|| format!("Invalid postgres api_url: {api_url}"))?;
                url.set_password(Some(&password))
                    .map_err(|()| anyhow::anyhow!("Failed to set password in postgres api_url"))?;
                Ok(Some(url.to_string()))
            }
            None => Ok(Some(api_url.clone())),
        }
    }
}

/// Configuration for ClickHouse OLAP engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickHouseConfig {
    /// Enable ClickHouse OLAP queries (default: true when the section is present)
    #[serde(default = "default_true")]
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

    /// PostgreSQL wire-protocol endpoint for this ClickHouse database, served
    /// by a Postgres instance with the pg_clickhouse extension. Enables
    /// `engine=clickhouse_pg` queries; when the chain has no `postgres`
    /// configured, `engine=postgres` is aliased to this endpoint.
    #[serde(default)]
    pub pg_url: Option<String>,

    /// Environment variable name containing the pg_clickhouse password.
    /// When set, the password portion of `pg_url` is replaced with this value.
    #[serde(default)]
    pub pg_password_env: Option<String>,

    /// Scan and repair historical derived-table gaps on startup (default: true).
    #[serde(default = "default_true")]
    pub repair_derived_on_startup: bool,
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

    /// Returns the pg_clickhouse endpoint URL with password resolved from
    /// environment if configured. `None` if `pg_url` is not set.
    pub fn resolved_pg_url(&self) -> Result<Option<String>> {
        let pg_url = match &self.pg_url {
            Some(url) => url,
            None => return Ok(None),
        };

        match &self.pg_password_env {
            Some(env_var) => {
                let password = std::env::var(env_var).with_context(|| {
                    format!(
                        "clickhouse pg_password_env '{env_var}' is set but environment variable not found"
                    )
                })?;
                let mut url = url::Url::parse(pg_url)
                    .with_context(|| format!("Invalid clickhouse pg_url: {pg_url}"))?;
                url.set_password(Some(&password))
                    .map_err(|()| anyhow::anyhow!("Failed to set password in clickhouse pg_url"))?;
                Ok(Some(url.to_string()))
            }
            None => Ok(Some(pg_url.clone())),
        }
    }
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            url: "http://clickhouse:8123".to_string(),
            failover_urls: Vec::new(),
            database: None,
            user: None,
            password_env: None,
            pg_url: None,
            pg_password_env: None,
            repair_derived_on_startup: true,
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
    /// Returns the RPC URL with Basic Auth credentials resolved from environment if configured.
    pub fn resolved_rpc_url(&self) -> Result<String> {
        let Some(env_var) = &self.rpc_auth_env else {
            return Ok(self.rpc_url.clone());
        };

        let auth = std::env::var(env_var).with_context(|| {
            format!("rpc_auth_env '{env_var}' is set but environment variable not found")
        })?;
        let (user, password) = auth.split_once(':').ok_or_else(|| {
            anyhow::anyhow!(
                "rpc_auth_env '{env_var}' must contain credentials as username:password"
            )
        })?;

        let mut url = url::Url::parse(&self.rpc_url)
            .with_context(|| format!("Invalid rpc_url: {}", self.rpc_url))?;
        url.set_username(user)
            .map_err(|()| anyhow::anyhow!("Failed to set username in rpc_url"))?;
        url.set_password(Some(password))
            .map_err(|()| anyhow::anyhow!("Failed to set password in rpc_url"))?;

        Ok(url.to_string())
    }

    /// Returns the configured RPC URL with any embedded credentials masked.
    pub fn redacted_rpc_url(&self) -> String {
        redact_url_credentials(&self.rpc_url)
    }

    /// Returns the ClickHouse config if the section is present and enabled.
    pub fn clickhouse_enabled(&self) -> Option<&ClickHouseConfig> {
        self.clickhouse.as_ref().filter(|ch| ch.enabled)
    }

    /// A chain must have at least one storage engine configured.
    pub fn validate(&self) -> Result<()> {
        if self.postgres.is_none() && self.clickhouse_enabled().is_none() {
            anyhow::bail!(
                "chain '{}': no storage engine configured. \
                 Add a [[chains]] `postgres` table and/or an enabled `clickhouse` table \
                 (at least one is required).",
                self.name
            );
        }
        Ok(())
    }
}

pub fn redact_url_credentials(raw_url: &str) -> String {
    let Ok(mut url) = url::Url::parse(raw_url) else {
        return "[invalid rpc_url]".to_string();
    };

    if url.username().is_empty() && url.password().is_none() {
        return raw_url.to_string();
    }

    let _ = url.set_username("****");
    let _ = url.set_password(Some("****"));
    url.to_string()
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

        Self::parse(&content).with_context(|| format!("Invalid config file: {}", path.display()))
    }

    pub fn parse(content: &str) -> Result<Self> {
        let raw: toml::Value = toml::from_str(content).context("Failed to parse config")?;
        check_legacy_chain_keys(&raw)?;

        let config: Config = raw.try_into().context("Failed to parse config")?;

        if config.chains.is_empty() {
            anyhow::bail!("No chains configured. Add at least one [[chains]] section.");
        }

        for chain in &config.chains {
            chain.validate()?;
        }

        Ok(config)
    }
}

/// Flat pg options were moved into the `[chains.postgres]` table. Fail with a
/// migration hint instead of silently ignoring unknown keys.
fn check_legacy_chain_keys(raw: &toml::Value) -> Result<()> {
    const MOVED: [(&str, &str); 4] = [
        ("pg_url", "url"),
        ("pg_password_env", "password_env"),
        ("api_pg_url", "api_url"),
        ("api_pg_password_env", "api_password_env"),
    ];

    let Some(chains) = raw.get("chains").and_then(|c| c.as_array()) else {
        return Ok(());
    };

    for chain in chains {
        let name = chain
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or("<unnamed>");
        for (legacy, replacement) in MOVED {
            if chain.get(legacy).is_some() {
                anyhow::bail!(
                    "chain '{name}': `{legacy}` has moved into the chain's `postgres` table; \
                     use `[chains.postgres]` with `{replacement} = ...`"
                );
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pg_only_chain(url: &str, password_env: Option<&str>) -> ChainConfig {
        ChainConfig {
            name: "test".to_string(),
            chain_id: 1,
            rpc_url: "http://localhost:8545".to_string(),
            rpc_auth_env: None,
            backfill: true,
            batch_size: 100,
            concurrency: 4,
            backfill_first: false,
            trust_rpc: false,
            postgres: Some(PostgresConfig {
                url: url.to_string(),
                password_env: password_env.map(String::from),
                api_url: None,
                api_password_env: None,
            }),
            clickhouse: None,
        }
    }

    #[test]
    fn test_chain_config_defaults() {
        let toml_str = r#"
            name = "test"
            chain_id = 1
            rpc_url = "http://localhost:8545"

            [postgres]
            url = "postgres://localhost/test"
        "#;

        let config: ChainConfig = toml::from_str(toml_str).unwrap();

        assert!(config.backfill);
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.concurrency, 4);
        assert_eq!(config.postgres.unwrap().url, "postgres://localhost/test");
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

            [chains.postgres]
            url = "postgres://localhost/chain1"

            [[chains]]
            name = "chain2"
            chain_id = 2
            rpc_url = "http://localhost:8546"

            [chains.postgres]
            url = "postgres://localhost/chain2"
        "#;

        let config = Config::parse(toml_str).unwrap();

        assert_eq!(config.chains.len(), 2);
    }

    #[test]
    fn test_clickhouse_only_chain_is_valid() {
        let toml_str = r#"
            [[chains]]
            name = "olap"
            chain_id = 1
            rpc_url = "http://localhost:8545"

            [chains.clickhouse]
            url = "http://clickhouse:8123"
            pg_url = "postgres://pgch:5432/tidx_1"
        "#;

        let config = Config::parse(toml_str).unwrap();
        let chain = &config.chains[0];

        assert!(chain.postgres.is_none());
        let ch = chain.clickhouse_enabled().unwrap();
        // `enabled` defaults to true when the section is present
        assert!(ch.enabled);
        assert_eq!(
            ch.resolved_pg_url().unwrap().as_deref(),
            Some("postgres://pgch:5432/tidx_1")
        );
    }

    #[test]
    fn test_chain_without_any_store_is_rejected() {
        let toml_str = r#"
            [[chains]]
            name = "empty"
            chain_id = 1
            rpc_url = "http://localhost:8545"
        "#;

        let err = Config::parse(toml_str).unwrap_err();
        assert!(err.to_string().contains("no storage engine configured"));
    }

    #[test]
    fn test_chain_with_disabled_clickhouse_only_is_rejected() {
        let toml_str = r#"
            [[chains]]
            name = "disabled"
            chain_id = 1
            rpc_url = "http://localhost:8545"

            [chains.clickhouse]
            enabled = false
            url = "http://clickhouse:8123"
        "#;

        let err = Config::parse(toml_str).unwrap_err();
        assert!(err.to_string().contains("no storage engine configured"));
    }

    #[test]
    fn test_legacy_flat_pg_keys_are_rejected_with_hint() {
        let toml_str = r#"
            [[chains]]
            name = "legacy"
            chain_id = 1
            rpc_url = "http://localhost:8545"
            pg_url = "postgres://localhost/test"
        "#;

        let err = Config::parse(toml_str).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("pg_url"), "got: {msg}");
        assert!(msg.contains("[chains.postgres]"), "got: {msg}");
    }

    #[test]
    fn test_clickhouse_config_with_failover() {
        let toml_str = r#"
            name = "test"
            chain_id = 1
            rpc_url = "http://localhost:8545"

            [postgres]
            url = "postgres://localhost/test"

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
        assert!(ch.repair_derived_on_startup);
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

            [clickhouse]
            enabled = true
            url = "http://clickhouse:8123"
        "#;

        let config: ChainConfig = toml::from_str(toml_str).unwrap();
        let ch = config.clickhouse.unwrap();

        assert!(ch.failover_urls.is_empty());
        assert_eq!(ch.all_urls(), vec!["http://clickhouse:8123"]);
        assert!(ch.repair_derived_on_startup);
        assert!(ch.pg_url.is_none());
    }

    #[test]
    fn test_clickhouse_config_can_disable_startup_derived_repair() {
        let toml_str = r#"
            name = "test"
            chain_id = 1
            rpc_url = "http://localhost:8545"

            [clickhouse]
            enabled = true
            repair_derived_on_startup = false
        "#;

        let config: ChainConfig = toml::from_str(toml_str).unwrap();
        let ch = config.clickhouse.unwrap();

        assert!(!ch.repair_derived_on_startup);
    }

    #[test]
    fn test_resolved_pg_url_without_env() {
        let config = pg_only_chain("postgres://user:pass@localhost/db", None);

        assert_eq!(
            config.postgres.unwrap().resolved_url().unwrap(),
            "postgres://user:pass@localhost/db"
        );
    }

    #[test]
    fn test_resolved_pg_url_with_env() {
        // PATH is always set, use it to test env var substitution
        let config = pg_only_chain("postgres://user:placeholder@localhost/db", Some("PATH"));

        let resolved = config.postgres.unwrap().resolved_url().unwrap();
        assert!(resolved.starts_with("postgres://user:"));
        assert!(resolved.ends_with("@localhost/db"));
        assert!(!resolved.contains("placeholder"));
    }

    #[test]
    fn test_resolved_pg_url_missing_env() {
        let config = pg_only_chain(
            "postgres://user:placeholder@localhost/db",
            Some("NONEXISTENT_VAR_XYZ_999"),
        );

        assert!(config.postgres.unwrap().resolved_url().is_err());
    }

    #[test]
    fn test_resolved_rpc_url_with_auth_env() {
        let mut config = pg_only_chain("postgres://user:pass@localhost/db", None);
        config.rpc_url = "https://rpc.example.com".to_string();
        config.rpc_auth_env = Some("PATH".to_string());

        let resolved = config.resolved_rpc_url().unwrap();
        assert!(resolved.starts_with("https://"));
        assert!(resolved.contains('@'));
        assert!(resolved.ends_with("rpc.example.com/"));
    }

    #[test]
    fn test_redact_url_credentials_masks_userinfo() {
        assert_eq!(
            redact_url_credentials("https://user:secret@rpc.example.com/path"),
            "https://****:****@rpc.example.com/path"
        );
        assert_eq!(
            redact_url_credentials("https://rpc.example.com/path"),
            "https://rpc.example.com/path"
        );
    }
}
