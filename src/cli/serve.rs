use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Args as ClapArgs;
use std::future::IntoFuture;
use tokio::sync::RwLock;
use tracing::{info, warn};

use tidx::api::{self, ApiLimits, ApiMode, ChainClickHouseConfig, SharedTrustedCidrs, head_poller};
use tidx::broadcast::Broadcaster;
use tidx::clickhouse::ClickHouseEngine;
use tidx::config::{ChainConfig, Config};
use tidx::db::{self, Pool};

use super::up::{shutdown_channel, start_prometheus};

/// How long shutdown waits for open connections before exiting.
const SHUTDOWN_DRAIN: Duration = Duration::from_secs(5);

#[derive(ClapArgs)]
pub struct Args {
    /// Path to config file
    #[arg(short, long, default_value = "config.toml")]
    pub config: PathBuf,

    /// HTTP port, overriding http.port
    #[arg(long)]
    pub port: Option<u16>,

    /// Prometheus port, overriding prometheus.port
    #[arg(long)]
    pub metrics_port: Option<u16>,

    /// How often to poll sync_state for new blocks to drive live streams, in milliseconds
    #[arg(long, default_value_t = 250)]
    pub live_poll_ms: u64,
}

/// Runs the HTTP API without indexing (`tidx serve`).
///
/// Serve is stateless: it reads the data and the sync progress that a
/// separate `tidx sync` (or `tidx up`) process writes, so any number of
/// copies can run behind a load balancer. It never runs migrations, creates
/// databases or issues ClickHouse DDL, and it connects with the API
/// credentials (`postgres.api_url`) when they are configured.
pub async fn run(args: Args) -> Result<()> {
    let mut config = Config::load(&args.config)?;
    // Overrides let serve share a config file and host with tidx sync,
    // which binds the configured ports.
    if let Some(port) = args.port {
        config.http.port = port;
    }
    if let Some(port) = args.metrics_port {
        config.prometheus.port = port;
    }
    if !config.http.enabled {
        bail!("tidx serve needs [http] enabled = true");
    }
    let Some(default_chain_id) = config.chains.first().map(|c| c.chain_id) else {
        bail!("tidx serve needs at least one [[chains]] entry");
    };

    info!(
        chains = config.chains.len(),
        "Loaded config (serve: API only, no indexing)"
    );

    start_prometheus(&config)?;

    let broadcaster = Arc::new(Broadcaster::new());
    let shutdown_tx = shutdown_channel();
    let live_poll = Duration::from_millis(args.live_poll_ms.max(50));

    let mut pools = HashMap::new();
    let mut clickhouse_configs = HashMap::new();
    let mut clickhouse_engines = HashMap::new();

    for chain in &config.chains {
        let (pool, poller_pool) = connect_api_pools(chain).await?;
        check_schema(&pool, chain).await?;

        if let Some(ch_config) = chain.clickhouse.as_ref().filter(|c| c.enabled) {
            clickhouse_configs.insert(
                chain.chain_id,
                ChainClickHouseConfig {
                    enabled: true,
                    url: ch_config.url.clone(),
                    failover_urls: ch_config.failover_urls.clone(),
                },
            );
            let engine =
                ClickHouseEngine::for_api(ch_config, chain.chain_id).with_context(|| {
                    format!("failed to configure ClickHouse for chain {}", chain.name)
                })?;
            clickhouse_engines.insert(chain.chain_id, Arc::new(engine));
            info!(chain = %chain.name, "ClickHouse OLAP engine configured");
        }

        head_poller::spawn(
            poller_pool,
            chain.chain_id,
            broadcaster.clone(),
            live_poll,
            shutdown_tx.subscribe(),
        );
        pools.insert(chain.chain_id, pool);
    }

    let trusted_cidrs: SharedTrustedCidrs = Arc::new(std::sync::RwLock::new(api::parse_cidrs(
        &config.http.trusted_cidrs,
    )?));
    let router = api::router_shared_with_mode(
        Arc::new(RwLock::new(pools)),
        default_chain_id,
        broadcaster,
        Arc::new(RwLock::new(clickhouse_configs)),
        Arc::new(RwLock::new(clickhouse_engines)),
        trusted_cidrs,
        ApiMode::Serve,
        ApiLimits::from(&config.http),
    );

    let addr: SocketAddr = format!("{}:{}", config.http.bind, config.http.port).parse()?;
    info!(addr = %addr, "Starting HTTP API server (serve mode)");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let mut drain_rx = shutdown_tx.subscribe();
    let server = tokio::spawn(
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            let _ = drain_rx.recv().await;
        })
        .into_future(),
    );

    let mut shutdown_rx = shutdown_tx.subscribe();
    let _ = shutdown_rx.recv().await;

    // Graceful shutdown waits for every open connection, and live streams
    // stay open until the client leaves. Give in-flight requests a bounded
    // drain, then exit so rolling deploys are not held up by SSE clients.
    match tokio::time::timeout(SHUTDOWN_DRAIN, server).await {
        Ok(result) => result??,
        Err(_) => warn!(
            drain_secs = SHUTDOWN_DRAIN.as_secs(),
            "Connections still open after the shutdown drain (live streams); exiting"
        ),
    }

    Ok(())
}

/// Connects to the chain's API database: `postgres.api_url` when set, else
/// the main URL. Never creates the database.
///
/// Returns the request pool and a one-connection pool for the live-stream
/// head poller. The poller gets its own connection so a busy request pool,
/// and its `api_pool_wait_ms` checkout limit, never stall live streams.
async fn connect_api_pools(chain: &ChainConfig) -> Result<(Pool, Pool)> {
    let (url, kind) = match chain.resolved_api_pg_url()? {
        Some(url) => (url, "api_url"),
        None => (chain.resolved_pg_url()?, "url"),
    };
    let size = chain.api_pool_size()?;
    let wait = chain.api_pool_wait()?;
    info!(chain = %chain.name, source = kind, size, wait_ms = ?wait.map(|w| w.as_millis()), "Connecting API pool");
    let context = || format!("failed to connect to PostgreSQL for chain {}", chain.name);
    let pool = db::connect_pool_with_wait(&url, size, wait)
        .await
        .with_context(context)?;
    let poller_pool = db::connect_pool(&url, 1).await.with_context(context)?;
    Ok((pool, poller_pool))
}

/// Fails fast when the database has not been set up by a sync process.
async fn check_schema(pool: &Pool, chain: &ChainConfig) -> Result<()> {
    let conn = pool.get().await?;
    conn.query_opt("SELECT tip_num FROM sync_state LIMIT 1", &[])
        .await
        .with_context(|| {
            format!(
                "chain {}: sync_state is missing; run tidx sync (or tidx up) against this database first",
                chain.name
            )
        })?;
    Ok(())
}
