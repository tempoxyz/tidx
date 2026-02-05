use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use clap::Args as ClapArgs;
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::sync::RwLock;
use tracing::{error, info};

use tidx::api::{self, ChainClickHouseConfig, SharedClickHouseConfigs, SharedClickHouseEngines, SharedPools};
use tidx::clickhouse::ClickHouseEngine;
use tidx::broadcast::Broadcaster;
use tidx::config::{ChainConfig, Config, ConfigWatcher, NewChainEvent};
use tidx::db::{self, ThrottledPool};
use tidx::sync::engine::SyncEngine;

#[derive(ClapArgs)]
pub struct Args {
    /// Path to config file
    #[arg(short, long, default_value = "config.toml")]
    pub config: PathBuf,

    /// Disable config hot-reloading
    #[arg(long)]
    pub no_watch: bool,
}

use std::path::PathBuf;

pub async fn run(args: Args) -> Result<()> {
    let config = Config::load(&args.config)?;

    info!(chains = config.chains.len(), "Loaded config");

    if config.prometheus.enabled {
        let metrics_addr: SocketAddr =
            format!("{}:{}", config.http.bind, config.prometheus.port).parse()?;
        info!(addr = %metrics_addr, "Starting Prometheus metrics server");
        PrometheusBuilder::new()
            .with_http_listener(metrics_addr)
            .install()?;
    }

    let broadcaster = Arc::new(Broadcaster::new());
    let (shutdown_tx, _shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

    tokio::spawn({
        let shutdown_tx = shutdown_tx.clone();
        async move {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutting down...");
            let _ = shutdown_tx.send(());
        }
    });

    let pools: SharedPools = Arc::new(RwLock::new(HashMap::new()));
    let clickhouse_configs: SharedClickHouseConfigs = Arc::new(RwLock::new(HashMap::new()));
    let clickhouse_engines: SharedClickHouseEngines = Arc::new(RwLock::new(HashMap::new()));
    let mut default_chain_id = 0u64;

    for chain in &config.chains {
        let throttled_pool = initialize_chain(
            chain,
            Arc::clone(&clickhouse_configs),
        ).await?;

        if default_chain_id == 0 {
            default_chain_id = chain.chain_id;
        }

        // Initialize ClickHouse if configured (for each chain)
        if let Some(ref ch_config) = chain.clickhouse {
            if ch_config.enabled {
                match ClickHouseEngine::new(ch_config, chain.chain_id, &chain.pg_url) {
                    Ok(engine) => {
                        let engine = Arc::new(engine);
                        
                        // Ensure replication is set up
                        let pg_url = chain.pg_url.clone();
                        let engine_clone = Arc::clone(&engine);
                        let chain_name = chain.name.clone();
                        tokio::spawn(async move {
                            if let Err(e) = engine_clone.ensure_replication(&pg_url).await {
                                error!(error = %e, chain = %chain_name, "Failed to set up ClickHouse replication");
                            }
                        });
                        
                        clickhouse_engines.write().await.insert(chain.chain_id, engine);
                        info!(chain = %chain.name, chain_id = chain.chain_id, "ClickHouse OLAP engine initialized");
                    }
                    Err(e) => {
                        error!(error = %e, chain = %chain.name, "Failed to create ClickHouse engine");
                    }
                }
            }
        }

        // API uses the shared pool (backfill is throttled, so API isn't starved)
        pools.write().await.insert(chain.chain_id, throttled_pool.pool.clone());

        spawn_sync_engine(
            chain.clone(),
            throttled_pool,
            broadcaster.clone(),
            shutdown_tx.subscribe(),
        );
    }

    let (chain_tx, mut chain_rx) = tokio::sync::mpsc::channel::<NewChainEvent>(16);

    if !args.no_watch {
        let watcher = ConfigWatcher::new(args.config.clone(), &config, chain_tx);
        let http_config = watcher.http_config();
        watcher.start()?;

        if config.http.enabled && default_chain_id != 0 {
            let addr: SocketAddr = format!("{}:{}", config.http.bind, config.http.port).parse()?;
            
            let router = api::router_shared(
                Arc::clone(&pools),
                default_chain_id,
                broadcaster.clone(),
                Arc::clone(&clickhouse_configs),
                http_config,
                Arc::clone(&clickhouse_engines),
                config.http.trusted_cidrs.clone(),
            );

            info!(addr = %addr, "Starting HTTP API server (hot-reload enabled)");

            let listener = tokio::net::TcpListener::bind(addr).await?;
            let mut shutdown_rx_api = shutdown_tx.subscribe();

            tokio::spawn(async move {
                axum::serve(
                    listener,
                    router.into_make_service_with_connect_info::<SocketAddr>(),
                )
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx_api.recv().await;
                })
                .await
                .ok();
            });
        }

        let pools_for_watcher = Arc::clone(&pools);
        let clickhouse_configs_for_watcher = Arc::clone(&clickhouse_configs);
        let broadcaster_for_watcher = broadcaster.clone();
        let shutdown_tx_for_watcher = shutdown_tx.clone();

        tokio::spawn(async move {
            while let Some(event) = chain_rx.recv().await {
                match initialize_chain(&event.chain, Arc::clone(&clickhouse_configs_for_watcher)).await {
                    Ok(throttled_pool) => {
                        pools_for_watcher.write().await.insert(event.chain.chain_id, throttled_pool.pool.clone());

                        spawn_sync_engine(
                            event.chain,
                            throttled_pool,
                            broadcaster_for_watcher.clone(),
                            shutdown_tx_for_watcher.subscribe(),
                        );
                    }
                    Err(e) => {
                        error!(error = %e, chain = %event.chain.name, "Failed to initialize new chain");
                    }
                }
            }
        });
    } else if config.http.enabled && default_chain_id != 0 {
        let addr: SocketAddr = format!("{}:{}", config.http.bind, config.http.port).parse()?;
        let router = api::router_with_options(
            pools.read().await.clone(),
            default_chain_id,
            broadcaster.clone(),
            clickhouse_configs.read().await.clone(),
            &config.http,
        );

        info!(addr = %addr, "Starting HTTP API server");

        let listener = tokio::net::TcpListener::bind(addr).await?;
        let mut shutdown_rx_api = shutdown_tx.subscribe();

        tokio::spawn(async move {
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx_api.recv().await;
            })
            .await
            .ok();
        });
    }

    let mut shutdown_rx = shutdown_tx.subscribe();
    let _ = shutdown_rx.recv().await;

    Ok(())
}

async fn initialize_chain(
    chain: &ChainConfig,
    clickhouse_configs: SharedClickHouseConfigs,
) -> Result<ThrottledPool> {
    info!(chain = %chain.name, db = %chain.pg_url, "Connecting to database with throttled pool...");
    let throttled_pool = ThrottledPool::new(&chain.pg_url).await?;

    info!(chain = %chain.name, "Running migrations...");
    db::run_migrations(&throttled_pool.pool).await?;

    // Store ClickHouse config for this chain (if enabled)
    if let Some(ref ch_config) = chain.clickhouse {
        let config = ChainClickHouseConfig {
            enabled: ch_config.enabled,
            url: ch_config.url.clone(),
            failover_urls: ch_config.failover_urls.clone(),
        };
        clickhouse_configs.write().await.insert(chain.chain_id, config);
        info!(chain = %chain.name, "ClickHouse OLAP engine configured");
    }

    Ok(throttled_pool)
}

fn spawn_sync_engine(
    chain: ChainConfig,
    throttled_pool: ThrottledPool,
    broadcaster: Arc<Broadcaster>,
    shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) {
    info!(
        chain = %chain.name,
        chain_id = chain.chain_id,
        rpc = %chain.rpc_url,
        backfill_limit = throttled_pool.backfill_semaphore.available_permits(),
        "Starting sync for chain (throttled pool: 16 connections, backfill limited)"
    );

    let backfill_first = chain.backfill_first;
    let trust_rpc = chain.trust_rpc;

    tokio::spawn(async move {
        // Create sync engine with throttled pool
        let mut engine = match SyncEngine::new(throttled_pool, &chain.rpc_url).await {
            Ok(e) => e
                .with_broadcaster(broadcaster)
                .with_batch_size(chain.batch_size)
                .with_concurrency(chain.concurrency)
                .with_backfill_first(backfill_first)
                .with_trust_rpc(trust_rpc),
            Err(e) => {
                error!(error = %e, chain = %chain.name, "Failed to create sync engine");
                return;
            }
        };

        // Run the sync engine - handles both realtime sync and gap sync
        // Gap sync fills ALL gaps from most recent to earliest (replaces backfill)
        if let Err(e) = engine.run(shutdown_rx).await {
            error!(error = %e, chain = %chain.name, "Sync engine failed");
        }
    });
}
